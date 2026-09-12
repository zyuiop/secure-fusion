use crate::arrow_helper::project_schema_safe;
use crate::ast_expr_ext::AstExprExt;
use crate::filtering::resolver::{IndexResolver, ResolveIndexResult};
use crate::get_catalog::CatalogGetter;
use crate::get_conn::ConnGetter;
use crate::metadata;
use crate::metadata::{
    ColumnName, EncryptedColumnMeta, EncryptedTableMeta, IndexInsertStrategy, PrimaryKey,
    PrimaryKeyColumnDef, supports_aad_binding,
};
use crate::planning::logical::{CURRENT_VALUE_PREFIX, DUPLICATE_VALUE_PFX, FILTER_PREFIX};
use crate::planning::physical::plans::create_rowid_plan::CreateRowidPlan;
use crate::planning::physical::plans::mysql_scan_plan::MySqlScanPlan;
use crate::planning::physical::transform::transform_select_with_locking;
use crate::providers::parser::{parse_column_type, parse_default_value_to_expr};
use crate::sinks::multiplex_sink::MultiplexSink;
use crate::sinks::mysql_delete_sink::MySqlDeleteSink;
use crate::sinks::mysql_dml_sink::MySqlDmlSink;
use crate::sinks::sink::{RecordBatchSink, RecordBatchSinkExec};
use async_trait::async_trait;
use common::conversions::column_def_ext::ColumnDefExt;
use common::conversions::datatypes::ArrowDatatypeConverter;
use common::metadata::{MetadataReads, MetadataWrites};
use crypto::planning::decrypt_planner::project_decrypt;
use crypto::planning::physical::compute_aad::ComputeAadExpr;
use crypto::planning::physical::encrypt::EncryptExpr;
use crypto::planning::physical::to_binary::ToBinaryExpr;
use crypto::row_id::RowIdColumn;
use crypto::{CipherContext, IdentifierContext, KeyManagerGetter};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    ColumnStatistics, Constraint, Constraints, DFSchema, ResolvedTableReference, Statistics,
    internal_datafusion_err, plan_datafusion_err, plan_err,
};
use datafusion::datasource::TableType;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{ColumnDef, LockType};
use datafusion::{logical_expr, physical_expr};
use log::{trace, warn};
use mysql_async::Conn;
use mysql_async::prelude::{FromRow, Queryable};
use rustc_hash::{FxHashMap, FxHashSet};
use std::any::Any;
use std::borrow::Cow;
use std::iter::once;
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum ColumnDefault {
    Expr(Expr),
    AutoIncrement,
}

#[derive(Debug, Clone)]
pub struct MySqlTableProvider {
    table_reference: ResolvedTableReference,
    pub(crate) columns_defaults: FxHashMap<String, ColumnDefault>,

    table_schema: SchemaRef,
    encryption_metadata: EncryptedTableMeta,
    constraints: Constraints,

    primary_key: Vec<ColumnName>,
    primary_key_def: Vec<ColumnDef>,

    statistics: Arc<RwLock<Arc<TableStatistics>>>,
}

#[derive(Debug)]
pub struct TableStatistics {
    /// Set to true if a task is already in the process of updating this entry
    update_signal: AtomicBool,

    table_reference: ResolvedTableReference,
    last_update: Instant,
    num_rows: Precision<usize>,
    indexing_columns: FxHashMap<String, Precision<usize>>,
}

#[derive(Debug, FromRow)]
#[mysql(rename_all = "UPPERCASE")]
struct MySqlIndexStats {
    column_name: String,
    cardinality: usize,
}

#[derive(Debug, FromRow)]
#[mysql(rename_all = "UPPERCASE")]
struct MySqlTableStats {
    table_rows: usize,
}

impl TableStatistics {
    const REFRESH_FREQ: Duration = Duration::from_hours(4);

    pub fn for_empty_table(table_reference: ResolvedTableReference) -> Self {
        Self {
            last_update: Instant::now(),
            indexing_columns: FxHashMap::default(),
            num_rows: Precision::Absent,
            update_signal: AtomicBool::new(false),
            table_reference,
        }
    }

    #[inline(always)]
    pub async fn new(
        table_reference: ResolvedTableReference,
        conn: &mut Conn,
    ) -> datafusion::common::Result<Self> {
        Self::build_for_table(table_reference, conn).await
    }

    pub async fn refresh_if_needed(
        &self,
        conn: &SessionConfig,
    ) -> datafusion::common::Result<Option<Self>> {
        if Instant::now().duration_since(self.last_update) > Self::REFRESH_FREQ {
            let should_update = self.update_signal.swap(true, Ordering::Relaxed);
            if !should_update {
                return Ok(None);
            }

            let conn = conn.get_conn();
            let mut locked_conn = conn.lock().await;
            match Self::build_for_table(self.table_reference.clone(), locked_conn.deref_mut()).await
            {
                Ok(value) => Ok(Some(value)),
                Err(e) => {
                    self.update_signal.store(false, Ordering::Relaxed);
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn column_stats(&self, col: &str) -> Option<Precision<usize>> {
        self.indexing_columns.get(col).copied()
    }

    pub fn column_selectivity(&self, col: &str) -> Option<f64> {
        self.column_stats(col)
            .and_then(|v| v.get_value().copied())
            .map(|v| 1f64.min(1f64 / v as f64))
    }

    pub fn num_rows(&self) -> Precision<usize> {
        self.num_rows
    }

    pub async fn build_for_table(
        table: ResolvedTableReference,
        conn: &mut Conn,
    ) -> datafusion::common::Result<Self> {
        let index_stats = conn.query::<MySqlIndexStats, _>(format!(
            "SELECT COLUMN_NAME, CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema='{}' AND table_name='{}'",
            table.schema,
            table.table
        )).await.map_err(|_| plan_datafusion_err!("failed to retrieve index statistics from server (for {table})"))?;

        let table_stats = conn.query::<MySqlTableStats, _>(format!(
            "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='{}' AND table_name='{}'",
            table.schema,
            table.table
        )).await.map_err(|_| plan_datafusion_err!("failed to retrieve tables statistics from server (for {table})"))?;

        if table_stats.len() > 1 {
            plan_err!("failed to retrieve statistics from server (for {table})")?;
        }

        let num_rows =
            Precision::Inexact(table_stats.get(0).map(|v| v.table_rows).unwrap_or_default());

        // TODO: actually, we should transform blind index columns to the real column that they index?
        let indexing_columns = index_stats
            .into_iter()
            .map(|stat| (stat.column_name, Precision::Inexact(stat.cardinality)))
            .collect();
        let last_update = Instant::now();

        Ok(Self {
            num_rows,
            indexing_columns,
            last_update,
            table_reference: table,
            update_signal: AtomicBool::new(false),
        })
    }

    fn to_df_stats(&self, schema: SchemaRef) -> Statistics {
        let mut stats = Statistics::default().with_num_rows(self.num_rows);

        for col in schema.fields().iter() {
            let column_stats = self
                .indexing_columns
                .get(col.name())
                .map(|cardinality| ColumnStatistics::default().with_distinct_count(*cardinality));

            stats = stats.add_column_statistics(column_stats.unwrap_or_default());
        }

        stats
    }
}

impl MySqlTableProvider {
    pub fn try_get_row_id_column(&self) -> datafusion::common::Result<&RowIdColumn> {
        self.get_row_id_column()
            .ok_or_else(|| plan_datafusion_err!("table must have a row_id column"))
    }

    pub fn get_row_id_column(&self) -> Option<&RowIdColumn> {
        self.encryption_metadata.row_id_column.as_ref()
    }

    pub fn get_row_id_field(&self) -> Option<Field> {
        self.get_row_id_column().map(|col| col.field())
    }

    /// Returns a plan that, when executed, creates the rowId column for this table
    pub async fn create_rowid_column(
        &self,
        session: &dyn Session,
    ) -> datafusion::common::Result<Option<(Arc<dyn ExecutionPlan>, RowIdColumn)>> {
        if self.get_row_id_column().is_some() {
            return Ok(None);
        }

        let projected_pk: Vec<_> = self.primary_key_def.iter().collect();
        let (column, column_def) = RowIdColumn::create_for_table(&projected_pk)?;
        let Some(column_def) = column_def else {
            // TODO: handle updating the metadata
            plan_err!(
                "inconsistent metadata state: row_id column exists in table but not in metadata"
            )?
        };

        let plan =
            CreateRowidPlan::create_row_id(self, session, column.clone(), column_def).await?;

        Ok(Some((plan, column)))
    }

    /// Returns a vector of expressions that can be passed to the ComputeAad function to compute the
    /// associated data for a row.
    pub fn get_aad_source(&self) -> Vec<Expr> {
        if !self.encryption_metadata.row_binding_aad {
            vec![]
        } else {
            self.primary_key
                .iter()
                .map(|column_name| logical_expr::col(column_name))
                .collect()
        }
    }

    /// Returns a vector of expressions that can be passed to the ComputeAad function to compute the
    /// associated data for a row.
    pub fn get_aad_source_physical(
        &self,
        schema: &Schema,
        prefix: Option<&str>,
    ) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
        if !self.encryption_metadata.row_binding_aad {
            Ok(vec![])
        } else if let Some(prefix) = prefix {
            self.primary_key
                .iter()
                .map(|column_name| {
                    physical_expr::expressions::col(
                        format!("{prefix}{column_name}").as_ref(),
                        schema,
                    )
                })
                .collect()
        } else {
            self.primary_key
                .iter()
                .map(|column_name| physical_expr::expressions::col(column_name, schema))
                .collect()
        }
    }

    // TODO: When initializing the table, ensure the column is present and set in metadata

    fn build_schema(
        table_name: &str,
        columns: &[ColumnDef],
        encryption_metadata: &Option<EncryptedTableMeta>,
    ) -> datafusion::common::Result<Schema> {
        if let Some(encryption_metadata) = encryption_metadata {
            let fields = columns
                .iter()
                .filter(|column| !encryption_metadata.is_column_hidden(&column.name.value))
                .map(|column| {
                    Self::build_field_from_column_def(
                        table_name,
                        column,
                        encryption_metadata.column(&column.name.value),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Schema::new(fields))
        } else {
            ArrowDatatypeConverter.table_to_schema(columns)
        }
    }

    pub fn build_field_from_column_def(
        table_name: &str,
        column: &ColumnDef,
        column_meta: Option<&EncryptedColumnMeta>,
    ) -> datafusion::common::Result<Field> {
        let Some(column_meta) = column_meta else {
            return ArrowDatatypeConverter.column_to_field(column);
        };

        let mut field = Field::new(
            column.name.value.clone(),
            column_meta.data_type(),
            column_meta.nullable,
        );

        field.set_raw_source_type(column_meta.original_type_raw.clone());

        if let Some(expr) = column_meta.default_value_raw.clone() {
            field.set_default_value(expr);
        }

        if let Some(collation) = column_meta.collation.clone() {
            field.set_raw_collation(collation);
        }

        field.set_encrypted(table_name);

        Ok(field)
    }

    pub fn build_column_default(
        column_def: &ColumnDef,
        field: &FieldRef,
    ) -> datafusion::common::Result<Option<ColumnDefault>> {
        let default_expr = if field.is_encrypted() {
            let base_data_type = field
                .raw_source_type()
                .map(|ct| parse_column_type(ct).expect("invalid data type for encrypted column"))
                .expect("missing data type for encrypted column");

            let Some(default_value) = field.default_value() else {
                return Ok(None);
            };

            Some(parse_default_value_to_expr(default_value, &base_data_type)?)
        } else {
            column_def.get_default_value()
        };

        if let Some(dv) = default_expr {
            let ctx = SessionContext::new();
            let default_value = ctx
                .state()
                .create_logical_expr(&dv.to_string(), &DFSchema::empty())?;

            return Ok(Some(ColumnDefault::Expr(default_value)));
        }

        Ok(if column_def.is_auto_increment() {
            Some(ColumnDefault::AutoIncrement)
        } else {
            None
        })
    }

    pub(crate) fn from_columns(
        table_reference: ResolvedTableReference,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColumnName>,
        encryption_metadata: Option<EncryptedTableMeta>,
        statistics: TableStatistics,
    ) -> Self {
        // Determine primary keys
        let constraints = if primary_key.is_empty() {
            Vec::new()
        } else {
            let primary_key_indices = columns
                .iter()
                .enumerate()
                .filter_map(|(index, col)| {
                    if primary_key.contains(&col.name.value) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect();

            vec![Constraint::PrimaryKey(primary_key_indices)]
        };
        let constraints = Constraints::new_unverified(constraints);

        let projected_primary_key: Vec<_> = primary_key
            .iter()
            .filter_map(|col_name| columns.iter().find(|col| &col.name.value == col_name))
            .cloned()
            .collect();

        // Build schema
        let schema = Self::build_schema(
            table_reference.table.as_ref(),
            &columns,
            &encryption_metadata,
        )
        .unwrap();

        let defaults = columns
            .iter()
            .zip(schema.fields().iter())
            .filter_map(|(column_def, field)| {
                match Self::build_column_default(column_def, field) {
                    Ok(v) => v.map(|default| (column_def.name.value.clone(), default)),
                    Err(e) => {
                        match column_def.get_default_value() {
                            None => {
                                log::warn!("Failed to parse column default for encrypted column {} in table {table_reference}: {e:?}", &column_def.name,);
                            }
                            Some(default_value) => {
                                log::warn!("Failed to parse column default for encrypted column {} (`{}`) in table {table_reference}: {e:?}", &column_def.name, default_value);
                            }
                        }
                        None
                    }
                }
            })
            .collect::<FxHashMap<_, _>>();

        let full_primary_key: PrimaryKey = primary_key
            .iter()
            .filter_map(|column_name| {
                let data_type = schema
                    .field_with_name(column_name)
                    .ok()?
                    .data_type()
                    .clone();

                Some(PrimaryKeyColumnDef {
                    column_name: column_name.clone(),
                    column_type: data_type,
                    has_default: defaults.contains_key(column_name),
                })
            })
            .collect();

        let mut encryption_metadata =
            encryption_metadata.unwrap_or_else(|| EncryptedTableMeta::new());

        if encryption_metadata.row_binding_aad && !supports_aad_binding(&full_primary_key) {
            warn!(
                "Disabled `row_binding_aad` on table {table_reference}: primary key is not deterministic"
            );
            encryption_metadata.row_binding_aad = false;
        }

        // TODO: verify this is in adequation with the actual column type...
        // TODO: This whole metadata initialization is a complete mess and should be refactored

        let primary_key = full_primary_key
            .into_iter()
            .map(|data| data.column_name)
            .collect();

        Self {
            columns_defaults: defaults,
            table_schema: SchemaRef::new(schema),
            table_reference,
            constraints,
            encryption_metadata,
            primary_key,
            primary_key_def: projected_primary_key,
            statistics: Arc::new(RwLock::new(Arc::new(statistics))),
        }
    }

    #[allow(unused)]
    pub(crate) fn has_encrypted_columns(&self) -> bool {
        self.encryption_metadata.has_encrypted_column()
    }

    #[allow(unused)]
    pub(crate) fn has_indices(&self) -> bool {
        !self.encryption_metadata.indices().is_empty()
    }

    /// Returns the list of all columns that are tracked by complex indices.
    /// If these columns are modified, their current value must be queried for the index.
    #[allow(unused)]
    pub(crate) fn columns_with_updates_tracking(&self) -> FxHashSet<ColumnName> {
        self.encryption_metadata
            .indices()
            .iter()
            .filter(|index| index.requires_old_value_on_update())
            .flat_map(|index| index.tracked_columns())
            .collect()
    }

    /// True if this table has one or more indexes that are not in this table
    /// If true, this means that some DML operations require additional queries to modify the
    /// additional structure
    pub(crate) fn has_index_storage(&self) -> bool {
        self.encryption_metadata
            .indices()
            .iter()
            .any(|index| index.requires_external_storage())
    }

    pub(crate) fn is_encrypted(&self, col_name: &str) -> bool {
        self.get_column_metadata(col_name).is_some()
    }

    pub(crate) fn get_column_metadata(
        &self,
        col_name: &str,
    ) -> Option<&metadata::EncryptedColumnMeta> {
        self.encryption_metadata.column(col_name)
    }

    pub(crate) fn primary_key(&self) -> &Vec<ColumnName> {
        &self.primary_key
    }

    pub(crate) fn encryption_metadata(&self) -> &EncryptedTableMeta {
        &self.encryption_metadata
    }

    pub(crate) fn encryption_metadata_mut(&mut self) -> &mut EncryptedTableMeta {
        // If we reach this point of the code, the column MUST exist
        &mut self.encryption_metadata
    }

    #[inline(always)]
    pub fn index_resolver<'a>(&'a self) -> IndexResolver<'a> {
        IndexResolver::from(self)
    }

    pub async fn scan_and_decrypt_all(
        &self,
        state: &dyn Session,
        projection: Schema,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let encrypted_column_meta = projection
            .fields()
            .iter()
            .map(|field| {
                self.get_column_metadata(field.name()).map(|col_meta| {
                    (
                        Arc::<str>::from(field.name().clone()),
                        col_meta.original_type.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();

        let keys_manager = state.config().get_long_term_keys_manager();
        let decrypt_projection = project_decrypt(
            self.table_reference(),
            &projection,
            encrypted_column_meta.as_slice(),
            keys_manager.as_ref(),
            self.get_aad_source_physical(&projection, None)?,
        )?;

        let base_plan = self
            .scan_with_schema(state, projection.clone(), filters, limit)
            .await?;
        let plan = ProjectionExec::try_new(decrypt_projection, base_plan)?;
        Ok(Arc::new(plan))
    }

    async fn refresh_stats(&self, conn: &SessionConfig) -> datafusion::common::Result<()> {
        let stats = {
            let lock = self.statistics.read().expect("statistics poisoned");
            Arc::clone(&lock)
        };

        if let Some(new_stats) = stats.refresh_if_needed(conn).await? {
            drop(stats);
            let mut guard = self.statistics.write().expect("statistics poisoned");
            drop(mem::replace(&mut *guard, Arc::new(new_stats)));
        }

        Ok(())
    }

    pub async fn scan_with_schema(
        &self,
        state: &dyn Session,
        projection: Schema,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.refresh_stats(state.config()).await?;

        let projection = self.transform_encrypted_fields(projection);
        let projected_schema = SchemaRef::new(projection);
        let key_manager = state.config().get_long_term_keys_manager();

        trace!("Scan {:?} with filters: {filters:?}", &self.table_reference);

        let ResolveIndexResult {
            forwarded_static,
            forwarded_dynamic,
            #[allow(unused)]
            index_selectivity,
            ..
        } = self
            .index_resolver()
            .resolve_indices_for_logical_filters(&key_manager, filters)?;

        let static_filter = forwarded_static.into_iter().reduce(|l, r| l.and(r));

        let self_reference = state
            .config()
            .mysql_schema_for_ref(&TableReference::from(self.table_reference.clone()))
            .expect("unregistered mysql table provider called??");

        let statistics = match self.statistics() {
            Some(mut stats) => {
                let project = projected_schema
                    .fields()
                    .iter()
                    .filter_map(|field| {
                        self.table_schema
                            .column_with_name(field.name())
                            .map(|(index, _)| index)
                    })
                    .collect::<Vec<usize>>();

                if !matches!(index_selectivity, Precision::Absent) {
                    // todo: find better rules to be able to use this effectively...
                    // sadly, in some cases, taking these stats into account can actually worsen the results :(
                    stats = stats.with_num_rows(index_selectivity);
                }

                stats.project(Some(&project))
            }
            None => Statistics::new_unknown(projected_schema.as_ref()),
        };

        let plan: Arc<dyn ExecutionPlan> = Arc::new(MySqlScanPlan::new_from_schema(
            self_reference,
            projected_schema.clone(),
            limit,
            static_filter,
            forwarded_dynamic,
            key_manager,
            statistics,
        ));

        Ok(plan)
    }

    fn transform_encrypted_fields(&self, schema: Schema) -> Schema {
        let fields = schema
            .fields
            .iter()
            .map(|field| {
                if self.is_encrypted(field.name()) {
                    FieldRef::new(
                        Arc::unwrap_or_clone(field.clone()).with_data_type(DataType::Binary),
                    )
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>();

        Schema::new_with_metadata(fields, schema.metadata)
    }

    pub fn update_schema(&mut self, schema: SchemaRef) {
        self.table_schema = schema;
    }

    pub fn table_reference(&self) -> &ResolvedTableReference {
        &self.table_reference
    }
}

// TODO:
// on connection establishment:
// 1. List databases
// 2. Parse Information Schema to extract table information, including statistics and defaults
// 3. Retrieve all variables and set their values

#[async_trait]
impl TableProvider for MySqlTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None // TODO: for views?
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        let default = self.columns_defaults.get(column)?;

        match default {
            ColumnDefault::Expr(ex) => Some(ex),
            ColumnDefault::AutoIncrement => None,
        }
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema_safe(&self.table_schema, projection)?;
        let result = self
            .scan_with_schema(state, Arc::unwrap_or_clone(schema), filters, limit)
            .await;
        result
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // TODO: most filters can be pushed down, do we just return true?
        // TODO: detect which functions are unavailable on MySQL?
        // TODO: take encrypted columns into account! (encrypted w/ index: allow pushdown, transparently insert intermediate step to handle index/refiltering ;; encrypted w/o index: disallow pushdown? (or allow and insert decryption step?))

        // We have a single schema here, so all filters should be pushable
        // TODO: ok, the `Inexact` filter is great for encrypted stuff!
        let resolver = self.index_resolver();
        filters
            .iter()
            .map(|&filter| {
                if self.has_encrypted_columns() {
                    let out = resolver.filter_supported_for_expr(filter);

                    trace!("Filter pushdown [{out:?}]: {filter:?}");

                    out
                } else {
                    // Shortcut for unencrypted tables
                    Ok(TableProviderFilterPushDown::Exact)
                }
            })
            .collect()
    }

    fn statistics(&self) -> Option<Statistics> {
        let stats = self.statistics.try_read().ok()?;
        Some(stats.to_df_stats(self.schema()))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: verify that the input schema is respected (in particular non null values)

        // If there is an indexable column, it must be added first so that it is accessible to all indices managed later on
        let input = if let Some(row_id) = self.get_row_id_column()
            && row_id.is_hidden()
        {
            let key_manager = state.config().get_long_term_keys_manager();
            let generator = key_manager.get_identifier_generator(&IdentifierContext::RowIdColumn {
                table_context: self.table_reference.clone(),
            });
            let gen_rowid = row_id
                .compute_rowid(generator, input.schema().as_ref())?
                .ok_or(plan_datafusion_err!("failed to generate row_id"))?;

            let project_add_indexable_column = input
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let base = physical_expr::expressions::Column::new(field.name(), idx);
                    ProjectionExpr::new(Arc::new(base), field.name().clone())
                })
                .chain(once(gen_rowid))
                .collect::<Vec<_>>();

            Arc::new(ProjectionExec::try_new(
                project_add_indexable_column,
                input,
            )?) as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        let (projected_input, insert_schema, more_sinks) =
            self.project_insert_update(state, input, false)?;

        let fmt = DisplayableExecutionPlan::new(projected_input.as_ref());
        trace!("INSERT sink: {}", fmt.indent(true));

        let sink = MySqlDmlSink::insert(self.table_reference.clone(), insert_op, insert_schema);
        let sink = Self::chain_sink(Arc::new(sink), more_sinks);
        let exec = RecordBatchSinkExec::new(projected_input, sink);

        Ok(Arc::new(exec))
    }
}

impl MySqlTableProvider {
    pub async fn delete_from(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // This operation is way simpler than insert, because the projection of the schema is already handled by the logical planner
        // We may need to redo/recheck, though

        // TODO: check to see if the plan is an empty select (full drop)
        let projected_schema = input.schema();

        // TODO: verify that this schema makes sense

        // TODO: update indices

        let sink = MySqlDeleteSink::new(self.table_reference.clone(), projected_schema.clone());
        let exec = RecordBatchSinkExec::new(input, Arc::new(sink));

        Ok(Arc::new(exec))
    }

    pub(crate) fn get_table_statistics(&self) -> datafusion::common::Result<Arc<TableStatistics>> {
        let lock = self
            .statistics
            .read()
            .map_err(|_| internal_datafusion_err!("poisoned table statistics lock"))?;

        Ok(Arc::clone(&lock))
    }

    pub async fn update(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = transform_select_with_locking(input, LockType::Update)?;

        // If we have a generated indexable column, add it to the underlying select plan
        // This cannot be done at logical planning because the indexable column is "invisible".
        let input = if self.has_index_storage()
            && let Some(indexable_column) = self.get_row_id_column().filter(|col| col.is_hidden())
        {
            let table_ref = self.table_reference.clone();
            let alias = format!("{CURRENT_VALUE_PREFIX}{}", indexable_column.name());

            input
                .transform_up(|node| {
                    let node_ref = node.as_ref().as_any();
                    if let Some(plan) = node_ref.downcast_ref::<MySqlScanPlan>() {
                        let mut plan = plan.clone();
                        plan.add_select_column(
                            &table_ref,
                            Arc::new(indexable_column.field()),
                            Some(alias.clone()),
                        );
                        Ok(Transformed::yes(Arc::new(plan)))
                    } else if let Some(plan) = node_ref.downcast_ref::<ProjectionExec>() {
                        let added_column = col(alias.as_str(), plan.input().schema().as_ref())?;
                        let add_project = ProjectionExpr::new(added_column, alias.clone());
                        let projections = plan.expr().iter().cloned().chain(once(add_project));
                        let project = ProjectionExec::try_new(projections, plan.input().clone())?;
                        Ok(Transformed::yes(Arc::new(project)))
                    } else {
                        Ok(Transformed::no(node))
                    }
                })?
                .data
        } else {
            input
        };

        let plan_display = DisplayableExecutionPlan::new(input.as_ref());
        trace!("UPDATE INPUT: {}", plan_display.indent(true));

        let (projected_input, schema, more_sinks) =
            self.project_insert_update(state, input, true)?;
        let sink = MySqlDmlSink::update(self.table_reference.clone(), schema)?;
        let sink = Self::chain_sink(Arc::new(sink), more_sinks);
        let exec = RecordBatchSinkExec::new(projected_input, sink);

        Ok(Arc::new(exec))
    }

    fn chain_sink(
        sink: Arc<dyn RecordBatchSink>,
        more_sinks: Vec<Arc<dyn RecordBatchSink>>,
    ) -> Arc<dyn RecordBatchSink> {
        if more_sinks.is_empty() {
            sink
        } else {
            let sink = more_sinks
                .into_iter()
                .fold(MultiplexSink::one(sink), |mut acc, elem| {
                    acc.add_sink(elem);
                    acc
                });

            Arc::new(sink)
        }
    }

    /// Returns:
    /// - an execution plan with all the columns required to insert/update + for the possible index sinks
    /// - the schema to send to insert/update
    /// - the index sinks
    fn project_insert_update(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        is_update: bool,
    ) -> datafusion::common::Result<(
        Arc<dyn ExecutionPlan>,
        SchemaRef,
        Vec<Arc<dyn RecordBatchSink>>,
    )> {
        // can we find encrypted info in the input schema?
        trace!("Project insert/update: {:?}", input.schema());
        let key_manager = state.config().get_long_term_keys_manager();

        // Columns requested by the indexes - must be available in plaintext
        let requested_cleartext_columns = self
            .encryption_metadata
            .indices()
            .iter()
            .flat_map(|index| index.tracked_columns())
            .collect::<FxHashSet<ColumnName>>();

        let index_insert_strategies = self
            .encryption_metadata
            .indices()
            .iter()
            .map(|index| {
                if is_update {
                    Arc::clone(index).update(self, key_manager.clone(), input.schema().clone())
                } else {
                    Arc::clone(index).insert(self, key_manager.clone(), input.schema().clone())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Split the index insert strategies among those that are forwarded to the main table, and those that are not
        let (forward, separate) = index_insert_strategies
            .into_iter()
            .partition::<Vec<_>, _>(|e| match e {
                IndexInsertStrategy::AddColumns(_) => true,
                IndexInsertStrategy::IndexSink(_) => false,
            });

        let forward_projections = forward
            .into_iter()
            .filter_map(|v| v.into_projection_vec())
            .flatten()
            .collect::<Vec<_>>();

        let (additional_projections, additional_sinks): (Vec<Vec<ProjectionExpr>>, Vec<_>) =
            separate
                .into_iter()
                .filter_map(|e| match e {
                    IndexInsertStrategy::IndexSink(index_sink) => Some((
                        index_sink.input().to_vec(),
                        index_sink.into_record_batch_sink(),
                    )),
                    _ => None,
                })
                .unzip();

        // TODO: some indices will require an additional sink

        // TODO: we may have column metadata that limit the size of the column (e.g. `VARCHAR(20) == max 20 chr`) ==> enforce these constraints here
        // TODO: we may want to move the encryption to a dedicated step - just think about how to maintain indexing info, maybe project as additional columns that get stripped by the sink?

        // Encrypted columns metadata, in the same order as the schema
        let encrypted_columns = input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let field_name: Arc<str> =
                    if let Some(name) = field.name().strip_prefix(DUPLICATE_VALUE_PFX) {
                        name
                    } else {
                        field.name().as_str()
                    }
                    .to_string()
                    .into();

                self.get_column_metadata(field_name.as_ref()).map(|meta| {
                    let cipher = key_manager.get_cipher(&CipherContext::TableColumn {
                        table_context: self.table_reference.clone(),
                        column_name: field_name.clone(),
                    });
                    (cipher, meta.original_type.clone())
                })
            })
            .collect::<Vec<_>>();

        // Project layer 1:
        // - cleartext: do nothing
        // - encrypted w/ index: do nothing
        // - encrypted w/o index: binary encode + encrypt (to same column)
        // Question: could the optimizer detect that we're doing the same op twice in cases 2 & 3 and simplify the logic?
        let input_schema = input.schema();
        let aad_source = self.get_aad_source_physical(
            input_schema.as_ref(),
            if is_update { Some(FILTER_PREFIX) } else { None },
        )?;

        let project_layer_1 = input_schema
            .fields()
            .iter()
            .zip(encrypted_columns.iter())
            .enumerate()
            .map(|(idx, (field, column_meta))| {
                let base = datafusion::physical_expr::expressions::Column::new(field.name(), idx);

                let expr = match column_meta {
                    None => Arc::new(base) as Arc<dyn PhysicalExpr>,
                    Some((cipher, source_type)) => {
                        if requested_cleartext_columns.contains(field.name()) {
                            // Return the column as-is
                            Arc::new(base)
                        } else {
                            let aad =
                                Arc::new(ComputeAadExpr::new(aad_source.clone(), source_type));
                            let cast = Arc::new(ToBinaryExpr::new(Arc::new(base)))
                                as Arc<dyn PhysicalExpr>;
                            let encrypt = EncryptExpr::new(cast, aad, cipher.clone());
                            Arc::new(encrypt) as Arc<dyn PhysicalExpr>
                        }
                    }
                };

                ProjectionExpr::new(expr, field.name().clone())
            });

        let project_layer_1 =
            Arc::new(ProjectionExec::try_new(project_layer_1, input)?) as Arc<dyn ExecutionPlan>;

        let (insert_schema, projected_input) = if requested_cleartext_columns.is_empty()
            && forward_projections.is_empty()
            && additional_projections.is_empty()
        {
            (project_layer_1.schema(), project_layer_1)
        } else {
            // Project layer 2
            // - cleartext w/ index: encode + convert (to new column)
            // - encrypted w/ index: encode + encrypt source column + convert second column
            let project_layer_2_base = input_schema
                .fields()
                .iter()
                .zip(encrypted_columns.iter())
                .enumerate()
                .map(|(idx, (field, column_meta))| {
                    let base = Arc::new(physical_expr::expressions::Column::new(field.name(), idx));
                    let expr = match column_meta {
                        Some((cipher, data_type))
                            if requested_cleartext_columns.contains(field.name()) =>
                        {
                            let aad = Arc::new(ComputeAadExpr::new(aad_source.clone(), data_type));
                            let cast = Arc::new(ToBinaryExpr::new(base));

                            let encrypt = EncryptExpr::new(cast, aad, cipher.clone());
                            Arc::new(encrypt) as Arc<dyn PhysicalExpr>
                        }
                        _ => base as Arc<dyn PhysicalExpr>,
                    };

                    ProjectionExpr::new(expr, field.name().clone())
                });

            let keep_columns = project_layer_1.schema().fields().len() + forward_projections.len();
            let project_layer_2 = project_layer_2_base
                .chain(forward_projections.into_iter())
                .chain(additional_projections.into_iter().flatten())
                .collect::<Vec<_>>();

            let project = Arc::new(ProjectionExec::try_new(project_layer_2, project_layer_1)?)
                as Arc<dyn ExecutionPlan>;

            // Only keep base columns + forward_projections. Order should be correct
            let mut new_schema = project.schema().fields().to_vec();
            new_schema.truncate(keep_columns);
            let new_schema = Schema::new(new_schema);

            (Arc::new(new_schema), project)
        };

        Ok((projected_input, insert_schema, additional_sinks))
    }
}
