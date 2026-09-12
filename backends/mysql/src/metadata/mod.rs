pub mod blind_index;
pub mod indices;
pub mod kw_search_func;
pub mod store;

use crate::backend_config::BackendConfig;
use crate::expr_util::identifiers_in_expr;
use crate::filtering::indexable_filter::SupportOptions;
use crate::filtering::logical::IndexableLogicalExpr;
use crate::filtering::physical::IndexablePhysicalExpr;
use crate::metadata::blind_index::BlindIndexConfig;
use crate::metadata::indices::inverted_index::inverted_index::InvertedIndexConfig;
use crate::metadata::indices::range_queries::{ObfuscationStrategy, RangeIndex, ValueDistribution};
use crate::providers::table_provider::MySqlTableProvider;
use crate::sinks::sink::RecordBatchSink;
use async_trait::async_trait;
use common::conversions::column_def_ext::ColumnDefExt;
use common::conversions::datatypes::ArrowDatatypeConverter;
use crypto::LongTermKeyManager;
use crypto::row_id::RowIdColumn;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::{exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::sqlparser::ast::{
    CheckConstraint, ColumnDef, ForeignKeyConstraint, FullTextOrSpatialConstraint, IndexConstraint,
    TableConstraint, UniqueConstraint,
};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::sql::ResolvedTableReference;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::CreateTable;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub type ColumnName = String;

#[derive(Debug, Clone)]
pub struct EncryptedSchemaMeta {
    encrypted_tables: FxHashMap<String, EncryptedTableMeta>,
}

impl EncryptedSchemaMeta {
    pub fn table(&self, name: &str) -> Option<&EncryptedTableMeta> {
        self.encrypted_tables.get(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEncryptedTableMeta {
    pub encrypted_columns: FxHashMap<ColumnName, EncryptedColumnMeta>,
    pub indices: Vec<EncryptedIndexConfigurationVariant>,
    pub row_id_column: Option<RowIdColumn>,

    #[serde(default)]
    pub row_binding_aad: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SerializableEncryptedSchemaMeta {
    pub encrypted_tables: FxHashMap<String, SerializableEncryptedTableMeta>,
}

impl SerializableEncryptedSchemaMeta {
    pub fn deserialize(self, catalog_name: Arc<str>, schema_name: Arc<str>) -> EncryptedSchemaMeta {
        let tables = self
            .encrypted_tables
            .into_iter()
            .map(|(k, v)| {
                let table_ref = ResolvedTableReference {
                    schema: schema_name.clone(),
                    table: k.clone().into(),
                    catalog: catalog_name.clone(),
                };
                let v = EncryptedTableMeta::deserialize(table_ref, v);
                (k, v)
            })
            .collect();

        EncryptedSchemaMeta {
            encrypted_tables: tables,
        }
    }
}

impl EncryptedIndexConfigurationVariant {
    pub fn build(
        self,
        table_name: &ResolvedTableReference,
        indexable_column: Option<&RowIdColumn>,
    ) -> Arc<dyn EncryptedIndex> {
        match self {
            EncryptedIndexConfigurationVariant::BlindIndex(bi) => {
                bi.into_index(table_name, indexable_column)
            }
            EncryptedIndexConfigurationVariant::InvertedIndex(ii) => {
                ii.into_index(table_name, indexable_column)
            }
            EncryptedIndexConfigurationVariant::RangeIndex(ii) => {
                ii.into_index(table_name, indexable_column)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptedIndexConfigurationVariant {
    BlindIndex(BlindIndexConfig),
    RangeIndex(RangeIndex),
    InvertedIndex(InvertedIndexConfig),
}

trait IndexConfig {
    fn into_index(
        self,
        _table_name: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
    ) -> Arc<dyn EncryptedIndex>;
}

const BLIND_INDEX_PFX: &str = "blind_bits_";
const RANGE_INDEX_PFX: &str = "range_";
const INVERTED_INDEX_NAME: &str = "inverted_index";

impl EncryptedIndexConfigurationVariant {
    pub fn requires_rowid(&self) -> bool {
        match self {
            EncryptedIndexConfigurationVariant::BlindIndex(_) => false,
            EncryptedIndexConfigurationVariant::RangeIndex(_) => false,
            EncryptedIndexConfigurationVariant::InvertedIndex(_) => true,
        }
    }

    pub fn parse_from_sql(
        name: String,
        columns: Vec<ColumnName>,
        table: Arc<MySqlTableProvider>,
        sql: &str,
    ) -> datafusion::common::Result<EncryptedIndexConfigurationVariant> {
        if let Some(rest) = sql.strip_prefix(BLIND_INDEX_PFX) {
            // Blind index
            let Some(size_bits): Option<usize> = rest.parse().ok() else {
                plan_err!("Cannot create blind index: invalid bit size provided")?
            };

            if columns.len() != 1 {
                plan_err!("Cannot create blind index: exactly one column is required")?
            }

            let column = columns[0].clone();

            Ok(EncryptedIndexConfigurationVariant::BlindIndex(
                BlindIndexConfig {
                    size_bits,
                    index_name: name,
                    column,
                },
            ))
        } else if let Some(rest) = sql.strip_prefix(RANGE_INDEX_PFX) {
            // Blind index
            if columns.len() != 1 {
                plan_err!("Cannot create blind index: exactly one column is required")?
            }

            let column = columns[0].clone();
            let schema = table.schema();
            let column = schema.field_with_name(&column)?;
            let data_type = column.data_type();

            let vd = ValueDistribution::parse(data_type, rest)?;

            Ok(EncryptedIndexConfigurationVariant::RangeIndex(
                RangeIndex::new(&name, column.name(), vd, ObfuscationStrategy::NoObfuscation),
            ))
        } else if sql == INVERTED_INDEX_NAME {
            Ok(EncryptedIndexConfigurationVariant::InvertedIndex(
                InvertedIndexConfig::initialize_for_columns(name, table, columns)?,
            ))
        } else {
            plan_err!("Unknown index variant {sql}.")?
        }
    }
}

impl EncryptedTableMeta {
    fn deserialize(
        table_name: ResolvedTableReference,
        value: SerializableEncryptedTableMeta,
    ) -> Self {
        let SerializableEncryptedTableMeta {
            encrypted_columns,
            indices,
            row_id_column,
            row_binding_aad,
        } = value;
        let indices = indices
            .into_iter()
            .map(|index| index.build(&table_name, row_id_column.as_ref()))
            .collect();

        Self {
            encrypted_columns,
            row_id_column,
            indices,
            row_binding_aad,
        }
    }
}

impl From<&'_ EncryptedTableMeta> for SerializableEncryptedTableMeta {
    fn from(value: &'_ EncryptedTableMeta) -> Self {
        Self {
            encrypted_columns: value.encrypted_columns.clone(),
            row_id_column: value.row_id_column.clone(),
            indices: value
                .indices
                .iter()
                .map(|index| index.to_config())
                .collect(),
            row_binding_aad: value.row_binding_aad,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedTableMeta {
    pub(crate) encrypted_columns: FxHashMap<ColumnName, EncryptedColumnMeta>,
    pub(crate) indices: Vec<Arc<dyn EncryptedIndex>>,
    pub(crate) row_id_column: Option<RowIdColumn>,

    /// If set, the associated data of each ciphertext will contain the row primary key.
    /// Will be ignored for non-deterministic primary keys.
    pub(crate) row_binding_aad: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrimaryKeyColumnDef {
    pub column_name: ColumnName,
    pub column_type: DataType,
    pub has_default: bool,
}

pub type PrimaryKey = Vec<PrimaryKeyColumnDef>;

pub fn supports_aad_binding(primary_key: &PrimaryKey) -> bool {
    !primary_key.is_empty() && primary_key.iter().all(|col| !col.has_default)
}

impl EncryptedTableMeta {
    pub fn new() -> Self {
        Self {
            indices: vec![],
            encrypted_columns: FxHashMap::default(),
            row_id_column: None,
            row_binding_aad: false,
        }
    }

    pub fn remove_column(&mut self, column: &ColumnName) {
        self.encrypted_columns.remove(column);
    }

    /// Builds an [EncryptedTableMeta] structure from a CreateTable statement, and modifies the types
    /// of the encrypted columns in the statement to BLOB
    pub fn build_from_statement(
        statement: &mut CreateTable,
        primary_key: &[ColumnName],
        config: &BackendConfig,
    ) -> Result<EncryptedTableMeta, DataFusionError> {
        let mut meta = Self::new();

        // Try to build the row-id column
        let projected_primary_key: Vec<_> = primary_key
            .iter()
            .filter_map(|col_name| {
                statement
                    .columns
                    .iter()
                    .find(|col| &col.name.value == col_name)
            })
            .collect();

        meta.row_id_column = RowIdColumn::get_natural_if_any(&projected_primary_key);

        for column in statement.columns.iter_mut() {
            let Some(metadata) = EncryptedColumnMeta::try_from_column(column, primary_key, config)?
            else {
                continue;
            };

            meta.add_column(column.name.value.clone(), metadata);
        }

        // TODO: handle indexes here (will also need to modify the CreateTable accordingly!)
        if meta.has_encrypted_column() {
            meta.row_binding_aad = config.row_binding_aad;

            let dropped_constraints = statement
                .constraints
                .extract_if(.., |constraint| {
                    match constraint {
                        TableConstraint::PrimaryKey { .. } => false, // Always fwd primary keys
                        TableConstraint::Unique(UniqueConstraint { columns, .. })
                        | TableConstraint::Index(IndexConstraint { columns, .. })
                        | TableConstraint::FulltextOrSpatial(FullTextOrSpatialConstraint {
                            columns,
                            ..
                        }) => columns
                            .iter()
                            .flat_map(|column| identifiers_in_expr(&column.column.expr))
                            .any(|column| meta.encrypted_columns.contains_key(&column)),
                        TableConstraint::Check(CheckConstraint { expr, .. }) => {
                            identifiers_in_expr(&expr)
                                .iter()
                                .any(|column| meta.encrypted_columns.contains_key(column))
                        }
                        TableConstraint::ForeignKey(ForeignKeyConstraint { columns, .. }) => {
                            columns
                                .iter()
                                .any(|column| meta.encrypted_columns.contains_key(&column.value))
                        }
                    }
                })
                .collect::<Vec<_>>();

            for dropped in dropped_constraints {
                log::warn!("Dropped table constraint: {dropped}")
            }
        }

        Ok(meta)
    }

    pub fn column(&self, column_name: &str) -> Option<&EncryptedColumnMeta> {
        self.encrypted_columns.get(column_name)
    }

    pub fn indices(&self) -> &[Arc<dyn EncryptedIndex>] {
        &self.indices
    }

    pub fn add_column(&mut self, column_name: ColumnName, meta: EncryptedColumnMeta) {
        self.encrypted_columns.insert(column_name, meta);
    }

    pub fn add_index(&mut self, index: Arc<dyn EncryptedIndex>) {
        self.indices.push(index);
    }

    pub fn is_column_hidden(&self, column_name: &ColumnName) -> bool {
        if let Some(row_id) = self.row_id_column.as_ref()
            && column_name == row_id.name()
            && row_id.is_hidden()
        {
            return true;
        }

        self.indices()
            .iter()
            .any(|index| index.is_column_hidden(column_name))
    }

    pub fn has_encrypted_column(&self) -> bool {
        !self.encrypted_columns.is_empty()
    }

    pub fn is_column_encrypted(&self, column_name: &str) -> bool {
        self.encrypted_columns.contains_key(column_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedColumnMeta {
    // TODO: we could add a cipher definition here so that each column can chose its decryption method?
    pub original_type: DataType,

    pub original_type_raw: String,
    pub default_value_raw: Option<String>,
    pub nullable: bool,
    pub collation: Option<String>, // original_column_definition: datafusion::logical_expr::sqlparser::ast::DataType
}

impl EncryptedColumnMeta {
    /// Builds encrypted column metadata for a given column, if it has the ENCRYPTED flag.
    /// Modifies the passed column def accordingly, but only if the column is encrypted.
    /// In other terms: if the return is None, column will not have changed.
    pub fn try_from_column(
        column: &mut ColumnDef,
        primary_key: &[ColumnName],
        config: &BackendConfig,
    ) -> datafusion::common::Result<Option<Self>> {
        if column.is_encrypted_column() && column.is_decrypted_column() {
            return plan_err!(
                "Ambiguous column with both ENCRYPTED and DECRYPTED flags: {}",
                column.name.value
            );
        }

        if column.is_decrypted_column() {
            column.clear_encryption_flag();
            return Ok(None);
        }

        // Should this column be encrypted?
        if !column.is_encrypted_column() {
            // Column was not explicitly set to encrypt
            if !config.encrypt_by_default {
                return Ok(None);
            }

            // Is it a primary key?
            if column.is_primary_key() || primary_key.contains(&column.name.value) {
                return Ok(None);
            }

            // Is this an integer type? (in which case we skip encryption)
            let dt = ArrowDatatypeConverter.convert_data_type(&column.data_type)?;
            if dt.is_integer() {
                return Ok(None);
            }
        }

        if column.is_primary_key() || primary_key.contains(&column.name.value) {
            exec_err!(
                "Invalid column definition: {} is both in primary key and encrypted",
                column.name.value
            )?;
        };

        let mut data_type = ArrowDatatypeConverter.column_to_datatype(column).unwrap();
        if let DataType::Timestamp(unit, None) = data_type {
            // toml suddenly dislikes Timestamp columns with no TZ info
            data_type = DataType::Timestamp(unit, Some("+00".into()))
        }

        let meta = EncryptedColumnMeta {
            original_type: data_type,
            original_type_raw: column.data_type.to_string(),
            collation: column.get_collation(),
            default_value_raw: column.pop_default_value().map(|expr| expr.to_string()),
            nullable: column.is_nullable(),
        };

        column.data_type = ast::DataType::MediumBlob; // TODO: adopt size to source type!
        column.clear_encryption_flag();

        Ok(Some(meta))
    }
}

impl EncryptedColumnMeta {
    pub fn data_type(&self) -> DataType {
        self.original_type.clone()
    }
}

/// TODO: this interface does too many different things and should be split in three.
///
/// - The simplest case (blind-index) is not an _index_ but more a piece of logic that can rewrite
///   a query to reduce the volume of data retrieved from the server.
///   We want to avoid premature abstractions here. For now, an index in this case is a dedicated
///   column that enables filtering. So we can rely on that to make a generic structure.
///   Naming idea: SelectionColumn, MatchingColumn, PredicateColumn
/// - The second case is traditional selection indices. We can have a look at datafusion-contrib's
///   implementation for inspiration here. Ideally, we want them to be implemented in a kind of
///   JOIN, with a forwarding implementation that forwards to the MySQL query operator.
/// - The last case is for sort indices only.
///
/// The key point is that all the indices require some operations when rows are inserted or updated.
/// The first index is quite simple, and should once again be handled differently.
#[async_trait]
pub(crate) trait EncryptedIndex: Send + Sync + Debug + Any {
    #[allow(unused)]
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;

    /// Returns a set of columns this index tracks. The index must be notified (via insert, update,
    /// delete functions) when these columns change.
    fn tracked_columns(&self) -> FxHashSet<ColumnName>;

    fn logical_support_options<'s, 'b>(&'s self) -> SupportOptions<'s, &'b Expr>;

    /// Queries the index with a filter.
    ///
    /// The filter passed to this function is entirely supported, so this function MUST return a
    /// result.
    fn logical_query(
        self: Arc<Self>,
        _table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        _key_manager: &Arc<LongTermKeyManager>,
        _filter: IndexableLogicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        unimplemented!("this index cannot be queried")
    }

    fn physical_support_options<'s, 'b>(
        &'s self,
    ) -> Option<SupportOptions<'s, &'b dyn PhysicalExpr>> {
        None
    }

    fn physical_query(
        self: Arc<Self>,
        _table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        _key_manager: &Arc<LongTermKeyManager>,
        _filter: IndexablePhysicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        unimplemented!("this index cannot be queried")
    }

    /// Returns true if this index has columns outside of the table to which it relates
    fn requires_external_storage(&self) -> bool;

    /// Returns true if this index needs both the old and new value when a column is updated in any
    /// of its tracked columns
    fn requires_old_value_on_update(&self) -> bool {
        self.requires_external_storage()
    }

    async fn create_index_plan(
        self: Arc<Self>,
        parent_table_ref: &ResolvedTableReference,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>>;

    fn insert(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy>;

    /// Processes an update
    /// The schema is already setup according to semantics expected by MySqlUpdateSink
    fn update(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        self.insert(table_ref, key_manager, schema)
    }

    fn is_column_hidden(&self, _column: &ColumnName) -> bool {
        false
    }

    fn to_config(&self) -> EncryptedIndexConfigurationVariant;
}

pub enum IndexInsertStrategy {
    /// Add new columns to the insert. The inner value is a mapping from new column name to row
    /// values, in the same order as the rows used to generate this modification.
    AddColumns(Vec<ProjectionExpr>),

    /// Do additional custom queries to insert the values
    IndexSink(Arc<dyn IndexSink>),
}

impl IndexInsertStrategy {
    #[allow(unused)]
    pub fn to_record_sink(&self) -> Option<Arc<dyn RecordBatchSink>> {
        match self {
            IndexInsertStrategy::AddColumns(_) => None,
            IndexInsertStrategy::IndexSink(s) => {
                validate_index_sink(s.as_ref());
                Some(Arc::clone(s).into_record_batch_sink())
            }
        }
    }

    pub fn into_projection_vec(self) -> Option<Vec<ProjectionExpr>> {
        match self {
            IndexInsertStrategy::AddColumns(c) => Some(c),
            IndexInsertStrategy::IndexSink(_) => None,
        }
    }
}

pub trait IndexSink: RecordBatchSink {
    /// Produces the input of this sink. Its schema must match.
    fn input(&self) -> &[ProjectionExpr];

    fn into_record_batch_sink(self: Arc<Self>) -> Arc<dyn RecordBatchSink>;
}

pub fn validate_index_sink(sink: &dyn IndexSink) {
    let schema = sink.input_schema();
    let input = sink.input();

    assert_eq!(input.len(), schema.fields().len());
    for project in input {
        assert!(schema.field_with_name(&project.alias).is_ok());
    }
}

#[async_trait]
pub trait DynamicFilter: Send + Sync + Debug {
    async fn execute_filter(
        &self,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<ast::Expr>;
}

#[derive(Debug)]
pub enum IndexQueryStrategy {
    Fixed(ast::Expr),
    Dynamic(Arc<dyn DynamicFilter>),
}
