pub mod blind_index;
mod indices;
pub mod kw_search_func;
pub mod store;

use crate::backend_config::BackendConfig;
use crate::expr_util::identifiers_in_expr;
use crate::metadata::blind_index::BlindIndexConfig;
use crate::metadata::indices::InvertedIndexConfig;
use crate::planning::physical::plans::mysql_scan_plan::DynamicFilter;
use crate::providers::table_provider::{IndexableColumn, IndexableColumnSize, MySqlTableProvider};
use crate::sinks::sink::RecordBatchSink;
use async_trait::async_trait;
use common::conversions::column_def_ext::ColumnDefExt;
use common::conversions::datatypes::ArrowDatatypeConverter;
use crypto::LongTermKeyManager;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::sqlparser::ast::{ColumnDef, TableConstraint};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::ResolvedTableReference;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::CreateTable;
use log::trace;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
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
    pub indexable_column: IndexableColumn,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SerializableEncryptedSchemaMeta {
    pub encrypted_tables: FxHashMap<String, SerializableEncryptedTableMeta>,
}

impl SerializableEncryptedSchemaMeta {
    pub fn deserialize(self) -> EncryptedSchemaMeta {
        let tables = self
            .encrypted_tables
            .into_iter()
            .map(|(k, v)| {
                let v = EncryptedTableMeta::deserialize(&k, v);
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
        table_name: &str,
        indexable_column: IndexableColumn,
    ) -> Arc<dyn EncryptedIndex> {
        match self {
            EncryptedIndexConfigurationVariant::BlindIndex(bi) => {
                bi.into_index(table_name, indexable_column)
            }
            EncryptedIndexConfigurationVariant::InvertedIndex(ii) => {
                ii.into_index(table_name, indexable_column)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptedIndexConfigurationVariant {
    BlindIndex(BlindIndexConfig),
    InvertedIndex(InvertedIndexConfig),
}

trait IndexConfig {
    fn into_index(
        self,
        table_name: &str,
        indexable_column: IndexableColumn,
    ) -> Arc<dyn EncryptedIndex>;
}

const BLIND_INDEX_PFX: &str = "blind_bits_";
const INVERTED_INDEX_NAME: &str = "inverted_index";

impl EncryptedIndexConfigurationVariant {
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
    fn deserialize(table_name: &str, value: SerializableEncryptedTableMeta) -> Self {
        let SerializableEncryptedTableMeta {
            encrypted_columns,
            indices,
            indexable_column,
        } = value;
        let indices = indices
            .into_iter()
            .map(|index| index.build(table_name, indexable_column))
            .collect();
        Self {
            encrypted_columns,
            indexable_column,
            indices,
        }
    }
}

impl From<&'_ EncryptedTableMeta> for SerializableEncryptedTableMeta {
    fn from(value: &'_ EncryptedTableMeta) -> Self {
        Self {
            encrypted_columns: value.encrypted_columns.clone(),
            indexable_column: value.indexable_column,
            indices: value
                .indices
                .iter()
                .map(|index| index.to_config())
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedTableMeta {
    encrypted_columns: FxHashMap<ColumnName, EncryptedColumnMeta>,
    indices: Vec<Arc<dyn EncryptedIndex>>,
    indexable_column: IndexableColumn,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrimaryKeyColumnDef {
    pub column_name: ColumnName,
    pub column_type: DataType,
    pub has_default: bool,
}

pub type PrimaryKey = Vec<PrimaryKeyColumnDef>;

pub fn compute_expected_indexable_column_type(primary_key: &PrimaryKey) -> IndexableColumn {
    const RANDOM_GENERATED: IndexableColumn =
        IndexableColumn::RandomGenerated(IndexableColumnSize::B96);

    if primary_key.len() == 1 {
        let PrimaryKeyColumnDef {
            has_default,
            column_type,
            ..
        } = &primary_key[0];
        if *has_default {
            RANDOM_GENERATED
        } else {
            match column_type {
                DataType::Int8 | DataType::UInt8 => {
                    IndexableColumn::UserProvided(IndexableColumnSize::B8)
                }
                DataType::Int16 | DataType::UInt16 => {
                    IndexableColumn::UserProvided(IndexableColumnSize::B16)
                }
                DataType::Int32 | DataType::UInt32 => {
                    IndexableColumn::UserProvided(IndexableColumnSize::B32)
                }
                DataType::Int64 | DataType::UInt64 => {
                    IndexableColumn::UserProvided(IndexableColumnSize::B64)
                }
                _ => RANDOM_GENERATED,
            }
        }
    } else {
        RANDOM_GENERATED
    }
}

impl EncryptedTableMeta {
    pub fn new() -> Self {
        Self {
            encrypted_columns: FxHashMap::default(),
            indices: Vec::new(),
            indexable_column: IndexableColumn::RandomGenerated(IndexableColumnSize::B96), // Default placeholder value
        }
    }

    pub fn indexable_column(&self) -> IndexableColumn {
        self.indexable_column
    }

    pub fn set_indexable_column(&mut self, value: IndexableColumn) {
        self.indexable_column = value
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
    ) -> Result<Option<EncryptedTableMeta>, DataFusionError> {
        // TODO: ensure primary key has no encrypted col
        trace!("Detected primary key: {primary_key:?}");

        let mut meta = Self::new();
        for column in statement.columns.iter_mut() {
            let Some(metadata) = EncryptedColumnMeta::try_from_column(column, primary_key, config)?
            else {
                continue;
            };

            meta.add_column(column.name.value.clone(), metadata);
        }

        // TODO: handle indexes here (will also need to modify the CreateTable accordingly!)
        if meta.has_encrypted_column() {
            let dropped_constraints = statement
                .constraints
                .extract_if(.., |constraint| {
                    match constraint {
                        TableConstraint::PrimaryKey { .. } => false, // Always fwd primary keys
                        TableConstraint::Unique { columns, .. }
                        | TableConstraint::Index { columns, .. }
                        | TableConstraint::FulltextOrSpatial { columns, .. } => columns
                            .iter()
                            .flat_map(|column| identifiers_in_expr(&column.column.expr))
                            .any(|column| meta.encrypted_columns.contains_key(&column)),
                        TableConstraint::Check { expr, .. } => identifiers_in_expr(&expr)
                            .iter()
                            .any(|column| meta.encrypted_columns.contains_key(column)),
                        TableConstraint::ForeignKey { columns, .. } => columns
                            .iter()
                            .any(|column| meta.encrypted_columns.contains_key(&column.value)),
                    }
                })
                .collect::<Vec<_>>();

            for dropped in dropped_constraints {
                log::warn!("Dropped table constraint: {dropped}")
            }
        }

        Ok(if meta.encrypted_columns.is_empty() {
            None
        } else {
            Some(meta)
        })
    }

    pub fn column(&self, column_name: &ColumnName) -> Option<&EncryptedColumnMeta> {
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
        self.indices()
            .iter()
            .any(|index| index.is_column_hidden(column_name))
    }

    pub fn has_encrypted_column(&self) -> bool {
        !self.encrypted_columns.is_empty()
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

        Ok(Some((meta)))
    }
}

impl EncryptedColumnMeta {
    pub fn data_type(&self) -> DataType {
        self.original_type.clone()
    }
}

#[async_trait]
pub(crate) trait EncryptedIndex: Send + Sync + Debug {
    /// Returns a list of all tables that are related to this tables, for example index tables.
    fn linked_table_names(&self) -> Vec<String>;

    /// Returns a set of columns this index tracks. The index must be notified (via insert, update,
    /// delete functions) when these columns change.
    fn tracked_columns(&self) -> FxHashSet<ColumnName>;

    // Is this expression supported by this index? if so, it will be included in the call to query.
    fn supports_expression(&self, filter: &Expr) -> bool;

    /// Returns true if this index has columns outside of the table to which it relates
    fn requires_external_storage(&self) -> bool;

    /// Returns true if this index needs both the old and new value when a column is updated in any
    /// of its tracked columns
    fn requires_old_value_on_update(&self) -> bool {
        self.requires_external_storage()
    }

    async fn create_index_plan(
        &self,
        parent_table_ref: ResolvedTableReference,
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

    // TODO: delete

    /// Queries the index
    ///
    /// ## Params
    ///
    /// filters: a slice of all filters in the initial expression that are supported
    fn query(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        filters: &[Expr],
    ) -> datafusion::common::Result<Option<IndexQueryStrategy>>;

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

#[derive(Debug)]
pub enum IndexQueryStrategy {
    AddFilterExpression(Expr),

    /// Returns an expression that will be added to the filter, and an expression that will be
    /// executed as part of the query execution, but before the query is sent to the server
    AddDynamicFilterExpression(Expr, Arc<dyn DynamicFilter>),
}
