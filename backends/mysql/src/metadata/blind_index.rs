use crate::ast_expr_ext::AstExprExt;
use crate::filtering::indexable_filter::{
    ColumnOrTuple, EqualityOperator, IndexSelectivity, IndexableFilterExpr, SupportOptions,
};
use crate::filtering::logical::IndexableLogicalExpr;
use crate::filtering::physical::IndexablePhysicalExpr;
use crate::metadata::{
    ColumnName, EncryptedIndex, EncryptedIndexConfigurationVariant, IndexConfig,
    IndexInsertStrategy, IndexQueryStrategy,
};
use crate::planning::physical::plans::create_columnar_index_plan::CreateColumnarIndexPlan;
use crate::providers::table_provider::{MySqlTableProvider, TableStatistics};
use async_trait::async_trait;
use crypto::identifiers::StableIdentifiersGenerator;
use crypto::planning::physical::to_binary::ToBinaryExpr;
use crypto::row_id::RowIdColumn;
use crypto::{IdentifierContext, LongTermKeyManager};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericByteArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, GenericBinaryType, Schema, SchemaRef};
use datafusion::common::{ResolvedTableReference, ScalarValue, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{ColumnarValue, Expr};
use datafusion::physical_expr;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::Ident;
use rustc_hash::{FxBuildHasher, FxHashSet};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const INDEX_COLUMN_PREFIX: &str = "prox_blid";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlindIndexConfig {
    pub index_name: String,
    pub column: ColumnName,
    pub size_bits: usize,
}

impl IndexConfig for BlindIndexConfig {
    fn into_index(
        self,
        _table_name: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
    ) -> Arc<dyn EncryptedIndex> {
        Arc::new(BlindIndex::new(
            self.index_name,
            self.column,
            self.size_bits,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlindIndex {
    pub name: Arc<str>,
    pub indexed_column: Arc<str>,
    pub size_bits: usize,
    pub index_column_name: Arc<str>,
}

impl BlindIndex {
    #[inline(always)]
    fn scalar_to_bytes(scalar_value: &ScalarValue) -> Vec<u8> {
        // TODO: we must use the same strategy to cast as when inserting values
        scalar_value.to_string().into_bytes()
    }

    #[inline(always)]
    fn identifier_context(&self, table: &ResolvedTableReference) -> IdentifierContext {
        IdentifierContext::NamedIndexInTable {
            table_context: table.clone(),
            index_name: Arc::clone(&self.name),
        }
    }

    pub fn new(name: String, column: ColumnName, size_bits: usize) -> Self {
        let mut index_column_name = format!("{INDEX_COLUMN_PREFIX}_{name}_{size_bits}");
        index_column_name.truncate(32);

        Self {
            name: name.into(),
            indexed_column: column.into(),
            size_bits,
            index_column_name: index_column_name.into(),
        }
    }

    fn value_to_blind_value(
        &self,
        generator: &dyn StableIdentifiersGenerator,
        value: &ScalarValue,
    ) -> ast::Expr {
        ast::Expr::Value(
            ast::Value::SingleQuotedString(
                generator.get_opaque_stable_identifier_hex(
                    &Self::scalar_to_bytes(&value),
                    self.size_bits,
                ),
            )
            .into(),
        )
    }

    fn is_indexable_expr_supported<E>(
        &self,
        logical: &IndexableFilterExpr<E>,
        table_rows: &TableStatistics,
    ) -> Option<IndexSelectivity> {
        match logical {
            IndexableFilterExpr::Eq(c, EqualityOperator::Eq, _)
            | IndexableFilterExpr::InList(ColumnOrTuple::Column(c), _)
                if c.column.name == self.indexed_column.as_ref() =>
            {
                if let Some(selectivity) =
                    table_rows.column_selectivity(self.index_column_name.as_ref())
                {
                    return Some(
                        table_rows
                            .num_rows()
                            .with_estimated_selectivity(selectivity),
                    );
                }

                // Assume uniform
                let possible_values = 1u32 << self.size_bits;
                Some(
                    table_rows
                        .num_rows()
                        .with_estimated_selectivity(1f64 / (possible_values as f64)),
                )
            }
            _ => None,
        }
    }

    fn indexable_query_to_expr<T: Debug + Clone>(
        &self,
        filter: IndexableFilterExpr<T>,
        table_reference: &ResolvedTableReference,
        key_manager: &Arc<LongTermKeyManager>,
    ) -> datafusion::common::Result<ast::Expr> {
        match filter {
            IndexableFilterExpr::<T>::And(l, r) => Ok(self
                .indexable_query_to_expr(*l, table_reference, key_manager)?
                .and(self.indexable_query_to_expr(*r, table_reference, key_manager)?)),
            IndexableFilterExpr::<T>::Or(l, r) => Ok(self
                .indexable_query_to_expr(*l, table_reference, key_manager)?
                .or(self.indexable_query_to_expr(*r, table_reference, key_manager)?)),
            IndexableFilterExpr::<T>::InList(ColumnOrTuple::Column(col), values) => {
                if col.column.name != self.indexed_column.as_ref() {
                    plan_err!("unsupported column for blind_index: {}", col.column.name)?;
                }

                let blind_index_gen =
                    key_manager.get_identifier_generator(&self.identifier_context(table_reference));
                let bi_values = values
                    .iter()
                    .map(|value| self.value_to_blind_value(blind_index_gen.as_ref(), value))
                    .collect::<Vec<_>>();

                Ok(ast::Expr::InList {
                    expr: Box::new(ast::Expr::CompoundIdentifier(vec![
                        Ident::new(table_reference.table.as_ref()),
                        Ident::new(self.index_column_name.as_ref()),
                    ])),
                    list: bi_values,
                    negated: false,
                })
            }
            IndexableFilterExpr::<T>::Eq(col, EqualityOperator::Eq, value) => {
                if col.column.name != self.indexed_column.as_ref() {
                    plan_err!("unsupported column for blind_index: {}", col.column.name)?;
                }

                let blind_index_gen =
                    key_manager.get_identifier_generator(&self.identifier_context(table_reference));
                let bi_value = self.value_to_blind_value(blind_index_gen.as_ref(), &value);

                Ok(ast::Expr::BinaryOp {
                    left: Box::new(ast::Expr::CompoundIdentifier(vec![
                        Ident::new(table_reference.table.as_ref()),
                        Ident::new(self.index_column_name.as_ref()),
                    ])),
                    right: Box::new(bi_value),
                    op: BinaryOperator::Eq,
                })
            }
            other => plan_err!("unsupported filter for blind_index: {other:?}"),
        }
    }
}

#[async_trait]
impl EncryptedIndex for BlindIndex {
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn tracked_columns(&self) -> FxHashSet<ColumnName> {
        let mut columns = FxHashSet::with_capacity_and_hasher(1, FxBuildHasher::default());
        columns.insert(self.indexed_column.as_ref().into());
        columns
    }

    fn logical_support_options<'s, 'b>(&'s self) -> SupportOptions<'s, &'b Expr> {
        SupportOptions {
            supports_not: false,
            supports_or: true,
            is_supported: Box::new(|v, s| self.is_indexable_expr_supported(v, s)),
        }
    }

    fn logical_query(
        self: Arc<Self>,
        table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        key_manager: &Arc<LongTermKeyManager>,
        filter: IndexableLogicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        Ok(IndexQueryStrategy::Fixed(self.indexable_query_to_expr(
            filter,
            table_reference,
            key_manager,
        )?))
    }

    fn physical_support_options<'s, 'b>(
        &'s self,
    ) -> Option<SupportOptions<'s, &'b dyn PhysicalExpr>> {
        Some(SupportOptions {
            supports_not: false,
            supports_or: true,
            is_supported: Box::new(|v, s| self.is_indexable_expr_supported(v, s)),
        })
    }

    fn physical_query(
        self: Arc<Self>,
        table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        key_manager: &Arc<LongTermKeyManager>,
        filter: IndexablePhysicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        Ok(IndexQueryStrategy::Fixed(self.indexable_query_to_expr(
            filter,
            table_reference,
            key_manager,
        )?))
    }

    fn requires_external_storage(&self) -> bool {
        false
    }

    async fn create_index_plan(
        self: Arc<Self>,
        parent_table_ref: &ResolvedTableReference,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let plan =
            CreateColumnarIndexPlan::blind_index(parent_table_ref, session_state, self).await?;

        Ok(Arc::new(plan))
    }

    fn insert(
        self: Arc<Self>,
        table: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        if schema.column_with_name(&self.indexed_column).is_none() {
            // Tracked column is absent, do nothing
            return Ok(IndexInsertStrategy::AddColumns(vec![]));
        }

        let blind_index_gen =
            key_manager.get_identifier_generator(&self.identifier_context(table.table_reference()));

        let sink = BlindIndexExpr {
            source_column: Arc::new(ToBinaryExpr::new(Arc::new(
                physical_expr::expressions::Column::new_with_schema(
                    &self.indexed_column,
                    schema.as_ref(),
                )?,
            ))),
            size_bits: self.size_bits,
            generator: blind_index_gen.into(),
        };

        let project = ProjectionExpr::new(Arc::new(sink), self.index_column_name.as_ref());

        Ok(IndexInsertStrategy::AddColumns(vec![project]))
    }

    fn is_column_hidden(&self, column: &ColumnName) -> bool {
        column == self.index_column_name.as_ref()
    }

    fn to_config(&self) -> EncryptedIndexConfigurationVariant {
        EncryptedIndexConfigurationVariant::BlindIndex(BlindIndexConfig {
            index_name: self.name.as_ref().into(),
            column: self.indexed_column.as_ref().into(),
            size_bits: self.size_bits,
        })
    }
}

struct BlindIndexExpr {
    generator: Arc<dyn StableIdentifiersGenerator>,
    size_bits: usize,
    source_column: Arc<dyn PhysicalExpr>,
}

impl Debug for BlindIndexExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlindIndex({:?})", self.source_column)
    }
}

impl Display for BlindIndexExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlindIndex({})", self.source_column)
    }
}

impl PartialEq for BlindIndexExpr {
    fn eq(&self, other: &Self) -> bool {
        other.source_column.eq(&self.source_column)
    }
}

impl Eq for BlindIndexExpr {}

impl Hash for BlindIndexExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_column.hash(state);
    }
}

impl BlindIndexExpr {
    fn for_array(&self, arr: &dyn Array) -> datafusion::common::Result<ArrayRef> {
        let arr: &GenericByteArray<GenericBinaryType<i32>> = arr.as_binary();
        let iter = arr.into_iter().map(|v| {
            v.map(|v| {
                self.generator
                    .get_opaque_stable_identifier_hex(v, self.size_bits)
            })
        });

        let column = GenericByteArray::<GenericBinaryType<i32>>::from_iter(iter);
        Ok(Arc::new(column))
    }
}

impl PhysicalExpr for BlindIndexExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::common::Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let parent = self.source_column.evaluate(batch)?;
        match parent {
            ColumnarValue::Array(array) => {
                let column = self.for_array(&array)?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = self.for_array(&array)?;
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source_column]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.is_empty() || children.len() > 1 {
            Err(DataFusionError::Plan(
                "Invalid number of children".to_string(),
            ))
        } else {
            Ok(Arc::new(BlindIndexExpr {
                source_column: children.pop().unwrap(),
                generator: self.generator.clone(),
                size_bits: self.size_bits,
            }))
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("blind_index(")?;
        self.source_column.fmt_sql(_f)?;
        _f.write_str(")")
    }
}
