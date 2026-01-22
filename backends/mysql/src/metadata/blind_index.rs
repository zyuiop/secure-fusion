use crate::metadata::indices::{expr_to_column, expr_to_value};
use crate::metadata::{
    ColumnName, EncryptedIndex, EncryptedIndexConfigurationVariant, IndexConfig,
    IndexInsertStrategy, IndexQueryStrategy,
};
use crate::planning::physical::plans::create_blind_index_plan::CreateBlindIndexPlan;
use crate::providers::table_provider::{IndexableColumn, MySqlTableProvider};
use async_trait::async_trait;
use crypto::identifiers::StableIdentifiersGenerator;
use crypto::key_manager::KeyManager;
use crypto::{IdentifierContext, LongTermKeyManager};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericByteArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, GenericBinaryType, Schema, SchemaRef};
use datafusion::common::{Column, ResolvedTableReference, ScalarValue, TableReference};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{BinaryExpr, ColumnarValue, Expr, Operator};
use datafusion::physical_expr;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
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
        _table_name: &str,
        _indexable_column: IndexableColumn,
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
    pub name: String,
    pub column: ColumnName,
    pub size_bits: usize,
    pub index_column_name: String,
}

impl BlindIndex {
    fn scalar_to_bytes(scalar_value: &ScalarValue) -> Vec<u8> {
        // TODO: we must use the same strategy to cast as when inserting values
        scalar_value.to_string().into_bytes()
    }

    fn identifier_context<'a>(&'a self, table: &'a TableReference) -> IdentifierContext<'a> {
        IdentifierContext::NamedIndexInTable {
            table: table.table(),
            index_name: &self.name,
        }
    }

    pub fn new(name: String, column: ColumnName, size_bits: usize) -> Self {
        let mut index_column_name = format!("{INDEX_COLUMN_PREFIX}_{name}_{size_bits}");
        index_column_name.truncate(32);

        Self {
            name,
            column,
            size_bits,
            index_column_name,
        }
    }

    fn apply_for_expr(
        &self,
        table_ref: &TableReference,
        key_manager: Arc<LongTermKeyManager>,
        expr: &BinaryExpr,
    ) -> Option<Expr> {
        let column =
            expr_to_column(table_ref, &expr.left).or(expr_to_column(table_ref, &expr.left))?;
        let value = expr_to_value(&expr.left).or(expr_to_value(&expr.right))?;

        if column.name != self.column {
            return None;
        }

        let blind_index_gen =
            key_manager.get_identifier_generator(&self.identifier_context(table_ref));
        let bi_value = blind_index_gen
            .get_opaque_stable_identifier_hex(&Self::scalar_to_bytes(value), self.size_bits);

        Some(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Literal(ScalarValue::Utf8(Some(bi_value)), None)),
            expr.op,
            Box::new(Expr::Column(Column::new(
                Some(table_ref.clone()),
                self.index_column_name.clone(),
            ))),
        )))
    }

    fn apply_for_query(
        &self,
        table_ref: &TableReference,
        key_manager: Arc<LongTermKeyManager>,
        filter: &Expr,
    ) -> Option<Expr> {
        if let Expr::Alias(alias) = filter {
            return self.apply_for_query(table_ref, key_manager, alias.expr.as_ref());
        }

        let Expr::BinaryExpr(binary_expr) = filter else {
            return None;
        };

        if binary_expr.op == Operator::Or || binary_expr.op == Operator::And {
            // Try both
            let left = self.apply_for_query(table_ref, key_manager.clone(), &binary_expr.left);
            let right = self.apply_for_query(table_ref, key_manager, &binary_expr.right);

            return match (left, right) {
                (None, None) => None,
                (Some(left), None) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    binary_expr.op,
                    binary_expr.right.clone(),
                ))),
                (None, Some(right)) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    binary_expr.right.clone(),
                    binary_expr.op,
                    Box::new(right),
                ))),
                (Some(left), Some(right)) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    binary_expr.op,
                    Box::new(right),
                ))),
            };
        }

        if binary_expr.op != Operator::Eq {
            return None;
        }

        self.apply_for_expr(table_ref, key_manager, &binary_expr)
    }
}

#[async_trait]
impl EncryptedIndex for BlindIndex {
    fn supports_expression(&self, filter: &Expr) -> bool {
        if let Expr::Alias(alias) = filter {
            return self.supports_expression(alias.expr.as_ref());
        }

        let Expr::BinaryExpr(binary_expr) = filter else {
            return false;
        };

        if binary_expr.op == Operator::Or || binary_expr.op == Operator::And {
            return self.supports_expression(&binary_expr.left)
                || self.supports_expression(&binary_expr.right);
        }

        if binary_expr.op != Operator::Eq {
            return false;
        }

        let Some(col) = binary_expr
            .left
            .try_as_col()
            .or(binary_expr.right.try_as_col())
        else {
            return false;
        };
        let Some(_) = binary_expr
            .left
            .as_literal()
            .or(binary_expr.right.as_literal())
        else {
            return false;
        };

        col.name == self.column
    }

    fn requires_external_storage(&self) -> bool {
        false
    }

    fn insert(
        self: Arc<Self>,
        table: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        if schema.column_with_name(&self.column).is_none() {
            // Tracked column is absent, do nothing
            return Ok(IndexInsertStrategy::AddColumns(vec![]));
        }

        let blind_index_gen =
            key_manager.get_identifier_generator(&self.identifier_context(&table.table_reference));

        let sink = BlindIndexExpr {
            source_column: Arc::new(physical_expr::expressions::Column::new_with_schema(
                &self.column,
                schema.as_ref(),
            )?),
            size_bits: self.size_bits,
            generator: blind_index_gen.into(),
        };

        let project = ProjectionExpr::new(Arc::new(sink), self.index_column_name.clone());

        Ok(IndexInsertStrategy::AddColumns(vec![project]))
    }

    fn query(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        key_manager: Arc<LongTermKeyManager>,
        filter: &[Expr],
    ) -> datafusion::common::Result<Option<IndexQueryStrategy>> {
        let Some(expr) = filter
            .into_iter()
            .map(|expr| {
                self.apply_for_query(&table_ref.table_reference, key_manager.clone(), &expr)
            })
            .reduce(|left, right| {
                if left.is_none() {
                    return right;
                }

                if right.is_none() {
                    return left;
                }

                Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left.unwrap()),
                    Operator::And,
                    Box::new(right.unwrap()),
                )))
            })
            .flatten()
        else {
            return Ok(None);
        };

        Ok(Some(IndexQueryStrategy::AddFilterExpression(expr)))
    }

    fn is_column_hidden(&self, column: &ColumnName) -> bool {
        column == &self.index_column_name
    }

    fn to_config(&self) -> EncryptedIndexConfigurationVariant {
        EncryptedIndexConfigurationVariant::BlindIndex(BlindIndexConfig {
            index_name: self.name.clone(),
            column: self.column.clone(),
            size_bits: self.size_bits,
        })
    }

    fn tracked_columns(&self) -> FxHashSet<ColumnName> {
        let mut columns = FxHashSet::with_capacity_and_hasher(1, FxBuildHasher::default());
        columns.insert(self.column.clone());
        columns
    }

    async fn create_index_plan(
        &self,
        parent_table_ref: ResolvedTableReference,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let plan =
            CreateBlindIndexPlan::new(parent_table_ref, session_state, Arc::new(self.clone()))
                .await;

        Ok(Arc::new(plan))
    }

    fn linked_table_names(&self) -> Vec<String> {
        vec![]
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
        todo!()
    }
}
