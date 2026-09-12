use crate::MySqlTableProvider;
use crate::ast_expr_ext::AstExprExt;
use crate::filtering::indexable_filter::{
    EqualityOperator, IndexSelectivity, IndexableFilterExpr, SupportOptions,
};
use crate::filtering::logical::IndexableLogicalExpr;
use crate::filtering::physical::IndexablePhysicalExpr;
use crate::metadata::{
    ColumnName, EncryptedIndex, EncryptedIndexConfigurationVariant, IndexConfig,
    IndexInsertStrategy, IndexQueryStrategy,
};
use crate::planning::physical::plans::create_columnar_index_plan::CreateColumnarIndexPlan;
use crate::providers::table_provider::TableStatistics;
use async_trait::async_trait;
use crypto::LongTermKeyManager;
use crypto::row_id::RowIdColumn;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, NullArray, RecordBatch, UInt32Builder};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalUnit, Schema, SchemaRef, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::common::{
    ResolvedTableReference, ScalarValue, exec_err, plan_datafusion_err, plan_err,
};
use datafusion::execution::SessionState;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::Ident;
use datafusion::logical_expr::{ColumnarValue, Expr};
use datafusion::physical_expr;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use rustc_hash::{FxBuildHasher, FxHashSet};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ValueDistribution {
    Uniform { min: i64, interval: i64 },
    // Normal, ...
}

impl ValueDistribution {
    fn value_to_indexable(&self, source_value: i64) -> u32 {
        match self {
            ValueDistribution::Uniform { min, interval } => {
                if source_value <= *min {
                    0
                } else {
                    ((source_value - *min) / interval) as u32
                }
            }
        }
    }
}

impl RangeIndexValues {
    fn value_to_indexable(&self, source_value: i64) -> u32 {
        // TODO: handle obfuscation strategy
        self.distribution.value_to_indexable(source_value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ObfuscationStrategy {
    NoObfuscation,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RangeIndexValues {
    pub distribution: ValueDistribution,
    pub obfuscation_strategy: ObfuscationStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeIndex {
    pub cfg: RangeIndexValues,
    pub name: Arc<str>,
    pub indexed_column: Arc<str>,
    pub index_column_name: Arc<str>,
}

impl IndexConfig for RangeIndex {
    fn into_index(
        self,
        _table_name: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
    ) -> Arc<dyn EncryptedIndex> {
        Arc::new(self)
    }
}

impl RangeIndex {
    pub fn new(
        name: &str,
        column: &str,
        distribution: ValueDistribution,
        obfuscation_strategy: ObfuscationStrategy,
    ) -> Self {
        let mut index_column_name = format!("prox_rg_{name}");
        index_column_name.truncate(32);

        Self {
            name: name.into(),
            indexed_column: column.into(),
            cfg: RangeIndexValues {
                obfuscation_strategy,
                distribution,
            },
            index_column_name: index_column_name.into(),
        }
    }

    #[inline(always)]
    fn is_indexable_expr_supported<E>(
        &self,
        logical: &IndexableFilterExpr<E>,
        table_stats: &TableStatistics,
    ) -> Option<IndexSelectivity> {
        match logical {
            IndexableFilterExpr::Between(c, l, r)
                if c.column.name == self.indexed_column.as_ref() =>
            {
                let low = self.for_value(l)?;
                let high = self.for_value(r)?;

                let Some(selectivity) =
                    table_stats.column_selectivity(self.index_column_name.as_ref())
                else {
                    log::warn!("Could not resolve selectivity of index {}", self.name);
                    return Some(IndexSelectivity::Absent);
                };

                let num_values = (high - low) as f64;
                let mut query_selectivity = selectivity * num_values;
                if query_selectivity >= 0.14 {
                    // It looks like MySQL does not like queries that scan more than 15% of the table, and will use a full scan in these cases
                    // TODO: determine better rules for index scans
                    query_selectivity *= 2.0;
                }

                Some(
                    table_stats
                        .num_rows()
                        .with_estimated_selectivity(query_selectivity.min(1.0)),
                )
            }
            IndexableFilterExpr::Eq(
                c,
                op @ (EqualityOperator::Eq
                | EqualityOperator::LtEq
                | EqualityOperator::GtEq
                | EqualityOperator::Lt
                | EqualityOperator::Gt),
                other,
            ) if c.column.name == self.indexed_column.as_ref()
                && self.for_value(other).is_some() =>
            {
                Some(if op == &EqualityOperator::Eq {
                    let Some(column_stat) = table_stats
                        .column_stats(self.index_column_name.as_ref())
                        .and_then(|v| v.get_value().copied())
                    else {
                        log::warn!("Could not resolve selectivity of index {}", self.name);
                        return Some(IndexSelectivity::Absent);
                    };

                    table_stats
                        .num_rows()
                        .with_estimated_selectivity(1f64 / column_stat as f64)
                } else {
                    // We cannot determine index selectivity because we don't know which part of the index is actually covered in the database
                    IndexSelectivity::Absent
                })
            }

            _ => None,
        }
    }

    fn indexable_query_to_expr<T: Debug + Clone>(
        &self,
        filter: IndexableFilterExpr<T>,
        table_reference: &ResolvedTableReference,
    ) -> datafusion::common::Result<ast::Expr> {
        match filter {
            IndexableFilterExpr::<T>::And(l, r) => Ok(self
                .indexable_query_to_expr(*l, table_reference)?
                .and(self.indexable_query_to_expr(*r, table_reference)?)),
            IndexableFilterExpr::<T>::Or(l, r) => Ok(self
                .indexable_query_to_expr(*l, table_reference)?
                .or(self.indexable_query_to_expr(*r, table_reference)?)),
            IndexableFilterExpr::Between(col, low, high) => {
                if col.column.name != self.indexed_column.as_ref() {
                    plan_err!("unsupported column for blind_index: {}", col.column.name)?;
                }

                let low = self
                    .for_value(&low)
                    .ok_or_else(|| plan_datafusion_err!("invalid null value in filter"))?
                    .to_string();
                let high = self
                    .for_value(&high)
                    .ok_or_else(|| plan_datafusion_err!("invalid null value in filter"))?
                    .to_string();

                Ok(ast::Expr::Between {
                    expr: Box::new(ast::Expr::CompoundIdentifier(vec![
                        Ident::new(table_reference.table.as_ref()),
                        Ident::new(self.index_column_name.as_ref()),
                    ])),
                    negated: false,
                    low: Box::new(ast::Expr::Value(
                        ast::Value::Number(low, false).with_empty_span(),
                    )),
                    high: Box::new(ast::Expr::Value(
                        ast::Value::Number(high, false).with_empty_span(),
                    )),
                })
            }
            IndexableFilterExpr::<T>::Eq(col, op, value) => {
                if col.column.name != self.indexed_column.as_ref() {
                    plan_err!("unsupported column for blind_index: {}", col.column.name)?;
                }

                let value = self
                    .for_value(&value)
                    .ok_or_else(|| plan_datafusion_err!("invalid null value in filter"))?
                    .to_string();

                Ok(ast::Expr::BinaryOp {
                    left: Box::new(ast::Expr::CompoundIdentifier(vec![
                        Ident::new(table_reference.table.as_ref()),
                        Ident::new(self.index_column_name.as_ref()),
                    ])),
                    right: Box::new(ast::Expr::Value(
                        ast::Value::Number(value, false).with_empty_span(),
                    )),
                    op: op.into(),
                })
            }
            other => plan_err!("unsupported filter for blind_index: {other:?}"),
        }
    }
}

#[async_trait]
impl EncryptedIndex for RangeIndex {
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
            supports_not: true,
            supports_or: true,
            is_supported: Box::new(|v, s| self.is_indexable_expr_supported(v, s)),
        }
    }

    fn logical_query(
        self: Arc<Self>,
        table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        _key_manager: &Arc<LongTermKeyManager>,
        filter: IndexableLogicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        Ok(IndexQueryStrategy::Fixed(self.indexable_query_to_expr(
            filter.optimize_cmp(),
            table_reference,
        )?))
    }

    fn physical_support_options<'s, 'b>(
        &'s self,
    ) -> Option<SupportOptions<'s, &'b dyn PhysicalExpr>> {
        Some(SupportOptions {
            supports_not: true,
            supports_or: true,
            is_supported: Box::new(|v, stats| self.is_indexable_expr_supported(v, stats)),
        })
    }

    fn physical_query(
        self: Arc<Self>,
        table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        _key_manager: &Arc<LongTermKeyManager>,
        filter: IndexablePhysicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        Ok(IndexQueryStrategy::Fixed(self.indexable_query_to_expr(
            filter.optimize_cmp(),
            table_reference,
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
            CreateColumnarIndexPlan::range_index(parent_table_ref, session_state, self).await?;
        Ok(Arc::new(plan))
    }

    fn insert(
        self: Arc<Self>,
        _: &MySqlTableProvider,
        _: Arc<LongTermKeyManager>, // TODO: for obfuscated variant we will need those
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        if schema.column_with_name(&self.indexed_column).is_none() {
            // Tracked column is absent, do nothing
            return Ok(IndexInsertStrategy::AddColumns(vec![]));
        }

        let sink = RangeIndexExpr {
            source_expr: Arc::new(physical_expr::expressions::Column::new_with_schema(
                &self.indexed_column,
                schema.as_ref(),
            )?),
            cfg: self.cfg,
        };

        let project = ProjectionExpr::new(Arc::new(sink), self.index_column_name.as_ref());
        Ok(IndexInsertStrategy::AddColumns(vec![project]))
    }

    fn is_column_hidden(&self, _column: &ColumnName) -> bool {
        _column == self.index_column_name.as_ref()
    }

    fn to_config(&self) -> EncryptedIndexConfigurationVariant {
        EncryptedIndexConfigurationVariant::RangeIndex(self.clone())
    }
}

#[derive(Debug, Clone, Eq, Hash)]
struct RangeIndexExpr {
    cfg: RangeIndexValues,
    source_expr: Arc<dyn PhysicalExpr>,
}

impl PartialEq for RangeIndexExpr {
    fn eq(&self, other: &Self) -> bool {
        other.cfg == self.cfg && other.source_expr.as_ref() == self.source_expr.as_ref()
    }
}

macro_rules! range_eval_array {
    ($cfg: expr, $source_array: expr, ($name: ident) => $transform: tt) => {{
        let array = $source_array;
        let mut output = UInt32Builder::with_capacity(array.len());
        for v in array {
            if let Some($name) = v {
                let transformed: i64 = $transform;
                output.append_value($cfg.value_to_indexable(transformed));
            } else {
                output.append_null()
            }
        }
        Ok(Arc::new(output.finish()))
    }};
}

#[allow(unused)]
fn supports_datatype(dt: &DataType) -> bool {
    dt.is_null()
        || dt.is_integer()
        || (dt.is_temporal() && !matches!(dt, DataType::Interval(_) | DataType::Duration(_)))
}

impl ValueDistribution {
    pub fn parse(
        data_type: &DataType,
        source: &str,
    ) -> datafusion::common::Result<ValueDistribution> {
        // TODO: support prefix for other distributions
        let [min, interval]: [&str; 2] = source
            .splitn(2, '_')
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        let (min, interval) = match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => (
                min.parse::<i64>()
                    .map_err(|_| plan_datafusion_err!("failed to parse {min} as an integer"))?,
                interval.parse::<i64>().map_err(|_| {
                    plan_datafusion_err!("failed to parse {interval} as an integer")
                })?,
            ),
            DataType::UInt64 => {
                let min = min
                    .parse::<u64>()
                    .map_err(|_| plan_datafusion_err!("failed to parse {min} as an integer"))?;
                (
                    (min >> 1) as i64,
                    interval.parse::<i64>().map_err(|_| {
                        plan_datafusion_err!("failed to parse {interval} as an integer")
                    })?,
                )
            }
            dt @ (DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64) => {
                let ScalarValue::Int64(Some(min)) = ScalarValue::Utf8(Some(
                    min.replace("spc", " ")
                        .replace("dash", "-")
                        .replace("col", ":"),
                ))
                .cast_to(dt)?
                .cast_to(&DataType::Int64)?
                else {
                    unreachable!()
                };

                let ScalarValue::IntervalDayTime(Some(interval)) = ScalarValue::Utf8(Some(
                    interval
                        .replace("spc", " ")
                        .replace("dash", "-")
                        .replace("col", ":"),
                ))
                .cast_to(&DataType::Interval(IntervalUnit::DayTime))?
                else {
                    unreachable!()
                };

                let interval = if matches!(dt, DataType::Date32 | DataType::Date64) {
                    interval.days as i64
                } else {
                    let DataType::Timestamp(tu, _) = dt else {
                        unreachable!()
                    };
                    match tu {
                        TimeUnit::Second => {
                            (interval.days as i64) * (24 * 60 * 60)
                                + (interval.milliseconds / 1000) as i64
                        }
                        TimeUnit::Millisecond => {
                            (interval.days as i64) * (24 * 60 * 60 * 1000)
                                + interval.milliseconds as i64
                        }
                        TimeUnit::Microsecond => {
                            (interval.days as i64) * (24 * 60 * 60 * 1000 * 1000)
                                + (interval.milliseconds as i64) * 1000
                        }
                        TimeUnit::Nanosecond => {
                            (interval.days as i64) * (24 * 60 * 60 * 1000 * 1000 * 1000)
                                + (interval.milliseconds as i64) * 1000 * 1000
                        }
                    }
                };

                (min, interval)
            }
            dt @ (DataType::Time32(tu) | DataType::Time64(tu)) => {
                let ScalarValue::Int64(Some(min)) = ScalarValue::Utf8(Some(
                    min.replace("spc", " ")
                        .replace("dash", "-")
                        .replace("col", ":"),
                ))
                .cast_to(dt)?
                .cast_to(&DataType::Int64)?
                else {
                    unreachable!()
                };

                let ScalarValue::Int64(Some(interval)) =
                    ScalarValue::Utf8(Some(interval.replace("spc", " ")))
                        .cast_to(&DataType::Duration(*tu))?
                        .cast_to(&DataType::Int64)?
                else {
                    unreachable!()
                };

                (min, interval)
            }
            dt @ (DataType::Decimal32(_, _) | DataType::Decimal64(_, _)) => {
                let ScalarValue::Int64(Some(min)) =
                    ScalarValue::Utf8(Some(min.replace("dot", ".")))
                        .cast_to(dt)?
                        .cast_to(&DataType::Int64)?
                else {
                    unreachable!()
                };

                let ScalarValue::Int64(Some(interval)) =
                    ScalarValue::Utf8(Some(interval.replace("dot", ".")))
                        .cast_to(dt)?
                        .cast_to(&DataType::Int64)?
                else {
                    unreachable!()
                };

                (min, interval)
            }
            other => plan_err!("unsupported datatype for range index: {other}")?,
        };

        Ok(ValueDistribution::Uniform { min, interval })
    }
}

impl RangeIndex {
    #[inline]
    fn for_value(&self, value: &ScalarValue) -> Option<u32> {
        match value {
            ScalarValue::Decimal32(v, _, _)
            | ScalarValue::Int32(v)
            | ScalarValue::Date32(v)
            | ScalarValue::Time32Second(v)
            | ScalarValue::Time32Millisecond(v) => v.map(|v| v as i64),
            ScalarValue::TimestampMillisecond(v, _)
            | ScalarValue::TimestampMicrosecond(v, _)
            | ScalarValue::TimestampNanosecond(v, _)
            | ScalarValue::Time64Nanosecond(v)
            | ScalarValue::TimestampSecond(v, _)
            | ScalarValue::Decimal64(v, _, _)
            | ScalarValue::Int64(v)
            | ScalarValue::Date64(v)
            | ScalarValue::Time64Microsecond(v) => *v,
            ScalarValue::Int8(v) => v.map(|v| v as i64),
            ScalarValue::Int16(v) => v.map(|v| v as i64),
            ScalarValue::UInt8(v) => v.map(|v| v as i64),
            ScalarValue::UInt16(v) => v.map(|v| v as i64),
            ScalarValue::UInt32(v) => v.map(|v| v as i64),
            ScalarValue::UInt64(v) => v.map(|v| (v >> 1) as i64),
            _ => None,
        }
        .map(|v| self.cfg.value_to_indexable(v))
    }
}

impl RangeIndexExpr {
    fn for_array(&self, arr: ArrayRef) -> datafusion::common::Result<ArrayRef> {
        match arr.data_type() {
            DataType::Null => Ok(Arc::new(NullArray::new(arr.len()))),
            DataType::Int8 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Int8Type>(), (value) => { value as i64 })
            }
            DataType::Int16 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Int16Type>(), (value) => { value as i64 })
            }
            DataType::Int32 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Int32Type>(), (value) => { value as i64 })
            }
            DataType::Int64 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Int64Type>(), (value) => value)
            }
            DataType::UInt8 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<UInt8Type>(), (value) => { value as i64 })
            }
            DataType::UInt16 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<UInt16Type>(), (value) => { value as i64 })
            }
            DataType::UInt32 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<UInt32Type>(), (value) => { value as i64 })
            }
            DataType::UInt64 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<UInt64Type>(), (value) => { (value >> 1) as i64 })
            }

            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => {
                    range_eval_array!(&self.cfg, arr.as_primitive::<TimestampSecondType>(), (value) => value)
                }
                TimeUnit::Millisecond => {
                    range_eval_array!(&self.cfg, arr.as_primitive::<TimestampMillisecondType>(), (value) => value)
                }
                TimeUnit::Microsecond => {
                    range_eval_array!(&self.cfg, arr.as_primitive::<TimestampMicrosecondType>(), (value) => value)
                }
                TimeUnit::Nanosecond => {
                    range_eval_array!(&self.cfg, arr.as_primitive::<TimestampNanosecondType>(), (value) => value)
                }
            },
            DataType::Date32 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Date32Type>(), (value) => { value as i64 })
            }
            DataType::Date64 => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Date64Type>(), (value) => value)
            }

            DataType::Time32(TimeUnit::Second) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Time32SecondType>(), (value) => { value as i64 })
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Time32MillisecondType>(), (value) => { value as i64 })
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Time64MicrosecondType>(), (value) => value)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Time64NanosecondType>(), (value) => value)
            }

            DataType::Decimal32(_, _) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Decimal32Type>(), (value) => { value as i64 })
            }
            DataType::Decimal64(_, _) => {
                range_eval_array!(&self.cfg, arr.as_primitive::<Decimal64Type>(), (value) => { value as i64 })
            }

            other => exec_err!("unsupported datatype for range index: {other:?}"),
        }
    }
}

impl Display for RangeIndexExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_sql(f)
    }
}

#[async_trait]
impl PhysicalExpr for RangeIndexExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn nullable(&self, input: &Schema) -> datafusion::common::Result<bool> {
        Ok(input.fields[0].is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let parent = self.source_expr.evaluate(batch)?;
        match parent {
            ColumnarValue::Array(array) => {
                let column = self.for_array(array)?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = self.for_array(array)?;
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            plan_err!("invalid number of children for RangeIndexExpr")?
        };

        Ok(Arc::new(Self {
            source_expr: children[0].clone(),
            cfg: self.cfg,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RangeIndexExpr(")?;
        self.source_expr.fmt_sql(f)?;
        f.write_str(")")
    }
}
