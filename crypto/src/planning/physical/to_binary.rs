use crate::arrow::cast_to_binary;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::statistics::Distribution;
use datafusion::physical_plan::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
pub struct ToBinaryExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl Hash for ToBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PartialEq for ToBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for ToBinaryExpr {}

impl Display for ToBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ToBinary({})", self.child)
    }
}

impl ToBinaryExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl PhysicalExpr for ToBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let parent = self.child.evaluate(batch)?;
        if parent.data_type() == DataType::Binary {
            return Ok(parent);
        }

        match parent {
            ColumnarValue::Array(array) => {
                let column = cast_to_binary(array.as_ref())?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = cast_to_binary(array.as_ref())?;
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
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
            Ok(Arc::new(ToBinaryExpr::new(children.pop().unwrap())))
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("to_binary(")?;
        self.child.fmt_sql(_f)?;
        _f.write_str(")")
    }

    fn is_volatile_node(&self) -> bool {
        self.child.is_volatile_node()
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> datafusion::common::Result<Option<Vec<Interval>>> {
        self.child.propagate_constraints(interval, children)
    }

    fn propagate_statistics(
        &self,
        parent: &Distribution,
        children: &[&Distribution],
    ) -> datafusion::common::Result<Option<Vec<Distribution>>> {
        self.child.propagate_statistics(parent, children)
    }

    fn evaluate_statistics(
        &self,
        children: &[&Distribution],
    ) -> datafusion::common::Result<Distribution> {
        self.child.evaluate_statistics(children)
    }

    fn evaluate_bounds(&self, _children: &[&Interval]) -> datafusion::common::Result<Interval> {
        self.child.evaluate_bounds(_children)
    }
}
