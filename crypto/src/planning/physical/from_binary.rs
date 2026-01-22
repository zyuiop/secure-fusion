use crate::arrow::cast_from_binary;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ScalarValue, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::statistics::Distribution;
use datafusion::physical_plan::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
pub struct FromBinaryExpr {
    target_type: DataType,
    target_nullable: bool,
    child: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FromBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.target_type == other.target_type
            && self.child.eq(&other.child)
            && self.target_nullable == other.target_nullable
    }
}

impl Hash for FromBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target_type.hash(state);
        self.child.hash(state);
        self.target_nullable.hash(state);
    }
}

impl Eq for FromBinaryExpr {}

impl Display for FromBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FromBinary[{}]({})", self.target_type, self.child)
    }
}

impl FromBinaryExpr {
    pub fn new(target_type: DataType, target_nullable: bool, child: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            target_type,
            target_nullable,
            child,
        }
    }
}

impl PhysicalExpr for FromBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(
        &self,
        input_schema: &Schema,
    ) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
        let parent_type = self.child.data_type(input_schema)?;
        if parent_type == DataType::Binary {
            Ok(self.target_type.clone())
        } else {
            exec_err!("from_binary: expected binary input, got {parent_type:?}")?
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::common::Result<bool> {
        Ok(self.target_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let parent = self.child.evaluate(batch)?;
        if parent.data_type() == self.target_type {
            return Ok(parent);
        }

        match parent {
            ColumnarValue::Array(array) => {
                let column = cast_from_binary(array.as_binary(), &self.target_type)?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = cast_from_binary(array.as_binary(), &self.target_type)?;
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
            Ok(Arc::new(FromBinaryExpr {
                child: children.pop().unwrap(),
                target_type: self.target_type.clone(),
                target_nullable: self.target_nullable,
            }))
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("from_binary(")?;
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
