use crate::arrow::IntermediateBinaryArrayType;
use crate::cipher::{AssociatedData, Cipher};
use datafusion::arrow::array::{ArrayRef, AsArray, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::statistics::Distribution;
use datafusion::physical_expr::PhysicalExpr;
use rand_core::{OsRng, TryRngCore};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub struct EncryptExpr {
    token: u128, // Random value that identifies this object

    child: Arc<dyn PhysicalExpr>, // We could optionally take a second child for AAD
    cipher: Arc<dyn Cipher>,
    associated_data: AssociatedData,
}

impl Debug for EncryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EncryptExpr{{token: {}, child: {:?}, aad: {:?}}}",
            self.token, self.child, self.associated_data
        )
    }
}

impl EncryptExpr {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        cipher: Arc<dyn Cipher>,
        associated_data: AssociatedData,
    ) -> Self {
        let mut bytes = [0u8; size_of::<u128>()];
        OsRng.try_fill_bytes(&mut bytes).unwrap();
        let token = u128::from_le_bytes(bytes);

        Self {
            token,
            child,
            cipher,
            associated_data,
        }
    }
}

impl PartialEq for EncryptExpr {
    fn eq(&self, other: &Self) -> bool {
        other.token == self.token
    }
}

impl Eq for EncryptExpr {}

impl Display for EncryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptExpr({})", self.child)
    }
}

impl Hash for EncryptExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u128(self.token);
    }
}

impl PhysicalExpr for EncryptExpr {
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
        match parent {
            ColumnarValue::Array(array) => {
                let column = self.encrypt_column(array.as_binary());
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = self.encrypt_column(array.as_binary());
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
            match Arc::try_unwrap(self) {
                Ok(EncryptExpr {
                    token,
                    child: _,
                    associated_data,
                    cipher,
                }) => Ok(Arc::new(EncryptExpr {
                    token,
                    associated_data,
                    cipher,
                    child: children.pop().unwrap(),
                })),
                Err(this) => Ok(Arc::new(Self::new(
                    children.pop().unwrap(),
                    this.cipher.clone(),
                    this.associated_data.clone(),
                ))),
            }
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("encrypt(")?;
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

impl EncryptExpr {
    fn encrypt_column(&self, bin_column: &BinaryArray) -> ArrayRef {
        let iterator = bin_column.into_iter().map(|entry| {
            if let Some(bytes) = entry {
                self.cipher
                    .encrypt_with_random_nonce(bytes, &self.associated_data)
                    .ok()
            } else {
                None
            }
        });

        Arc::new(IntermediateBinaryArrayType::from_iter(iterator))
    }
}
