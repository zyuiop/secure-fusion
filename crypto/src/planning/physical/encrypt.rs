use crate::arrow::IntermediateBinaryArrayType;
use crate::cipher::Cipher;
use datafusion::arrow::array::{ArrayRef, AsArray, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct EncryptExpr {
    encrypt_child: Arc<dyn PhysicalExpr>, // We could optionally take a second child for AAD
    aad_source_child: Arc<dyn PhysicalExpr>,
    cipher: Arc<dyn Cipher>,
}

impl Debug for EncryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EncryptExpr{{children: {:?}, {:?}}}",
            self.encrypt_child, self.aad_source_child
        )
    }
}

impl EncryptExpr {
    pub fn new(
        encrypt_child: Arc<dyn PhysicalExpr>,
        aad_source_child: Arc<dyn PhysicalExpr>,
        cipher: Arc<dyn Cipher>,
    ) -> Self {
        Self {
            encrypt_child,
            aad_source_child,
            cipher,
        }
    }
}

impl PartialEq for EncryptExpr {
    fn eq(&self, other: &Self) -> bool {
        &self.encrypt_child == &other.encrypt_child
            && &self.aad_source_child == &other.aad_source_child
    }
}

impl Eq for EncryptExpr {}

impl Display for EncryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EncryptExpr({}, {})",
            self.encrypt_child, self.aad_source_child
        )
    }
}

impl Hash for EncryptExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encrypt_child.hash(state);
        self.aad_source_child.hash(state);
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
        self.encrypt_child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let parent = self.encrypt_child.evaluate(batch)?;
        let aad = self
            .aad_source_child
            .evaluate(batch)?
            .to_array_of_size(batch.num_rows())?;

        match parent {
            ColumnarValue::Array(array) => {
                let column = encrypt_array(&self.cipher, array.as_binary(), aad.as_binary());
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = encrypt_array(&self.cipher, array.as_binary(), aad.as_binary());
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.encrypt_child, &self.aad_source_child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 {
            Err(DataFusionError::Plan(
                "Invalid number of children".to_string(),
            ))
        } else {
            let mut new_me = Arc::unwrap_or_clone(self);
            new_me.aad_source_child = children.pop().unwrap();
            new_me.encrypt_child = children.pop().unwrap();
            Ok(Arc::new(new_me))
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("encrypt(")?;
        self.encrypt_child.fmt_sql(_f)?;
        _f.write_str(", ")?;
        self.aad_source_child.fmt_sql(_f)?;
        _f.write_str(")")
    }

    fn is_volatile_node(&self) -> bool {
        false
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
pub fn encrypt_array(
    cipher: &Arc<dyn Cipher>,
    bin_column: &BinaryArray,
    aad_column: &BinaryArray,
) -> ArrayRef {
    let iterator = bin_column.into_iter().zip(aad_column.into_iter());

    let iterator = iterator.map(|(entry, aad)| {
        if let Some(bytes) = entry {
            if let Some(aad) = aad {
                cipher.encrypt_with_random_nonce(bytes, aad).ok()
            } else {
                None
            }
        } else {
            None
        }
    });

    Arc::new(IntermediateBinaryArrayType::from_iter(iterator))
}
