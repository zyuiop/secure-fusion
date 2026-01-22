use crate::cipher::{AssociatedData, Cipher};
use crate::encrypted_column_meta::EncryptedColumnMeta;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::common::{DataFusionError, exec_err};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::statistics::Distribution;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use rand_core::{OsRng, TryRngCore};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub struct DecryptExpr {
    token: u128, // Random value that identifies this object

    child: Arc<dyn PhysicalExpr>, // We could optionally take a second child for AAD
    cipher: Arc<dyn Cipher>,
    associated_data: AssociatedData,
    // TODO: Move to cast
}

impl Debug for DecryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DecryptExpr{{token: {}, child: {:?}, aad: {:?}}}",
            self.token, self.child, self.associated_data
        )
    }
}

impl DecryptExpr {
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

impl PartialEq for DecryptExpr {
    fn eq(&self, other: &Self) -> bool {
        other.token == self.token
    }
}

impl Eq for DecryptExpr {}

impl Display for DecryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DecryptExpr({})", self.child)
    }
}

impl Hash for DecryptExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u128(self.token);
    }
}

impl PhysicalExpr for DecryptExpr {
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
                let column = decrypt_array(
                    self.cipher.as_ref(),
                    array.as_binary(),
                    &self.associated_data,
                )?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = decrypt_array(
                    self.cipher.as_ref(),
                    array.as_binary(),
                    &self.associated_data,
                )?;
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
                Ok(DecryptExpr {
                    token,
                    child: _,
                    associated_data,
                    cipher,
                }) => Ok(Arc::new(DecryptExpr {
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
        _f.write_str("decrypt(")?;
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

#[cfg(not(feature = "decrypt-array-in-place"))]
pub(crate) fn decrypt_array(
    cipher: &dyn Cipher,
    column: &BinaryArray,
    associated_data: &AssociatedData,
) -> datafusion::common::Result<ArrayRef> {
    let nonce_size = cipher.nonce_size();
    let entries = column.len();
    let total_nonce_size = entries * nonce_size;
    let total_decrypted_size = column.get_buffer_memory_size() - total_nonce_size;

    let mut builder = datafusion::arrow::array::GenericByteBuilder::<
        datafusion::arrow::datatypes::BinaryType,
    >::with_capacity(entries, total_decrypted_size);

    for v in column.iter() {
        let Some(ciphertext) = v else {
            builder.append_null();
            continue;
        };

        let decrypted_value = cipher.decrypt_with_nonce(ciphertext, associated_data);

        match decrypted_value {
            Ok(value) => builder.append_value(value),
            Err(_) => exec_err!(
                "Could not decrypt value (corrupted data or bad plan?)! {:?}",
                associated_data
            )?,
        }
    }

    let new_col = builder.finish();
    Ok(Arc::new(new_col))
}

#[cfg(feature = "decrypt-array-in-place")]
pub(crate) fn decrypt_array(
    cipher: &dyn Cipher,
    column: &BinaryArray,
    associated_data: &AssociatedData,
) -> datafusion::common::Result<ArrayRef> {
    use datafusion::arrow::array::{NullBufferBuilder, OffsetBufferBuilder};
    use datafusion::arrow::buffer::MutableBuffer;

    let entries = column.len() - column.null_count();
    let total_extra_size = entries * (cipher.nonce_size() + cipher.tag_size());
    let total_decrypted_size = column.get_buffer_memory_size() - total_extra_size;

    let mut offset_buffer = OffsetBufferBuilder::<i32>::new(entries);
    let mut null_offset_buffer = NullBufferBuilder::new(entries);
    let mut array = MutableBuffer::from_len_zeroed(total_decrypted_size);

    let mut written_bytes = 0usize;

    for v in column.iter() {
        null_offset_buffer.append(v.is_some());

        let Some(ciphertext) = v else {
            offset_buffer.push_length(0);
            continue;
        };

        let decrypted_bytes = cipher.decrypt_with_nonce_to_slice(
            &mut array[written_bytes..],
            ciphertext,
            associated_data,
        );

        match decrypted_bytes {
            Ok(value) => {
                written_bytes += value;
                offset_buffer.push_length(value);
            }
            Err(_) => exec_err!(
                "Could not decrypt value (corrupted data or bad plan?)! {:?}",
                associated_data
            )?,
        }
    }

    array.truncate(written_bytes);
    let new_col = BinaryArray::new(
        offset_buffer.finish(),
        array.into(),
        null_offset_buffer.finish(),
    );
    Ok(Arc::new(new_col))
}

#[derive(Eq, PartialEq, Debug, Hash)]
pub struct DecryptUdf {
    signature: Signature,
    pub(crate) output_field: FieldRef,
    pub(crate) meta: EncryptedColumnMeta,
}

impl DecryptUdf {
    pub fn new(output_field: FieldRef, meta: EncryptedColumnMeta) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
            output_field,
            meta,
        }
    }
}

pub(crate) const DECRYPT_PSEUDOFUNC_NAME: &str = "__internal__decrypt__";

impl ScalarUDFImpl for DecryptUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        DECRYPT_PSEUDOFUNC_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.output_field.data_type().clone())
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(self.output_field.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("decrypt function should have been rewritten!")
    }
}
