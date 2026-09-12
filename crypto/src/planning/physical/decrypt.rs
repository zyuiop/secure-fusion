use crate::arrow::decode_array_from_binary;
use crate::cipher::Cipher;
use crate::{CipherContext, LongTermKeyManager};
use datafusion::arrow::array::{Array, ArrayIter, ArrayRef, AsArray, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, exec_err, plan_datafusion_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct DecryptExpr {
    decrypt_child: Arc<dyn PhysicalExpr>,
    aad_source_child: Arc<dyn PhysicalExpr>,
    cipher: Arc<dyn Cipher>,
}

impl Debug for DecryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DecryptExpr{{children: {:?}, {:?}}}",
            self.decrypt_child, self.aad_source_child
        )
    }
}

impl DecryptExpr {
    pub fn new(
        decrypt_child: Arc<dyn PhysicalExpr>,
        aad_source_child: Arc<dyn PhysicalExpr>,
        cipher: Arc<dyn Cipher>,
    ) -> Self {
        Self {
            decrypt_child,
            aad_source_child,
            cipher,
        }
    }
}

impl PartialEq for DecryptExpr {
    fn eq(&self, other: &Self) -> bool {
        &self.decrypt_child == &other.decrypt_child
            && &self.aad_source_child == &other.aad_source_child
    }
}

impl Eq for DecryptExpr {}

impl Display for DecryptExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DecryptExpr({}, {})",
            self.decrypt_child, self.aad_source_child
        )
    }
}

impl Hash for DecryptExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.decrypt_child.hash(state);
        self.aad_source_child.hash(state);
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
        self.decrypt_child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let decrypt = self.decrypt_child.evaluate(batch)?;
        let aad = self.aad_source_child.evaluate(batch)?;

        match decrypt {
            ColumnarValue::Array(array) => {
                let column = decrypt_array(&self.cipher, array.as_binary(), aad)?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = decrypt_array(&self.cipher, array.as_binary(), aad)?;
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.decrypt_child, &self.aad_source_child]
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
            new_me.decrypt_child = children.pop().unwrap();
            Ok(Arc::new(new_me))
        }
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("decrypt(")?;
        self.decrypt_child.fmt_sql(_f)?;
        _f.write_str(", ")?;
        self.aad_source_child.fmt_sql(_f)?;
        _f.write_str(")")
    }

    fn is_volatile_node(&self) -> bool {
        false
    }
}

enum AssociatedDataIter<'a> {
    ScalarValue(&'a [u8]),
    BinaryArray(ArrayIter<&'a BinaryArray>),
}

impl<'a> Iterator for AssociatedDataIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            AssociatedDataIter::ScalarValue(v) => Some(Some(*v)),
            AssociatedDataIter::BinaryArray(v) => v.next(),
        }
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
pub fn decrypt_array(
    cipher: &Arc<dyn Cipher>,
    column: &BinaryArray,
    associated_data: ColumnarValue,
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

    let aad_iter = match &associated_data {
        ColumnarValue::Array(arr) => AssociatedDataIter::BinaryArray(arr.as_binary().iter()),
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Binary(Some(v)) => AssociatedDataIter::ScalarValue(v.as_slice()),
            _ => exec_err!("invalid aad type: {}", sv.data_type())?,
        },
    };

    for (v, aad) in column.iter().zip(aad_iter) {
        null_offset_buffer.append(v.is_some());

        let Some(ciphertext) = v else {
            offset_buffer.push_length(0);
            continue;
        };
        let Some(aad) = aad else {
            exec_err!("null AAD encountered in row")?
        };

        let decrypted_bytes =
            cipher.decrypt_with_nonce_to_slice(&mut array[written_bytes..], ciphertext, aad);

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

#[derive(Debug)]
pub struct DecryptUdf {
    signature: Signature,
    cipher_context: CipherContext,
    output_field: FieldRef,
    key_manager: Arc<LongTermKeyManager>,
}

impl DecryptUdf {
    pub const DECRYPT_UDF_NAME: &str = "__internal__decrypt__";

    #[inline(always)]
    pub fn eliminate_decrypt_in_expr(expr: &Expr) -> datafusion::common::Result<&Expr> {
        if let Expr::ScalarFunction(sf) = expr
            && sf.func.name() == Self::DECRYPT_UDF_NAME
        {
            // Strip decryption function as it may prevent filters from being detected
            sf.args.get(0).ok_or(plan_datafusion_err!(
                "encountered decryption function with no argument"
            ))
        } else {
            Ok(expr)
        }
    }

    pub fn eliminate_decrypt_recursively(expr: Expr) -> datafusion::common::Result<Expr> {
        Ok(expr
            .transform_up(|sub_expr| match sub_expr {
                Expr::ScalarFunction(sf) if sf.name() == Self::DECRYPT_UDF_NAME => {
                    let arg = sf.args.get(0).cloned().ok_or(plan_datafusion_err!(
                        "encountered decryption function with no argument"
                    ))?;
                    Ok(Transformed::yes(arg))
                }
                other => Ok(Transformed::no(other)),
            })?
            .data)
    }
}

impl PartialEq for DecryptUdf {
    fn eq(&self, other: &Self) -> bool {
        other.signature == self.signature
            && other.output_field == self.output_field
            && self.cipher_context == self.cipher_context
    }
}

impl Hash for DecryptUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.output_field.hash(state);
        self.cipher_context.hash(state);
    }
}

impl Eq for DecryptUdf {}

impl DecryptUdf {
    pub fn new(
        output_field: FieldRef,
        cipher_context: CipherContext,
        long_term_key_manager: Arc<LongTermKeyManager>,
    ) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
            output_field,
            cipher_context,
            key_manager: long_term_key_manager,
        }
    }

    pub fn invoke(
        output_field: FieldRef,
        cipher_context: CipherContext,
        long_term_key_manager: Arc<LongTermKeyManager>,
        encrypted_column: Expr,
        computed_aad: Expr,
    ) -> Expr {
        ScalarUDF::new_from_impl(Self::new(
            output_field,
            cipher_context,
            long_term_key_manager,
        ))
        .call(vec![encrypted_column, computed_aad])
    }
}

pub(crate) const DECRYPT_PSEUDOFUNC_NAME: &str = DecryptUdf::DECRYPT_UDF_NAME;

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
        mut args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // Note: this implementation provides both `from_binary` and `decrypt` at once.
        let cipher = self.key_manager.get_cipher(&self.cipher_context);

        let aad = args.args.pop().unwrap();

        let enc = args.args.pop().unwrap();

        match enc {
            ColumnarValue::Array(array) => {
                let column = decrypt_array(&cipher, array.as_binary(), aad)?;
                let column =
                    decode_array_from_binary(column.as_binary(), &self.output_field.data_type())?;
                Ok(ColumnarValue::Array(column))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let column = decrypt_array(&cipher, array.as_binary(), aad)?;
                let column =
                    decode_array_from_binary(column.as_binary(), &self.output_field.data_type())?;
                let scalar = ScalarValue::try_from_array(&column, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }
}
