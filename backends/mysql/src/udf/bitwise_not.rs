use datafusion::arrow::array::{AsArray, GenericByteArray, PrimitiveArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{
    GenericBinaryType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use std::any::Any;
use std::ops::Not;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitwiseNotUdf {
    signature: Signature,
}

impl Default for BitwiseNotUdf {
    fn default() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Coercible(vec![Coercion::Exact {
                        desired_type: TypeSignatureClass::Binary,
                    }]),
                    TypeSignature::Coercible(vec![Coercion::Exact {
                        desired_type: TypeSignatureClass::Integer,
                    }]),
                ]),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
        }
    }
}

macro_rules! impl_bitwise_not_for_integer {
    ($arg: expr, $arrow_type: ty, $variant_name: tt) => {
        match $arg {
            ColumnarValue::Scalar(ScalarValue::$variant_name(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::$variant_name(v.map(Not::not)),
            )),
            ColumnarValue::Scalar(other) => {
                exec_err!(
                    "Scalar value type `{}` does not match with expected type `{}`!",
                    other.data_type(),
                    stringify!($variant_name)
                )
            }
            ColumnarValue::Array(arr) => {
                let array = arr.as_primitive::<$arrow_type>();
                let rewritten = array.iter().map(|v| v.map(Not::not));
                let rewritten = PrimitiveArray::<$arrow_type>::from_iter(rewritten);
                Ok(ColumnarValue::Array(Arc::new(rewritten)))
            }
        }
    };
}

impl ScalarUDFImpl for BitwiseNotUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitwise_not"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        mut args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arg = args.args.remove(0);

        match arg.data_type() {
            DataType::Int8 => impl_bitwise_not_for_integer!(arg, Int8Type, Int8),
            DataType::UInt8 => impl_bitwise_not_for_integer!(arg, UInt8Type, UInt8),
            DataType::Int16 => impl_bitwise_not_for_integer!(arg, Int16Type, Int16),
            DataType::UInt16 => impl_bitwise_not_for_integer!(arg, UInt16Type, UInt16),
            DataType::Int32 => impl_bitwise_not_for_integer!(arg, Int32Type, Int32),
            DataType::UInt32 => impl_bitwise_not_for_integer!(arg, UInt32Type, UInt32),
            DataType::Int64 => impl_bitwise_not_for_integer!(arg, Int64Type, Int64),
            DataType::UInt64 => impl_bitwise_not_for_integer!(arg, UInt64Type, UInt64),

            DataType::Binary => match arg {
                ColumnarValue::Scalar(ScalarValue::Binary(v)) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Binary(v.map(|v| v.iter().map(Not::not).collect())),
                )),
                ColumnarValue::Scalar(other) => {
                    exec_err!(
                        "Scalar value type `{}` does not match with expected type `{}`!",
                        other.data_type(),
                        stringify!($variant_name)
                    )
                }
                ColumnarValue::Array(arr) => {
                    let array = arr.as_bytes::<GenericBinaryType<i32>>();
                    let rewritten = array
                        .iter()
                        .map(|v| v.map(|bytes| bytes.iter().map(Not::not).collect::<Vec<_>>()));
                    let rewritten =
                        GenericByteArray::<GenericBinaryType<i32>>::from_iter(rewritten);
                    Ok(ColumnarValue::Array(Arc::new(rewritten)))
                }
            },
            other => exec_err!("Unsupported datatype: {other:?}"),
        }
    }
}
