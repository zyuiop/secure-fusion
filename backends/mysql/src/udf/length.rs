use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Int32Array, Int64Array, LargeBinaryArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_datafusion_err;
use datafusion::functions::unicode::character_length::CharacterLengthFunc;
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LengthUdf {
    native: CharacterLengthFunc,
    signature: Signature,
}

impl Default for LengthUdf {
    fn default() -> LengthUdf {
        let native = CharacterLengthFunc::default();
        let native_signature = native.signature().type_signature.clone();

        Self {
            native,
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Uniform(
                        1,
                        vec![
                            DataType::Binary,
                            DataType::LargeBinary,
                            DataType::BinaryView,
                        ],
                    ),
                    native_signature,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl LengthUdf {
    fn should_handle(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
    }
}

impl ScalarUDFImpl for LengthUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.native.name()
    }

    fn aliases(&self) -> &[String] {
        self.native.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        let arg = arg_types.get(0).ok_or_else(|| {
            plan_datafusion_err!("invalid call to function LENGTH, requires one argument")
        })?;
        if Self::should_handle(arg) {
            if arg == &DataType::LargeBinary {
                Ok(DataType::Int64)
            } else {
                Ok(DataType::Int32)
            }
        } else {
            self.native.return_type(arg_types)
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        if Self::should_handle(&args.args[0].data_type()) {
            make_scalar_function(length_funct, vec![])(&args.args)
        } else {
            self.native.invoke_with_args(args)
        }
    }
}

macro_rules! length_func_for {
    ($array: expr, $input_array_type: ty, $native_type: ty, $output_array_type: ty) => {{
        let array = $array.as_any().downcast_ref::<$input_array_type>().unwrap();
        let new_array = array
            .iter()
            .map(|v| v.map(|array| array.len() as $native_type));

        let arr = <$output_array_type>::from_iter(new_array);
        Ok(Arc::new(arr))
    }};
}

fn length_funct(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Binary => length_func_for!(args[0], BinaryArray, i32, Int32Array),
        DataType::BinaryView => length_func_for!(args[0], BinaryViewArray, i32, Int32Array),
        DataType::LargeBinary => length_func_for!(args[0], LargeBinaryArray, i64, Int64Array),
        _ => unreachable!("invalid type for length_funct"),
    }
}
