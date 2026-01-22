use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatabaseUdf;

impl ScalarUDFImpl for DatabaseUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "database"
    }

    fn signature(&self) -> &Signature {
        &Signature {
            type_signature: TypeSignature::Nullary,
            volatility: Volatility::Immutable,
            parameter_names: None,
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            args.config_options.catalog.default_schema.clone(),
        ))))
    }
}
