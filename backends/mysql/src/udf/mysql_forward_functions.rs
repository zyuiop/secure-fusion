use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rustc_hash::FxHashSet;
use std::any::Any;
use std::sync::LazyLock;

pub static FORWARDED_FUNCTIONS: LazyLock<Vec<MySqlForwardedFunction>> =
    LazyLock::new(init_forwarded_functions);
pub static FORWARDED_FUNCTIONS_NAMES: LazyLock<FxHashSet<String>> =
    LazyLock::new(init_forwarded_functions_names);

fn init_forwarded_functions() -> Vec<MySqlForwardedFunction> {
    vec![MySqlForwardedFunction::new(
        vec!["last_insert_id".to_string()],
        DataType::Int64,
        TypeSignature::Nullary,
    )]
}

fn init_forwarded_functions_names() -> FxHashSet<String> {
    FORWARDED_FUNCTIONS
        .iter()
        .flat_map(|function| function.names.iter())
        .cloned()
        .collect()
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct MySqlForwardedFunction {
    output_type: DataType,
    signature: Signature,
    names: Vec<String>,
}

impl MySqlForwardedFunction {
    pub fn new(names: Vec<String>, output_type: DataType, signature: TypeSignature) -> Self {
        MySqlForwardedFunction {
            output_type,
            signature: Signature {
                parameter_names: None,
                volatility: Volatility::Stable,
                type_signature: signature,
            },
            names,
        }
    }
}

impl ScalarUDFImpl for MySqlForwardedFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.names[0]
    }

    fn aliases(&self) -> &[String] {
        self.names.as_slice()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("MYSQL_FORWARDED_FUNCTIONS should never be called in proxy!")
    }
}
