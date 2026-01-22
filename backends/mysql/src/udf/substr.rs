use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, Int64Array, LargeBinaryArray,
};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::plan_datafusion_err;
use datafusion::functions::unicode::substr::SubstrFunc;
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SubstrUdf {
    native: SubstrFunc,
    signature: Signature,
}

#[derive(Debug)]
pub struct SubstrPlanner {
    substr_func: Arc<ScalarUDF>,
}

impl SubstrPlanner {
    pub fn new(substr_func: Arc<ScalarUDF>) -> Arc<Self> {
        Arc::new(Self { substr_func })
    }
}

impl ExprPlanner for SubstrPlanner {
    fn plan_substring(
        &self,
        args: Vec<Expr>,
    ) -> datafusion::common::Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(self.substr_func.clone(), args),
        )))
    }
}

impl Default for SubstrUdf {
    fn default() -> SubstrUdf {
        let native = SubstrFunc::default();

        assert_eq!(
            native.signature().type_signature,
            TypeSignature::UserDefined
        );

        Self {
            native,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl SubstrUdf {
    fn should_handle(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
    }
}

impl ScalarUDFImpl for SubstrUdf {
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
            plan_datafusion_err!("invalid call to function SUBSTR, requires three arguments")
        })?;

        if Self::should_handle(arg) {
            Ok(arg_types[0].clone())
        } else {
            self.native.return_type(arg_types)
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        if Self::should_handle(&args.args[0].data_type()) {
            make_scalar_function(substr_func, vec![])(&args.args)
        } else {
            self.native.invoke_with_args(args)
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        let arg = arg_types.get(0).ok_or_else(|| {
            plan_datafusion_err!("invalid call to function SUBSTR, requires three arguments")
        })?;

        if Self::should_handle(arg) {
            let first_type = arg.clone();
            let mut base = self.native.coerce_types(&[
                DataType::Utf8,
                arg_types[1].clone(),
                arg_types[2].clone(),
            ])?;
            base[0] = first_type;
            Ok(base)
        } else {
            // self.native.coerce_types(arg_types)
            panic!()
        }
    }
}

macro_rules! substr_func_for {
    ($array: expr, $input_array_type: ty, $start: expr, $count: expr) => {{
        let array = $array.as_any().downcast_ref::<$input_array_type>().unwrap();
        let default_count = Int64Array::new_null(array.len());
        let count = $count.unwrap_or(&default_count);
        let start = $start;

        let new_values = array
            .iter()
            .zip(start)
            .zip(count)
            .map(|((value, start), count)| {
                let start = start.unwrap_or(0);
                let count = count.map(|v| v as usize);

                value.map(|array| {
                    let start = if start > 0 {
                        start as usize
                    } else {
                        array
                            .len()
                            .checked_sub((-start) as usize)
                            .unwrap_or(array.len())
                    };

                    let array = if start < array.len() - 1 {
                        &array[(start + 1)..]
                    } else {
                        &[]
                    };

                    if let Some(count) = count
                        && count < array.len()
                    {
                        &array[..count]
                    } else {
                        array
                    }
                })
            });

        Ok(Arc::new(<$input_array_type>::from_iter(new_values)))
    }};
}

fn substr_func(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    let start_array = args[1].as_primitive::<Int64Type>();
    let count_array = if args.len() == 3 {
        Some(args[2].as_primitive::<Int64Type>())
    } else {
        None
    };

    match args[0].data_type() {
        DataType::Binary => substr_func_for!(args[0], BinaryArray, start_array, count_array),
        DataType::BinaryView => {
            substr_func_for!(args[0], BinaryViewArray, start_array, count_array)
        }
        DataType::LargeBinary => {
            substr_func_for!(args[0], LargeBinaryArray, start_array, count_array)
        }
        _ => unreachable!("invalid type for substr_func"),
    }
}
