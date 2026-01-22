use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

const EXISTS_SIGNATURE: Signature = Signature {
    type_signature: TypeSignature::VariadicAny,
    volatility: Volatility::Stable,
    parameter_names: None,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ExistsUdf;

impl ExistsUdf {
    pub fn get_udf() -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::new_from_impl(Self))
    }
}

impl AggregateUDFImpl for ExistsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "exists"
    }

    fn signature(&self) -> &Signature {
        &EXISTS_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExistsUdfAccumulator::default()))
    }
}

#[derive(Debug, Clone)]
struct ExistsUdfAccumulator {
    result: bool,
}

impl Default for ExistsUdfAccumulator {
    fn default() -> Self {
        Self { result: false }
    }
}

impl Accumulator for ExistsUdfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.result |= !values[0].is_empty();
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Boolean(Some(self.result)))
    }

    fn size(&self) -> usize {
        1
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(states)
    }
}
