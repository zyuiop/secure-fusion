use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GroupConcatUdf {
    signature: Signature,
    options_struct: DataType,
}

impl Default for GroupConcatUdf {
    fn default() -> Self {
        let options_struct = DataType::Struct(Fields::from(vec![Field::new(
            "separator",
            DataType::Utf8,
            true,
        )]));

        Self {
            signature: Signature {
                type_signature: TypeSignature::UserDefined,
                volatility: Volatility::Stable,
                parameter_names: None,
            },
            options_struct,
        }
    }
}

#[derive(Debug)]
struct GroupConcatAccumulator {
    num_args: usize,
    string_accumulator: Option<String>,
}

impl Accumulator for GroupConcatAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let (options_structs, values): (Vec<_>, Vec<_>) = values
            .iter()
            .partition(|v| matches!(v.data_type(), DataType::Struct(_)));

        // Number of arguments to this function -- it seems sometimes we get more arguments than needed, so we should ignore them
        let num_args = self.num_args - options_structs.len();

        // Find a separator, if any
        let separator = options_structs
            .into_iter()
            .find_map(|value| {
                if matches!(value.data_type(), DataType::Struct(_)) {
                    let as_struct = value.as_struct();
                    let sep = as_struct.column_by_name("separator")?;
                    let sep = sep.as_string::<i32>();

                    if sep.is_empty() || sep.is_null(0) {
                        Some("")
                    } else {
                        Some(sep.value(0))
                    }
                } else {
                    unreachable!();
                }
            })
            .unwrap_or(&",");

        let num_values = (&values[0]).len();
        let values = &values[..num_args];
        for i in 0..num_values {
            if let Some(acc) = self.string_accumulator.as_mut() {
                acc.push_str(separator);
            }

            for (column, array) in values.iter().enumerate() {
                if array.is_null(i) {
                    continue;
                }

                let string = match array.data_type() {
                    DataType::Utf8 => array.as_string::<i32>().value(i),
                    DataType::LargeUtf8 => array.as_string::<i64>().value(i),
                    typ => exec_err!(
                        "Reached invalid datatype {typ:?} for argument {}",
                        column + 1
                    )?,
                };

                self.string_accumulator
                    .get_or_insert_default()
                    .push_str(string);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Utf8(self.string_accumulator.take()))
    }

    fn size(&self) -> usize {
        self.string_accumulator
            .as_ref()
            .map(|v| v.capacity())
            .unwrap_or_default()
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(states)
    }
}

impl AggregateUDFImpl for GroupConcatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "group_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(GroupConcatAccumulator {
            num_args: _acc_args.exprs.len(),
            string_accumulator: None,
        }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::HardRequirement
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        Ok(arg_types
            .iter()
            .map(|typ| {
                if typ == &DataType::Null {
                    DataType::Null
                } else if matches!(typ, DataType::Struct(_)) {
                    // SPECIAL: Our good friend the separator
                    self.options_struct.clone()
                } else {
                    DataType::Utf8
                }
            })
            .collect())
    }
}
