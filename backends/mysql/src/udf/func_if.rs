use datafusion::arrow::array::*;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err, not_impl_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IfUdf {
    signature: Signature,
}

impl Default for IfUdf {
    fn default() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(3),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
        }
    }
}

macro_rules! impl_if_for_array_type {
    ($iff: expr, $arr1: expr, $arr2: expr, $arr_type: ty) => {{
        let out_arr_1 = $arr1
            .as_any()
            .downcast_ref::<$arr_type>()
            .ok_or_else(|| exec_datafusion_err!("failed to downcast array!"))?;
        let out_arr_2 = $arr2
            .as_any()
            .downcast_ref::<$arr_type>()
            .ok_or_else(|| exec_datafusion_err!("failed to downcast array!"))?;

        let result = $iff
            .into_iter()
            .zip(out_arr_1.into_iter())
            .zip(out_arr_2.into_iter())
            .map(
                |((iff, then), elze)| {
                    if iff.is_some_and(|v| v) { then } else { elze }
                },
            );

        let array = <$arr_type>::from_iter(result);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }};
}

impl ScalarUDFImpl for IfUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        // https://dev.mysql.com/doc/refman/8.4/en/flow-control-functions.html#function_if
        let [_, then, elze] = arg_types else {
            plan_err!("invalid call to function: if")?
        };

        if then == &DataType::Null {
            Ok(elze.clone())
        } else if elze == &DataType::Null {
            Ok(then.clone())
        } else if matches!(then, DataType::Utf8) || matches!(elze, DataType::Utf8) {
            Ok(DataType::Utf8)
        } else if then.is_floating() || elze.is_floating() {
            Ok(DataType::Float64)
        } else if then.is_integer() || elze.is_integer() {
            Ok(DataType::Int64)
        } else {
            plan_err!(
                "could not determine return type for function if: input types ({then:?}, {elze:?})"
            )
        }
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        // https://dev.mysql.com/doc/refman/8.4/en/flow-control-functions.html#function_if
        let [iff, then, elze] = args.arg_fields else {
            plan_err!("invalid call to function: if")?
        };

        let return_type = self.return_type(&[
            iff.data_type().clone(),
            then.data_type().clone(),
            elze.data_type().clone(),
        ])?;

        let is_nullable = then.is_nullable() || elze.is_nullable();

        Ok(FieldRef::new(Field::new(
            self.name(),
            return_type,
            is_nullable,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let return_field = args.return_field;
        let boolean_arg = &args.args[0];
        let boolean_arg = boolean_arg.cast_to(
            &DataType::Boolean,
            Some(&CastOptions {
                format_options: CastOptions::default().format_options,
                safe: true, // cast invalid values to null
            }),
        )?;

        let return_type = return_field.data_type();

        match boolean_arg {
            ColumnarValue::Scalar(ScalarValue::Boolean(opt_bool)) => {
                let value = if opt_bool.is_some_and(|v| v) {
                    &args.args[1]
                } else {
                    &args.args[2]
                };

                Ok(value.cast_to(&return_type, None)?)
            }
            ColumnarValue::Scalar(_) => exec_err!("unreachable: casting iff value failed"),
            ColumnarValue::Array(_) if return_type == &DataType::Null => {
                Ok(ColumnarValue::Scalar(ScalarValue::Null))
            }
            ColumnarValue::Array(arr) => {
                let return_nullable = return_field.is_nullable();
                let options = CastOptions {
                    format_options: CastOptions::default().format_options,
                    safe: return_nullable, // cast invalid values to null only if return can be null
                };

                let bool_arr = arr.as_boolean();

                let num_values = bool_arr.len();

                let out_arr_1 = args.args[1]
                    .cast_to(&return_type, Some(&options))?
                    .to_array(num_values)?;
                let out_arr_2 = args.args[2]
                    .cast_to(&return_type, Some(&options))?
                    .to_array(num_values)?;

                match return_type {
                    DataType::Null => unreachable!(),
                    DataType::Boolean => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, BooleanArray)
                    }
                    DataType::Int8 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Int8Array)
                    }
                    DataType::Int16 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Int16Array)
                    }
                    DataType::Int32 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Int32Array)
                    }
                    DataType::Int64 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Int64Array)
                    }
                    DataType::UInt8 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, UInt8Array)
                    }
                    DataType::UInt16 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, UInt16Array)
                    }
                    DataType::UInt32 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, UInt32Array)
                    }
                    DataType::UInt64 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, UInt64Array)
                    }
                    DataType::Float16 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Float16Array)
                    }
                    DataType::Float32 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Float32Array)
                    }
                    DataType::Float64 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Float64Array)
                    }
                    DataType::Date32 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Date32Array)
                    }
                    DataType::Date64 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Date64Array)
                    }
                    DataType::Binary => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, BinaryArray)
                    }
                    DataType::LargeBinary => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, LargeBinaryArray)
                    }
                    DataType::BinaryView => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, BinaryViewArray)
                    }
                    DataType::Utf8 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, StringArray)
                    }
                    DataType::LargeUtf8 => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, LargeStringArray)
                    }
                    DataType::Utf8View => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, StringViewArray)
                    }
                    DataType::Decimal32(_, _) => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Decimal32Array)
                    }
                    DataType::Decimal64(_, _) => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Decimal64Array)
                    }
                    DataType::Decimal128(_, _) => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Decimal128Array)
                    }
                    DataType::Decimal256(_, _) => {
                        impl_if_for_array_type!(bool_arr, out_arr_1, out_arr_2, Decimal256Array)
                    }
                    DataType::Duration(time_unit) => match time_unit {
                        TimeUnit::Second => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            DurationSecondArray
                        ),
                        TimeUnit::Millisecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            DurationMillisecondArray
                        ),
                        TimeUnit::Microsecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            DurationMicrosecondArray
                        ),
                        TimeUnit::Nanosecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            DurationNanosecondArray
                        ),
                    },
                    DataType::Interval(interval_unit) => match interval_unit {
                        IntervalUnit::YearMonth => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            IntervalYearMonthArray
                        ),
                        IntervalUnit::DayTime => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            IntervalDayTimeArray
                        ),
                        IntervalUnit::MonthDayNano => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            IntervalMonthDayNanoArray
                        ),
                    },
                    DataType::Timestamp(time_unit, _) => match time_unit {
                        TimeUnit::Second => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            TimestampSecondArray
                        ),
                        TimeUnit::Millisecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            TimestampMillisecondArray
                        ),
                        TimeUnit::Microsecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            TimestampMicrosecondArray
                        ),
                        TimeUnit::Nanosecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            TimestampNanosecondArray
                        ),
                    },
                    DataType::Time32(time_unit) | DataType::Time64(time_unit) => match time_unit {
                        TimeUnit::Second => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            Time32SecondArray
                        ),
                        TimeUnit::Millisecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            Time32MillisecondArray
                        ),
                        TimeUnit::Microsecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            Time64MicrosecondArray
                        ),
                        TimeUnit::Nanosecond => impl_if_for_array_type!(
                            bool_arr,
                            out_arr_1,
                            out_arr_2,
                            Time64NanosecondArray
                        ),
                    },
                    other => not_impl_err!("not implemented: if with a {other:?} return type"),
                }
            }
        }
    }
}
