use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{IntervalUnit, SchemaRef, TimeUnit};
use mysql_common::chrono::DateTime;
use mysql_common::{Value, params::Params};
use std::sync::Arc;

macro_rules! downcast_to_value {
    ($column:ident, $column_type:ident, $index:expr) => {
        downcast_to_value!($column, $column_type, $index, map(value) value)
    };
    ($column:ident, $column_type:ident, $index:expr, map($value_ident: ident) $map_fn: expr) => {{
        let column = $column
            .as_any()
            .downcast_ref::<datafusion::arrow::array::$column_type>();

        if let Some(column) = column {
            if column.is_null($index) {
                Value::NULL
            } else {
                let $value_ident = column.value($index);
                let value = $map_fn;
                value.into()
            }
        } else {
            Value::NULL
        }
    }};
}

macro_rules! downcast_to_timestamp {
    ($column:ident, $column_type:ident, $index:expr, $ts_factor_to_nanos: expr) => {{
        let column = $column
            .as_any()
            .downcast_ref::<datafusion::arrow::array::$column_type>();

        let Some(column) = column else {
            return Value::NULL;
        };
        let value = if column.is_null($index) {
            return Value::NULL;
        } else {
            column.value($index)
        };

        DateTime::from_timestamp_nanos(value * $ts_factor_to_nanos)
            .naive_local()
            .into()
    }};
}

/// Maps a record-batch to an iterator of Params
/// The input is a column-oriented arrow batch.
/// The output is a row-oriented MySQL parameters iterator, with each column in the input appearing
/// in order.
pub(super) fn map_rows_to_params(
    batch: RecordBatch,
) -> Result<impl Iterator<Item = Params>, mysql_async::Error> {
    map_rows_to_params_reordered(batch, None)
}

/// Produces a projection vector that can be used in `map_rows_to_params_reordered`
#[allow(unused)]
pub(super) fn project_to_schema(batch: &RecordBatch, output_schema: SchemaRef) -> Vec<usize> {
    let input_schema = batch.schema();
    output_schema
        .fields()
        .iter()
        .map(|field| {
            input_schema
                .index_of(field.name())
                .expect("invalid output_schema provided: some columns don't appear in the input")
        })
        .collect::<Vec<_>>()
}

/// Maps a record-batch to an iterator of Params
/// The input is a column-oriented arrow batch.
/// The output is a row-oriented MySQL parameters iterator. The order of the columns is defined by the
/// `project` parameter, which is an array of positions. A single column may appear multiple times,
/// or not at all, in the projection array.
pub(super) fn map_rows_to_params_reordered(
    batch: RecordBatch,
    project: Option<&[usize]>,
) -> Result<impl Iterator<Item = Params>, mysql_async::Error> {
    let (_, columns, size) = batch.into_parts();

    let columns = if let Some(order) = project {
        order
            .into_iter()
            .map(|&index| Arc::clone(&columns[index]))
            .collect::<Vec<_>>()
    } else {
        columns
    };

    let iterator = (0..size).map(move |index| {
        let values: Vec<Value> = columns
            .iter()
            .map(|col| {
                match col.data_type() {
                    // TODO: verify that all these casts make sense -- for some non-trivial datatypes (like Decimals or timestamps), this may be completely wrong
                    // Use this as reference: https://github.com/datafusion-contrib/datafusion-table-providers/blob/main/core/src/sql/arrow_sql_gen/statement.rs#L236
                    DataType::Null => Value::NULL,
                    DataType::Boolean => downcast_to_value!(col, BooleanArray, index),
                    DataType::Int8 => downcast_to_value!(col, Int8Array, index),
                    DataType::Int16 => downcast_to_value!(col, Int16Array, index),
                    DataType::Int32 => downcast_to_value!(col, Int32Array, index),
                    DataType::Int64 => downcast_to_value!(col, Int64Array, index),
                    DataType::UInt8 => downcast_to_value!(col, UInt8Array, index),
                    DataType::UInt16 => downcast_to_value!(col, UInt16Array, index),
                    DataType::UInt32 => downcast_to_value!(col, UInt32Array, index),
                    DataType::UInt64 => downcast_to_value!(col, UInt64Array, index),
                    DataType::Float16 => unimplemented!("cannot convert float16 to mysql value"), // downcast_to_value!(col, Float16Array, index),
                    DataType::Float32 => downcast_to_value!(col, Float32Array, index),
                    DataType::Float64 => downcast_to_value!(col, Float64Array, index),
                    DataType::Decimal32(_, scale) => downcast_to_value!(col, Decimal32Array, index, map(value) {
                        rust_decimal::Decimal::from_i128_with_scale(value as i128, *scale as u32)
                    }),
                    DataType::Decimal64(_, scale) => downcast_to_value!(col, Decimal64Array, index, map(value) {
                        rust_decimal::Decimal::from_i128_with_scale(value as i128, *scale as u32)
                    }),
                    DataType::Decimal256(_, _) => {
                        unimplemented!("cannot convert decimal256 to mysql value")
                    }
                    DataType::Decimal128(_, scale) => downcast_to_value!(col, Decimal128Array, index, map(value) {
                        rust_decimal::Decimal::from_i128_with_scale(value, *scale as u32)
                    }),
                    DataType::Timestamp(unit, _) => {
                        // MySQL server has no notion of timezone, the server converts everything to UTC based on its configuration
                        match unit {
                            TimeUnit::Second => downcast_to_timestamp!(
                                col,
                                TimestampSecondArray,
                                index,
                                1_000_000_000
                            ),
                            TimeUnit::Millisecond => downcast_to_timestamp!(
                                col,
                                TimestampMillisecondArray,
                                index,
                                1_000_000
                            ),
                            TimeUnit::Microsecond => {
                                downcast_to_timestamp!(col, TimestampMicrosecondArray, index, 1_000)
                            }
                            TimeUnit::Nanosecond => {
                                downcast_to_timestamp!(col, TimestampNanosecondArray, index, 1)
                            }
                        }
                    }
                    DataType::Date32 => downcast_to_value!(col, Date32Array, index),
                    DataType::Date64 => downcast_to_value!(col, Date64Array, index),
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => downcast_to_value!(col, Time32SecondArray, index),
                        TimeUnit::Millisecond => {
                            downcast_to_value!(col, Time32MillisecondArray, index)
                        }
                        other => panic!("Unsupported time32: time32::{other:?}"),
                    },
                    DataType::Time64(unit) => match unit {
                        TimeUnit::Microsecond => {
                            downcast_to_value!(col, Time64MicrosecondArray, index)
                        }
                        TimeUnit::Nanosecond => {
                            downcast_to_value!(col, Time64NanosecondArray, index)
                        }
                        other => panic!("Unsupported time64: time64::{other:?}"),
                    },
                    DataType::Duration(unit) => match unit {
                        TimeUnit::Second => downcast_to_value!(col, DurationSecondArray, index),
                        TimeUnit::Millisecond => {
                            downcast_to_value!(col, DurationMillisecondArray, index)
                        }
                        TimeUnit::Microsecond => {
                            downcast_to_value!(col, DurationMicrosecondArray, index)
                        }
                        TimeUnit::Nanosecond => {
                            downcast_to_value!(col, DurationNanosecondArray, index)
                        }
                    },
                    DataType::Interval(unit) => match unit {
                        IntervalUnit::YearMonth => {
                            downcast_to_value!(col, IntervalYearMonthArray, index)
                        }
                        IntervalUnit::MonthDayNano => {
                            downcast_to_value!(col, IntervalYearMonthArray, index)
                        }
                        IntervalUnit::DayTime => {
                            unimplemented!("unsupported interval type for sink: DayTime")
                        }
                    },
                    DataType::Binary => downcast_to_value!(col, BinaryArray, index),
                    DataType::FixedSizeBinary(_) => {
                        downcast_to_value!(col, FixedSizeBinaryArray, index)
                    }
                    DataType::LargeBinary => downcast_to_value!(col, LargeBinaryArray, index),
                    DataType::BinaryView => downcast_to_value!(col, BinaryViewArray, index),
                    DataType::Utf8 => downcast_to_value!(col, StringArray, index),
                    DataType::LargeUtf8 => downcast_to_value!(col, LargeStringArray, index),
                    DataType::Utf8View => downcast_to_value!(col, StringViewArray, index),

                    // other => unimplemented!("unsupported type for sink: {:?}", other),
                    other @ (DataType::List(_)
                    | DataType::ListView(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::LargeList(_)
                    | DataType::LargeListView(_)
                    | DataType::Struct(_)
                    | DataType::Union(_, _)
                    | DataType::Dictionary(_, _)
                    | DataType::Map(_, _)
                    | DataType::RunEndEncoded(_, _)) => {
                        unimplemented!("unsupported type for sink: {:?}", other)
                    }
                }
            })
            .collect();

        Params::Positional(values)
    });

    Ok(iterator)
}
