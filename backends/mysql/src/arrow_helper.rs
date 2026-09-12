use async_stream::stream;
use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    Decimal256Builder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, LargeBinaryBuilder,
    LargeStringBuilder, ListBuilder, NullBuilder, RecordBatch, StringBuilder,
    StringDictionaryBuilder, StructBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::datatypes::{Date32Type, Int8Type, SchemaRef, UInt16Type, i256};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::StreamExt;
use futures_util::lock::Mutex;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use mysql_common::bigdecimal::{BigDecimal, ToPrimitive};
use mysql_common::chrono::Timelike;
use mysql_common::chrono::{NaiveDate, NaiveTime};
use mysql_common::constants::{ColumnFlags, ColumnType};
use mysql_common::prelude::FromValue;
use mysql_common::time::PrimitiveDateTime;
use mysql_common::{FromValueError, Row, Value, num_bigint};
use std::sync::{Arc, LazyLock};

macro_rules! downcast {
    ($builder:expr, $builder_ty:ty, $dbg_schema:expr, $dbg_column_index:expr) => {
        $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Failed to downcast builder {:?} to correct type {:?} (column: {})",
                    stringify!($builder),
                    stringify!($builder_ty),
                    {
                        SchemaRef::as_ref(&$dbg_schema)
                            .field($dbg_column_index)
                            .name()
                    }
                ))
            })?
    };
}

macro_rules! get_value_or_set_null {
    ($builder:expr, $value_ty:ty, $row_value:expr, $dbg_schema:expr, $dbg_column_index:expr) => {
        if matches!($row_value, Value::NULL) {
            $builder.append_null();
            return Ok(());
        } else {
            <$value_ty>::from_value_opt($row_value).map_err(|_| {
                DataFusionError::Execution(format!(
                    "Failed to downcast builder {:?} to correct type {:?} (column: {})",
                    stringify!($builder),
                    stringify!($builder_ty),
                    {
                        SchemaRef::as_ref(&$dbg_schema)
                            .field($dbg_column_index)
                            .name()
                    }
                ))
            })?
        }
    };
}

macro_rules! handle_primitive_type {
    ($builder:expr, $builder_ty:ty, $value_ty:ty, $row_value:expr, $dbg_schema:expr, $dbg_column_index:expr) => {{
        let builder = downcast!($builder, $builder_ty, $dbg_schema, $dbg_column_index);
        let v = get_value_or_set_null!(
            builder,
            $value_ty,
            $row_value,
            $dbg_schema,
            $dbg_column_index
        );
        builder.append_value(v);
    }};
}

/// This function was hugely inspired by datafusion-table-providers
/// Licensed under the Apache 2.0 License (C) datafusion-table-providers contributors
pub(crate) fn query_arrow(
    conn: Arc<Mutex<Conn>>,
    query: &str,
    schema: SchemaRef,
    chunk_by: usize,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    let cloned_schema = schema.clone();
    let query = query.replace('"', "");
    let data_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    let s = stream! {
        let mut conn = conn.try_lock().unwrap();

        // DEBATABLE: should we use text or binary protocol here? (benchmark!)
        let stream = conn.query_stream::<Row, _>(query);

        #[cfg(feature = "tracing")]
        let stream = tracing::instrument::Instrument::instrument(stream, tracing::info_span!("mysql_query_time"));

        let mut iterator = stream.await.map_err(|err| DataFusionError::External(Box::new(err)))?;
        let mut current_builders: Option<Vec<Box<dyn ArrayBuilder>>> = None;

        let mut mysql_types: Vec<ColumnType> = Vec::new();
        let mut column_is_binary_stats: Vec<bool> = Vec::new();
        let mut column_is_enum_stats: Vec<bool> = Vec::new();
        let mut column_use_large_str_or_blob_stats: Vec<bool> = Vec::new();

        let mut row_count = 0;

        let finish = |current_builders: &mut Option<Vec<Box<dyn ArrayBuilder>>>| {
            let rows = current_builders
                .take()
                .unwrap()
                .iter_mut()
                .map(|builder| builder.finish())
                .collect::<Vec<_>>();

            RecordBatch::try_new(schema.clone(), rows)
                .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
        };

        while let Some(row) = iterator.next().await {
            let row = row
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            if row_count >= chunk_by {
                yield finish(&mut current_builders);
                row_count = 0;
            }

            if mysql_types.is_empty() {
                // Fill the metadata arrays
                for column in row.columns().iter() {
                    let column_type = column.column_type();
                    let column_is_binary = column.flags().contains(ColumnFlags::BINARY_FLAG);
                    let column_is_enum = column.flags().contains(ColumnFlags::ENUM_FLAG);
                    let column_use_large_str_or_blob = column.column_length() > 2_u32.pow(31) - 1;

                    mysql_types.push(column_type);
                    column_is_binary_stats.push(column_is_binary);
                    column_is_enum_stats.push(column_is_enum);
                    column_use_large_str_or_blob_stats.push(column_use_large_str_or_blob);
                }
            }

            let builders = current_builders.get_or_insert_with(||
                schema.fields().iter().map(|field| {
                    map_data_type_to_array_builder(field.data_type())
                }).collect()
            );

            let row = row.unwrap_raw();

            builders.iter_mut()
                .zip(mysql_types.iter().zip(data_types.iter()))
                .zip(row.into_iter())
                .enumerate()
                .try_for_each::<_, datafusion::common::Result<()>>(|(i, ((builder, (mysql_type, arrow_type)), value))| {
                    let Some(value) = value else { unreachable!(); };

                    match *mysql_type {
                        ColumnType::MYSQL_TYPE_NULL => downcast!(builder, NullBuilder, schema, i).append_null(),
                        ColumnType::MYSQL_TYPE_BIT => {
                                let builder = downcast!(builder, UInt64Builder, schema, i);

                                match value {
                                    Value::Bytes(mut bytes) => {
                                        while bytes.len() < 8 {
                                            bytes.insert(0, 0);
                                        }
                                        let mut array = [0u8; 8];
                                        array.copy_from_slice(&bytes);
                                        builder.append_value(u64::from_be_bytes(array));
                                    }
                                    _ => builder.append_null(),
                                }
                        }
                        ColumnType::MYSQL_TYPE_TINY => {
                            handle_primitive_type!(builder, Int8Builder, i8, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                            handle_primitive_type!(builder, Int16Builder, i16, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
                            handle_primitive_type!(builder, Int32Builder, i32, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_LONGLONG => {
                            handle_primitive_type!(builder, Int64Builder, i64, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_FLOAT => {
                            handle_primitive_type!(builder, Float32Builder, f32, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_DOUBLE => {
                            handle_primitive_type!(builder, Float64Builder, f64, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                            let arrow_field = schema.field(i);

                            match arrow_field.data_type() {
                                DataType::Decimal128(_, _) => {
                                    let builder = downcast!(builder, Decimal128Builder, schema, i);

                                    let val = get_value_or_set_null!(builder, BigDecimal, value, schema, i);
                                        let scale = val.fractional_digit_count();

                                        let val = to_decimal_128(&val, scale)
                                            .ok_or(DataFusionError::Execution("Failed to convert to decimal".to_string()))?;

                                    builder.append_value(val);
                                }
                                DataType::Decimal256(_, _) => {
                                    let builder = downcast!(builder, Decimal256Builder, schema, i);
                                    let val = get_value_or_set_null!(builder, BigDecimal, value, schema, i);
                                    let val = to_decimal_256(&val);

                                    builder.append_value(val);
                                }
                                // ColumnType::MYSQL_TYPE_DECIMAL & ColumnType::MYSQL_TYPE_NEWDECIMAL are only mapped to Decimal128/Decimal256 in `map_column_to_data_type` function
                                _ => unreachable!(),
                            }
                        }
                        ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_JSON => {
                            handle_primitive_type!(builder, LargeStringBuilder, String, value, schema, i);
                        }
                        ColumnType::MYSQL_TYPE_BLOB => {
                            match (
                                column_use_large_str_or_blob_stats[i],
                                column_is_binary_stats[i],
                            ) {
                                (true, true) => handle_primitive_type!(
                                    builder,
                                    LargeBinaryBuilder,
                                    Vec<u8>,
                                        value, schema, i
                                ),
                                (true, false) => handle_primitive_type!(
                                    builder,
                                    LargeStringBuilder,
                                    String,
                                        value, schema, i
                                ),
                                (false, true) => handle_primitive_type!(
                                    builder,
                                    BinaryBuilder,
                                    Vec<u8>,
                                        value, schema, i
                                ),
                                (false, false) => handle_primitive_type!(
                                    builder,
                                    StringBuilder,
                                    String,
                                        value, schema, i
                                ),
                            }
                        }
                        ColumnType::MYSQL_TYPE_ENUM => {
                            // ENUM and SET values are returned as strings. For these, check that the type value is MYSQL_TYPE_STRING and that the ENUM_FLAG or SET_FLAG flag is set in the flags value.
                            // https://dev.mysql.com/doc/c-api/9.0/en/c-api-data-structures.html
                            unreachable!()
                        }
                        ColumnType::MYSQL_TYPE_STRING | ColumnType::MYSQL_TYPE_VAR_STRING => {
                            // Handle MYSQL_TYPE_ENUM value
                            if column_is_enum_stats[i] {
                                let builder = downcast!(builder, StringDictionaryBuilder<UInt16Type>, schema, i);
                                let val = get_value_or_set_null!(builder, String, value, schema, i);
                                builder.append_value(val);
                            } else if column_is_binary_stats[i] && arrow_type != &DataType::Utf8 {
                                handle_primitive_type!(
                                    builder,
                                    BinaryBuilder,
                                    Vec<u8>,
                                    value, schema, i
                                );
                            } else {
                                handle_primitive_type!(
                                    builder,
                                    StringBuilder,
                                    String,
                                    value, schema, i
                                );
                            }
                        }
                        ColumnType::MYSQL_TYPE_DATE => {
                                    let builder = downcast!(builder, Date32Builder, schema, i);
                                    let result = NaiveDate::from_value_opt(value);

                                    let value = match result {
                                        Ok(val) => Ok(Some(val)),
                                        Err(FromValueError(Value::NULL)) => Ok(None),
                                        Err(FromValueError(Value::Date(0, 0, 0, 0, 0, 0, 0))) => Ok(None),
                                        Err(_) => Err(DataFusionError::Execution("FromValueError".to_string())),
                                    };

                                    match value? {
                                        Some(v) => {
                                            builder.append_value(Date32Type::from_naive_date(v));
                                        }
                                        None => builder.append_null(),
                                    }
                        }
                        ColumnType::MYSQL_TYPE_TIME => {
                            let builder = downcast!(builder, Time64NanosecondBuilder, schema, i);
                            let value = get_value_or_set_null!(builder, NaiveTime, value, schema, i);
                            builder.append_value(
                                i64::from(value.num_seconds_from_midnight()) * 1_000_000_000
                                    + i64::from(value.nanosecond()),
                            );
                        }
                        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
                                    let builder = downcast!(builder, TimestampMicrosecondBuilder, schema, i);
                                    let result = PrimitiveDateTime::from_value_opt(value);

                                    let value = match result {
                                        Ok(val) => Ok(Some(val)),
                                        Err(FromValueError(Value::NULL)) => Ok(None),
                                        Err(FromValueError(Value::Date(0, 0, 0, 0, 0, 0, 0))) => Ok(None),
                                        Err(_) => Err(DataFusionError::Execution("FromValueError".to_string())),
                                    };

                            match value? {
                                Some(v) => {
                                    #[allow(clippy::cast_possible_truncation)]
                                    let timestamp_micros =
                                        (v.assume_utc().unix_timestamp_nanos() / 1_000) as i64;
                                    builder.append_value(timestamp_micros);
                                }
                                None => builder.append_null(),
                            }
                        }
                        _ => unimplemented!("Unsupported column type {:?}", mysql_type),
                    };

                Ok(())
            })?;

            row_count += 1;
        }

        drop(iterator);
        drop(conn);

        if row_count > 0 {
            yield finish(&mut current_builders);
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(cloned_schema, s)))
}

/// This function was taken from datafusion-table-providers
/// Licensed under the Apache 2.0 License (C) datafusion-table-providers contributors
fn to_decimal_128(decimal: &BigDecimal, scale: i64) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
}

/// This function was taken from datafusion-table-providers
/// Licensed under the Apache 2.0 License (C) datafusion-table-providers contributors
fn to_decimal_256(decimal: &BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}

/// This function was taken from datafusion-table-providers
/// Licensed under the Apache 2.0 License (C) datafusion-table-providers contributors
static ONE_COLUMN_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, true)])));

pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::common::Result<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                // If the projection is Some([]) then it gets unparsed as `SELECT 1`, so return a schema with a single Int64 column.
                //
                // See: <https://github.com/apache/datafusion/blob/83ce79c39412a4f150167d00e40ea05948c4870f/datafusion/sql/src/unparser/plan.rs#L998>
                Arc::clone(&ONE_COLUMN_SCHEMA)
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

/// This function was taken from datafusion-table-providers
/// Licensed under the Apache 2.0 License (C) datafusion-table-providers contributors
#[allow(clippy::too_many_lines)]
pub fn map_data_type_to_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int8 => Box::new(Int8Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::UInt16 => Box::new(UInt16Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::LargeBinary => Box::new(LargeBinaryBuilder::new()),
        DataType::FixedSizeBinary(_) => Box::new(BinaryBuilder::new()),
        DataType::Interval(_) => Box::new(IntervalMonthDayNanoBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            Decimal128Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Decimal256(precision, scale) => Box::new(
            Decimal256Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Timestamp(time_unit, time_zone) => match time_unit {
            TimeUnit::Microsecond => {
                Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Second => {
                Box::new(TimestampSecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Millisecond => {
                Box::new(TimestampMillisecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Nanosecond => {
                Box::new(TimestampNanosecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
        },
        DataType::Dictionary(key_type, value_type) => match (&**key_type, &**value_type) {
            (DataType::Int8, DataType::Utf8) => {
                Box::new(StringDictionaryBuilder::<Int8Type>::new())
            }
            (DataType::UInt16, DataType::Utf8) => {
                Box::new(StringDictionaryBuilder::<UInt16Type>::new())
            }
            _ => unimplemented!("Unimplemented dictionary type"),
        },
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Date64 => Box::new(Date64Builder::new()),
        // For time format, always use nanosecond
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new()),
        // We can't recursively call map_data_type_to_array_builder here because downcasting will not work if the
        // values_builder is boxed.
        DataType::List(values_field) | DataType::LargeList(values_field) => {
            match values_field.data_type() {
                DataType::Int8 => Box::new(ListBuilder::new(Int8Builder::new())),
                DataType::Int16 => Box::new(ListBuilder::new(Int16Builder::new())),
                DataType::Int32 => Box::new(ListBuilder::new(Int32Builder::new())),
                DataType::Int64 => Box::new(ListBuilder::new(Int64Builder::new())),
                DataType::UInt32 => Box::new(ListBuilder::new(UInt32Builder::new())),
                DataType::Float32 => Box::new(ListBuilder::new(Float32Builder::new())),
                DataType::Float64 => Box::new(ListBuilder::new(Float64Builder::new())),
                DataType::Utf8 => Box::new(ListBuilder::new(StringBuilder::new())),
                DataType::Boolean => Box::new(ListBuilder::new(BooleanBuilder::new())),
                DataType::Binary => Box::new(ListBuilder::new(BinaryBuilder::new())),
                _ => unimplemented!("Unsupported list value data type {:?}", data_type),
            }
        }
        DataType::FixedSizeList(values_field, size) => match values_field.data_type() {
            DataType::Int8 => Box::new(FixedSizeListBuilder::new(
                Int8Builder::new(),
                size.to_owned(),
            )),
            DataType::Int16 => Box::new(FixedSizeListBuilder::new(
                Int16Builder::new(),
                size.to_owned(),
            )),
            DataType::Int32 => Box::new(FixedSizeListBuilder::new(
                Int32Builder::new(),
                size.to_owned(),
            )),
            DataType::Int64 => Box::new(FixedSizeListBuilder::new(
                Int64Builder::new(),
                size.to_owned(),
            )),
            DataType::UInt32 => Box::new(FixedSizeListBuilder::new(
                UInt32Builder::new(),
                size.to_owned(),
            )),
            DataType::Float32 => Box::new(FixedSizeListBuilder::new(
                Float32Builder::new(),
                size.to_owned(),
            )),
            DataType::Float64 => Box::new(FixedSizeListBuilder::new(
                Float64Builder::new(),
                size.to_owned(),
            )),
            DataType::Utf8 => Box::new(FixedSizeListBuilder::new(
                StringBuilder::new(),
                size.to_owned(),
            )),
            DataType::Boolean => Box::new(FixedSizeListBuilder::new(
                BooleanBuilder::new(),
                size.to_owned(),
            )),
            _ => unimplemented!("Unsupported list value data type {:?}", data_type),
        },
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Struct(fields) => {
            let mut field_builders = Vec::with_capacity(fields.len());
            for field in fields {
                field_builders.push(map_data_type_to_array_builder(field.data_type()));
            }
            Box::new(StructBuilder::new(fields.clone(), field_builders))
        }
        _ => unimplemented!("Unsupported data type {:?}", data_type),
    }
}
