use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeListBuilder, Float32Builder, GenericByteArray,
    GenericByteBuilder, NullArray, PrimitiveArray, types,
};
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, GenericBinaryType, IntervalUnit, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{ScalarValue, exec_datafusion_err};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;
use zerocopy::{FromBytes, Immutable, IntoBytes};

pub type IntermediateBinaryType = GenericBinaryType<i32>;
pub type IntermediateBinaryArrayType = GenericByteArray<IntermediateBinaryType>;

fn primitive_array_from_bytes<T: ArrowPrimitiveType>(
    array: &IntermediateBinaryArrayType,
) -> Result<ArrayRef, ArrowError>
where
    T::Native: FromBytes,
{
    let array = unwrapped_primitive_array_from_bytes::<T>(array)?;
    Ok(Arc::new(array))
}

fn primitive_array_from_bytes_with_datatype<T: ArrowPrimitiveType>(
    array: &IntermediateBinaryArrayType,
    dt: DataType,
) -> Result<ArrayRef, ArrowError>
where
    T::Native: FromBytes,
{
    let array = unwrapped_primitive_array_from_bytes::<T>(array)?;
    Ok(Arc::new(array.with_data_type(dt)))
}

fn unwrapped_primitive_array_from_bytes<T: ArrowPrimitiveType>(
    array: &IntermediateBinaryArrayType,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T::Native: FromBytes,
{
    let iter = array
        .iter()
        .map(|v| v.and_then(|v| T::Native::read_from_bytes(v).ok()));

    // Soundness: map preserves trusted len
    let array = unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) };
    Ok(array)
}

fn primitive_array_to_bytes<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError>
where
    T::Native: IntoBytes + Immutable,
{
    let array = array.as_primitive::<T>();

    let mut builder = GenericByteBuilder::<IntermediateBinaryType>::with_capacity(
        array.len(),
        (array.len() - array.null_count()) * size_of::<T::Native>(), // That's an estimate, but it should reduce needless allocations
    );

    for v in array.into_iter() {
        if let Some(v) = v {
            builder.append_value(v.as_bytes())
        } else {
            builder.append_null()
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
pub fn decode_array_from_binary(
    array: &IntermediateBinaryArrayType,
    target_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    if can_cast_types(&DataType::Binary, target_type) {
        return cast(array, target_type);
    }

    match target_type {
        DataType::Null => Ok(Arc::new(NullArray::new(array.len()))),
        // Wouldn't it be easier to encode everything to strings instead? requires a bit more space but may be more stable
        DataType::Boolean => {
            let array =
                BooleanArray::from_iter(array.iter().map(|v| v.map(|v| v.len() == 1 && v[0] == 1)));
            Ok(Arc::new(array))
        }
        DataType::Int8 => primitive_array_from_bytes::<types::Int8Type>(array),
        DataType::Int16 => primitive_array_from_bytes::<types::Int16Type>(array),
        DataType::Int32 => primitive_array_from_bytes::<types::Int32Type>(array),
        DataType::Int64 => primitive_array_from_bytes::<types::Int64Type>(array),
        DataType::UInt8 => primitive_array_from_bytes::<types::UInt8Type>(array),
        DataType::UInt16 => primitive_array_from_bytes::<types::UInt16Type>(array),
        DataType::UInt32 => primitive_array_from_bytes::<types::UInt32Type>(array),
        DataType::UInt64 => primitive_array_from_bytes::<types::UInt64Type>(array),
        DataType::Float16 => primitive_array_from_bytes::<types::Float16Type>(array),
        DataType::Float32 => primitive_array_from_bytes::<types::Float32Type>(array),
        DataType::Float64 => primitive_array_from_bytes::<types::Float64Type>(array),
        DataType::Date32 => primitive_array_from_bytes::<types::Date32Type>(array),
        DataType::Date64 => primitive_array_from_bytes::<types::Date64Type>(array),

        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive_array_from_bytes_with_datatype::<
            types::TimestampNanosecondType,
        >(array, target_type.clone()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive_array_from_bytes_with_datatype::<types::TimestampMicrosecondType>(
                array,
                target_type.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive_array_from_bytes_with_datatype::<types::TimestampMillisecondType>(
                array,
                target_type.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Second, _) => primitive_array_from_bytes_with_datatype::<
            types::TimestampSecondType,
        >(array, target_type.clone()),
        DataType::Time32(TimeUnit::Second) => {
            primitive_array_from_bytes::<types::Time32SecondType>(array)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive_array_from_bytes::<types::Time32MillisecondType>(array)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_array_from_bytes::<types::Time64MicrosecondType>(array)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            primitive_array_from_bytes::<types::Time64NanosecondType>(array)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            primitive_array_from_bytes::<types::DurationNanosecondType>(array)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive_array_from_bytes::<types::DurationMicrosecondType>(array)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive_array_from_bytes::<types::DurationMillisecondType>(array)
        }
        DataType::Duration(TimeUnit::Second) => {
            primitive_array_from_bytes::<types::DurationSecondType>(array)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_array_from_bytes::<types::IntervalYearMonthType>(array)
        }

        // TODO(perf): study internals to avoid doublecast
        DataType::Decimal32(p, s) => {
            let array = unwrapped_primitive_array_from_bytes::<types::Decimal32Type>(array)?
                .with_precision_and_scale(*p, *s)?;
            Ok(Arc::new(array))
        }
        DataType::Decimal64(p, s) => {
            let array = unwrapped_primitive_array_from_bytes::<types::Decimal64Type>(array)?
                .with_precision_and_scale(*p, *s)?;
            Ok(Arc::new(array))
        }
        DataType::Decimal128(p, s) => {
            let array = unwrapped_primitive_array_from_bytes::<types::Decimal128Type>(array)?
                .with_precision_and_scale(*p, *s)?;
            Ok(Arc::new(array))
        }

        DataType::FixedSizeList(dt, num) if dt.data_type() == &DataType::Float32 => {
            let elem_size = dt.data_type().primitive_width().unwrap();
            let num_elems = num.as_usize();
            let size_per_vector = elem_size * num_elems;

            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::new(),
                *num,
                array.len() / size_per_vector, // That's an estimate, but it should reduce needless allocations
            )
            .with_field(Arc::clone(dt));

            for value in array.iter() {
                if let Some(value) = value {
                    assert_eq!(value.len(), size_per_vector);

                    for i in 0..num_elems {
                        let slice = &value[(i * elem_size)..((i + 1) * elem_size)];
                        let value =
                            <types::Float32Type as ArrowPrimitiveType>::Native::read_from_bytes(
                                slice,
                            )
                            .map_err(|_| {
                                exec_datafusion_err!("invalid binary value for f32 type")
                            })?;
                        builder.values().append_value(value);
                    }

                    builder.append(true);
                } else {
                    for _ in 0..num_elems {
                        builder.values().append_null();
                    }
                    builder.append(false);
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        // DataType::Decimal256(_, _) =>  map_array_zerocopy::<types::Decimal256Type>(array),

        /*
        DataType::Timestamp(_, _) => {}
        DataType::Time32(_) => {}
        DataType::Time64(_) => {}
        DataType::Duration(_) => {}
        DataType::Interval(_) => {}
        DataType::Binary => {}
        DataType::FixedSizeBinary(_) => {}
        DataType::LargeBinary => {}
        DataType::BinaryView => {}
        DataType::Utf8 => {}
        DataType::LargeUtf8 => {}
        DataType::Utf8View => {}
        DataType::List(_) => {}
        DataType::ListView(_) => {}
        DataType::FixedSizeList(_, _) => {}
        DataType::LargeList(_) => {}
        DataType::LargeListView(_) => {}
        DataType::Struct(_) => {}
        DataType::Union(_, _) => {}
        DataType::Dictionary(_, _) => {}
        DataType::Map(_, _) => {}
        DataType::RunEndEncoded(_, _) => {}*/
        other => Err(ArrowError::CastError(format!(
            "Casting not implemented from binary to {other:?}"
        ))),
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
pub fn encode_array_to_binary(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    if can_cast_types(array.data_type(), &DataType::Binary) {
        return cast(array, &DataType::Binary);
    }

    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_boolean();
            let mut builder = GenericByteBuilder::<IntermediateBinaryType>::with_capacity(
                array.len(),
                array.len() - array.null_count(),
            );

            for v in array.into_iter() {
                if let Some(v) = v {
                    builder.append_value(if v { &[1u8] } else { &[0u8] });
                } else {
                    builder.append_null()
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => primitive_array_to_bytes::<types::Int8Type>(array),
        DataType::Int16 => primitive_array_to_bytes::<types::Int16Type>(array),
        DataType::Int32 => primitive_array_to_bytes::<types::Int32Type>(array),
        DataType::Int64 => primitive_array_to_bytes::<types::Int64Type>(array),
        DataType::UInt8 => primitive_array_to_bytes::<types::UInt8Type>(array),
        DataType::UInt16 => primitive_array_to_bytes::<types::UInt16Type>(array),
        DataType::UInt32 => primitive_array_to_bytes::<types::UInt32Type>(array),
        DataType::UInt64 => primitive_array_to_bytes::<types::UInt64Type>(array),
        DataType::Float16 => primitive_array_to_bytes::<types::Float16Type>(array),
        DataType::Float32 => primitive_array_to_bytes::<types::Float32Type>(array),
        DataType::Float64 => primitive_array_to_bytes::<types::Float64Type>(array),

        DataType::Decimal32(_, _) => primitive_array_to_bytes::<types::Decimal32Type>(array),
        DataType::Decimal64(_, _) => primitive_array_to_bytes::<types::Decimal64Type>(array),
        DataType::Decimal128(_, _) => primitive_array_to_bytes::<types::Decimal128Type>(array),

        DataType::Date32 => primitive_array_to_bytes::<types::Date32Type>(array),
        DataType::Date64 => primitive_array_to_bytes::<types::Date64Type>(array),

        DataType::FixedSizeList(dt, num) if dt.data_type() == &DataType::Float32 => {
            let array = array.as_fixed_size_list();

            let elem_size = dt.data_type().primitive_width().unwrap();
            let num_elems = num.as_usize();
            let size_per_vector = elem_size * num_elems;

            let mut builder = GenericByteBuilder::<IntermediateBinaryType>::with_capacity(
                array.len(),
                (array.len() - array.null_count()) * size_per_vector, // That's an estimate, but it should reduce needless allocations
            );

            for v in array.iter() {
                if let Some(v) = v {
                    // TODO: extend for other types
                    let v = primitive_array_to_bytes::<types::Float32Type>(v.as_ref())?;
                    let v = v.as_bytes::<IntermediateBinaryType>();
                    builder.append_value(v.value_data());
                } else {
                    builder.append_null()
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive_array_to_bytes::<types::TimestampNanosecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive_array_to_bytes::<types::TimestampMicrosecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive_array_to_bytes::<types::TimestampMillisecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            primitive_array_to_bytes::<types::TimestampSecondType>(array)
        }
        DataType::Time32(TimeUnit::Second) => {
            primitive_array_to_bytes::<types::Time32SecondType>(array)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive_array_to_bytes::<types::Time32MillisecondType>(array)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_array_to_bytes::<types::Time64MicrosecondType>(array)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            primitive_array_to_bytes::<types::Time64NanosecondType>(array)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            primitive_array_to_bytes::<types::DurationNanosecondType>(array)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive_array_to_bytes::<types::DurationMicrosecondType>(array)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive_array_to_bytes::<types::DurationMillisecondType>(array)
        }
        DataType::Duration(TimeUnit::Second) => {
            primitive_array_to_bytes::<types::DurationSecondType>(array)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_array_to_bytes::<types::IntervalYearMonthType>(array)
        }
        // DataType::Interval(IntervalUnit::DayTime) => primitive_array_to_bytes::<types::IntervalDayTimeType>(array),
        // DataType::Interval(IntervalUnit::MonthDayNano) => primitive_array_to_bytes::<types::IntervalMonthDayNanoType>(array),

        /*
        DataType::Binary => {}
        DataType::FixedSizeBinary(_) => {}
        DataType::LargeBinary => {}
        DataType::BinaryView => {}
        DataType::Utf8 => {}
        DataType::LargeUtf8 => {}
        DataType::Utf8View => {}
        DataType::List(_) => {}
        DataType::ListView(_) => {}
        DataType::FixedSizeList(_, _) => {}
        DataType::LargeList(_) => {}
        DataType::LargeListView(_) => {}
        DataType::Struct(_) => {}
        DataType::Union(_, _) => {}
        DataType::Dictionary(_, _) => {}
        DataType::Decimal256(_, _) => {}
        DataType::Map(_, _) => {}
        DataType::RunEndEncoded(_, _) => {}
        DataType::Null => {}*/
        other => Err(ArrowError::CastError(format!(
            "Casting not implemented to binary from {other:?}"
        ))),
    }
}

pub fn encode_columnar_value_to_binary(
    value: ColumnarValue,
) -> datafusion::common::Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let column = encode_array_to_binary(array.as_ref())?;
            Ok(ColumnarValue::Array(column))
        }
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            let column = encode_array_to_binary(array.as_ref())?;
            let scalar = ScalarValue::try_from_array(&column, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

pub fn decode_columnar_value_from_binary(
    value: ColumnarValue,
    target_type: &DataType,
) -> datafusion::common::Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let column = decode_array_from_binary(array.as_binary(), target_type)?;
            Ok(ColumnarValue::Array(column))
        }
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            let column = decode_array_from_binary(array.as_binary(), target_type)?;
            let scalar = ScalarValue::try_from_array(&column, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}
