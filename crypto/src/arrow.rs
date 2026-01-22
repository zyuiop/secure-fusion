use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericByteArray, GenericByteBuilder, NullArray,
    PrimitiveArray, types,
};
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, GenericBinaryType};
use datafusion::arrow::error::ArrowError;
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

pub fn cast_from_binary(
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
        // DataType::Decimal256(_, _) =>  map_array_zerocopy::<types::Decimal256Type>(array),

        /*DataType::Timestamp(_, _) => {}
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

pub fn cast_to_binary(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
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

        /*DataType::Timestamp(_, _) => {}
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
        DataType::Decimal256(_, _) => {}
        DataType::Map(_, _) => {}
        DataType::RunEndEncoded(_, _) => {}
        DataType::Null => {}*/
        other => Err(ArrowError::CastError(format!(
            "Casting not implemented to binary from {other:?}"
        ))),
    }
}
