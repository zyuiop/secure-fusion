use crate::command_phase::error::CommandPhaseError;
use crate::command_phase::row_handlers::RowHandler;
use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{GenericByteArray, PrimitiveArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{
    ArrowNativeType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::common::arrow::datatypes::ByteArrayType;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use mysql_common::io::BufMutExt;
use mysql_common::packets::NullBitmap;
use mysql_common::value::ClientSide;
use mysql_interop::connection::MySQLConnection;

trait ItemIsNull {
    /// Returns true if the given item is null
    fn is_null(&self, position: usize) -> bool;
}

trait BinaryColumnSerializer: ItemIsNull {
    /// Serializes the given item
    fn serialize(&self, item: usize, output: &mut BytesMut) -> Result<(), ArrowError>;
}

impl<T: Array> ItemIsNull for T {
    fn is_null(&self, position: usize) -> bool {
        Array::is_null(self, position)
    }
}

impl<T: ByteArrayType> BinaryColumnSerializer for &'_ GenericByteArray<T> {
    fn serialize(&self, position: usize, output: &mut BytesMut) -> Result<(), ArrowError> {
        let value_length = self.value_length(position).as_usize();
        let offset = self.value_offsets()[position].as_usize();
        let offset_end = offset + value_length;

        let value = &self.value_data()[offset..offset_end];
        let value = value.as_ref();
        output.put_lenenc_str(value);
        Ok(())
    }
}

macro_rules! native_serializer {
    ($value_type:ty, $func:ident) => {
        impl BinaryColumnSerializer for &'_ PrimitiveArray<$value_type> {
            fn serialize(&self, item: usize, output: &mut BytesMut) -> Result<(), ArrowError> {
                BytesMut::$func(output, self.value(item));
                Ok(())
            }
        }
    };
}

native_serializer!(UInt8Type, put_u8);
native_serializer!(Int8Type, put_i8);
native_serializer!(UInt16Type, put_u16_le);
native_serializer!(Int16Type, put_i16_le);
native_serializer!(UInt32Type, put_u32_le);
native_serializer!(Int32Type, put_i32_le);
native_serializer!(UInt64Type, put_u64_le);
native_serializer!(Int64Type, put_i64_le);
native_serializer!(Float32Type, put_f32_le);
native_serializer!(Float64Type, put_f64_le);

fn build_serializer<'a>(arr: &'a ArrayRef) -> Box<dyn BinaryColumnSerializer + 'a> {
    // Override for string/binary, we can copy the bytes directly
    match arr.data_type() {
        DataType::Binary => Box::new(arr.as_binary::<i32>()),
        DataType::LargeBinary => Box::new(arr.as_binary::<i64>()),
        DataType::Utf8 => Box::new(arr.as_string::<i32>()),
        DataType::LargeUtf8 => Box::new(arr.as_string::<i64>()),

        DataType::Int8 => Box::new(arr.as_primitive::<Int8Type>()),
        DataType::UInt8 => Box::new(arr.as_primitive::<UInt8Type>()),
        DataType::Int16 => Box::new(arr.as_primitive::<Int16Type>()),
        DataType::UInt16 => Box::new(arr.as_primitive::<UInt16Type>()),
        DataType::Int32 => Box::new(arr.as_primitive::<Int32Type>()),
        DataType::UInt32 => Box::new(arr.as_primitive::<UInt32Type>()),
        DataType::Int64 => Box::new(arr.as_primitive::<Int64Type>()),
        DataType::UInt64 => Box::new(arr.as_primitive::<UInt64Type>()),
        DataType::Float32 => Box::new(arr.as_primitive::<Float32Type>()),
        DataType::Float64 => Box::new(arr.as_primitive::<Float64Type>()),

        // TODO: timestamps........
        other => unimplemented!("no binary serializer for type: {:?}", other),
    }
}

pub struct BinaryProtocol;

#[async_trait::async_trait]
impl RowHandler for BinaryProtocol {
    async fn send_records_stream(
        conn: &mut MySQLConnection,
        mut results: SendableRecordBatchStream,
    ) -> Result<(), CommandPhaseError> {
        let mut buffer = BytesMut::new();

        while let Some(v) = results.next().await {
            let v = v?;
            let num_columns = v.columns().len();
            let mut nulls = NullBitmap::<ClientSide>::new(num_columns);
            let bitmap_len = NullBitmap::<ClientSide>::bitmap_len(num_columns);

            let mut columns = v
                .columns()
                .iter()
                .map(|col| build_serializer(col))
                .collect::<Vec<_>>();

            for pos in 0..v.num_rows() {
                buffer.put_u8(0); // Packet header
                buffer.put_bytes(0, bitmap_len); // Reserve size for null bitmap

                for col in columns.iter_mut() {
                    if col.is_null(pos) {
                        nulls.set(pos, true);
                    } else {
                        nulls.set(pos, false);
                        col.serialize(pos, &mut buffer).unwrap();
                    }
                }

                // Set null bitmap in packet
                buffer[1..(1 + bitmap_len)].copy_from_slice(nulls.as_ref());
                conn.write_buf(&mut buffer)?;
            }
        }

        Ok(())
    }
}
