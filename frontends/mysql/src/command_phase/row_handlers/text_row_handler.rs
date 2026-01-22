use crate::command_phase::error::CommandPhaseError;
use crate::command_phase::row_handlers::RowHandler;
use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::array::{BooleanArray, GenericByteArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    ArrowNativeType, ByteArrayType, DataType, GenericBinaryType, GenericStringType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use mysql_common::io::BufMutExt;
use mysql_interop::connection::MySQLConnection;
use std::fmt::Write;

pub const MAGIC_TEXT_NULL: u8 = 0xFB;

pub struct TextRowHandler;

trait TextColumnSerializer {
    fn serialize_next(&mut self, out: &mut BytesMut) -> Result<(), ArrowError>;
}

fn serialize_nullable_string<OT: AsRef<[u8]>>(out: &mut BytesMut, nullable_string: Option<OT>) {
    match nullable_string {
        None => out.put_u8(MAGIC_TEXT_NULL),
        Some(e) => out.put_lenenc_str(e.as_ref()),
    }
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Debug)]
enum SizeHint {
    /// Value is less than 251B long
    Small,
    /// Value is between 251B and 65_536B long
    Medium,
    /// Value is between 65_536B and 16_777_216B long
    Large,
    /// Value is larger than 16_777_216B
    Huge,
}

impl SizeHint {
    #[inline(always)]
    pub const fn get_size_bytes(&self) -> usize {
        match self {
            SizeHint::Small => 1,
            SizeHint::Medium => 3,
            SizeHint::Large => 4,
            SizeHint::Huge => 9,
        }
    }

    #[inline(always)]
    pub const fn max_str_size(&self) -> usize {
        match self {
            SizeHint::Small => 251,
            SizeHint::Medium => 65_536,
            SizeHint::Large => 16_777_216,
            SizeHint::Huge => usize::MAX,
        }
    }

    #[inline(always)]
    pub const fn min_str_size(&self) -> usize {
        match self {
            SizeHint::Small => 0,
            SizeHint::Medium => SizeHint::Small.max_str_size(),
            SizeHint::Large => SizeHint::Medium.max_str_size(),
            SizeHint::Huge => SizeHint::Large.max_str_size(),
        }
    }

    #[inline(always)]
    pub const fn from_size(size: usize) -> Self {
        if size < SizeHint::Small.max_str_size() {
            SizeHint::Small
        } else if size < SizeHint::Medium.max_str_size() {
            SizeHint::Medium
        } else if size < SizeHint::Large.max_str_size() {
            SizeHint::Large
        } else {
            SizeHint::Huge
        }
    }
}

struct NullableStringFormatterSerializer<'a> {
    formatter: ArrayFormatter<'a>,
    nulls: Option<&'a NullBuffer>,
    len: usize,
    position: usize,
    size_hint: Box<dyn Fn(usize) -> SizeHint + 'a>,
}

impl<'a> TextColumnSerializer for NullableStringFormatterSerializer<'a> {
    fn serialize_next(&mut self, out: &mut BytesMut) -> Result<(), ArrowError> {
        if self.position >= self.len {
            return Ok(());
        }

        let size_hint = &self.size_hint;

        match self
            .nulls
            .map(|x| x.is_null(self.position))
            .unwrap_or_default()
        {
            true => out.put_u8(MAGIC_TEXT_NULL),
            false => {
                let size_hint = size_hint(self.position);
                let bytes_for_size = size_hint.get_size_bytes();

                let start_pos = out.len();
                out.put_bytes(0, bytes_for_size);

                self.formatter.value(self.position).write(out)?;

                let end_pos = out.len();
                let str_size = end_pos - start_pos - bytes_for_size;

                // We need to len-enc the size, so we need a slice where we can do that
                let mut size_target = if str_size >= size_hint.min_str_size()
                    && str_size < size_hint.max_str_size()
                {
                    // FAST CASE - we can directly write the length
                    &mut out[start_pos..(start_pos + bytes_for_size)]
                } else {
                    let actual_size_hint = SizeHint::from_size(str_size);
                    let new_bytes_for_size = actual_size_hint.get_size_bytes();

                    // Split contains a view of the string to copy
                    let split = out.split_off(start_pos + bytes_for_size);

                    // We need to set ourselves to the correct position (at new_bytes_for_size)
                    if new_bytes_for_size > bytes_for_size {
                        // We fill the missing bytes with 0s
                        out.put_bytes(0, new_bytes_for_size - bytes_for_size);
                    } else {
                        // We just drop the additional bytes
                        out.truncate(start_pos + new_bytes_for_size);
                    }
                    out.put(split);

                    // We can finally write the size
                    &mut out[start_pos..(start_pos + new_bytes_for_size)]
                };
                size_target.put_lenenc_int(str_size as u64);
            }
        }

        self.position += 1;
        Ok(())
    }
}

struct NullableCopyAsIsSerializer<'a, T: ByteArrayType> {
    array_ref: &'a GenericByteArray<T>,
    len: usize,
    position: usize,
}

impl<'a, T: ByteArrayType> NullableCopyAsIsSerializer<'a, T> {
    fn new(array_ref: &'a ArrayRef) -> Box<dyn TextColumnSerializer + 'a> {
        Box::new(Self {
            array_ref: array_ref
                .as_any()
                .downcast_ref()
                .expect("bug: invalid array type specified"),
            position: 0,
            len: array_ref.len(),
        })
    }
}

impl<'a, T: ByteArrayType> TextColumnSerializer for NullableCopyAsIsSerializer<'a, T> {
    fn serialize_next(&mut self, out: &mut BytesMut) -> Result<(), ArrowError> {
        if self.position >= self.len {
            return Ok(());
        }

        if self.array_ref.is_null(self.position) {
            out.put_u8(MAGIC_TEXT_NULL);
        } else {
            let value_length = self.array_ref.value_length(self.position).as_usize();
            let offset = self.array_ref.value_offsets()[self.position].as_usize();
            let offset_end = offset + value_length;

            let value = &self.array_ref.value_data()[offset..offset_end];
            let value = value.as_ref();
            out.put_lenenc_str(value);
        }

        self.position += 1;
        Ok(())
    }
}

struct BooleanDisplayIndex<'a>(&'a BooleanArray);

impl<'a> DisplayIndex for BooleanDisplayIndex<'a> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value = self.0.value(idx);
        if value {
            f.write_char('1')?
        } else {
            f.write_char('0')?
        };
        Ok(())
    }
}

// TODO: add timestamp format to options
const FORMAT_OPTIONS: FormatOptions =
    FormatOptions::new().with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.3f"));
// .with_timestamp_format()

fn build_serializer<'a>(arr: &'a ArrayRef) -> Box<dyn TextColumnSerializer + 'a> {
    // Override for string/binary, we can copy the bytes directly
    match arr.data_type() {
        DataType::Binary => return NullableCopyAsIsSerializer::<GenericBinaryType<i32>>::new(arr),
        DataType::LargeBinary => {
            return NullableCopyAsIsSerializer::<GenericBinaryType<i64>>::new(arr);
        }
        DataType::Utf8 => return NullableCopyAsIsSerializer::<GenericStringType<i32>>::new(arr),
        DataType::LargeUtf8 => {
            return NullableCopyAsIsSerializer::<GenericStringType<i64>>::new(arr);
        }
        _ => {}
    }

    let formatter = match arr.data_type() {
        DataType::Boolean => {
            ArrayFormatter::new(Box::new(BooleanDisplayIndex(arr.as_boolean())), false)
        }
        _ => ArrayFormatter::try_new(arr, &FORMAT_OPTIONS).unwrap(),
    };
    let nulls = arr.nulls();

    let hint = match arr.data_type() {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            Box::new(|_: usize| SizeHint::Small) as Box<dyn Fn(usize) -> SizeHint>
        }
        DataType::BinaryView => {
            let l = arr.as_binary_view();
            Box::new(|x: usize| SizeHint::from_size(l.value(x).len()))
        }
        DataType::FixedSizeBinary(size) => {
            let size = SizeHint::from_size(*size as usize);
            Box::new(move |_| size)
        }

        DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8 => {
            unreachable!()
        }
        _ => Box::new(|_| SizeHint::Small),
    };

    Box::new(NullableStringFormatterSerializer {
        len: arr.len(),
        size_hint: hint,
        position: 0,
        nulls,
        formatter,
    })
}

#[async_trait::async_trait]
impl RowHandler for TextRowHandler {
    async fn send_records_stream(
        conn: &mut MySQLConnection,
        mut results: SendableRecordBatchStream,
    ) -> Result<(), CommandPhaseError> {
        let mut buffer = BytesMut::new();

        while let Some(v) = results.next().await {
            let v = v?;
            let mut columns = v
                .columns()
                .iter()
                .map(|col| build_serializer(col))
                .collect::<Vec<_>>();

            for _ in 0..v.num_rows() {
                for col in columns.iter_mut() {
                    col.serialize_next(&mut buffer).unwrap();
                }

                conn.write_buf(&mut buffer)?;
            }
        }

        Ok(())
    }
}

impl TextRowHandler {
    pub async fn send_rows(
        conn: &mut MySQLConnection,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<(), CommandPhaseError> {
        let mut buffer = BytesMut::new();

        for row in rows {
            for col in row {
                serialize_nullable_string(&mut buffer, col);
            }
            conn.write_buf(&mut buffer)?;
        }

        Ok(())
    }
}
