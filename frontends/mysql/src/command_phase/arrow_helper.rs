use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use mysql_common::constants::{ColumnFlags, ColumnType};
use mysql_common::io::ParseBuf;

fn unexpected_buf_eof() -> DataFusionError {
    DataFusionError::Execution("unexpected buf eof".to_string())
}

pub(crate) fn deserialize_parameter(
    parse_buf: &mut ParseBuf,
    declared_column_type: ColumnType,
    column_flags: ColumnFlags,
    expected_data_type: &DataType,
) -> datafusion::common::Result<ScalarValue> {
    let mut read_byte_array = || {
        parse_buf
            .checked_eat_lenenc_str()
            .ok_or_else(unexpected_buf_eof)
    };

    let is_unsigned = expected_data_type.is_unsigned_integer()
        || column_flags.contains(ColumnFlags::UNSIGNED_FLAG);

    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
    let column = match declared_column_type {
        ColumnType::MYSQL_TYPE_NULL => ScalarValue::Null,
        ColumnType::MYSQL_TYPE_BIT => {
            ScalarValue::Boolean(Some(!read_byte_array()?.iter().all(|&b| b == 0)))
        }
        ColumnType::MYSQL_TYPE_TINY => {
            if is_unsigned {
                ScalarValue::UInt8(Some(parse_buf.eat_u8()))
            } else {
                ScalarValue::Int8(Some(parse_buf.eat_i8()))
            }
        }
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            if is_unsigned {
                ScalarValue::UInt16(Some(parse_buf.eat_u16_le()))
            } else {
                ScalarValue::Int16(Some(parse_buf.eat_i16_le()))
            }
        }
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            if is_unsigned {
                ScalarValue::UInt32(Some(parse_buf.eat_u32_le()))
            } else {
                ScalarValue::Int32(Some(parse_buf.eat_i32_le()))
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if is_unsigned {
                ScalarValue::UInt64(Some(parse_buf.eat_u64_le()))
            } else {
                ScalarValue::Int64(Some(parse_buf.eat_i64_le()))
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT => ScalarValue::Float32(Some(parse_buf.eat_f32_le())),
        ColumnType::MYSQL_TYPE_DOUBLE => ScalarValue::Float64(Some(parse_buf.eat_f64_le())),

        ColumnType::MYSQL_TYPE_DATE => todo!("date"),
        ColumnType::MYSQL_TYPE_TIME => todo!("time"),
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => todo!("timestamp"),
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => todo!("decimals"),

        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING => {
            let str = read_byte_array()?.to_vec();
            let str = String::from_utf8(str).map_err(|_| {
                DataFusionError::Execution("Could not convert string to UTF8".to_string())
            })?;
            ScalarValue::Utf8(Some(str))
        }
        ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            ScalarValue::Binary(Some(read_byte_array()?.to_vec()))
        }
        typ => unimplemented!("Unsupported column type {:?}", typ),
    };

    column.cast_to(expected_data_type)
}
