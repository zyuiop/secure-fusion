use datafusion::arrow::datatypes::DataType;
use mysql_common::constants::ColumnType;

#[repr(transparent)]
pub struct DataTypeOps<'a>(pub &'a DataType);

impl<'a> From<DataTypeOps<'a>> for ColumnType {
    fn from(value: DataTypeOps<'a>) -> Self {
        match value.0 {
            DataType::Null => ColumnType::MYSQL_TYPE_NULL,
            DataType::Boolean => ColumnType::MYSQL_TYPE_BIT,
            DataType::Int8 => ColumnType::MYSQL_TYPE_TINY,
            DataType::Int16 => ColumnType::MYSQL_TYPE_SHORT,
            DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
            DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
            DataType::UInt8 => ColumnType::MYSQL_TYPE_TINY, //TODO: switch to bigger types? (test if signs cause issues)
            DataType::UInt16 => ColumnType::MYSQL_TYPE_SHORT,
            DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
            DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
            DataType::Float16 => ColumnType::MYSQL_TYPE_FLOAT,
            DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
            DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
            DataType::Timestamp(_time_unit, _tz) => ColumnType::MYSQL_TYPE_TIMESTAMP,
            DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
            DataType::Date64 => ColumnType::MYSQL_TYPE_DATE,
            DataType::Time32(_) => ColumnType::MYSQL_TYPE_TIME,
            DataType::Time64(_) => ColumnType::MYSQL_TYPE_TIME,
            DataType::Duration(_) => ColumnType::MYSQL_TYPE_LONG,
            DataType::Interval(_) => ColumnType::MYSQL_TYPE_LONG,
            DataType::Binary => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::FixedSizeBinary(_) => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::LargeBinary => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::BinaryView => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::Utf8 => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::LargeUtf8 => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::Utf8View => ColumnType::MYSQL_TYPE_LONG_BLOB,
            DataType::Decimal32(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::Decimal64(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::Decimal128(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::Decimal256(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::List(_) => todo!(),
            DataType::ListView(_) => todo!(),
            DataType::FixedSizeList(_, _) => ColumnType::MYSQL_TYPE_VECTOR,
            DataType::LargeList(_) => todo!(),
            DataType::LargeListView(_) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Union(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}
