use crate::conversions::column_def_ext::ColumnDefExt;
use crate::metadata::MetadataWrites;
use datafusion::arrow;
use datafusion::arrow::datatypes::DataType::{Decimal128, Decimal256};
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, IntervalUnit, Schema, TimeUnit,
};
use datafusion::common::not_impl_err;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{BinaryLength, ColumnDef, ExactNumberInfo, TimezoneInfo};

impl ArrowDatatypeConverter {
    /// Taken and upgraded from DataFusion [datafusion::sql::planner::SqlToRel]
    pub fn convert_data_type(
        &self,
        sql_type: &ast::DataType,
    ) -> datafusion::common::Result<DataType> {
        match sql_type {
            ast::DataType::Character(_) |
            ast::DataType::Char(_) |
            ast::DataType::CharacterVarying(_) |
            ast::DataType::CharVarying(_) |
            ast::DataType::Varchar(_) |
            ast::DataType::Nvarchar(_) |
            ast::DataType::Clob(_) |
            ast::DataType::JSON |
            ast::DataType::CharacterLargeObject(_) |
            ast::DataType::CharLargeObject(_) |
            /* TODO: uuid encoding may depend on dialect */
            ast::DataType::Uuid |
            ast::DataType::Text |
            ast::DataType::TinyText |
            ast::DataType::MediumText |
            ast::DataType::LongText |
            ast::DataType::String(_)
            => {
                Ok(DataType::Utf8)
            }

            ast::DataType::Blob(Some(len)) |
            ast::DataType::Bytes(Some(len)) |
            ast::DataType::Binary(Some(len)) |
            ast::DataType::Varbinary(Some(BinaryLength::IntegerLength { length: len }))
            => {
                match i32::try_from(*len) {
                    Ok(_) => Ok(DataType::Binary),
                    Err(_) => Ok(DataType::LargeBinary),
                }
            }

            ast::DataType::Blob(_) |
            ast::DataType::Bytes(_) |
            ast::DataType::Bytea |
            ast::DataType::Binary(_) |
            ast::DataType::Varbinary(_) |
            ast::DataType::TinyBlob |
            ast::DataType::JSONB |
            ast::DataType::MediumBlob => {
                Ok(DataType::Binary)
            }

            ast::DataType::LongBlob => {
                Ok(DataType::LargeBinary)
            }

            ast::DataType::Boolean |
            ast::DataType::Bool => Ok(DataType::Boolean),

            ast::DataType::TinyInt(_) => Ok(DataType::Int8),

            ast::DataType::SmallInt(_) | ast::DataType::Int2(_) | ast::DataType::Int16 => Ok(DataType::Int16),

            ast::DataType::Int(_) |
            ast::DataType::MediumInt(_) |
            ast::DataType::Integer(_) |
            ast::DataType::SignedInteger |
            ast::DataType::Signed |
            ast::DataType::Int32 |
            ast::DataType::Int4(_) => {
                Ok(DataType::Int32)
            }

            ast::DataType::BigInt(_) |
            ast::DataType::Int8(_) |
            ast::DataType::Int64 => Ok(DataType::Int64),

            ast::DataType::TinyIntUnsigned(_) |
            ast::DataType::UTinyInt |
            ast::DataType::UInt8 => Ok(DataType::UInt8),

            ast::DataType::SmallIntUnsigned(_) |
            ast::DataType::USmallInt |
            ast::DataType::Int2Unsigned(_) |
            ast::DataType::UInt16 => {
                Ok(DataType::UInt16)
            }

            ast::DataType::BigIntUnsigned(_) |
            ast::DataType::UBigInt |
            ast::DataType::Int8Unsigned(_) |
            ast::DataType::UInt64 => {
                Ok(DataType::UInt64)
            }

            ast::DataType::Float(_) => Ok(DataType::Float32),

            ast::DataType::Real | ast::DataType::Float4 => Ok(DataType::Float32),

            ast::DataType::Double(_)
            | ast::DataType::DoublePrecision
            | ast::DataType::Float8 => Ok(DataType::Float64),

            ast::DataType::IntUnsigned(_)
            | ast::DataType::IntegerUnsigned(_)
            | ast::DataType::Unsigned
            | ast::DataType::UnsignedInteger
            | ast::DataType::MediumIntUnsigned(_)
            | ast::DataType::UInt32
            | ast::DataType::Int4Unsigned(_) => Ok(DataType::UInt32),

            ast::DataType::Timestamp(_, _) | ast::DataType::Datetime(_) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)), // MySQL only supports micro-second precision
            ast::DataType::Date => Ok(DataType::Date32),
            ast::DataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We don't support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!("Unsupported SQL type {sql_type:?}")
                }
            }
            ast::DataType::Time(Some(_), _) => {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            }

            ast::DataType::Numeric(exact_number_info) |
            ast::DataType::Dec(exact_number_info) |
            ast::DataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE),
                    ExactNumberInfo::Precision(precision) => (precision as u8, DECIMAL128_MAX_SCALE),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => (precision as u8, scale as i8)
                };

                Ok(Decimal128(precision, scale))
            }

            ast::DataType::BigNumeric(exact_number_info)
            | ast::DataType::BigDecimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE),
                    ExactNumberInfo::Precision(precision) => (precision as u8, DECIMAL256_MAX_SCALE),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => (precision as u8, scale as i8)
                };

                // TODO: verify this is correct, maybe it's Decimal128 in all cases
                Ok(Decimal256(precision, scale))
            }

            ast::DataType::Interval { .. } => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),

            ast::DataType::Enum(_, _) | ast::DataType::Set(_) => {
                Ok(DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                ))
            }

            ast::DataType::Regclass
            | ast::DataType::Custom(_, _)
            | ast::DataType::Array(_)
            | ast::DataType::Unspecified
            | ast::DataType::Datetime64(_, _)
            | ast::DataType::FixedString(_)
            | ast::DataType::Map(_, _)
            | ast::DataType::Tuple(_)
            | ast::DataType::Nested(_)
            | ast::DataType::Union(_)
            | ast::DataType::Nullable(_)
            | ast::DataType::LowCardinality(_)
            | ast::DataType::Trigger
            | ast::DataType::Bit(_)
            | ast::DataType::BitVarying(_)
            | ast::DataType::AnyType
            | ast::DataType::Table(_)
            | ast::DataType::VarBit(_)
            | ast::DataType::Int128
            | ast::DataType::Int256
            | ast::DataType::UInt128
            | ast::DataType::TimestampNtz
            | ast::DataType::TsVector
            | ast::DataType::TsQuery
            | ast::DataType::HugeInt
            | ast::DataType::UHugeInt
            | ast::DataType::UInt256
            | ast::DataType::Float32
            | ast::DataType::Date32
            | ast::DataType::Float64
            | ast::DataType::NamedTable { .. }
            | ast::DataType::DoublePrecisionUnsigned
            | ast::DataType::DecimalUnsigned(_)
            | ast::DataType::DecUnsigned(_)
            | ast::DataType::FloatUnsigned(_)
            | ast::DataType::RealUnsigned
            | ast::DataType::DoubleUnsigned(_)
            | ast::DataType::Struct(_, _)
            | ast::DataType::GeometricType(_) => {
                not_impl_err!("Unsupported SQL type {sql_type:?}")
            },


        }
    }

    /// Taken and upgraded from DataFusion [datafusion::sql::planner::SqlToRel]
    pub fn table_to_schema(&self, columns: &[ColumnDef]) -> datafusion::common::Result<Schema> {
        Ok(Schema::new(
            columns
                .iter()
                .map(|v| self.column_to_field(v))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    pub fn column_to_field(&self, column: &ColumnDef) -> datafusion::common::Result<Field> {
        let data_type = self.convert_data_type(&column.data_type)?;
        let mut field = Field::new(column.name.value.clone(), data_type, column.is_nullable());
        field.set_raw_source_type(column.data_type.to_string());

        if let Some(expr) = column.get_default_value() {
            field.set_default_value(expr.to_string());
        }

        if let Some(collation) = column.get_collation() {
            field.set_raw_collation(collation);
        }

        Ok(field)
    }

    pub fn column_to_datatype(
        &self,
        column: &ColumnDef,
    ) -> datafusion::common::Result<arrow::datatypes::DataType> {
        self.convert_data_type(&column.data_type)
    }
}

#[derive(Debug)]
pub struct ArrowDatatypeConverter;
