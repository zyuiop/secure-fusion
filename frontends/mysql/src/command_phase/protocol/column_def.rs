use crate::command_phase::protocol::charset::CharsetOps;
use crate::command_phase::protocol::field_type::DataTypeOps;
use common::charset::Charset;
use common::metadata::{MetadataReader, MetadataReads};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use mysql_common::collations::CollationId;
use mysql_common::constants::{ColumnFlags, ColumnType};
use mysql_common::misc::raw::int::LeU16;
use mysql_common::misc::raw::int::LeU32;
use mysql_common::misc::raw::int::LenEnc;
use mysql_common::misc::raw::{Const, RawBytes, RawInt, Skip};
use mysql_common::packets::{ColumnDefinitionCatalog, FixedLengthFieldsLen};
use mysql_common::proto::MySerialize;

pub struct FieldRefSerializer<'a>(pub &'a FieldRef);

const EMPTY_BUF: [u8; 0] = [];

impl<'a> MySerialize for FieldRefSerializer<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let metadata = MetadataReader(self.0.metadata());

        let column_type = ColumnType::from(DataTypeOps(self.0.data_type()));
        let column_charset = CollationId::from(CharsetOps(
            metadata
                .charset()
                .unwrap_or(Charset::Utf8ExtendedCaseInsensitive),
        ));
        let column_charset = column_charset as u16;

        let decimals = match self.0.data_type() {
            DataType::Decimal128(_, dec) | DataType::Decimal256(_, dec) => *dec,
            _ => 0,
        };

        // https://mariadb.com/kb/en/result-set-packets/#column-definition-packet
        // https://docs.rs/mysql_common/latest/src/mysql_common/packets/mod.rs.html#216-227
        ColumnDefinitionCatalog::default().serialize(buf);
        // Schema and table are always empty (todo: include in metadata?)
        RawBytes::<LenEnc>::from(
            metadata
                .schema()
                .map(|s| s.as_bytes())
                .unwrap_or(&EMPTY_BUF),
        )
        .serialize(buf); // Schema
        RawBytes::<LenEnc>::from(metadata.table().map(|s| s.as_bytes()).unwrap_or(&EMPTY_BUF))
            .serialize(buf); // Table
        RawBytes::<LenEnc>::from(metadata.table().map(|s| s.as_bytes()).unwrap_or(&EMPTY_BUF))
            .serialize(buf); // Table (physical)
        RawBytes::<LenEnc>::from(self.0.name().as_bytes()).serialize(buf); // Column name
        RawBytes::<LenEnc>::from(self.0.name().as_bytes()).serialize(buf); // Column name (physical)

        FixedLengthFieldsLen::default().serialize(buf);

        RawInt::<LeU16>::new(column_charset).serialize(buf);
        RawInt::<LeU32>::new(metadata.column_length().unwrap_or(12)).serialize(buf);
        Const::<ColumnType, u8>::new(column_type).serialize(buf);
        Const::<ColumnFlags, LeU16>::new(ColumnFlags::default()).serialize(buf); // TODO FLAGS?
        RawInt::<u8>::new(decimals as u8).serialize(buf);
        Skip::<2>.serialize(buf);
    }
}
