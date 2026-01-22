use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::RawInt;
use mysql_common::misc::raw::int::LenEnc;
use mysql_common::proto::{MyDeserialize, MySerialize};
use std::io;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnCount {
    column_count: RawInt<LenEnc>,
}

impl ColumnCount {
    pub(crate) fn new(count: usize) -> Self {
        ColumnCount {
            column_count: RawInt::new(count as u64),
        }
    }
}

impl MySerialize for ColumnCount {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.column_count.serialize(buf);
    }
}

// TODO: remove? (if we don't use this as mysql backend)
impl<'de> MyDeserialize<'de> for ColumnCount {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(ColumnCount {
            column_count: buf.parse::<RawInt<LenEnc>>(())?,
        })
    }
}
