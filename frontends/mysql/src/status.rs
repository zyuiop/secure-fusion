use bytes::BufMut;
use mysql_common::constants::StatusFlags;
use mysql_common::misc::raw::int::{LeU16, LenEnc};
use mysql_common::misc::raw::{Const, RawBytes, RawInt};
use mysql_common::packets::{CommonOkPacket, OkPacketKind, ResultSetTerminator};
use mysql_common::proto::MySerialize;

pub use mysql_interop::sql_error::SqlError;

#[derive(Clone)]
pub enum SqlOk {
    /// Received to indicate that all the records have been received
    EndOfResults,
    // DetailedEndOfResults(DetailedEndOfResults),
    /// Received to indicate a success with no other info
    Ok,
    DetailedOk(DetailedOk),
}

#[derive(Clone)]
pub struct DetailedOk {
    pub affected_rows: Option<usize>,
    pub last_insert_id: Option<usize>,
    // TODO
}

// OK packet
pub struct CustomOkPacket<'a> {
    packet_header: u8,
    affected_rows: RawInt<LenEnc>,
    last_insert_id: RawInt<LenEnc>,
    status_flags: Const<StatusFlags, LeU16>,
    warnings: RawInt<LeU16>,
    info: RawBytes<'a, LenEnc>,
    session_state_info: RawBytes<'a, LenEnc>,
}

impl<'a> CustomOkPacket<'a> {
    pub fn new(
        affected_rows: u64,
        last_insert_id: u64,
        status_flags: StatusFlags,
        warnings: u16,
        info: Option<String>,
        session_state_info: Option<String>,
    ) -> Self {
        CustomOkPacket {
            packet_header: CommonOkPacket::HEADER,
            affected_rows: RawInt::new(affected_rows),
            last_insert_id: RawInt::new(last_insert_id),
            status_flags: Const::new(status_flags),
            info: if let Some(info) = info {
                RawBytes::from(info.into_bytes())
            } else {
                RawBytes::default()
            },
            session_state_info: if let Some(info) = session_state_info {
                RawBytes::from(info.into_bytes())
            } else {
                RawBytes::default()
            },
            warnings: RawInt::new(warnings),
        }
    }
    pub fn new_eof(
        status_flags: StatusFlags,
        warnings: u16,
        info: Option<String>,
        session_state_info: Option<String>,
    ) -> Self {
        CustomOkPacket {
            packet_header: ResultSetTerminator::HEADER,
            affected_rows: RawInt::new(0),
            last_insert_id: RawInt::new(0),
            status_flags: Const::new(status_flags),
            info: if let Some(info) = info {
                RawBytes::from(info.into_bytes())
            } else {
                RawBytes::default()
            },
            session_state_info: if let Some(info) = session_state_info {
                RawBytes::from(info.into_bytes())
            } else {
                RawBytes::default()
            },
            warnings: RawInt::new(warnings),
        }
    }
}

impl<'a> MySerialize for CustomOkPacket<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(self.packet_header);
        self.affected_rows.serialize(buf);
        self.last_insert_id.serialize(buf);
        self.status_flags.serialize(buf);
        self.warnings.serialize(buf);

        // Not sure these should be included in all cases!
        self.info.serialize(buf);
        self.session_state_info.serialize(buf);
    }
}

pub struct NewEofPacket {
    pub warnings: u16,
    pub status_flags: StatusFlags,
}

impl MySerialize for NewEofPacket {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(0xFE);
        buf.put_u16_le(self.warnings);
        buf.put_u16_le(self.status_flags.into());
    }
}
