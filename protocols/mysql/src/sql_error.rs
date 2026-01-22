use bytes::BufMut;
use mysql_common::constants::CapabilityFlags;
use mysql_common::io::ParseBuf;
use mysql_common::packets::{ErrPacket, SqlState};
use mysql_common::proto::{MyDeserialize, MySerialize};

#[derive(Debug, Clone)]
pub struct SqlError {
    pub message: String,
    pub code: Option<u64>,
    pub state: Option<String>,
}

const EMPTY_SLICE: &[u8] = &[];

impl MySerialize for SqlError {
    fn serialize(&self, target: &mut Vec<u8>) {
        target.put_u8(0xFF); // Error packet header
        target.put_u16_le(self.code.map_or(0, |v| v as u16));

        // if self
        //    .capabilities()
        //    .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        // {
        let src_bytes = self
            .state
            .as_ref()
            .map(|s| s.as_bytes())
            .unwrap_or(EMPTY_SLICE);
        let mut bytes = [0u8; 5];
        for i in 0..5 {
            if i < src_bytes.len() {
                bytes[i] = src_bytes[i];
            } else {
                bytes[i] = 30u8; // Character '0'
            }
        }

        SqlState::new(bytes).serialize(target);
        // }
        target.put_slice(self.message.as_bytes());
    }
}

pub fn try_parse_error(capability_flags: CapabilityFlags, buf: &mut ParseBuf) -> Option<SqlError> {
    if *buf.0.first().unwrap_or(&0) != 0xFF {
        return None;
    }

    let error = ErrPacket::deserialize(capability_flags, buf).ok()?;
    if !error.is_error() {
        return None;
    }

    let error = error.server_error();
    Some(SqlError {
        message: String::from(error.message_str()),
        state: error
            .sql_state_ref()
            .map(|s| String::from(String::from_utf8_lossy(&s.as_bytes()[..]))),
        code: Some(error.error_code() as u64),
    })
}
