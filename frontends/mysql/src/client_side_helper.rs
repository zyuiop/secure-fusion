use super::status::{CustomOkPacket, NewEofPacket, SqlError, SqlOk};
use mysql_common::constants::{CapabilityFlags, StatusFlags};
use mysql_common::packets::{OkPacketKind, ResultSetTerminator};
use mysql_interop::ConnectionWrapper;

pub trait ClientHelper: ConnectionWrapper {
    fn capabilities(&self) -> CapabilityFlags;

    fn handle_error(&mut self, error: SqlError) {
        self.send_packet(&error);
        self.connection().reset_seqno();
    }

    fn handle_ok(&mut self, ok: SqlOk, status_flags: StatusFlags) {
        // TODO: rewrite this block with less SqlOk types (ideally, only two), using the state to determine which OK should be sent
        match ok {
            SqlOk::EndOfResults
                if !self
                    .capabilities()
                    .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) =>
            {
                if self
                    .capabilities()
                    .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
                {
                    self.send_packet(&NewEofPacket {
                        warnings: 0,
                        status_flags,
                    })
                } else {
                    self.send_packet_raw(&[ResultSetTerminator::HEADER][..])
                }
            }
            SqlOk::EndOfResults => {
                self.send_packet(&CustomOkPacket::new_eof(
                    status_flags,
                    0,
                    None,
                    None, // TODO
                ))
            }
            SqlOk::Ok => self.send_packet(&CustomOkPacket::new(
                0,
                0,
                status_flags,
                0,
                None,
                None, // TODO
            )),
            SqlOk::DetailedOk(ok_data) => self.send_packet(&CustomOkPacket::new(
                ok_data.affected_rows.unwrap_or(0) as u64,
                ok_data.last_insert_id.unwrap_or(0) as u64,
                status_flags,
                0,
                None,
                None, // TODO
            )),
        }
    }
}
