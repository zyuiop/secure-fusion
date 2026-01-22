use bytes::BufMut;
use mysql_common::proto::MySerialize;
use mysql_interop::constants::CommandId;

#[allow(unused)]
pub struct StatementPacket {
    pub statement_id: u32,
    pub num_columns: u16,
    pub num_params: u16,
}

impl MySerialize for StatementPacket {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(CommandId::CmdOk as u8);
        buf.put_u32_le(self.statement_id);
        buf.put_u16_le(self.num_columns);
        buf.put_u16_le(self.num_params);
        buf.put_u8(0); // reserved_1
        buf.put_u16_le(0); // warning count
    }
}
