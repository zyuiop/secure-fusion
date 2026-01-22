use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Debug, Eq, IntoPrimitive, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum CommandId {
    CmdOk = 0x00,
    CmdQuit = 0x01,
    CmdInitDb = 0x02,
    CmdQuery = 0x03,
    CmdFieldList = 0x04,
    CmdPing = 0x0E,
    CmdStmtPrepare = 0x16,
    CmdStmtExecute = 0x17,
    CmdStmtClose = 0x19,
    CmdStmtReset = 0x1A,
    // https://mariadb.com/docs/server/reference/clientserver-protocol/2-text-protocol/com_set_option
    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_set_option.html
    CmdSetOption = 0x1B,
    MagicTextNull = 0xFB,
}
