use log::debug;
use mysql_common::constants::CapabilityFlags;
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::RawBytes;
use mysql_common::misc::raw::bytes::EofBytes;
use mysql_common::proto::MyDeserialize;
use mysql_interop::constants::CommandId;
use std::io;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Query,
    PreparedStatement,
}

// Query/PrepareStatement packet (de)serializer
#[derive(Debug, Clone, PartialEq)]
pub struct ComQuery<'a> {
    pub query_type: QueryType,
    pub query: RawBytes<'a, EofBytes>,
}

impl<'de> MyDeserialize<'de> for ComQuery<'de> {
    const SIZE: Option<usize> = None; // TODO, find correct size if any
    type Ctx = CapabilityFlags;

    fn deserialize(flags: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let command = buf.eat_u8();
        let command = CommandId::try_from(command)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

        if command == CommandId::CmdQuery
            && flags.contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES)
        {
            // Read param count/param sets
            let param_count = buf.eat_lenenc_int();
            let _ = buf.eat_lenenc_int();

            if param_count > 0 {
                debug!("[debug] full packet: {:?}", buf.0.to_vec());
                todo!("Client parameters are not supported.")
            }
        }

        let query = buf.parse(())?;
        let query_type = match command {
            CommandId::CmdQuery => QueryType::Query,
            CommandId::CmdStmtPrepare => QueryType::PreparedStatement,
            v => panic!("Unexpected ComQuery header {v:?}"),
        };

        Ok(Self { query, query_type })
    }
}
