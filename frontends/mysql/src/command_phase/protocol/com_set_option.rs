use mysql_common::constants::CapabilityFlags;
use mysql_common::io::ParseBuf;
use mysql_common::proto::MyDeserialize;
use mysql_interop::constants::CommandId;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::io;

#[derive(Debug, Clone, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u16)]
pub enum SetOption {
    MultiStatementsOn = 0,
    MultiStatementsOff = 1,
}

// Query/PrepareStatement packet (de)serializer
#[derive(Debug, Clone, PartialEq)]
pub struct ComSetOption {
    pub opt: SetOption,
}

impl<'de> MyDeserialize<'de> for ComSetOption {
    const SIZE: Option<usize> = Some(3);
    type Ctx = CapabilityFlags;

    fn deserialize(_flags: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let command = buf.eat_u8();
        let command = CommandId::try_from(command)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        assert_eq!(command, CommandId::CmdSetOption);

        let value = buf.eat_u16_le();
        let set_opt = SetOption::try_from(value).map_err(io::Error::other)?;

        Ok(Self { opt: set_opt })
    }
}
