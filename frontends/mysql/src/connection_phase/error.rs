use crate::status::SqlError;
use mysql_common::constants::CapabilityFlags;
use mysql_common::proto::codec::error::PacketCodecError;
use std::io::Error;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum ConnectError {
    IoError(std::io::Error),
    PacketCodecError(PacketCodecError),
    SqlError(SqlError),
    MissingCapabilities(CapabilityFlags),
    InvalidString,
    IncompatibleAuthPlugin,
}

impl From<std::io::Error> for ConnectError {
    fn from(value: Error) -> Self {
        Self::IoError(value)
    }
}

impl From<PacketCodecError> for ConnectError {
    fn from(value: PacketCodecError) -> Self {
        Self::PacketCodecError(value)
    }
}

impl From<FromUtf8Error> for ConnectError {
    fn from(_value: FromUtf8Error) -> Self {
        Self::IncompatibleAuthPlugin
    }
}

impl From<&ConnectError> for SqlError {
    fn from(value: &ConnectError) -> Self {
        match value {
            ConnectError::SqlError(s) => s.clone(),
            value => SqlError {
                code: None,
                state: None,
                message: format!("{value:?}"),
            },
        }
    }
}

pub type ConnectResult<T> = Result<T, ConnectError>;
