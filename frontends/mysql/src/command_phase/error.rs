use crate::command_phase::statements::StatementId;
use common::HandlerError;
use common::parser::ParseError;
use datafusion::common::DataFusionError;
use mysql_common::proto::codec::error::PacketCodecError;
use mysql_interop::constants::CommandId;
use mysql_interop::sql_error::SqlError;
use num_enum::TryFromPrimitiveError;

#[derive(Debug)]
pub enum CommandPhaseError {
    IoError(std::io::Error),
    PacketCodecError(PacketCodecError),
    SqlError(SqlError),
    DataFusionError(DataFusionError),
    UnknownCommandId(u8),
    UnknownStatement(StatementId),
    UnhandledCommand(CommandId),
    ParserError(ParseError),
    OtherError(String),
}

impl From<std::io::Error> for CommandPhaseError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<PacketCodecError> for CommandPhaseError {
    fn from(value: PacketCodecError) -> Self {
        Self::PacketCodecError(value)
    }
}

impl From<DataFusionError> for CommandPhaseError {
    fn from(value: DataFusionError) -> Self {
        Self::DataFusionError(value)
    }
}

impl From<TryFromPrimitiveError<CommandId>> for CommandPhaseError {
    fn from(value: TryFromPrimitiveError<CommandId>) -> Self {
        Self::UnknownCommandId(value.number)
    }
}

impl From<ParseError> for CommandPhaseError {
    fn from(value: ParseError) -> Self {
        Self::ParserError(value)
    }
}

impl From<HandlerError> for CommandPhaseError {
    fn from(value: HandlerError) -> Self {
        match value {
            HandlerError::DatafusionError(e) => Self::DataFusionError(e),
            HandlerError::CustomStatement => Self::OtherError("server error".to_string()),
            // TODO: convert to sql error
            HandlerError::InvalidParameter => Self::OtherError("invalid parameter".to_string()),
        }
    }
}

impl From<CommandPhaseError> for SqlError {
    fn from(value: CommandPhaseError) -> Self {
        match value {
            CommandPhaseError::SqlError(s) => s,
            value => {
                let mut message = format!("{value:?}");
                message.truncate(256);

                SqlError {
                    code: Some(1),
                    state: Some("HY000".to_string()),
                    message,
                }
            }
        }
    }
}

pub type CommandPhaseResult<T> = Result<T, CommandPhaseError>;
