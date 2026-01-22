mod binary_protocol;
mod text_row_handler;

use crate::command_phase::error::CommandPhaseError;
use datafusion::execution::SendableRecordBatchStream;
use mysql_interop::connection::MySQLConnection;

pub(super) use binary_protocol::BinaryProtocol;
pub(super) use text_row_handler::TextRowHandler;

#[async_trait::async_trait]
pub trait RowHandler {
    async fn send_records_stream(
        conn: &mut MySQLConnection,
        results: SendableRecordBatchStream,
    ) -> Result<(), CommandPhaseError>;
}
