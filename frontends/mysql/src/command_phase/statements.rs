use crate::command_phase::client_side_wrapper::ClientWrapper;
use crate::command_phase::error::{CommandPhaseError, CommandPhaseResult};
use crate::command_phase::protocol::column_def::FieldRefSerializer;
use crate::command_phase::protocol::statement_packet::StatementPacket;
use common::{ProxyImplementation, StatementResponse};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use mysql_common::constants::CapabilityFlags;
use mysql_common::packets::{OkPacketKind, OldEofPacket};
use mysql_interop::ConnectionWrapper;
use rustc_hash::FxHashMap;

pub(super) type StatementId = u32;

#[derive(Clone)]
pub(super) struct SavedStatement {
    pub(super) statement_id: StatementId,
    pub(super) backend_statement_id: common::StatementId,
    /* parameter parser, columns, ... */
    pub(super) should_return_ok: bool,
    pub(super) parameters: Vec<DataType>,
    pub(super) columns: Vec<FieldRef>,
}

impl SavedStatement {
    #[allow(unused)]
    pub(crate) fn new_from_statement(
        statement_id: StatementId,
        should_return_ok: bool,
        backend_statement: StatementResponse,
    ) -> Self {
        let parameters = (0..backend_statement.parameters.len())
            .map(|index| backend_statement.parameters[&format!("${}", index + 1)].clone())
            .collect();

        Self {
            statement_id,
            should_return_ok,
            backend_statement_id: backend_statement.statement_id,
            columns: backend_statement.columns,
            parameters,
        }
    }

    #[allow(unused)]
    pub fn send_to_client<T: ProxyImplementation>(
        &self,
        client: &mut ClientWrapper<T>,
    ) -> CommandPhaseResult<()> {
        client.send_packet(&StatementPacket {
            statement_id: self.statement_id,
            num_params: self.parameters.len() as u16,
            num_columns: self.columns.len() as u16,
        });

        if !self.parameters.is_empty() {
            for param in self.parameters.iter() {
                let field_ref = FieldRef::new(Field::new("", param.clone(), true));

                client.send_packet(&FieldRefSerializer(&field_ref))
            }

            if !client
                .capabilities
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                // Send EOF
                client.send_packet_raw(&mut &[OldEofPacket::HEADER][..])
            }
        }

        if !self.columns.is_empty() {
            for col in self.columns.iter() {
                client.send_packet(&FieldRefSerializer(col));
            }

            if !client
                .capabilities
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                // Send EOF
                client.send_packet_raw(&mut &[OldEofPacket::HEADER][..])
            }
        }
        Ok(())
    }
}

/// This struct is used to hold the statements details local to this frontend
/// Operations on statements must also be forwarded to the backend for processing
#[allow(unused)]
pub(super) struct PreparedStatementsHolder {
    saved_statements: FxHashMap<StatementId, SavedStatement>,
    next_statement_id: StatementId,
}

impl PreparedStatementsHolder {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            saved_statements: FxHashMap::default(),
            next_statement_id: 0,
        }
    }

    #[allow(unused)]
    pub fn get_statement(&self, statement_id: StatementId) -> CommandPhaseResult<&SavedStatement> {
        self.saved_statements
            .get(&statement_id)
            .ok_or(CommandPhaseError::UnknownStatement(statement_id))
    }

    /// Removes a statement for the list and returns it.
    ///
    /// The statement must also be closed on the respective backend.
    #[allow(unused)]
    pub fn close_statement(
        &mut self,
        statement_id: StatementId,
    ) -> CommandPhaseResult<SavedStatement> {
        self.saved_statements
            .remove(&statement_id)
            .ok_or(CommandPhaseError::UnknownStatement(statement_id))
    }

    #[allow(unused)]
    pub fn create_statement(
        &mut self,
        should_return_ok: bool,
        statement: common::StatementResponse,
    ) -> (StatementId, &SavedStatement) {
        let initial_statement_id = self.next_statement_id.wrapping_sub(1);
        while self.next_statement_id != initial_statement_id {
            self.next_statement_id = self.next_statement_id.wrapping_add(1);
            if !self.saved_statements.contains_key(&self.next_statement_id) {
                let statement = SavedStatement::new_from_statement(
                    self.next_statement_id,
                    should_return_ok,
                    statement,
                );
                self.saved_statements
                    .insert(self.next_statement_id, statement);
                return (
                    self.next_statement_id,
                    &self.saved_statements[&self.next_statement_id],
                );
            }
        }
        panic!("no free statement id")
    }
}
