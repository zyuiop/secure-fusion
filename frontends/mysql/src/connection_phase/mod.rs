use crate::connection_phase::connection_phase_state::ConnectionPhaseState;
use crate::connection_phase::error::ConnectResult;
use common::AuthenticationHandler;
use mysql_common::constants::CapabilityFlags;
use mysql_interop::connection::MySQLConnection;
use std::sync::Arc;

mod auth;
mod client_handshake;
mod connection_phase_state;
pub mod error;

pub async fn handle_connection_sequence(
    client: MySQLConnection,
    auth_handler: Arc<dyn AuthenticationHandler + Sync + Send>,
) -> ConnectResult<(MySQLConnection, ConnectionResponse)> {
    let mut state = ConnectionPhaseState::new(client, auth_handler);
    match state.do_handle_connection_sequence().await {
        Ok(r) => {
            let client = state.finish();
            Ok((client, r))
        }
        Err(e) => {
            state.handle_error(&e);
            Err(e)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionResponse {
    pub capability_flags: CapabilityFlags,
    pub _max_allowed_packet: usize,
    pub initial_db: Option<String>,
}
