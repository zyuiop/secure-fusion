use super::client_handshake::CustomClientHandshake;
use crate::client_side_helper::ClientHelper;
use crate::connection_phase::ConnectionResponse;
use crate::connection_phase::auth::{AuthData, AuthPluginExt};
use crate::connection_phase::error::{ConnectError, ConnectResult};
use crate::status::{DetailedOk, SqlOk};
use common::AuthenticationHandler;
use log::info;
use mysql_common::collations::CollationId;
use mysql_common::constants::{CapabilityFlags, DEFAULT_MAX_ALLOWED_PACKET, StatusFlags};
use mysql_common::io::ParseBuf;
use mysql_common::packets::{AuthPlugin, HandshakePacket};
use mysql_common::proto::MyDeserialize;
use mysql_common::proto::codec::error::PacketCodecError;
use mysql_interop::ConnectionWrapper;
use mysql_interop::connection::{MySQLConnection, WrappedBuffer};
use std::cmp::min;
use std::sync::Arc;

pub(super) struct ConnectionPhaseState {
    client: ClientSide,
    initial_db: Option<String>,
    max_packet_size: usize,
    auth_backend: Arc<dyn AuthenticationHandler + Sync + Send>,
}
pub(crate) struct ClientSide {
    conn: MySQLConnection,
    capabilities: Option<CapabilityFlags>,
    auth_method: AuthPlugin<'static>,
}

impl ConnectionWrapper for ClientSide {
    fn connection(&mut self) -> &mut MySQLConnection {
        &mut self.conn
    }
}

impl ClientHelper for ClientSide {
    fn capabilities(&self) -> CapabilityFlags {
        self.capabilities.unwrap_or_default()
    }
}

impl ClientSide {
    // Slightly different implementations from the server side
    // Not needed elsewhere, we only expect packets from clients during the connection protocol
    pub(crate) fn receive_next_packet(&mut self) -> Result<WrappedBuffer, PacketCodecError> {
        let packet = self.connection().read();

        packet.expect("no next packet to receive in connection") // TODO: find something to do with missing options
    }
}

#[inline(always)]
fn required_capabilities() -> CapabilityFlags {
    CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_PLUGIN_AUTH
}

#[inline(always)]
fn supported_capabilities() -> CapabilityFlags {
    required_capabilities()
        | CapabilityFlags::CLIENT_LONG_PASSWORD
        | CapabilityFlags::CLIENT_LONG_FLAG
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_LOCAL_FILES
        | CapabilityFlags::CLIENT_INTERACTIVE
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_MULTI_STATEMENTS
        | CapabilityFlags::CLIENT_MULTI_RESULTS
        | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
        | CapabilityFlags::CLIENT_CONNECT_ATTRS
        | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CapabilityFlags::CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
        | CapabilityFlags::CLIENT_SESSION_TRACK
}

impl ConnectionPhaseState {
    pub(super) fn handle_error(&mut self, error: &ConnectError) {
        self.client.handle_error(error.into());
    }

    pub(super) fn new(
        client: MySQLConnection,
        auth_backend: Arc<dyn AuthenticationHandler + Sync + Send>,
    ) -> Self {
        ConnectionPhaseState {
            client: ClientSide {
                conn: client,
                capabilities: None,
                auth_method: AuthPlugin::MysqlNativePassword,
            },
            auth_backend,
            initial_db: None,
            // TODO: destructure handshake to get this value
            max_packet_size: DEFAULT_MAX_ALLOWED_PACKET,
        }
    }

    async fn client_authentication(&mut self, auth_data: AuthData) -> ConnectResult<()> {
        let packet = self.client.receive_next_packet()?;
        info!("client handshake received: {}", hex::encode(&packet));

        let mut parsebuf = ParseBuf(&packet);
        let handshake =
            CustomClientHandshake::deserialize(supported_capabilities(), &mut parsebuf)?;
        info!("client handshake parsed");

        let client_caps = handshake.capabilities();
        let client_caps = client_caps.intersection(supported_capabilities());

        let missing_required = required_capabilities().difference(client_caps);

        if missing_required != CapabilityFlags::empty() {
            return Err(ConnectError::MissingCapabilities(missing_required));
        }

        self.client.capabilities = Some(client_caps);
        self.max_packet_size = min(self.max_packet_size, handshake.max_packet_size.0 as usize);

        self.initial_db = handshake
            .db_name()
            .and_then(|v| String::from_utf8(Vec::from(v)).ok());

        let other_auth_plugin = handshake
            .auth_plugin
            .as_ref()
            .filter(|v| self.client.auth_method.is_auth_switch(*v));

        let user = handshake.user();
        if let Some(other_plugin) = other_auth_plugin {
            other_plugin
                .handle_switch_exchange(self.auth_backend.clone(), user, &mut self.client)
                .await
        } else {
            let password_data = handshake.scramble_buf();

            info!("Received login data: {user:?} {password_data:?}");

            self.client
                .auth_method
                .finish(self.auth_backend.clone(), auth_data, user, password_data)
                .await
        }
    }

    fn send_server_handshake(&mut self) -> ConnectResult<AuthData> {
        let auth_data = self.client.auth_method.gen_initial_data()?;

        let handshake = HandshakePacket::new(
            10,
            "0.1.0".as_bytes(),
            1, // TODO: do we need to forward this info for any reason?
            *auth_data.scramble_1(),
            auth_data.scramble_2(),
            supported_capabilities(),
            CollationId::UTF8MB3_BIN as u16 as u8,
            StatusFlags::empty(),
            Some(self.client.auth_method.name()),
        )
        .into_owned();

        self.client.send_packet(&handshake);
        Ok(auth_data)
    }

    pub(super) async fn do_handle_connection_sequence(
        &mut self,
    ) -> ConnectResult<ConnectionResponse> {
        // Step 1: server sends a handshake
        let auth_data = self.send_server_handshake()?;

        // Step 2: client sends handshake response
        self.client_authentication(auth_data).await?;

        // Step 3: send an OK packet
        self.client.handle_ok(
            SqlOk::DetailedOk(DetailedOk {
                last_insert_id: None,
                affected_rows: None,
            }),
            StatusFlags::empty(),
        );

        // Step 4: Reset protocol
        self.client.conn.reset_seqno();

        Ok(ConnectionResponse {
            capability_flags: self.client.capabilities.unwrap(),
            initial_db: self.initial_db.clone(),
            _max_allowed_packet: self.max_packet_size,
        })
    }

    pub(super) fn finish(self) -> MySQLConnection {
        self.client.conn
    }
}
