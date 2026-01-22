use super::connection_phase_state::ClientSide;
use crate::connection_phase::error::{ConnectError, ConnectResult};
use common::{AuthenticationHandler, LoginMethod};
use log::debug;
use mysql_common::packets::{AuthPlugin, AuthSwitchRequest};
use mysql_interop::ConnectionWrapper;
use rand_core::{OsRng, TryRngCore};
use std::borrow::Cow;
use std::sync::Arc;

pub struct AuthData(Vec<u8>);

impl AuthData {
    pub fn scramble_1(&self) -> &[u8; 8] {
        self.0.first_chunk().unwrap()
    }

    pub fn scramble_2(&self) -> Option<&[u8]> {
        if self.0.len() > 8 {
            Some(&self.0.as_slice()[8..])
        } else {
            None
        }
    }

    pub fn all(&self) -> &[u8] {
        self.0.as_slice()
    }
}

#[async_trait::async_trait]
pub trait AuthPluginExt {
    fn name(&self) -> &[u8];

    fn gen_initial_data(&self) -> ConnectResult<AuthData>;

    fn is_auth_switch(&self, other: &dyn AuthPluginExt) -> bool;

    async fn finish(
        &self,
        auth_handler: Arc<dyn AuthenticationHandler + Sync + Send>,
        client_auth_data: AuthData,
        username: &[u8],
        password_data: &[u8],
    ) -> ConnectResult<()>;

    async fn handle_switch_exchange(
        &self,
        auth_handler: Arc<dyn AuthenticationHandler + Sync + Send>,
        username: &[u8],
        conn: &mut ClientSide,
    ) -> ConnectResult<()>;
}

#[async_trait::async_trait]
impl<'a> AuthPluginExt for AuthPlugin<'a> {
    fn name(&self) -> &[u8] {
        self.as_bytes()
    }

    fn gen_initial_data(&self) -> ConnectResult<AuthData> {
        match self {
            // AuthPlugin::MysqlOldPassword => {}
            AuthPlugin::MysqlClearPassword => Ok(AuthData(vec![0u8; 20])),
            // https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__native__password_8cc.html
            // https://github.com/mysql/mysql-server/blob/ff05628a530696bc6851ba6540ac250c7a059aa7/libmysql/authentication_native_password/mysql_native_password.cc#L4
            AuthPlugin::MysqlNativePassword => {
                // https://dev.mysql.com/doc/dev/mysql-server/8.4.6/page_protocol_connection_phase_authentication_methods_native_password_authentication.html
                // Removed in MySQL 9
                let mut bytes = vec![0u8; 21];
                OsRng
                    .try_fill_bytes(&mut bytes)
                    .expect("could not generate random data!");
                bytes[20] = 0;
                Ok(AuthData(bytes))
            }
            // AuthPlugin::CachingSha2Password => {}
            // AuthPlugin::Ed25519 => {}
            // AuthPlugin::Other(_) => {}
            _ => Err(ConnectError::IncompatibleAuthPlugin),
        }
    }

    fn is_auth_switch(&self, other: &dyn AuthPluginExt) -> bool {
        self.name() != other.name()
    }

    async fn finish(
        &self,
        auth_handler: Arc<dyn AuthenticationHandler + Sync + Send>,
        client_auth_data: AuthData,
        username: &[u8],
        password_data: &[u8],
    ) -> ConnectResult<()> {
        let username = String::from_utf8_lossy(username);
        match self {
            AuthPlugin::MysqlClearPassword => {
                auth_handler
                    .try_login(
                        &username,
                        LoginMethod::Cleartext {
                            password: String::from_utf8(password_data.to_vec())?,
                        },
                    )
                    .await
                    .map_err(|_| ConnectError::IncompatibleAuthPlugin) /* TODO */
            }
            AuthPlugin::MysqlNativePassword => {
                auth_handler
                    .try_login(
                        &username,
                        LoginMethod::MySqlNativePassword {
                            password: password_data.to_vec(),
                            scramble_data: client_auth_data.all().to_vec(),
                        },
                    )
                    .await
                    .map_err(|_| ConnectError::IncompatibleAuthPlugin) /* TODO */
            }
            AuthPlugin::MysqlOldPassword => todo!(),
            AuthPlugin::CachingSha2Password => todo!(),
            AuthPlugin::Ed25519 => todo!(),
            AuthPlugin::Other(_) => todo!(),
        }
    }

    async fn handle_switch_exchange(
        &self,
        auth_handler: Arc<dyn AuthenticationHandler + Sync + Send>,
        username: &[u8],
        conn: &mut ClientSide,
    ) -> ConnectResult<()> {
        // Detect unsupported auth methods
        if self == &AuthPlugin::MysqlNativePassword {
            // Fallback on another plugin
            return AuthPlugin::MysqlClearPassword
                .handle_switch_exchange(auth_handler, username, conn)
                .await;
        }

        debug!(
            "Switching authentication to {}",
            String::from_utf8_lossy(self.name())
        );

        let auth_data = self.gen_initial_data()?;
        let change_packet =
            AuthSwitchRequest::new(Cow::Borrowed(self.name()), Cow::Borrowed(auth_data.all()));
        conn.send_packet(&change_packet);

        // Expect a response
        let response = conn.receive_next_packet()?;
        self.finish(auth_handler, auth_data, username, &response)
            .await
    }
}
