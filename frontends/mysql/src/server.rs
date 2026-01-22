use crate::MySqlFrontend;
use crate::command_phase::HandleResult;
use crate::command_phase::client_side_wrapper::ClientWrapper;
use crate::connection_phase::handle_connection_sequence;
use common::{Frontend, ProxyImplementation};
use log::{error, info};
use mysql_interop::connection::MySQLConnection;
use std::net::TcpListener;
use std::os::linux::net::TcpStreamExt;
use std::thread;

impl<T: ProxyImplementation + 'static> Frontend for MySqlFrontend<T> {
    async fn start_listening(self) -> ! {
        let port: u16 = self.config.port.unwrap_or(13306);
        let host = self.config.host.clone().unwrap_or(String::from("0.0.0.0"));

        let socket = TcpListener::bind((host.clone(), port)).expect("failed to bind port");

        info!("Server listening on {host}:{port}");

        loop {
            if let Ok((stream, _addr)) = socket.accept() {
                // This copy is only temporary and is used because we cannot make the closure `move`
                let thread_handler = self.handler.clone();
                stream.set_quickack(true).unwrap();
                stream.set_nodelay(true).unwrap();

                // TODO: migrate from threads to pure async tasks
                thread::spawn(|| {
                    let auth_handler = thread_handler.authentication_handler();

                    // Establish connection
                    let peer_addr = stream.peer_addr().unwrap();
                    info!("[{peer_addr}] New connection received");

                    // Spawn request handler
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    // Handle connection sequence
                    let client = MySQLConnection::new(stream);
                    let conn = runtime
                        .block_on(async { handle_connection_sequence(client, auth_handler).await });

                    let (client, info) = match conn {
                        Ok(pair) => pair,
                        Err(e) => {
                            error!(
                                "[{peer_addr}] Error handling connection sequence for peer: {e:?}"
                            );
                            return;
                        }
                    };

                    info!("[{peer_addr}] Connection sequence complete");

                    runtime.block_on(async move {
                        let session = thread_handler
                            .new_session(info.initial_db.as_ref().map(|x| x.as_str()))
                            .await;
                        info!("[{peer_addr}] Obtained proxy session");
                        let mut wrapped_client: ClientWrapper<T> =
                            ClientWrapper::new(client, info, session);

                        loop {
                            if let HandleResult::ConnectionClosed { reason } =
                                wrapped_client.read_handle_next_command().await
                            {
                                info!("[{peer_addr}] Connection closed: {reason}");
                                return;
                            }
                        }
                    });
                });
            }
        }
    }
}
