use super::parser::{MySqlFrontendCommand, StatementAction, sql_to_statements};
use crate::client_side_helper::ClientHelper;
use crate::command_phase::HandleResult;
use crate::command_phase::arrow_helper::deserialize_parameter;
use crate::command_phase::char_utils::extract_invalid_bytes;
use crate::command_phase::error::CommandPhaseError::UnhandledCommand;
use crate::command_phase::error::{CommandPhaseError, CommandPhaseResult};
use crate::command_phase::placeholders::ParametersToDatafusionVisitor;
use crate::command_phase::protocol::column_count::ColumnCount;
use crate::command_phase::protocol::column_def::FieldRefSerializer;
use crate::command_phase::protocol::com_query::ComQuery;
use crate::command_phase::protocol::com_set_option::{ComSetOption, SetOption};
use crate::command_phase::protocol::field_type::DataTypeOps;
use crate::command_phase::rewrites::rewrite_statement;
use crate::command_phase::row_handlers::{BinaryProtocol, RowHandler, TextRowHandler};
use crate::command_phase::statements::SavedStatement;
use crate::connection_phase::ConnectionResponse;
use crate::status::{DetailedOk, NewEofPacket, SqlOk};
use common::dml::DmlResult;
use common::statement::ParsedStatement;
use common::{ProxyImplementation, ProxySession, profile};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{ParamValues, ScalarValue, not_impl_err, plan_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::sqlparser::ast::VisitMut;
use futures::StreamExt;
use futures::lock::Mutex;
use log::{error, warn};
use mysql_common::constants::{
    CapabilityFlags, ColumnFlags, ColumnType, StatusFlags, StmtExecuteParamsFlags,
};
use mysql_common::io::ParseBuf;
use mysql_common::packets::{NullBitmap, OkPacketKind, OldEofPacket};
use mysql_common::proto::MyDeserialize;
use mysql_common::value::ClientSide;
use mysql_interop::ConnectionWrapper;
use mysql_interop::connection::{MySQLConnection, WrappedBuffer};
use mysql_interop::constants::CommandId;
use nohash_hasher::IntMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::AcqRel;
/*
struct SavedStatement {
    backend_statement_id: common::StatementId,
    /* parameter parser, columns, ... */
    should_return_ok: bool,
}
*/

pub struct ClientWrapper<T: ProxyImplementation> {
    pub(super) client: MySQLConnection,
    pub(super) capabilities: CapabilityFlags,
    pub(super) session: T::SessionType,

    statements: Mutex<IntMap<u32, Arc<SavedStatement>>>,
    current_statement_id: AtomicU32,

    supports_multiple_statements: bool, // statements_holder: PreparedStatementsHolder,
}

impl<T: ProxyImplementation> ConnectionWrapper for ClientWrapper<T> {
    fn connection(&mut self) -> &mut MySQLConnection {
        &mut self.client
    }
}

impl<T: ProxyImplementation> ClientHelper for ClientWrapper<T> {
    fn capabilities(&self) -> CapabilityFlags {
        self.capabilities
    }
}

const LONG_QUERY_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(33);

impl<T: ProxyImplementation> ClientWrapper<T> {
    pub(crate) fn new(
        conn: MySQLConnection,
        info: ConnectionResponse,
        session: T::SessionType,
    ) -> Self {
        ClientWrapper {
            client: conn,
            session,
            capabilities: info.capability_flags,
            supports_multiple_statements: false,
            statements: Mutex::new(IntMap::default()),
            current_statement_id: AtomicU32::new(1),
            // statements_holder: PreparedStatementsHolder::new(),
        }
    }

    pub(crate) async fn read_handle_next_command(&mut self) -> HandleResult {
        coz::begin!("read_handle_next_command");
        let Some(pack) = profile!("read_packet", self.client.read()) else {
            return HandleResult::ConnectionClosed {
                reason: "closed by peer".to_string(),
            };
        };

        let incoming = match pack {
            Ok(incoming) => incoming,
            Err(err) => {
                error!("Failed to parse incoming packet: {err}");
                return HandleResult::ConnectionClosed {
                    reason: "failed to handle incoming packet".to_string(),
                };
            }
        };

        let result = profile!(
            "handle_packet",
            self.handle_packet_can_error(incoming).await
        );
        coz::end!("read_handle_next_command");

        match result {
            Ok(HandleResult::HandledPacket) => {
                self.client.reset_seqno();
                HandleResult::HandledPacket
            }
            Ok(closed) => closed,
            Err(e) => {
                error!(
                    "[{}] An error occurred in the proxy while handling user command: {e:?}",
                    self.client.peer_addr()
                );
                self.handle_error(e.into());
                self.client.reset_seqno();
                HandleResult::HandledPacket
            }
        }
    }

    async fn do_prepare_statement(
        &mut self,
        query: &str,
    ) -> CommandPhaseResult<Arc<SavedStatement>> {
        #[cfg(feature = "log_queries")]
        log::info!("P: {}", query);

        let mut q = profile!("parse", sql_to_statements(query)?);
        let num_queries = q.len();

        if num_queries == 0 {
            plan_err!("Empty statement.")?;
        }

        if num_queries > 1 {
            // Impl notes: bundle responses in a single local transaction object, and send all transactions at once whenever one comes
            not_impl_err!("TODO: Multiple queries in single statement are not implemented yet.")?;
        }

        let q = q.pop().unwrap();
        match q {
            StatementAction::Forward {
                statement: mut q,
                should_return_ok,
            } => {
                rewrite_statement(q.as_mut(), self.session.underlying_engine()).await?;

                let mut visitor = ParametersToDatafusionVisitor::new();
                if let ParsedStatement::Statement(s) = q.as_mut() {
                    // Replace place-holders with numbered equivalents
                    let _ = s.visit(&mut visitor);
                }

                let result = profile!("compute plan total", self.session.statement_open(*q).await?);

                let statement_id = self.current_statement_id.fetch_add(1, AcqRel);
                let mapped_statement = Arc::new(SavedStatement::new_from_statement(
                    statement_id,
                    should_return_ok,
                    result,
                ));

                Ok(mapped_statement)
            }
            StatementAction::HandleLocal(_) => {
                todo!("Prepared local commands");
            }
        }
    }

    async fn handle_prepare(&mut self, query: &str) -> CommandPhaseResult<()> {
        let mapped_statement = self.do_prepare_statement(query).await?;
        let statement_id = mapped_statement.statement_id;

        {
            let mut map = self.statements.lock().await;
            map.insert(statement_id, mapped_statement.clone());
        }

        mapped_statement.send_to_client(self)?;
        Ok(())
    }

    async fn handle_execute<'a>(&mut self, mut parsebuf: ParseBuf<'a>) -> CommandPhaseResult<()> {
        let statement_id = parsebuf.eat_u32_le();
        let statement = {
            let statements = self.statements.lock().await;
            let Some(statement) = statements.get(&statement_id) else {
                return Err(CommandPhaseError::UnknownStatement(statement_id));
            };
            Arc::clone(statement)
        };

        parsebuf.eat_u8(); // flags

        // iter_count, always 1
        assert_eq!(
            parsebuf.eat_u32_le(),
            1u32,
            "iter_count > 1 not implemented"
        );

        let parameters_count = if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES)
        {
            parsebuf.eat_lenenc_int() as usize
        } else {
            statement.parameters.len()
        };

        let parameter_values: Vec<ScalarValue> = if parameters_count > 0 {
            let bitmap_size = NullBitmap::<ClientSide>::bitmap_len(parameters_count);

            let _bitmap =
                parsebuf
                    .checked_eat(bitmap_size)
                    .ok_or(CommandPhaseError::OtherError(
                        "failed reading packet".to_string(),
                    ))?;
            let _bitmap = NullBitmap::<ClientSide>::from_bytes(Vec::from(_bitmap));

            let new_params_flags = StmtExecuteParamsFlags::from_bits_truncate(parsebuf.eat_u8());

            // TODO: for now we ignore the new parameters bound, but ideally we should parse them properly
            let parameters = &statement.parameters;

            let declared_column_types = if new_params_flags
                .contains(StmtExecuteParamsFlags::NEW_PARAMS_BOUND)
            {
                let mut parameters: Vec<ColumnType> = Vec::with_capacity(parameters_count);
                // Bind new params
                for _ in 0..parameters_count {
                    let param_type =
                        u8::try_from(parsebuf.eat_u16_le()).expect("invalid param type");
                    let param_type = ColumnType::try_from(param_type).expect("invalid param type");
                    let param = if self
                        .capabilities
                        .contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES)
                    {
                        /* let attr_name = */
                        parsebuf.eat_lenenc_str();
                        param_type
                    } else {
                        param_type
                    };

                    parameters.push(param)
                }

                assert_eq!(parameters.len(), statement.parameters.len());
                parameters
            } else if self
                .capabilities
                .contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES)
            {
                todo!(
                    "uncertain how to handle a param_count without a redefinition or parameters... skipping"
                )
            } else {
                parameters
                    .iter()
                    .map(|p| ColumnType::from(DataTypeOps(p)))
                    .collect()
            };

            let mut values = Vec::with_capacity(parameters_count);
            let parameters = parameters.iter().zip(declared_column_types.into_iter());

            for (param_type, column_type) in parameters {
                values.push(deserialize_parameter(
                    &mut parsebuf,
                    column_type,
                    ColumnFlags::empty(),
                    param_type,
                )?);
            }
            values
        } else {
            Vec::new()
        };

        let parameter_values = parameter_values.into_iter().map(|v| v.into()).collect();
        let parameter_values = ParamValues::List(parameter_values);

        let result = profile!(
            "compute plan total",
            self.session
                .statement_execute(statement.backend_statement_id.clone(), parameter_values)
                .await?
        );
        self.handle_query_result::<BinaryProtocol>(
            result,
            statement.should_return_ok,
            StatusFlags::empty(),
        )
        .await
        .map_err(|err| {
            log::error!("Failed statement execute {}: {err:?}", statement_id);
            err
        })?;

        Ok(())
    }

    async fn handle_query(&mut self, query: &str) -> CommandPhaseResult<()> {
        #[cfg(feature = "log_queries")]
        log::info!("Q: {}", query);

        let _instant = std::time::Instant::now();
        let q = profile!("parse", sql_to_statements(query)?);
        let num_queries = q.len();

        if num_queries == 0 {
            warn!("Empty query parsed");
            self.handle_ok(SqlOk::Ok, StatusFlags::empty());
            return Ok(());
        }

        let mut i = 0;
        for q in q {
            i += 1;
            let status = if i == num_queries {
                StatusFlags::empty()
            } else {
                StatusFlags::SERVER_MORE_RESULTS_EXISTS
            };

            match q {
                StatementAction::Forward {
                    statement: mut q,
                    should_return_ok,
                } => {
                    rewrite_statement(q.as_mut(), self.session.underlying_engine()).await?;

                    let result = profile!(
                        "compute plan total",
                        self.session.query_immediate(*q, None).await?
                    );
                    self.handle_query_result::<TextRowHandler>(result, should_return_ok, status)
                        .await
                        .map_err(|err| {
                            log::error!("Failed query {}: {err:?}", query);
                            err
                        })?;
                }
                StatementAction::HandleLocal(cmd) => {
                    self.handle_local_command(*cmd, status).await?;
                }
            }
        }

        let dur = _instant.elapsed();
        if dur >= LONG_QUERY_THRESHOLD {
            log::warn!("Long query: query took {dur:?}: {}", query);
        }

        Ok(())
    }

    async fn handle_packet_can_error(
        &mut self,
        packet: WrappedBuffer,
    ) -> CommandPhaseResult<HandleResult> {
        let mut parsebuf = ParseBuf(&packet);
        let header = parsebuf.0.first().expect("Empty packet!");
        let command = CommandId::try_from(*header)?;

        match command {
            CommandId::CmdQuit => {
                return Ok(HandleResult::ConnectionClosed {
                    reason: "closed gracefully (received QUIT command)".to_string(),
                });
            }
            CommandId::CmdInitDb => {
                parsebuf.eat_u8();
                let schema_name = String::from_utf8(Vec::from(parsebuf.eat_null_str()))
                    .expect("failed to read schema name!");
                self.handle_local_command(
                    MySqlFrontendCommand::SwitchDatabase(schema_name),
                    StatusFlags::empty(),
                )
                .await?;
            }
            CommandId::CmdQuery => {
                let com_query = ComQuery::deserialize(self.capabilities, &mut parsebuf)
                    .expect("should receive valid query");

                let (query_string, params) = extract_invalid_bytes(&com_query.query.as_bytes())?;

                if let Some(params) = params {
                    log::info!("Caught invalid bytes in query, rewriting as prepared statement");
                    let prepared_statement = self.do_prepare_statement(&query_string).await?;

                    let result = profile!(
                        "compute plan total",
                        self.session
                            .statement_execute(
                                prepared_statement.backend_statement_id.clone(),
                                params
                            )
                            .await?
                    );

                    self.handle_query_result::<TextRowHandler>(
                        result,
                        prepared_statement.should_return_ok,
                        StatusFlags::empty(),
                    )
                    .await
                    .map_err(|err| {
                        log::error!("Failed statement execute: {err:?}");
                        err
                    })?;
                } else {
                    self.handle_query(&query_string).await?;
                }
            }
            CommandId::CmdFieldList => {
                // let field_list = proxy.server().handle_field_list(packet)?;

                // for column in field_list.0 {
                // self.client.send_packet(&column);
                // }
                self.handle_ok(SqlOk::EndOfResults, StatusFlags::empty());
            }
            CommandId::CmdStmtPrepare => {
                let com_query = ComQuery::deserialize(self.capabilities, &mut parsebuf)
                    .expect("should receive valid query");

                self.handle_prepare(&com_query.query.as_str()).await?;
            }
            CommandId::CmdSetOption => {
                let set_opt = ComSetOption::deserialize(CapabilityFlags::empty(), &mut parsebuf)
                    .expect("should receive valid packet");

                match set_opt.opt {
                    SetOption::MultiStatementsOn => {
                        self.supports_multiple_statements = true;
                        self.handle_ok(SqlOk::EndOfResults, StatusFlags::empty());
                    }
                    SetOption::MultiStatementsOff => {
                        self.supports_multiple_statements = false;
                        self.handle_ok(SqlOk::EndOfResults, StatusFlags::empty());
                    }
                }
            }
            CommandId::CmdStmtExecute => {
                parsebuf.eat_u8();
                self.handle_execute(parsebuf).await?;
            }
            CommandId::CmdStmtClose => {
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
                // as per specs: no response packet is sent to the client
                parsebuf.eat_u8();
                let statement_id = parsebuf.eat_u32_le();

                let statement = {
                    let mut statements = self.statements.lock().await;
                    statements.remove(&statement_id)
                };

                if let Some(statement) = statement {
                    self.session
                        .statement_close(statement.backend_statement_id)
                        .await?;
                }
            }
            CommandId::CmdPing => {
                self.handle_ok(SqlOk::Ok, StatusFlags::empty());
            }
            /*
            0x0E /* COM_PING */ => {
            }
            0x16 => {
            /* STATEMENT PREPARE */
                let q = ComQuery::deserialize(CapabilityFlags::empty(), &mut parsebuf).expect("should receive valid query");

                // TODO: move query parsing elsewhere
                let mut statement = Parser::parse_sql(&MySqlDialect{}, &q.query.as_str())?;
                let mut statement = statement.remove(0);
                introduce_numbered_parameter_names(&mut statement);

                let results = proxy.prepare_statement(statement)?;

                self.handle_prepared_statement(results);
            }
            0x17 => {
                /* STATEMENT EXECUTE */
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
                parsebuf.eat_u8();
                let statement_id = parsebuf.eat_u32_le();
                let statement_data = self.saved_statements.get(&statement_id);
                let Some(statement_parameter_types) = statement_data else {
                    self.client.handle_error(SqlError {
                        state: None, code: None, message: String::from("Invalid statement ID")
                    });
                    // Don't change the state and exit now
                    return Ok(());
                };

                let _flags = CursorType::from_bits_truncate(parsebuf.eat_u8());
                /* iter_count, always 1 */ assert_eq!(parsebuf.eat_u32_le(), 1u32, "iter_count > 1 not implemented");

                let parameters_count = if self.capabilities.contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES) {
                    parsebuf.eat_lenenc_int() as usize
                } else {
                    statement_parameter_types.len()
                };

                let values = if parameters_count > 0 {
                    let bitmap_size = NullBitmap::<ClientSide>::bitmap_len(parameters_count);

                    let _bitmap = parsebuf.checked_eat(bitmap_size).ok_or(ProxyError::UnknownServerError("failed reading packet".to_string()))?;
                    let _bitmap = NullBitmap::<ClientSide>::from_bytes(Vec::from(_bitmap));

                    let new_params_flags = StmtExecuteParamsFlags::from_bits_truncate(parsebuf.eat_u8());

                    let mut parameters = Vec::with_capacity(parameters_count);
                    let parameters = if new_params_flags.contains(StmtExecuteParamsFlags::NEW_PARAMS_BOUND) {
                        // Bind new params
                        for _ in 0..parameters_count {
                            let param_type = u8::try_from(parsebuf.eat_u16_le()).expect("invalid param type");
                            let param_type = ColumnType::try_from(param_type).expect("invalid param type").into();
                            let param = if self.capabilities.contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES) {
                                /* let attr_name = */ parsebuf.eat_lenenc_str();
                                param_type
                            } else {
                                param_type
                            };

                            parameters.push(param)
                        }
                        &parameters
                    }
                    else if self.capabilities.contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES) {
                        todo!("uncertain how to handle a param_count without a redefinition or parameters... skipping")
                    }
                    else {
                        &statement_parameter_types
                    };

                    let mut values = Vec::with_capacity(parameters_count);
                    for param in parameters {
                        let v = ValueDeserializer::<BinValue>::deserialize(
                            ((*param).into(), ColumnFlags::empty()), &mut parsebuf
                        ).expect("incorrect value deserialized").0;
                        values.push(PreparedParameter(*param, v.into()));
                    }
                    values
                } else {
                    Vec::new()
                };

                let values = rewrite_client_side_parameters(values);
                let results = proxy.execute_statement(statement_id, values)?;
                self.client.handle_query_result(results, ProtocolType::Binary);
            }
            0x18 => {
                /* STATEMENT SEND DATA */
                todo!("statement_send_data")
            }
            0x19 => {
                /* STATEMENT CLOSE */
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
                parsebuf.eat_u8();
                let statement_id = parsebuf.eat_u32_le();
                self.saved_statements.remove(&statement_id);
                proxy.close_statement(statement_id)?;
            }
            0x1A => {
                /* STATEMENT RESET */
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html
                parsebuf.eat_u8();
                let statement_id = parsebuf.eat_u32_le();
                let ok = proxy.reset_statement(statement_id)?;
                self.client.handle_ok(ok);
            }*/
            o => return Err(UnhandledCommand(o)),
        };

        Ok(HandleResult::HandledPacket)
    }

    async fn handle_query_result<PT: RowHandler>(
        &mut self,
        results: SendableRecordBatchStream,
        should_ok: bool,
        flags: StatusFlags,
    ) -> CommandPhaseResult<()> {
        if should_ok {
            let res = profile!("execute total", results.collect::<Vec<_>>().await);
            let res = res.into_iter().collect::<Result<Vec<_>, _>>();
            let res = res?;

            if let Some(res) = res.first() {
                let dml_result = DmlResult::try_from(res)?;

                self.handle_ok(
                    SqlOk::DetailedOk(DetailedOk {
                        affected_rows: Some(dml_result.num_rows() as usize),
                        last_insert_id: dml_result.last_insert_id().map(|v| v as usize),
                    }),
                    flags,
                );
            } else {
                warn!(
                    "No DML result returned for query with `should_ok=true`. Returning empty OK."
                );
                self.handle_ok(SqlOk::Ok, flags); // TODO!
            }
        } else {
            self.send_result_columns(results.schema());

            PT::send_records_stream(&mut self.client, results).await?;

            /* while let Some(value) = rows_iterator.next() {
               self.handle_result_row(vec![value], protocol_type, &cols);
            }*/

            // println!("Done handling rows!");
            self.send_result_eof(flags);
        }

        Ok(())
    }

    pub(crate) fn send_result_columns(&mut self, table_def: SchemaRef) {
        // Send column count
        self.send_packet(&ColumnCount::new(table_def.fields.len()));

        // Send columns
        for column in table_def.fields() {
            self.send_packet(&FieldRefSerializer(column));
        }

        if !self
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            // Send EOF
            if self
                .capabilities
                .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
            {
                self.send_packet(&NewEofPacket {
                    warnings: 0,
                    status_flags: StatusFlags::SERVER_STATUS_AUTOCOMMIT,
                })
            } else {
                self.send_packet_raw(&[OldEofPacket::HEADER][..])
            }
        }
    }

    pub(crate) fn send_result_eof(&mut self, flags: StatusFlags) {
        if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            self.handle_ok(SqlOk::EndOfResults, flags);
        } else {
            self.send_packet(&NewEofPacket {
                warnings: 0,
                status_flags: flags | StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            })
        }
    }
}
