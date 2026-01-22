use crate::client_side_helper::ClientHelper;
use crate::command_phase::client_side_wrapper::ClientWrapper;
use crate::command_phase::error::CommandPhaseResult;
use crate::command_phase::parser::MySqlFrontendCommand;
use crate::command_phase::row_handlers::TextRowHandler;
use crate::status::SqlOk;
use common::ext::session_ext::SessionExt;
use common::extensions::variable_store::VariableStoreGetter;
use common::metadata::MetadataReads;
use common::{ProxyImplementation, ProxySession};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Constraint, TableReference};
use datafusion::sql::sqlparser::ast::ShowStatementFilter;
use datafusion::variable::VarType;
use log::warn;
use mysql_common::constants::StatusFlags;

impl<T: ProxyImplementation> ClientWrapper<T> {
    async fn handle_command_rows(
        &mut self,
        schema: SchemaRef,
        results: Vec<Vec<Option<String>>>,
        flags: StatusFlags,
    ) -> CommandPhaseResult<()> {
        self.send_result_columns(schema);
        TextRowHandler::send_rows(&mut self.client, results).await?;

        self.send_result_eof(flags);
        Ok(())
    }

    pub(super) async fn handle_local_command(
        &mut self,
        command: MySqlFrontendCommand,
        flags_to_client: StatusFlags,
    ) -> CommandPhaseResult<()> {
        match command {
            MySqlFrontendCommand::SwitchDatabase(db) => {
                self.session.switch_database(&db).await?;
                self.handle_ok(SqlOk::Ok, flags_to_client);
                Ok(())
            }
            MySqlFrontendCommand::ShowVariables => {
                let schema = SchemaRef::new(Schema::new(vec![
                    Field::new("Variable_name", DataType::Utf8, true),
                    Field::new("Value", DataType::Utf8, false),
                ]));

                let variable_store = self
                    .session
                    .underlying_engine()
                    .state()
                    .config()
                    .get_variable_store(VarType::System);

                let rows = if let Some(variable_store) = variable_store {
                    variable_store
                        .read()
                        .iter()
                        .map(|(k, v)| vec![Some(k.clone()), Some(v.clone())])
                        .collect()
                } else {
                    vec![]
                };

                warn!("handle show variables, returning empty");
                self.handle_command_rows(schema, rows, flags_to_client)
                    .await
            }
            MySqlFrontendCommand::SetNames { .. } => {
                warn!("handle set names returning empty");
                self.handle_ok(SqlOk::Ok, flags_to_client);
                Ok(())
            }
            MySqlFrontendCommand::SetVariable => {
                warn!("handle set variable returning empty");
                self.handle_ok(SqlOk::Ok, flags_to_client);
                Ok(())
            }
            MySqlFrontendCommand::ShowTables {
                extended: _,
                full,
                filter,
            } => {
                let current_schema = self.session.underlying_engine().get_current_schema();

                let mut columns = vec![Field::new(
                    format!("Tables_in_{current_schema}"),
                    DataType::Utf8,
                    true,
                )];
                if full {
                    columns.push(Field::new("Table_type", DataType::Utf8, false))
                }
                let schema = SchemaRef::new(Schema::new(columns));

                let rows = self
                    .session
                    .underlying_engine()
                    .list_tables(&current_schema)
                    .into_iter()
                    .filter(|table| {
                        if let Some(ref f) = filter {
                            match f {
                                ShowStatementFilter::Like(v) => {
                                    // TODO: partial equalities
                                    table == v
                                }
                                _ => false, // TODO
                            }
                        } else {
                            true
                        }
                    })
                    .map(|table_name| {
                        if full {
                            vec![Some(table_name), Some("BASE TABLE".to_string())]
                        } else {
                            vec![Some(table_name)]
                        }
                    })
                    .collect();

                self.handle_command_rows(schema, rows, flags_to_client)
                    .await
            }
            MySqlFrontendCommand::ShowColumns {
                schema,
                table,
                full,
            } => {
                let schema =
                    schema.unwrap_or_else(|| self.session.underlying_engine().get_current_schema());
                let table = self
                    .session
                    .underlying_engine()
                    .table_provider(TableReference::partial(schema, table))
                    .await?;

                let table_schema = table.schema();

                let (schema, rows) = if full {
                    let schema = SchemaRef::new(Schema::new(vec![
                        Field::new("Field", DataType::Utf8, false),
                        Field::new("Type", DataType::Utf8, false),
                        Field::new("Collation", DataType::Utf8, false),
                        Field::new("Null", DataType::Utf8, false),
                        Field::new("Key", DataType::Utf8, false),
                        Field::new("Default", DataType::Utf8, false),
                        Field::new("Extra", DataType::Utf8, false),
                        Field::new("Privileges", DataType::Utf8, false),
                        Field::new("Comment", DataType::Utf8, false),
                    ]));

                    let rows = table_schema
                        .fields
                        .iter()
                        .map(|field| {
                            vec![
                                Some(field.name().clone()),
                                field.raw_source_type().cloned(),
                                field.raw_collation().cloned(),
                                Some(if field.is_nullable() {
                                    "YES".to_string()
                                } else {
                                    "NO".to_string()
                                }),
                                Some("".to_string()), /* TODO KEY */
                                field.default_value().cloned(),
                                field
                                    .default_value()
                                    .map(|_| "DEFAULT_GENERATED".to_string())
                                    .or(Some("".to_string())), // TODO: Auto Increment
                                Some("select,insert,update,references".to_string()), /* TODO */
                                Some("".to_string()),
                            ]
                        })
                        .collect();

                    (schema, rows)
                } else {
                    let schema = SchemaRef::new(Schema::new(vec![
                        Field::new("Field", DataType::Utf8, false),
                        Field::new("Type", DataType::Utf8, false),
                        Field::new("Null", DataType::Utf8, false),
                        Field::new("Key", DataType::Utf8, false),
                        Field::new("Default", DataType::Utf8, false),
                        Field::new("Extra", DataType::Utf8, false),
                    ]));

                    let rows = table_schema
                        .fields
                        .iter()
                        .map(|field| {
                            vec![
                                Some(field.name().clone()),
                                field.raw_source_type().cloned(),
                                Some(if field.is_nullable() {
                                    "YES".to_string()
                                } else {
                                    "NO".to_string()
                                }),
                                Some("".to_string()), /* TODO KEY */
                                field.default_value().cloned(),
                                field
                                    .default_value()
                                    .map(|_| "DEFAULT_GENERATED".to_string())
                                    .or(Some("".to_string())), // TODO: Auto Increment
                            ]
                        })
                        .collect();

                    (schema, rows)
                };

                self.handle_command_rows(schema, rows, flags_to_client)
                    .await
            }
            MySqlFrontendCommand::ShowIndex {
                table: table_name,
                schema,
            } => {
                let schema =
                    schema.unwrap_or_else(|| self.session.underlying_engine().get_current_schema());
                let table = self
                    .session
                    .underlying_engine()
                    .table_provider(TableReference::partial(schema, table_name.clone()))
                    .await?;

                let schema = SchemaRef::new(Schema::new(vec![
                    Field::new("Table", DataType::Utf8, false),
                    Field::new("Non_unique", DataType::Utf8, false),
                    Field::new("Key_name", DataType::Utf8, false),
                    Field::new("Seq_in_index", DataType::Utf8, false),
                    Field::new("Column_name", DataType::Utf8, false),
                    Field::new("Collation", DataType::Utf8, false),
                    Field::new("Cardinality", DataType::Utf8, false),
                    Field::new("Sub_part", DataType::Utf8, false),
                    Field::new("Packed", DataType::Utf8, false),
                    Field::new("Null", DataType::Utf8, false),
                    Field::new("Index_type", DataType::Utf8, false),
                    Field::new("Comment", DataType::Utf8, false),
                    Field::new("Index_comment", DataType::Utf8, false),
                    Field::new("Visible", DataType::Utf8, false),
                    Field::new("Expression", DataType::Utf8, false),
                ]));

                let mut rows = Vec::new();
                let table_schema = table.schema();

                if let Some(constraints) = table.constraints() {
                    for constraint in constraints.iter() {
                        let (index_type, index_rows) = match constraint {
                            Constraint::PrimaryKey(positions) => (
                                "PRI".to_string(),
                                positions
                                    .iter()
                                    .map(|pos| table_schema.field(*pos).name().clone())
                                    .collect::<Vec<String>>(),
                            ),
                            Constraint::Unique(positions) => {
                                let head = table_schema.field(positions[0]).name().clone();
                                (
                                    head,
                                    positions
                                        .iter()
                                        .map(|pos| table_schema.field(*pos).name().clone())
                                        .collect::<Vec<String>>(),
                                )
                            }
                        };

                        // TODO: other constraints/indices later on
                        rows.extend(index_rows.into_iter().enumerate().map(|(pos, col_name)| {
                            let pos = pos + 1;
                            vec![
                                Some(table_name.clone()),
                                Some("0".to_string()),
                                Some(index_type.clone()),
                                Some(pos.to_string()),
                                Some(col_name),
                                Some("A".to_string() /* COLLATION */),
                                Some("0".to_string() /* CARDINALITY */),
                                None, /* SUB PART */
                                None, /* PACKED */
                                Some("".to_string()),
                                Some("BTREE".to_string()),
                                Some("".to_string()),
                                Some("".to_string()),
                                Some("YES".to_string()),
                                None,
                            ]
                        }))
                    }
                }

                self.handle_command_rows(schema, rows, flags_to_client)
                    .await
            }
            MySqlFrontendCommand::ShowDatabases => {
                let catalog = self
                    .session
                    .underlying_engine()
                    .catalog(
                        &self
                            .session
                            .underlying_engine()
                            .state()
                            .config_options()
                            .catalog
                            .default_catalog,
                    )
                    .unwrap();

                let rows = catalog
                    .schema_names()
                    .into_iter()
                    .map(|db_name| vec![Some(db_name)]);

                self.handle_command_rows(
                    SchemaRef::new(Schema::new(vec![Field::new(
                        "Database",
                        DataType::Utf8,
                        false,
                    )])),
                    rows.collect(),
                    flags_to_client,
                )
                .await
            }
        }
    }
}
