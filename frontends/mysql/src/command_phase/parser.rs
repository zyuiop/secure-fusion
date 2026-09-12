use common::parser::{ParseError, SqlStatement, try_parse_encrypted_modifier};
use common::statement::ParsedStatement;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{
    ColumnOption, Expr, Set, ShowStatementFilter, ShowStatementFilterPosition, Statement, Use,
};
use datafusion::sql::sqlparser::dialect::{Dialect, MySqlDialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use log::trace;
use std::any::TypeId;

static DIALECT: CustomMysqlDialect = CustomMysqlDialect {
    base: MySqlDialect {},
};

pub enum StatementAction {
    /// The statement should be handled down the line by the proxy
    Forward {
        statement: Box<ParsedStatement>,
        should_return_ok: bool,
    },

    /// The statement should be handled locally in this frontend
    HandleLocal(Box<MySqlFrontendCommand>),
}

#[allow(clippy::large_enum_variant)]
pub enum MySqlFrontendCommand {
    ShowVariables,
    ShowDatabases,
    SwitchDatabase(String),
    ShowTables {
        #[allow(unused)]
        extended: bool,
        full: bool,
        filter: Option<ShowStatementFilter>,
    },
    SetNames {
        #[allow(unused)]
        charset_name: String,
        #[allow(unused)]
        collation_name: Option<String>,
    },
    ShowColumns {
        full: bool,
        table: String,
        schema: Option<String>,
    },
    ShowIndex {
        table: String,
        schema: Option<String>,
    },
    SetVariable, /* TODO */
}

#[cfg_attr(feature = "tracing", tracing::instrument)]
pub fn sql_to_statements(query: &str) -> Result<Vec<StatementAction>, ParseError> {
    trace!("Query: {}", query);
    let parsed = common::parser::sql_to_statements(&DIALECT, query)
        // Offer a second chance to failing queries, as some parts of the parser check for the precise instance of the dialect
        .or_else(|_| common::parser::sql_to_statements(&MySqlDialect {}, query))?;

    parsed.into_iter().map(|parsed| {
        let r = match parsed {
            ParsedStatement::Statement(Statement::ShowDatabases { .. }) => {
                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::ShowDatabases))
            }
            ParsedStatement::Statement(Statement::ShowVariables { .. }) => {
                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::ShowVariables))
            }
            ParsedStatement::Statement(Statement::ShowTables { extended, full, show_options, .. }) => {
                let filter = show_options.filter_position.map(|fp| match fp {
                    ShowStatementFilterPosition::Infix(f) |
                    ShowStatementFilterPosition::Suffix(f) => {
                        f
                    }
                });


                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::ShowTables { full, extended, filter }))
            }
            ParsedStatement::Statement(Statement::ShowColumns { full, show_options, .. }) => {
                let show_in = show_options.show_in.ok_or(ParseError::NoStatement)?;
                let mut object_name = show_in.parent_name.ok_or(ParseError::NoStatement)?;

                let table = object_name.0.pop()
                    .and_then(|ident| ident.as_ident().map(|ident| ident.value.clone()))
                    .ok_or(ParseError::NoStatement)?;

                let schema = object_name.0.pop()
                    .and_then(|ident| ident.as_ident().map(|ident| ident.value.clone()));

                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::ShowColumns { full, table, schema }))
            }
            ParsedStatement::Statement(Statement::ShowVariable { mut variable })
            // SHOW INDEX
            if &variable[0].value.to_lowercase() == "index" &&
                &variable[1].value.to_lowercase() == "from" => {
                drop(variable.drain(..2));

                let table = variable.first()
                    .map(|ident| ident.value.clone())
                    .ok_or(ParseError::NoStatement)?;

                let schema = variable.get(2)
                    .map(|ident| ident.value.clone());

                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::ShowIndex { table, schema }))
            }

            ParsedStatement::Statement(
                Statement::Set(
                    Set::SetNames { charset_name, collation_name })) =>
                StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::SetNames {
                    charset_name: charset_name.value,
                    collation_name,
                })),

            ParsedStatement::Statement(
                Statement::Set(
                    Set::SingleAssignment { variable, values, .. }
                )
            ) => {
                if let Some(ident) = variable.0[0].as_ident() && ident.value.to_lowercase() == "autocommit" {
                    let forward = ParsedStatement::Statement(Statement::Set(
                        ast::Set::SingleAssignment { variable, values, scope: None, hivevar: false }
                    ));

                    StatementAction::Forward {
                        statement: Box::new(forward),
                        should_return_ok: true,
                    }
                } else {
                    // TODO
                    StatementAction::HandleLocal(Box::new(MySqlFrontendCommand::SetVariable))
                }
            }

            ParsedStatement::Statement(
                Statement::Use(Use::Database(db) | Use::Object(db))
            ) if db.0.len() == 1 => {
                StatementAction::HandleLocal(
                    Box::new(MySqlFrontendCommand::SwitchDatabase(
                        db.0[0].to_string()
                    ))
                )
            }

            /*

            ParsedStatement::Statement(
                Statement::StartTransaction { .. }
            ) => StatementAction::HandleLocal(MySqlFrontendCommand::TransactionStart),

            ParsedStatement::Statement(
                Statement::Commit { .. }
            ) => StatementAction::HandleLocal(MySqlFrontendCommand::TransactionCommit),

            ParsedStatement::Statement(
                Statement::Rollback { .. }
            ) => StatementAction::HandleLocal(MySqlFrontendCommand::TransactionRollback), */

            other => {
                StatementAction::Forward {
                    should_return_ok: should_return_ok(&other),
                    statement: Box::new(other),
                }
            }
        };

        Ok(r)
    }).collect::<Result<Vec<StatementAction>, ParseError>>()
}

fn should_return_ok(statement: &ParsedStatement) -> bool {
    match statement {
        ParsedStatement::Statement(s) => matches!(
            s,
            SqlStatement::Insert(_)
                | SqlStatement::Update { .. }
                | SqlStatement::Delete(_)
                | SqlStatement::CreateView { .. }
                | SqlStatement::CreateTable(_)
                | SqlStatement::CreateVirtualTable { .. }
                | SqlStatement::CreateIndex(_)
                | SqlStatement::CreateRole { .. }
                | SqlStatement::CreateSecret { .. }
                | SqlStatement::CreatePolicy { .. }
                | SqlStatement::CreateConnector(_)
                | SqlStatement::AlterTable { .. }
                | SqlStatement::AlterIndex { .. }
                | SqlStatement::AlterView { .. }
                | SqlStatement::AlterType(_)
                | SqlStatement::AlterRole { .. }
                | SqlStatement::AlterPolicy { .. }
                | SqlStatement::AlterConnector { .. }
                | SqlStatement::AlterSession { .. }
                | SqlStatement::AttachDatabase { .. }
                | SqlStatement::LockTables { .. }
                | SqlStatement::UnlockTables
                | SqlStatement::Set { .. }
                | SqlStatement::Commit { .. }
                | SqlStatement::Rollback { .. }
                | SqlStatement::StartTransaction { .. }
                | SqlStatement::Drop { .. }
        ),
        ParsedStatement::Placeholder(_) => panic!(),
    }
}

#[derive(Debug)]
struct CustomMysqlDialect {
    base: MySqlDialect,
}

impl Dialect for CustomMysqlDialect {
    fn dialect(&self) -> TypeId {
        self.base.dialect()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.base.is_delimited_identifier_start(ch)
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        self.base.identifier_quote_style(_identifier)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.base.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.base.is_identifier_part(ch)
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        self.base.supports_string_literal_backslash_escape()
    }

    fn supports_numeric_prefix(&self) -> bool {
        self.base.supports_numeric_prefix()
    }

    fn supports_limit_comma(&self) -> bool {
        self.base.supports_limit_comma()
    }

    fn supports_user_host_grantee(&self) -> bool {
        self.base.supports_user_host_grantee()
    }

    fn supports_match_against(&self) -> bool {
        self.base.supports_match_against()
    }

    fn parse_infix(
        &self,
        _parser: &mut Parser,
        _expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        self.base.parse_infix(_parser, _expr, _precedence)
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        self.base.parse_statement(parser)
    }

    fn parse_column_option(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        let base = self.base.parse_column_option(parser)?;
        if base.is_some() {
            return Ok(base);
        }

        let result = try_parse_encrypted_modifier(parser);
        Ok(result)
    }

    fn require_interval_qualifier(&self) -> bool {
        self.base.require_interval_qualifier()
    }

    fn supports_create_table_select(&self) -> bool {
        self.base.supports_create_table_select()
    }

    fn supports_insert_set(&self) -> bool {
        self.base.supports_insert_set()
    }

    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, _parser: &mut Parser) -> bool {
        self.base.is_table_factor_alias(explicit, kw, _parser)
    }

    fn supports_table_hints(&self) -> bool {
        self.base.supports_table_hints()
    }

    fn requires_single_line_comment_whitespace(&self) -> bool {
        self.base.requires_single_line_comment_whitespace()
    }
}
