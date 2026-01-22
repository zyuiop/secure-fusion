use datafusion::sql::sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};

use crate::statement::ParsedStatement;
pub use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
pub use datafusion::sql::sqlparser::dialect;
use datafusion::sql::sqlparser::tokenizer::Token;

#[derive(Debug)]
pub enum ParseError {
    InvalidDialect,
    ParserError(ParserError),
    TooManyStatements,
    NoStatement,
}

pub fn sql_to_statements(
    dialect: &dyn Dialect,
    query: &str,
) -> Result<Vec<ParsedStatement>, ParseError> {
    /*
        // Inspiration for potential parser extensions later on!
        let mut statements = DFParserBuilder::new(sql)
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;
    */

    let statements = Parser::parse_sql(dialect, query).map_err(ParseError::ParserError)?;

    Ok(statements
        .into_iter()
        .map(|statement| {
            match statement {
                // Statement::StartTransaction { .. } => ParsedStatement::Statement(SqlStatement::StartTransaction),
                other => ParsedStatement::Statement(other),
            }
        })
        .collect())
}

const ENCRYPTED_KW: &str = "encrypted";
const DECRYPTED_KW: &str = "decrypted";

/// Checks if a given token is the ENCRYPTED token
pub fn is_token_encrypted_kw(token: &Token) -> bool {
    if let Token::Word(w) = token
        && w.value.to_lowercase() == ENCRYPTED_KW
    {
        true
    } else {
        false
    }
}

/// Checks if a given token is the DECRYPTED token
pub fn is_token_decrypted_kw(token: &Token) -> bool {
    if let Token::Word(w) = token
        && w.value.to_lowercase() == DECRYPTED_KW
    {
        true
    } else {
        false
    }
}

/// Try to parse the `ENCRYPTED` column modifier on a column
pub fn try_parse_encrypted_modifier(
    parser: &mut Parser,
) -> Option<Result<Option<ColumnOption>, ParserError>> {
    let next_token = parser.peek_token().token;
    if is_token_encrypted_kw(&next_token) {
        let token = parser.next_token();
        Some(Ok(Some(ColumnOption::DialectSpecific(vec![token.token]))))
    } else {
        None
    }
}

/// Checks if a given parsed [ColumnDef] has been defined as encrypted
pub fn has_encrypted_option(column_def: &ColumnDef) -> bool {
    column_def.options.iter().any(is_encrypted_option)
}

/// Checks if a given parsed [ColumnDef] has been defined as decrypted
pub fn has_decrypted_option(column_def: &ColumnDef) -> bool {
    column_def.options.iter().any(is_decrypted_option)
}

/// Checks if a given parsed [ColumnOptionDef] has been defined as encrypted
pub fn is_encrypted_option(column_option: &ColumnOptionDef) -> bool {
    if let ColumnOption::DialectSpecific(tokens) = &column_option.option {
        for tok in tokens {
            if is_token_encrypted_kw(tok) {
                return true;
            }
        }
    }
    false
}

pub fn is_decrypted_option(column_option: &ColumnOptionDef) -> bool {
    if let ColumnOption::DialectSpecific(tokens) = &column_option.option {
        for tok in tokens {
            if is_token_decrypted_kw(tok) {
                return true;
            }
        }
    }
    false
}
