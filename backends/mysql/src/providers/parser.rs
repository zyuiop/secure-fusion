use common::parser::dialect::MySqlDialect;
use datafusion::logical_expr::sqlparser;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{Expr, escape_quoted_string};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, Expr as SQLExpr};

fn do_parse_default_value(default_value: &str) -> Result<SQLExpr, ParserError> {
    let mut parser =
        sqlparser::parser::Parser::new(&MySqlDialect {}).try_with_sql(default_value)?;
    parser.parse_expr()
}

fn is_str_datatype(column_type: &SQLDataType) -> bool {
    matches!(
        column_type,
        SQLDataType::Enum(_, _)
            | SQLDataType::Varchar(_)
            | SQLDataType::Char(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::Text
            | SQLDataType::TinyText
            | SQLDataType::MediumText
            | SQLDataType::LongText
            | SQLDataType::String(_)
    )
}

pub fn parse_default_value_to_expr(
    default_value: &str,
    column_type: &SQLDataType,
) -> Result<SQLExpr, ParserError> {
    let mut default_value = do_parse_default_value(default_value).or_else(|original_error| {
        if is_str_datatype(column_type) {
            let new_string = format!("'{}'", escape_quoted_string(default_value, '\''));
            do_parse_default_value(&new_string)
        } else {
            Err(original_error)
        }
    })?;

    if let Expr::Identifier(ident) = &default_value {
        let value = ident.value.clone();
        default_value = Expr::value(ast::Value::SingleQuotedString(value))
    }

    Ok(default_value)
}

pub fn parse_column_type(column_type: &str) -> Result<SQLDataType, ParserError> {
    let mut parser = sqlparser::parser::Parser::new(&MySqlDialect {}).try_with_sql(column_type)?;

    parser.parse_data_type()
}
