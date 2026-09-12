//! Some expressions are not supported by datafusion
//! This is where we rewrite them

use common::parser::SqlStatement;
use common::statement::ParsedStatement;
use datafusion::prelude::SessionContext;

mod rewrite_bitwise_not;
mod rewrite_function_args;
mod rewrite_projection;

pub async fn rewrite_statement(
    statement: &mut ParsedStatement,
    _session_context: &SessionContext,
) -> datafusion::common::Result<()> {
    let ParsedStatement::Statement(statement) = statement else {
        // Don't rewrite other statements
        return Ok(());
    };

    if let SqlStatement::Query(query) = statement {
        rewrite_projection::rewrite_projection(query);
    }
    rewrite_function_args::rewrite_function_args(statement);
    rewrite_bitwise_not::rewrite_unsupported_ops(statement);

    Ok(())
}
