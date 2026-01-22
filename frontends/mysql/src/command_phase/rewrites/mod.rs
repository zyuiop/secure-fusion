//! Some expressions are not supported by datafusion
//! This is where we rewrite them

use crate::command_phase::rewrites::rewrite_aggregates::rewrite_aggregates;
use crate::command_phase::rewrites::rewrite_bitwise_not::rewrite_unsupported_ops;
use crate::command_phase::rewrites::rewrite_function_args::rewrite_function_args;
use crate::command_phase::rewrites::rewrite_projection::rewrite_projection;
use common::parser::SqlStatement;
use common::statement::ParsedStatement;
use datafusion::prelude::SessionContext;

mod rewrite_aggregates;
mod rewrite_bitwise_not;
mod rewrite_function_args;
mod rewrite_projection;

pub async fn rewrite_statement(
    statement: &mut ParsedStatement,
    session_context: &SessionContext,
) -> datafusion::common::Result<()> {
    let ParsedStatement::Statement(statement) = statement else {
        // Don't rewrite other statements
        return Ok(());
    };

    if let SqlStatement::Query(query) = statement {
        rewrite_projection(query);
        rewrite_aggregates(query, session_context).await?;
    }
    rewrite_function_args(statement);
    rewrite_unsupported_ops(statement);

    Ok(())
}
