use crate::get_catalog::TableGetter;
use crate::planning::logical::MySqlLogicalPlanner;
use common::{HandlerResult, default_statement_to_plan};
use datafusion::common::plan_err;
use datafusion::execution::SessionState;
use datafusion::logical_expr::sqlparser::ast::Statement;
use datafusion::logical_expr::{DmlStatement, Expr, LogicalPlan, Projection, WriteOp};
use datafusion::prelude::Column;
use datafusion::sql::TableReference;
use log::trace;
use std::sync::Arc;

/// Prefixes columns that are inserted at planning to filter the updates. These columns are to be
/// used in the `WHERE` part of an UPDATE query.
pub const FILTER_PREFIX: &str = "__pr_flt__";

/// Prefixes columns that are inserted at planning to select current values, as needed by some
/// indices. These columns should be ignored by the main update sink.
pub const CURRENT_VALUE_PREFIX: &str = "__pr_cur__";

impl MySqlLogicalPlanner {
    fn is_simple_alias(default_relation: &TableReference, expr: &Expr) -> bool {
        match expr {
            this @ Expr::Alias(alias) => {
                match alias.expr.as_ref() {
                    Expr::Alias(_) => Self::is_simple_alias(default_relation, this),
                    Expr::Column(col) => {
                        let output_rel = alias.relation.as_ref().unwrap_or(default_relation);
                        let base_rel = col.relation.as_ref().unwrap_or(default_relation);

                        // TODO: match schema?
                        output_rel.table() == base_rel.table() && col.name == alias.name
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    pub(super) async fn plan_update(
        &self,
        mut statement: Statement,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan> {
        let Statement::Update { .. } = &mut statement else {
            panic!("invalid parameter for plan_update")
        };

        let base_plan = default_statement_to_plan(statement, session_state).await?;

        let LogicalPlan::Dml(DmlStatement {
            input,
            table_name: table_ref,
            op: WriteOp::Update,
            target,
            output_schema: _,
        }) = base_plan
        else {
            plan_err!("UPDATE statement produced invalid plan")?
        };

        let Some(table) = target.as_mysql_opt() else {
            plan_err!("UPDATE statement contains invalid table")?
        };

        /* if !table.has_encrypted_columns() && !table.has_indices() {
            return Ok(ForwardStatement::update(statement));
        } */

        if table.primary_key().is_empty() {
            plan_err!("Cannot UPDATE without a primary key")?
        }

        // The structure of the base plan is as follows:
        // Projection <the full schema of the table, including modified columns>
        //      Filter <the WHERE conditions>
        //
        // We want to modify it as such:
        // Projection <the modified columns only> <the primary key of the table, prefixed with `filter.`>
        //      Filter <the WHERE conditions>
        let input = Arc::unwrap_or_clone(input);
        let LogicalPlan::Projection(project) = input else {
            plan_err!("UPDATE statement produced invalid plan with no projection")?
        };

        let Projection {
            input: project_input,
            expr: source_expr,
            ..
        } = project;

        let mut projection = source_expr
            .into_iter()
            .filter(|expr| !Self::is_simple_alias(&table_ref, expr))
            .collect::<Vec<_>>();

        // Append current values for columns where index requires current values for tracking
        let current_values_required = table.columns_with_updates_tracking();
        if !current_values_required.is_empty() {
            let mut add_columns = projection
                .iter()
                .map(|expr| expr.schema_name().to_string())
                .filter(|name| current_values_required.contains(name))
                .map(|name| {
                    let alias = format!("{CURRENT_VALUE_PREFIX}{name}");
                    let column = Column::new(Some(table_ref.clone()), name);
                    Expr::Column(column).alias(alias)
                })
                .collect::<Vec<_>>();
            projection.append(&mut add_columns);
        }

        let pk_columns = table.primary_key().iter().map(|col_name| {
            let target_name = format!("{FILTER_PREFIX}{col_name}");
            let column = Column::new(Some(table_ref.clone()), col_name);

            Expr::Column(column).alias(target_name)
        });
        projection.extend(pk_columns);

        let new_project = Projection::try_new(projection, project_input)?;

        let new_dml = LogicalPlan::Dml(DmlStatement::new(
            table_ref,
            target,
            WriteOp::Update,
            Arc::new(LogicalPlan::Projection(new_project)),
        ));

        trace!("Update plan: {new_dml}");

        Ok(new_dml)
    }
}
