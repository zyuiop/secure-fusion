use crate::get_catalog::CatalogGetter;
use crate::planning::logical::MySqlLogicalPlanner;
use crate::planning::logical::custom_forward_statement::ForwardStatement;
use crate::planning::unparser;
use common::{HandlerResult, default_statement_to_plan};
use datafusion::catalog::TableProvider;
use datafusion::common::plan_err;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion::logical_expr::sqlparser::ast::{ObjectNamePart, Statement, TableFactor};
use datafusion::logical_expr::{
    DmlStatement, Expr, LogicalPlan, Projection, TableProviderFilterPushDown, WriteOp,
};
use datafusion::prelude::Column;
use datafusion::sql::sqlparser::ast::{Delete, FromTable, Ident};
use log::trace;
use std::sync::Arc;

impl MySqlLogicalPlanner {
    pub(super) async fn plan_delete(
        &self,
        mut delete: Delete,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan> {
        let FromTable::WithFromKeyword(table) = &mut delete.from else {
            plan_err!("DELETE without FROM is not supported")?
        };

        if table.len() > 1 {
            plan_err!("DELETE with more than one relation is not supported")?
        }

        let Some(relations) = table.first_mut() else {
            plan_err!("DELETE without any relation is not supported")?
        };

        if !relations.joins.is_empty() {
            plan_err!("DELETE with more than one relation is not supported")?
        }

        let TableFactor::Table { ref mut name, .. } = relations.relation else {
            plan_err!("DELETE with more than one relation is not supported")?
        };

        if name.0.len() == 1 {
            // Insert the schema first
            name.0.insert(
                0,
                ObjectNamePart::Identifier(Ident::new(session_state.default_schema())),
            );
        }

        let obj_name = unparser::table_name_to_ref(name);
        let Some(table) = session_state.mysql_schema_for_ref(&obj_name) else {
            plan_err!("Table not found {}", &obj_name)?
        };

        // We have an empty delete selection -> this is trivial, return a Delete with an empty operation
        if delete.selection.is_none() {
            return Ok(LogicalPlan::Dml(DmlStatement::new(
                obj_name,
                Arc::new(DefaultTableSource::new(table)),
                WriteOp::Delete,
                Arc::new(Self::make_empty_plan()),
            )));
        }

        // We have a DELETE with some filters - we need to parse the filters
        // The easiest way to do that is to delegate to the default parser
        let plan =
            default_statement_to_plan(Statement::Delete(delete.clone()).into(), session_state)
                .await?;

        let LogicalPlan::Dml(dml) = plan else {
            plan_err!("Delete statement produced invalid plan")?
        };

        // Typically, the input schema should be a Filter (...)
        let LogicalPlan::Filter(filter) = dml.input.as_ref() else {
            plan_err!("Delete statement produced invalid plan with no filter")?
        };

        // Finally, the last forward case
        // If there is no index, and the conditions can be forwarded as-is, forward
        if !table.has_index_storage() {
            let pushdown_support = table.supports_filters_pushdown(&[&filter.predicate])?;
            if pushdown_support[0] == TableProviderFilterPushDown::Exact {
                return Ok(ForwardStatement::delete(delete));
            }
        }

        // Then, we have two modes:
        // - if we have indices on the table, we MUST have the list of deleted entries
        // - if we don't have indices on the table, we don't care
        // Do we do this at the exec stage?
        let pk = table
            .primary_key()
            .iter()
            .map(|column_name| Expr::Column(Column::new(Some(obj_name.clone()), column_name)))
            .collect::<Vec<_>>();

        if pk.is_empty() {
            plan_err!("Cannot DELETE without a primary key")?
        }

        let project = LogicalPlan::Projection(Projection::try_new(pk, dml.input)?);

        let delete = LogicalPlan::Dml(DmlStatement::new(
            dml.table_name,
            dml.target,
            WriteOp::Delete,
            Arc::new(project),
        ));

        trace!("Delete plan: {delete}");

        Ok(delete)
    }
}
