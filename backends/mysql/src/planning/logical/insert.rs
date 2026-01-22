use crate::planning::logical::MySqlLogicalPlanner;
use common::{HandlerResult, default_statement_to_plan};
use datafusion::common::{Column, not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::sqlparser::ast::{
    AssignmentTarget, Insert, ObjectNamePart, Query, SetExpr, Statement, Values,
};
use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan, Projection};
use datafusion::sql::TableReference;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::{
    Assignment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, OnInsert,
    TableObject, VisitMut, VisitorMut,
};
use log::warn;
use std::mem;
use std::ops::ControlFlow;
use std::string::ToString;
use std::sync::Arc;

#[allow(unused)]
pub const DUPLICATE_VALUE_PFX: &str = "__pr_dup__";

struct ValuesRewriter(TableReference);

impl ValuesRewriter {
    fn rewrite_function(expr: &mut Expr) -> ControlFlow<<Self as VisitorMut>::Break> {
        let Expr::Function(func) = expr else {
            return ControlFlow::Continue(());
        };

        if !func.name.0[0]
            .as_ident()
            .is_some_and(|ident| &ident.value == "values")
        {
            return ControlFlow::Continue(());
        }

        // VALUES function must be replaced by actual value
        let FunctionArguments::List(lst) = &mut func.args else {
            return ControlFlow::Break(plan_err!(
                "Invalid VALUES function call in ON DUPLICATE KEY CLAUSE: no arguments found (in: `{expr}`)"
            ));
        };
        if lst.args.is_empty() {
            return ControlFlow::Break(plan_err!(
                "Invalid VALUES function call in ON DUPLICATE KEY CLAUSE: empty arguments list (in: `{expr}`)"
            ));
        };

        let argument = lst.args.remove(0);
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(ident)) = argument else {
            return ControlFlow::Break(plan_err!(
                "Invalid VALUES function call in ON DUPLICATE KEY CLAUSE: invalid argument found `{argument}"
            ));
        };

        *expr = ident;
        ControlFlow::Continue(())
    }

    /// Checks if the expression is an identifier that refers directly to the table.
    /// This is not supported for now, as this is essentially an UPDATE expression
    fn is_bad_identifier(&self, expr: &Expr) -> bool {
        let Expr::CompoundIdentifier(ident) = expr else {
            return false;
        };

        let len = ident.len();
        assert!(len >= 2);
        let ident = &ident[len - 2];
        &ident.value == self.0.table()
    }

    /// This function removes expressions that refer to the inserted table, if any.
    /// These expressions are equivalent to UPDATEs and are not supported yet.
    ///
    /// TODO: A workaround for those would be to force an update after this query (UPDATE table SET <...> WHERE id = ?) ; there are some edgecases to consider however
    /// TODO: queries that just set a=a may be forwarded as is?
    fn rewrite_bad_identifier(&self, expr: &mut Expr) -> ControlFlow<<Self as VisitorMut>::Break> {
        let Expr::BinaryOp { right, left, .. } = expr else {
            return ControlFlow::Continue(());
        };

        if self.is_bad_identifier(left.as_ref()) {
            warn!(
                "Removing unsupported expression {left} from a ON DUPLICATE KEY query (unsupported)"
            );
            *expr = Expr::clone(right);
        } else if self.is_bad_identifier(right.as_ref()) {
            warn!(
                "Removing unsupported expression {right} from a ON DUPLICATE KEY query (unsupported)"
            );
            *expr = Expr::clone(left);
        }

        ControlFlow::Continue(())
    }
}

impl VisitorMut for ValuesRewriter {
    type Break = datafusion::common::Result<()>;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        Self::rewrite_function(expr)?;
        self.rewrite_bad_identifier(expr)?;

        ControlFlow::Continue(())
    }
}

impl MySqlLogicalPlanner {
    fn parse_assignments(
        assignments: Vec<Assignment>,
    ) -> datafusion::common::Result<(Vec<Ident>, Vec<Expr>)> {
        let mut values = Vec::new();
        let mut columns = Vec::new();

        for assignment in assignments.into_iter() {
            let AssignmentTarget::ColumnName(mut cn) = assignment.target else {
                plan_err!("invalid SET expression: {assignment}")?
            };

            if cn.0.is_empty() {
                plan_err!("invalid empty target for SET expression")?
            }

            let ObjectNamePart::Identifier(cn) = cn.0.remove(0) else {
                plan_err!("invalid target for SET expression: {cn}")?
            };

            columns.push(cn);
            values.push(assignment.value);
        }

        Ok((columns, values))
    }

    pub(super) async fn plan_insert(
        &self,
        mut ins: Insert,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan> {
        if ins.ignore {
            ins.ignore = false;
            log::warn!("UNIMPLEMENTED: ignoring `ignore` flag on insert for now");
        }

        // Rewrite "set-style" inserts
        if ins.source.is_none() {
            if ins.assignments.is_empty() {
                not_impl_err!("insert without a source nor assignments!")?;
            }

            let assignments = mem::take(&mut ins.assignments);
            let (mut columns, values) = Self::parse_assignments(assignments)?;
            ins.columns.append(&mut columns);

            ins.source = Some(Box::new(Query {
                body: Box::new(SetExpr::Values(Values {
                    explicit_row: false,
                    rows: vec![values],
                })),

                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
                with: None,
            }))
        }

        // Not supported by base plan
        let insert_on = ins.on.take();
        let on_duplicate =
            if let Some(OnInsert::DuplicateKeyUpdate(on_duplicate_key_update)) = insert_on {
                let (columns, mut values) = Self::parse_assignments(on_duplicate_key_update)?;
                let TableObject::TableName(table_name) = ins.table.clone() else {
                    plan_err!("Invalid target for INSERT INTO")?
                };
                let table_name = object_name_to_table_reference(
                    table_name,
                    session_state
                        .config_options()
                        .sql_parser
                        .enable_ident_normalization,
                )?;
                let mut visitor = ValuesRewriter(table_name);

                // Some replacement values use the `VALUES(...)` function that refers to the expression set in the VALUES field, we must replace it
                for expr in values.iter_mut() {
                    let result = expr.visit(&mut visitor);

                    if let ControlFlow::Break(result) = result {
                        result?;
                    }
                }
                Some((columns, values))
            } else {
                None
            };

        let mut base_plan =
            default_statement_to_plan(Statement::Insert(ins), session_state).await?;

        let LogicalPlan::Dml(dml) = &mut base_plan else {
            plan_err!("INSERT statement produced invalid plan")?
        };

        if let Some((columns, values)) = on_duplicate {
            // Wrap the input plan in a projection that adds the new columns
            let mut base_projection = dml
                .input
                .schema()
                .fields()
                .iter()
                .map(|field| LogicalExpr::Column(Column::new(None::<TableReference>, field.name())))
                .collect::<Vec<_>>();

            for (target, expr) in columns.into_iter().zip(values.into_iter()) {
                let expr = expr.to_string();
                let expr = session_state.create_logical_expr(&expr, dml.input.schema().as_ref())?;

                base_projection.push(LogicalExpr::Alias(Alias::new(
                    expr,
                    None::<TableReference>,
                    format!("{DUPLICATE_VALUE_PFX}{}", target.value),
                )))
            }

            let project =
                LogicalPlan::Projection(Projection::try_new(base_projection, dml.input.clone())?);
            dml.input = Arc::new(project);
        }

        Ok(base_plan)
    }
}
