//! Some functions accept subquery arguments even when these subqueries are not well formed datafusion scalar subqueries
//! Datafusion expects subqueries in that case to have a single row, but sometimes the expression itself can sort it out (e.g. coalesce)
//! This rule is there to rewrite these expressions so that the invariants analyser accepts them

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, plan_datafusion_err};
use datafusion::config::ConfigOptions;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Aggregate, Expr, Filter, LogicalPlan, col, is_not_null};
use datafusion::optimizer::AnalyzerRule;
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct RewriteSubqueryArguments;

impl RewriteSubqueryArguments {
    fn transform_coalesce(&self, arg: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        match arg {
            Expr::ScalarSubquery(v) => {
                // The inner plan must be an aggregate, or a filter that has an aggregate as child,
                // and the aggregate must return a single row
                if v.subquery.max_rows().is_some_and(|v| v <= 1) {
                    return Ok(Transformed::no(Expr::ScalarSubquery(v)));
                }

                if let LogicalPlan::Aggregate(agg) = v.subquery.as_ref()
                    && !agg.aggr_expr.is_empty()
                {
                    return Ok(Transformed::no(Expr::ScalarSubquery(v)));
                }

                if let LogicalPlan::Filter(Filter { input, .. }) = v.subquery.as_ref() {
                    if let LogicalPlan::Aggregate(agg) = input.as_ref()
                        && !agg.aggr_expr.is_empty()
                    {
                        return Ok(Transformed::no(Expr::ScalarSubquery(v)));
                    }
                }

                // The inner plan does not seem to be an aggregate - we can therefore try to apply our rule
                let filter_plan = LogicalPlan::Filter(Filter::try_new(
                    conjunction(
                        v.subquery
                            .schema()
                            .iter()
                            .map(|field| is_not_null(col(field))),
                    )
                    .ok_or(plan_datafusion_err!("no columns in scalar subquery"))?,
                    v.subquery.clone(),
                )?);

                let aggregate_expr = Aggregate::try_new(
                    Arc::new(filter_plan),
                    vec![],
                    v.subquery
                        .schema()
                        .iter()
                        .map(|field| first_value(col(field), vec![]))
                        .collect(),
                )?;

                let subquery = v.with_plan(Arc::new(LogicalPlan::Aggregate(aggregate_expr)));
                Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
            }
            other => Ok(Transformed::no(other)),
        }
    }
    fn transform_expr(&self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        expr.transform(|expr| match expr {
            Expr::ScalarFunction(mut sf) if sf.name() == "coalesce" => {
                // Some args may be subqueries - wrap them
                let mut changed = false;
                for arg in mem::take(&mut sf.args) {
                    let transformed = self.transform_coalesce(arg)?;
                    sf.args.push(transformed.data);
                    changed = changed | transformed.transformed;
                }

                Ok(Transformed::new_transformed(
                    Expr::ScalarFunction(sf),
                    changed,
                ))
            }
            other => Ok(Transformed::no(other)),
        })
    }
}

impl AnalyzerRule for RewriteSubqueryArguments {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let Transformed { data, .. } =
            plan.transform_up(|plan| plan.map_expressions(|expr| self.transform_expr(expr)))?;

        Ok(data)
    }

    fn name(&self) -> &str {
        "rewrite_subquery_arguments"
    }
}
