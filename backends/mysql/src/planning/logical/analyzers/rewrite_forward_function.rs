use crate::planning::logical::custom_forward_statement::ForwardStatement;
use crate::udf::mysql_forward_functions::FORWARDED_FUNCTIONS_NAMES;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef, DataFusionError, Spans};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{EmptyRelation, Expr, Extension, LogicalPlan, Projection, Subquery};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::MySqlDialect;
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct RewriteForwardedFunctions;

impl RewriteForwardedFunctions {
    fn transform_expr(&self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        match expr {
            Expr::ScalarFunction(func)
                if FORWARDED_FUNCTIONS_NAMES.contains(&func.name().to_lowercase()) =>
            {
                // We build a simple SELECT func(...) plan from the function
                let sub_query = LogicalPlan::Projection(Projection::try_new(
                    vec![Expr::ScalarFunction(func)],
                    Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    })),
                )?);
                let sub_query_output = sub_query.schema();

                let unparser = Unparser::new(&MySqlDialect {});
                let unparsed_sub_query = unparser.plan_to_sql(&sub_query)?;

                let forwarded_query = ForwardStatement::new_with_schema(
                    unparsed_sub_query,
                    Arc::clone(sub_query_output),
                );
                let forwarded_query = LogicalPlan::Extension(Extension {
                    node: Arc::new(forwarded_query),
                });

                // We must wrap in a Projection, otherwise some optimizer nodes struggle
                let field_name = sub_query_output.fields()[0].name();
                let forwarded_query = LogicalPlan::Projection(Projection::try_new(
                    vec![Expr::Column(Column::new(
                        None::<TableReference>,
                        field_name,
                    ))],
                    Arc::new(forwarded_query),
                )?);

                let expr = Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(forwarded_query),
                    outer_ref_columns: vec![],
                    spans: Spans::new(),
                });

                Ok(Transformed::yes(expr))
            }
            other => Ok(Transformed::no(other)),
        }
    }
}

impl AnalyzerRule for RewriteForwardedFunctions {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let Transformed { data, .. } = plan.transform_up(|mut plan| {
            let LogicalPlan::Projection(ref mut project) = plan else {
                return Ok(Transformed::no(plan));
            };

            let num_entries = project.expr.len();
            let Transformed {
                data, transformed, ..
            } = mem::take(&mut project.expr)
                .into_iter()
                .map(|projection| projection.transform(|expr| self.transform_expr(expr)))
                .try_fold(
                    Transformed::<Vec<Expr>>::no(Vec::with_capacity(num_entries)),
                    |mut vec, field_result| {
                        let Transformed {
                            transformed, data, ..
                        } = field_result?;

                        vec.data.push(data);
                        if transformed {
                            Ok::<_, DataFusionError>(Transformed::yes(vec.data))
                        } else {
                            Ok(vec)
                        }
                    },
                )?;

            project.expr = data;
            Ok(Transformed::new_transformed(plan, transformed))
        })?;

        Ok(data)
    }

    fn name(&self) -> &str {
        "rewrite_forwarded_functions"
    }
}
