use crate::udf::exists::ExistsUdf;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DataFusionError, ScalarValue, Spans, TableReference};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Aggregate, AggregateUDF, Expr, LogicalPlan, Subquery};
use datafusion::optimizer::AnalyzerRule;
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct RewriteExists {
    udf: Arc<AggregateUDF>,
}

impl Default for RewriteExists {
    fn default() -> Self {
        Self {
            udf: ExistsUdf::get_udf(),
        }
    }
}

impl RewriteExists {
    fn transform_expr(&self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        expr.transform(|expr| match expr {
            Expr::Exists(exists) => {
                let subquery = exists.subquery.subquery.clone();
                if subquery.schema().fields().len() <= 1 {
                    return Ok(Transformed::no(Expr::Exists(exists)));
                }

                let sub_schema = subquery
                    .schema()
                    .fields()
                    .first()
                    .map(|field_ref| {
                        Expr::Column(Column::new(None::<TableReference>, field_ref.name()))
                    })
                    .unwrap_or(Expr::Literal(ScalarValue::Null, None));

                let aggregate_expression = Expr::AggregateFunction(AggregateFunction::new_udf(
                    self.udf.clone(),
                    vec![sub_schema],
                    false,
                    None,
                    vec![],
                    None,
                ));

                let aggregate_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    subquery,
                    vec![],
                    vec![aggregate_expression],
                )?);

                let expr = Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(aggregate_plan),
                    outer_ref_columns: vec![],
                    spans: Spans::new(),
                });

                Ok(Transformed::yes(expr))
            }
            other => Ok(Transformed::no(other)),
        })
    }
}

impl AnalyzerRule for RewriteExists {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let Transformed { data, .. } = plan.transform_up(|mut plan| {
            match plan {
                LogicalPlan::Projection(ref mut project) => {
                    let num_entries = project.expr.len();
                    let Transformed {
                        data, transformed, ..
                    } = mem::take(&mut project.expr)
                        .into_iter()
                        .map(|projection| self.transform_expr(projection))
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
                }
                LogicalPlan::Filter(ref mut filter) => {
                    let transform = self.transform_expr(filter.predicate.clone())?;
                    if transform.transformed {
                        /* const TMP_COL_NAME: &str = "_exists";

                        let input = filter.input.schema();
                        let base_columns = input.columns()
                            .into_iter().map(|col| Expr::Column(col));

                        let schema = base_columns
                            .chain(once(transform.data.alias(TMP_COL_NAME)));

                        let new_input = LogicalPlan::Projection(Projection::try_new(schema.collect(), filter.input.clone())?);
                        let new_filter = Expr::Column(Column::from_name(TMP_COL_NAME));

                        filter.input = Arc::new(new_input); */
                        filter.predicate = transform.data;
                        Ok(Transformed::yes(plan))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                }
                other => Ok(Transformed::no(other)),
            }
        })?;

        Ok(data)
    }

    fn name(&self) -> &str {
        "rewrite_exists"
    }
}
