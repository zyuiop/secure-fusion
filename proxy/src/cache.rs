use common::statement::ParsedStatement;
use common::{HandlerResult, LogicalPrePlanner, profile};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{Value, VisitMut, VisitorMut};
use datafusion::optimizer::Optimizer;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::Statement;
use rustc_hash::FxHashMap;
use std::mem;
use std::ops::ControlFlow;
use std::sync::Arc;

fn ast_value_to_scalar(value: Value) -> ScalarValue {
    match value {
        Value::Number(n, _) => {
            // TODO: we may be smarter here if we know what is the expected type
            // For now, we'll accept false negatives and cache misses
            if let Ok(n) = n.parse::<i64>() {
                ScalarValue::Int64(Some(n))
            } else if let Ok(n) = n.parse::<u64>() {
                ScalarValue::UInt64(Some(n))
            } else {
                ScalarValue::Float64(n.parse::<f64>().ok())
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => ScalarValue::Utf8(Some(s)),
        Value::Null => ScalarValue::Null,
        Value::Boolean(n) => ScalarValue::from(n),
        Value::Placeholder(_) => {
            unimplemented!("placeholder")
        }
        Value::HexStringLiteral(s) => {
            let bytes = hex::decode(s).ok();
            ScalarValue::Binary(bytes)
        }
        Value::DollarQuotedString(s) => ScalarValue::from(s.value),
        Value::EscapedStringLiteral(s) => ScalarValue::from(s),
        other => unimplemented!("invalid value {other:?}"),
    }
}

pub struct CachingPlannerManager {
    pre_planner: Arc<dyn LogicalPrePlanner + Sync + Send>,
    /// An optimizer applied after getting the plan out of cache. Try to keep it as small as possible,
    /// only put rules that benefit from having full parameterized queries (for example, constant simplification)
    optimizer: Optimizer,
    cache: FxHashMap<Statement, Arc<CacheEntry>>,
}
struct CacheEntry {
    plan: LogicalPlan,
    param_types: Vec<Option<DataType>>,
}

impl CacheEntry {
    fn new(plan: LogicalPlan) -> HandlerResult<Self> {
        let param_types = plan.get_parameter_types()?;
        if param_types.is_empty() {
            return Ok(Self {
                plan,
                param_types: Vec::new(),
            });
        }

        let param_types = (0..param_types.len())
            .map(|index| {
                param_types
                    .get(&format!("${}", index + 1))
                    .unwrap()
                    .clone()
                    .filter(|typ| typ != &DataType::Null)
            })
            .collect::<Vec<_>>();

        Ok(Self { plan, param_types })
    }

    fn coerce_values(&self, values: &mut [ScalarValue]) -> HandlerResult<()> {
        values
            .iter_mut()
            .zip(self.param_types.iter())
            .try_for_each::<_, HandlerResult<()>>(|(value, tpe)| {
                let Some(tpe) = tpe else { return Ok(()) };

                if &value.data_type() == tpe {
                    return Ok(());
                }

                *value = value.cast_to(tpe)?;
                Ok(())
            })?;

        Ok(())
    }
}

#[derive(Debug)]
struct ReplaceValuesVisitor {
    original_values: Vec<ScalarValue>,
}

impl ReplaceValuesVisitor {
    fn new() -> Self {
        Self {
            original_values: Vec::new(),
        }
    }

    fn finish(self) -> Vec<ScalarValue> {
        self.original_values
    }
}

impl VisitorMut for ReplaceValuesVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        match expr {
            ast::Expr::Value(inner) => {
                let value_id = self.original_values.len() + 1;
                let placeholder = ast::Value::Placeholder(format!("${value_id}"));
                let old_value = mem::replace(&mut inner.value, placeholder);
                let old_value = ast_value_to_scalar(old_value);

                self.original_values.push(old_value);
                ControlFlow::Continue(())
            }
            // Placeholders unsupported in this
            ast::Expr::Interval { .. } => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update { .. }
            | Statement::Delete(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }
}

impl CachingPlannerManager {
    pub fn new(pre_planner: Arc<dyn LogicalPrePlanner + Sync + Send>) -> Self {
        Self {
            pre_planner,
            cache: FxHashMap::default(),
            optimizer: Optimizer::with_rules(vec![Arc::new(SimplifyExpressions::new())]),
        }
    }

    fn get_cacheable_query(&self, statement: &mut Statement) -> Option<Vec<ScalarValue>> {
        let mut visitor = ReplaceValuesVisitor::new();
        // Determine which part of the statement can be rewritten to enable caching
        match statement {
            Statement::Query(q) => {
                let _ = q.body.visit(&mut visitor);
                Some(visitor.finish())
            }
            Statement::Update {
                selection,
                assignments,
                ..
            } => {
                if let Some(selection) = selection {
                    let _ = selection.visit(&mut visitor);
                }

                for assignment in assignments.iter_mut() {
                    let _ = assignment.visit(&mut visitor);
                }

                Some(visitor.finish())
            }
            Statement::Delete(delete) => {
                if let Some(selection) = &mut delete.selection {
                    let _ = selection.visit(&mut visitor);
                    Some(visitor.finish())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Prepares and optimizes a plan, without any attempt at caching
    pub(super) async fn prepare_optimize_plan(
        &self,
        session: &SessionState,
        statement: ParsedStatement,
    ) -> HandlerResult<LogicalPlan> {
        let plan = profile!(
            "cache::plan_logical",
            self.pre_planner
                .statement_to_plan(statement, session)
                .await?
        );

        let plan = profile!("cache::optimize", session.optimize(&plan)?);

        Ok(plan)
    }

    async fn prepare_logical_or_get_cached(
        &mut self,
        session: &SessionState,
        normalized_query: Statement,
        can_cache: bool,
    ) -> HandlerResult<Arc<CacheEntry>> {
        if let Some(found) = self.cache.get(&normalized_query) {
            return Ok(found.clone());
        };

        let plan = self
            .prepare_optimize_plan(
                session,
                ParsedStatement::Statement(normalized_query.clone()),
            )
            .await?;
        let plan = Arc::new(CacheEntry::new(plan.clone())?);

        if can_cache {
            self.cache.insert(normalized_query.clone(), plan.clone());
        }

        Ok(plan)
    }

    async fn get_logical_statement(
        &mut self,
        session: &SessionState,
        query: ParsedStatement,
    ) -> HandlerResult<LogicalPlan> {
        let plan = if let ParsedStatement::Statement(mut statement) = query {
            // Normalize query
            let variables = self.get_cacheable_query(&mut statement); // visitor.finish();

            let base = self
                .prepare_logical_or_get_cached(session, statement, variables.is_some())
                .await?;

            let plan = if let Some(mut variables) = variables {
                base.coerce_values(&mut variables)?;
                base.plan.clone().with_param_values(variables)?
            } else {
                base.plan.clone()
            };

            profile!(
                "get_logical_statement::re-optimize",
                self.optimizer.optimize(plan, session, |_, _| {})?
            )
        } else {
            let plan = self.pre_planner.statement_to_plan(query, session).await?;

            profile!("query_immediate::optimize", session.optimize(&plan)?)
        };

        Ok(plan)
    }

    pub(crate) async fn prepare_query_with_cache(
        &mut self,
        session_state: &SessionState,
        query: ParsedStatement,
    ) -> HandlerResult<Arc<dyn ExecutionPlan>> {
        let plan = profile!(
            "cache::get_logical_statement",
            self.get_logical_statement(session_state, query).await?
        );

        #[cfg(feature = "debug-plans")]
        log::info!("Logical plan: {}", plan.display_indent());

        Ok(profile!(
            "cache::plan_physical",
            session_state
                .query_planner()
                .create_physical_plan(&plan, session_state)
                .await?
        ))
    }
}
