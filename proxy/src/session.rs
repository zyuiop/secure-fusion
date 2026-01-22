use crate::cache::CachingPlannerManager;
use async_trait::async_trait;
use common::statement::ParsedStatement;
use common::{
    BackendWrappedSession, HandlerError, HandlerResult, LogicalPrePlanner, ProxySession,
    StatementId, StatementResponse, profile,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, HashMap, ParamValues};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use futures::lock::Mutex;
use nohash_hasher::IntMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "df-trace")]
use std::time::Instant;

pub struct DataFusionSession<T: BackendWrappedSession> {
    session: T,
    pre_planner: Mutex<CachingPlannerManager>,
    prepared_queries: Mutex<IntMap<StatementId, Statement>>,
    current_plan_id: AtomicU64,
}

struct Statement {
    plan: LogicalPlan,
    #[allow(unused)]
    params: HashMap<String, DataType>,
}

impl Statement {
    #[allow(unused)]
    fn coerce_parameters(&self, parameters: &mut ParamValues) -> HandlerResult<()> {
        match parameters {
            ParamValues::List(params) => {
                params
                    .iter_mut()
                    .enumerate()
                    .try_for_each::<_, HandlerResult<()>>(|(index, param)| {
                        let param_type = self
                            .params
                            .get(&format!("${}", index + 1))
                            .ok_or_else(|| HandlerError::InvalidParameter)?;

                        if &param.value.data_type() == param_type {
                            return Ok(());
                        }

                        param.value = param.value.cast_to(param_type)?;
                        Ok(())
                    })?;
            }
            ParamValues::Map(param_map) => {
                param_map
                    .iter_mut()
                    .try_for_each::<_, HandlerResult<()>>(|(index, param)| {
                        let param_type = self
                            .params
                            .get(index)
                            .ok_or_else(|| HandlerError::InvalidParameter)?;

                        if &param.value.data_type() == param_type {
                            return Ok(());
                        }

                        param.value = param.value.cast_to(param_type)?;
                        Ok(())
                    })?;
            }
        }

        Ok(())
    }
}

impl<T: BackendWrappedSession> DataFusionSession<T> {
    pub fn new(session: T, pre_planner: Arc<dyn LogicalPrePlanner + Sync + Send>) -> Self {
        Self {
            session,
            pre_planner: Mutex::new(CachingPlannerManager::new(pre_planner)),
            prepared_queries: Mutex::new(IntMap::default()),
            current_plan_id: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl<T: BackendWrappedSession> ProxySession for DataFusionSession<T> {
    fn underlying_engine(&self) -> &SessionContext {
        &self.session.session()
    }

    async fn switch_database(&self, database: &str) -> HandlerResult<()> {
        self.session.switch_database(database).await?;
        Ok(())
    }

    async fn query_immediate(
        &self,
        query: ParsedStatement,
        _parameters: Option<ParamValues>,
    ) -> HandlerResult<SendableRecordBatchStream> {
        let session_state = self.session.session().state();

        let mut planner = self.pre_planner.try_lock().unwrap();
        let plan = planner
            .prepare_query_with_cache(&session_state, query)
            .await?;

        #[cfg(feature = "df-trace")]
        let plan = crate::physical_tracer::PhysicalTracer::apply_recursive(plan)?;

        let task_ctx = Arc::new(TaskContext::from(&session_state));

        #[cfg(feature = "df-trace")]
        let new_config = task_ctx
            .session_config()
            .clone()
            .with_extension(Arc::new(Instant::now()));
        #[cfg(feature = "df-trace")]
        let task_ctx = task_ctx.with_session_config(new_config);

        #[cfg(feature = "debug-plans")]
        {
            let displayable =
                datafusion::physical_plan::display::DisplayableExecutionPlan::new(plan.as_ref());
            log::info!("Physical plan: {}", displayable.indent(false));
        }

        //let displayable = DisplayableExecutionPlan::new(plan.as_ref());
        //info!("Query plan:\n{}", displayable.indent(true));

        let stream = profile!(
            "query_immediate::execute_stream",
            execute_stream(plan, task_ctx)
        )?;

        Ok(stream)
    }

    async fn statement_execute(
        &self,
        statement_id: StatementId,
        parameters: ParamValues,
    ) -> HandlerResult<SendableRecordBatchStream> {
        let session_state = self.session.session().state();

        let plan = {
            let prepared_map = self.prepared_queries.try_lock().unwrap();
            let statement = prepared_map.get(&statement_id).ok_or_else(|| {
                DataFusionError::Execution(format!("Statement not found: {statement_id}"))
            })?;

            // statement.coerce_parameters(&mut parameters)?;
            statement.plan.clone().with_param_values(parameters)?
        };

        let phys_plan = profile!(
            "plan_physical",
            session_state
                .query_planner()
                .create_physical_plan(&plan, &session_state)
                .await?
        );

        let task_ctx = Arc::new(TaskContext::from(&session_state));

        #[cfg(feature = "debug-plans")]
        {
            let displayable = datafusion::physical_plan::display::DisplayableExecutionPlan::new(
                phys_plan.as_ref(),
            );
            log::info!("Physical plan: {}", displayable.indent(false));
        }

        let stream = profile!(
            "statement_execute::execute_stream",
            execute_stream(phys_plan, task_ctx)
        )?;

        Ok(stream)
    }

    async fn statement_open(&self, query: ParsedStatement) -> HandlerResult<StatementResponse> {
        let session_state = self.session.session().state();

        let planner = self.pre_planner.try_lock().unwrap();

        // TODO: this may be done in the frontend instead - as datafusion is agnostic to the kind of parameters passed

        let plan = profile!(
            "cache::plan_logical",
            planner.prepare_optimize_plan(&session_state, query).await?
        );

        let param_types: HashMap<String, DataType> = plan
            .get_parameter_types()?
            .into_iter()
            .map(|(key, value)| (key, value.expect("placeholder with no type encountered")))
            .collect();

        let output = plan.schema().fields();

        let response = StatementResponse {
            statement_id: self.current_plan_id.fetch_add(1, Ordering::AcqRel),
            columns: output.into_iter().cloned().collect(),
            parameters: param_types.clone(),
        };

        let mut prepared_map = self.prepared_queries.try_lock().unwrap();
        prepared_map.insert(
            response.statement_id.clone(),
            Statement {
                params: param_types,
                plan,
            },
        );

        Ok(response)
        /*
        let ParsedStatement::Statement(query) = query else {
            return Err(HandlerError::CustomStatement);
        };

        let plan = self
            .session
            .state()
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                query,
            )))
            .await?;

        let plan = self.session.state().optimize(&plan)?;
        */

        // TODO: at this step, it would be interesting to send a "warm-up" to the MySQL database so that it
        // SUGGESTION: add a TransactionCapableEngine in the session state config
        // It has three methods: open(statementID, statement), execute(statementID, arguments), close(statementID)
        // When an engine implements this (provides an impl in session), execution is deferred to that engine

        // self.session.state().config().

        /* let statement_id = self
            .current_plan_id
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if statement_id == u64::MAX {
            panic!("session has reached maximal statement id!")
        }

        // Determine parameters
        let param_types = plan.get_parameter_types()?;
        info!("param_types: {:?}", param_types);

        let mut param_types = vec![];
        plan.apply_expressions(|expr| {
            expr.apply(|expr| match expr {
                Expr::Placeholder(placeholder) => {
                    // placeholder.
                    info!("placeholder here: {:?}", placeholder);
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            })
        })
        .unwrap();

        let mut prep_queries = self
            .prepared_queries
            .write()
            .expect("could not lock prepared_queries map for writing");
        prep_queries.insert(
            statement_id,
            PreparedPlan {
                plan: Arc::new(plan),
                param_types,
            },
        );

        Ok(StatementResponse {
            statement_id,
            parameters: vec![],
            columns: vec![],
        })*/
    }

    async fn statement_close(&self, statement_id: StatementId) -> HandlerResult<()> {
        let mut prepared_map = self.prepared_queries.try_lock().unwrap();
        prepared_map.remove(&statement_id);
        Ok(())
    }
}
