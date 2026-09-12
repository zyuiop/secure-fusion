use common::statement::ParsedStatement;
use common::{HandlerResult, LogicalPrePlanner, profile};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, Query, Statement, TableFactor, Visit, Visitor,
};
use rustc_hash::FxHashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;

pub struct CachingPlannerManager {
    pre_planner: Arc<dyn LogicalPrePlanner + Sync + Send>,
    cache: Cache,
}

#[derive(Debug)]
struct Cache {
    cache: FxHashMap<Statement, CacheEntry>,
    used_memory: usize, // approximation
}

#[derive(Debug)]
struct CacheEntry {
    plan: Arc<LogicalPlan>,

    last_used: Instant,

    memory_size: usize,
}

impl Cache {
    pub fn insert(&mut self, statement: Statement, plan: Arc<LogicalPlan>) {
        let entry = CacheEntry::new(&statement, plan);

        if let Some(entry) = entry {
            self.used_memory += entry.memory_size;
            self.cache.insert(statement, entry);
            profile!("cache cleanup", self.free_space());
        }
    }

    pub fn get(&mut self, statement: &Statement) -> Option<Arc<LogicalPlan>> {
        let entry = self.cache.get_mut(statement)?;
        entry.last_used = Instant::now();
        Some(entry.plan.clone())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "info"))]
    fn free_space(&mut self) {
        while self.used_memory >= QUERY_CACHE_SIZE {
            // locate least recently used
            let Some((to_evict_key, value)) =
                self.cache.iter().min_by_key(|(_, val)| val.last_used)
            else {
                // Weird, map is empty
                self.used_memory = 0;
                return;
            };

            self.used_memory = self
                .used_memory
                .checked_sub(value.memory_size)
                .unwrap_or_default();

            let to_evict_key = to_evict_key.clone(); // We need to clone to be able to re-borrow
            self.cache.remove(&to_evict_key);
        }
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            used_memory: 0,
            cache: FxHashMap::default(),
        }
    }
}

const MAX_CACHEABLE_STATEMENT_SIZE: usize = 16 * 1024;
const QUERY_CACHE_SIZE: usize = 1048576;

struct StatementSizeVisitor(usize);

impl StatementSizeVisitor {
    fn add<T>(&mut self, other: &T) -> ControlFlow<usize> {
        let size = size_of_val(other);
        self.0 += size;

        if size >= MAX_CACHEABLE_STATEMENT_SIZE {
            ControlFlow::Break(self.0)
        } else {
            ControlFlow::Continue(())
        }
    }
}

impl Visitor for StatementSizeVisitor {
    type Break = usize;

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.add(_query)
    }

    fn pre_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.add(_relation)
    }

    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.add(_table_factor)
    }

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.add(_expr)
    }

    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        self.add(_statement)
    }

    fn pre_visit_value(&mut self, _value: &ast::Value) -> ControlFlow<Self::Break> {
        self.add(_value)
    }
}

impl CacheEntry {
    fn new(statement: &Statement, plan: Arc<LogicalPlan>) -> Option<Self> {
        let mut visitor = StatementSizeVisitor(0);
        let _ = Visit::visit(statement, &mut visitor);
        let _ = plan.apply_with_subqueries(|elem| {
            visitor.0 += size_of_val(elem);
            if visitor.0 >= MAX_CACHEABLE_STATEMENT_SIZE {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });

        let memory_size = visitor.0;

        if memory_size >= MAX_CACHEABLE_STATEMENT_SIZE {
            None
        } else {
            Some(Self {
                plan: plan.clone(),
                last_used: Instant::now(),
                memory_size,
            })
        }
    }
}

impl CachingPlannerManager {
    pub fn new(pre_planner: Arc<dyn LogicalPrePlanner + Sync + Send>) -> Self {
        Self {
            pre_planner,
            cache: Cache::default(),
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
    ) -> HandlerResult<Arc<LogicalPlan>> {
        if let Some(found) = self.cache.get(&normalized_query) {
            #[cfg(feature = "tracing")]
            tracing::info!("cache_hit");

            return Ok(found.clone());
        };

        let plan = self
            .prepare_optimize_plan(
                session,
                ParsedStatement::Statement(normalized_query.clone()),
            )
            .await?;
        let plan = Arc::new(plan);
        self.cache.insert(normalized_query.clone(), plan.clone());

        Ok(plan)
    }

    async fn get_logical_statement(
        &mut self,
        session: &SessionState,
        query: ParsedStatement,
    ) -> HandlerResult<Arc<LogicalPlan>> {
        let plan = if let ParsedStatement::Statement(statement) = query {
            self.prepare_logical_or_get_cached(session, statement)
                .await?
        } else {
            let plan = self.pre_planner.statement_to_plan(query, session).await?;
            let plan = profile!("query_immediate::optimize", session.optimize(&plan)?);
            Arc::new(plan)
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
                .create_physical_plan(plan.as_ref(), session_state)
                .await?
        ))
    }
}
