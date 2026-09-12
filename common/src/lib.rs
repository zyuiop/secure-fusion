#![deny(
    unused_must_use,
    unreachable_code,
    unreachable_patterns,
    unused_imports,
    dead_code,
    irrefutable_let_patterns,
    unused_unsafe,
    unused_mut,
    unused_variables
)]
#![warn(unused_lifetimes, redundant_lifetimes)]
#![deny(clippy::perf)]

pub mod charset;
pub mod conversions;
pub mod dml;
pub mod ext;
pub mod extensions;
pub mod metadata;
pub mod parser;
pub mod profile;
pub mod statement;

use crate::parser::SqlStatement;
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{DataFusionError, HashMap, ParamValues};
use datafusion::execution::{SendableRecordBatchStream, SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{Analyzer, Optimizer};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::prelude::{SessionConfig, SessionContext};
use statement::ParsedStatement;
use std::sync::Arc;

/// Represents a "frontend" to the proxy, that is an entry point via which clients connect to it
pub trait Frontend {
    fn start_listening(self) -> !;
}

#[async_trait]
pub trait BackendWrappedSession: Send + Sync {
    fn session(&self) -> &SessionContext;

    async fn switch_database(&self, database: &str) -> datafusion::common::Result<()>;
}

#[async_trait]
pub trait Backend: Send + Sync {
    type SessionType: BackendWrappedSession;

    async fn start_init_session(
        &self,
        base_config: SessionConfig,
        initial_db: Option<&str>,
    ) -> SessionStateBuilder;

    fn finish_init_session(&self, state: SessionState) -> Self::SessionType;

    fn get_extension_planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Sync + Send>> {
        Vec::new()
    }

    /// Allows a backend to provide a mechanism to handle some queries that are not handled by
    /// datafusion natively
    fn get_logical_planner(&self) -> Arc<dyn LogicalPrePlanner + Sync + Send> {
        Arc::new(DefaultLogicalPrePlanner)
    }

    fn add_physical_optimizer_rules(&self, _optimizer_rules: &mut PhysicalOptimizer) {}

    fn add_optimizer_rules(&self, _optimizer_rules: &mut Optimizer) {}

    fn add_analyzer_rules(&self, _builder: &mut Analyzer) {}
}

#[async_trait]
pub trait LogicalPrePlanner {
    /// Similarly to [datafusion::execution::SessionState::statement_to_plan], creates a logical
    /// plan from a statement.
    /// The planning can be delegated to the underlying DataFusion engine by using the
    /// [datafusion::execution::SessionState::statement_to_plan] method on the provided session_state
    async fn statement_to_plan(
        &self,
        statement: ParsedStatement,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan>;
}

pub async fn default_statement_to_plan(
    statement: SqlStatement,
    session_state: &SessionState,
) -> HandlerResult<LogicalPlan> {
    // Default behavior
    let plan = session_state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            statement,
        )))
        .await?;

    Ok(plan)
}

pub struct DefaultLogicalPrePlanner;

#[async_trait]
impl LogicalPrePlanner for DefaultLogicalPrePlanner {
    async fn statement_to_plan(
        &self,
        statement: ParsedStatement,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan> {
        // Default behavior
        let ParsedStatement::Statement(query) = statement else {
            return Err(HandlerError::CustomStatement);
        };

        default_statement_to_plan(query, session_state).await
    }
}

pub type StatementId = u64;

#[derive(Debug)]
pub enum HandlerError {
    DatafusionError(DataFusionError),
    /// A statement not handled by the proxy was sent to the proxy
    CustomStatement,
    /// A parameter was sent in a prepared query but was not allowed
    InvalidParameter,
}

impl From<DataFusionError> for HandlerError {
    fn from(error: DataFusionError) -> Self {
        HandlerError::DatafusionError(error)
    }
}

pub enum LoginError {}

pub enum LoginMethod {
    Cleartext {
        password: String,
    },
    MySqlNativePassword {
        password: Vec<u8>,
        scramble_data: Vec<u8>,
    },
}

pub type HandlerResult<T> = Result<T, HandlerError>;

// TODO: find the correct type for the parameters to return
#[derive(Debug, Clone)]
pub struct StatementResponse {
    pub statement_id: StatementId,
    pub columns: Vec<FieldRef>,
    pub parameters: HashMap<String, arrow::datatypes::DataType>,
}

#[async_trait]
pub trait ProxyImplementation: Sync + Send {
    type SessionType: ProxySession + Sync + Send;
    type AuthenticationHandler: AuthenticationHandler + Sync + Send;

    /// Obtains a new query handler session.
    /// There should only be one session per client connection, ever.
    async fn new_session(&self, initial_database: Option<&str>) -> Self::SessionType;

    fn authentication_handler(&self) -> Arc<Self::AuthenticationHandler>;
}

#[async_trait::async_trait]
pub trait AuthenticationHandler {
    async fn try_login(&self, username: &str, password_data: LoginMethod)
    -> Result<(), LoginError>;
}

pub enum OkResult {
    Empty,
}

/// Represents the core of the proxy, used to enable the frontend to communicate with it
///
/// # Implementation notes
///
/// - close on drop
#[async_trait]
pub trait ProxySession {
    fn underlying_engine(&self) -> &SessionContext;

    async fn switch_database(&self, database: &str) -> HandlerResult<()>;

    async fn query_immediate(
        &self,
        query: ParsedStatement,
        parameters: Option<ParamValues>,
    ) -> HandlerResult<SendableRecordBatchStream>;

    async fn statement_open(&self, query: ParsedStatement) -> HandlerResult<StatementResponse>;

    async fn statement_execute(
        &self,
        statement_id: StatementId,
        parameters: ParamValues,
    ) -> HandlerResult<SendableRecordBatchStream>;

    async fn statement_close(&self, statement_id: StatementId) -> HandlerResult<()>;
}
