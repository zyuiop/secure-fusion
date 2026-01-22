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

use crate::errors::{MySqlBackendError, MySqlResult};
use crate::metadata::kw_search_func::register_kw_search;
use crate::planning::logical::MySqlLogicalPlanner;
use crate::planning::physical::MySqlPhysicalPlanner;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use async_trait::async_trait;
use common::extensions::variable_store::VariableStoreExtension;
use common::{Backend, BackendWrappedSession, LogicalPrePlanner};
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
use datafusion::optimizer::{Analyzer, Optimizer};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::variable::VarType;
use futures_util::lock::Mutex;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use rustc_hash::FxHashMap;
use std::any::Any;
use std::ops::DerefMut;
use std::sync::Arc;

mod errors;
mod get_catalog;
mod get_conn;
mod metadata;
mod planning;
mod providers;
mod serder;
mod sinks;
mod transaction_control;
mod variables;

#[cfg(not(feature = "old-scan-converter"))]
mod arrow_helper;
mod backend_config;
mod expr_util;
mod udf;

use crate::backend_config::BackendConfig;
use crate::get_conn::ConnGetter;
use crate::planning::logical::analyzers::insert_analyzer_rules;
use crate::store::MetadataStore;
use crate::transaction_control::TransactionControl;
use crate::udf::{expr_planners, register_aggregates, register_udfs, register_udfs_late};
pub use metadata::store;

pub use backend_config::BackendConfig as MySqlBackendConfig;

const DEFAULT_CATALOG: &str = "def";

#[derive(Debug)]
pub(crate) struct MySqlCatalogProviderList {
    inner: Arc<MySqlCatalogProvider>,
}

pub struct MySqlBackend {
    catalog_provider: Arc<MySqlCatalogProviderList>,
    logical_planner: Arc<MySqlLogicalPlanner>,
    physical_planner: Arc<MySqlPhysicalPlanner>,
    metadata_store: MetadataStore,
    backend_config: Arc<BackendConfig>,
}

impl MySqlCatalogProviderList {
    pub(crate) fn default_catalog(&self) -> Arc<MySqlCatalogProvider> {
        self.inner.clone()
    }
}

impl CatalogProviderList for MySqlCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![DEFAULT_CATALOG.to_string()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == DEFAULT_CATALOG {
            Some(self.inner.clone())
        } else {
            None
        }
    }
}

// TODO: use a two tier backend structure:
// 1. MySqlBackend is started **once** at server startup - it maintains the list of tables up to date
// 2. A "new_session" operation is used to initiate a new session dedicated to a single client

impl MySqlBackend {
    pub async fn new(config: BackendConfig) -> Result<Self, MySqlBackendError> {
        let conn = config.get_conn().await?;
        let metadata_store = MetadataStore::from(&config.metadata_store);

        let catalog_provider =
            MySqlCatalogProvider::introspect(conn, &metadata_store, config.normalize_identifiers)
                .await?;

        // TODO: obtain list of variables?
        // TODO: obtain list of functions?

        Ok(MySqlBackend {
            catalog_provider: Arc::new(MySqlCatalogProviderList {
                inner: catalog_provider,
            }),
            logical_planner: Arc::new(MySqlLogicalPlanner),
            physical_planner: Arc::new(MySqlPhysicalPlanner::default()),
            backend_config: Arc::new(config),
            metadata_store,
        })
    }

    async fn get_connection_for_session(&self) -> Arc<Mutex<Conn>> {
        let conn = self.backend_config.get_conn().await.unwrap();
        // conn.query_drop("SET autocommit=0").await.unwrap();
        Arc::new(Mutex::new(conn))
    }

    pub async fn new_connection(&self, ctx: Arc<TaskContext>) -> MySqlResult<Conn> {
        let mut conn = self.backend_config.get_conn().await?;
        let db_name = ctx
            .session_config()
            .options()
            .catalog
            .default_schema
            .clone();
        conn.query_drop(format!("USE {db_name}")).await?;
        Ok(conn)
    }
}

trait GetBackend {
    fn get_backend(&self) -> Arc<MySqlBackend>;
}

impl GetBackend for SessionConfig {
    fn get_backend(&self) -> Arc<MySqlBackend> {
        self.get_extension()
            .expect("proxy did not insert a backend reference!")
    }
}

impl GetBackend for Arc<TaskContext> {
    fn get_backend(&self) -> Arc<MySqlBackend> {
        self.session_config().get_backend()
    }
}

#[repr(transparent)]
pub struct WrappedSession(SessionContext);

#[async_trait]
impl BackendWrappedSession for WrappedSession {
    #[inline(always)]
    fn session(&self) -> &SessionContext {
        &self.0
    }

    async fn switch_database(&self, database: &str) -> datafusion::common::Result<()> {
        let write_state = self.0.state_ref();

        {
            // Wrap access in a block to auto-drop the lock after
            let mut write_state = write_state.write();

            write_state
                .config_mut()
                .options_mut()
                .catalog
                .default_schema = String::from(database);
        }

        let conn = self.0.state().get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop(format!("USE {database}")).await.unwrap();

        Ok(())
    }
}

#[async_trait]
impl Backend for MySqlBackend {
    type SessionType = WrappedSession;

    async fn start_init_session(
        &self,
        base_config: SessionConfig,
        initial_database: Option<&str>,
    ) -> SessionStateBuilder {
        let connection = self.get_connection_for_session().await;

        let mut variable_store = VariableStoreExtension::default();

        {
            let mut tmp_conn = connection.try_lock().unwrap();

            if let Some(initial_database) = initial_database {
                tmp_conn
                    .query_drop(format!("USE {initial_database}"))
                    .await
                    .unwrap();
            }

            variable_store.set_variables(
                VarType::System,
                self.get_session_variables(tmp_conn.deref_mut())
                    .await
                    .unwrap(),
            );
        }
        variable_store.set_variables(VarType::UserDefined, FxHashMap::default());

        let variable_store = Arc::new(variable_store);

        let mut config = base_config
            .with_default_catalog_and_schema(DEFAULT_CATALOG, initial_database.unwrap_or(""))
            .with_extension(Arc::new(self.metadata_store.clone()))
            .with_extension(connection)
            .with_extension(Arc::new(TransactionControl::default()))
            .with_extension(variable_store.clone())
            .with_extension(self.backend_config.clone());

        config.options_mut().sql_parser.enable_ident_normalization =
            self.backend_config.normalize_identifiers;
        config
            .options_mut()
            .execution
            .use_row_number_estimates_to_optimize_partitioning = true;
        config
            .options_mut()
            .execution
            .skip_physical_aggregate_schema_check = true;
        config.options_mut().execution.target_partitions = 1;
        config.options_mut().execution.batch_size = 4000; // Try to see if it's faster with smaller batches?

        let mut ssb = SessionStateBuilder::new()
            .with_config(config)
            .with_catalog_list(self.catalog_provider.clone())
            .with_expr_planners(expr_planners())
            .with_default_features()
            .with_file_formats(Vec::new())
            .with_query_planner(self.physical_planner.clone());

        let exec_props = ssb.execution_props().get_or_insert_default();
        exec_props.add_var_provider(VarType::System, variable_store.store(VarType::System));
        exec_props.add_var_provider(
            VarType::UserDefined,
            variable_store.store(VarType::UserDefined),
        );

        let functions = ssb.scalar_functions().get_or_insert_default();
        register_kw_search(functions);
        register_udfs(functions);
        register_aggregates(ssb.aggregate_functions().get_or_insert_default());

        ssb
    }

    fn finish_init_session(&self, mut state: SessionState) -> Self::SessionType {
        register_udfs_late(&mut state);

        WrappedSession(SessionContext::new_with_state(state))
    }

    fn get_extension_planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Sync + Send>> {
        planning::physical::get_extensions()
    }

    fn get_logical_planner(&self) -> Arc<dyn LogicalPrePlanner + Sync + Send> {
        self.logical_planner.clone()
    }

    fn add_physical_optimizer_rules(&self, optimizer_rules: &mut PhysicalOptimizer) {
        optimizer_rules
            .rules
            .extend_from_slice(&planning::physical::get_optimizers());
    }

    fn add_optimizer_rules(&self, optimizer_rules: &mut Optimizer) {
        optimizer_rules
            .rules
            .extend_from_slice(&planning::logical::optimizers::get_optimizers());
    }

    fn add_analyzer_rules(&self, analizer: &mut Analyzer) {
        insert_analyzer_rules(analizer);
    }
}
