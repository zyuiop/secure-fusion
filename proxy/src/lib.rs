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

mod analyzer;
mod cache;
mod physical_optimizer;
pub mod session;

#[cfg(feature = "df-trace")]
mod physical_tracer;

use crate::analyzer::add_decryption_rule::AddDecryptionRule;
use crate::session::DataFusionSession;
use ::crypto::LongTermKeyManager;
use common::{Backend, LogicalPrePlanner};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

pub struct DataFusionProxy<B: Backend> {
    backend: Arc<B>,
    long_term_key_manager: Arc<LongTermKeyManager>,
    logical_planner: Arc<dyn LogicalPrePlanner + Send + Sync>,

    #[cfg(feature = "tracing")]
    tracing_enabled: bool,
}

impl<B: Backend + 'static> DataFusionProxy<B> {
    pub fn new(
        backend: B,
        long_term_key_manager: Arc<LongTermKeyManager>,

        #[cfg(feature = "tracing")] tracing_enabled: bool,
    ) -> Self {
        Self {
            logical_planner: backend.get_logical_planner(),
            backend: Arc::new(backend),
            long_term_key_manager,
            #[cfg(feature = "tracing")]
            tracing_enabled,
        }
    }

    pub async fn new_session(&self, default_db: Option<&str>) -> DataFusionSession<B::SessionType> {
        // TODO: decouble from actual backend
        // TODO: replace these with a proper connection mechanism!
        /*let mysql_params = to_secret_map(HashMap::from([
            (
                "connection_string".to_string(),
                "mysql://root:root@localhost:3306/enron".to_string(),
            ),
            ("sslmode".to_string(), "disabled".to_string()),
        ]));

        let mysql_pool = Arc::new(
            MySQLConnectionPool::new(mysql_params)
                .await
                .expect("unable to create MySQL connection pool"),
        );

        // Create database catalog provider
        // This allows us to access tables through catalog structure (catalog.schema.table)
        let table_factory = MySQLTableFactory::new(mysql_pool.clone());

        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, "enron" /* todo configure default DB */)
                .with_create_default_catalog_and_schema(true)
            // .with_option_extension() // TODO: extension for encryption and stuff!
        );

        let conn = mysql_pool.connect().await.unwrap();
        let conn = conn.as_async().unwrap();
        for schema in conn.schemas().await.unwrap() {
            for table in conn.tables(&schema).await.unwrap() {
                let table_ref = TableReference::partial(schema.clone(), table);
                ctx.register_table(table_ref.clone(), table_factory.read_write_table_provider(table_ref).await.unwrap()).unwrap();
            }
        }*/

        // TODO: read from config

        // TODO: default db should be a parameter
        let config = SessionConfig::new()
            .with_extension(self.long_term_key_manager.clone())
            .with_extension(self.backend.clone());

        let builder = self
            .backend
            .start_init_session(config, default_db)
            .await
            .with_default_features()
            .with_runtime_env(Arc::new(RuntimeEnv::default()));

        let builder = builder.with_analyzer_rule(Arc::new(AddDecryptionRule(
            self.long_term_key_manager.clone(),
        )));

        let mut builder = physical_optimizer::register_rules(builder);

        self.backend
            .add_optimizer_rules(builder.optimizer().get_or_insert_default());
        self.backend
            .add_analyzer_rules(builder.analyzer().get_or_insert_default());
        self.backend
            .add_physical_optimizer_rules(builder.physical_optimizers().get_or_insert_default());

        #[cfg(feature = "tracing")]
        if self.tracing_enabled {
            let exec_options = datafusion_tracing::InstrumentationOptions::builder()
                .record_metrics(true)
                .preview_limit(0)
                .build();

            builder = builder.with_physical_optimizer_rule(
                datafusion_tracing::instrument_with_info_spans!(
                    options: exec_options,
                ),
            );
        }

        let state = builder.build();

        #[cfg(feature = "tracing")]
        let state = if self.tracing_enabled {
            let rule_options =
                datafusion_tracing::RuleInstrumentationOptions::full().with_plan_diff();

            datafusion_tracing::instrument_rules_with_info_spans!(
                options: rule_options,
                state: state
            )
        } else {
            state
        };

        // let ctx = SessionContext::new_with_state(builder.build());
        DataFusionSession::new(
            self.backend.finish_init_session(state),
            self.logical_planner.clone(),
        )
    }
}
