use crate::providers::catalog_provider::MySqlCatalogProvider;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use std::sync::Arc;

pub mod push_down_joins;

pub fn get_optimizers(
    _catalog: Arc<MySqlCatalogProvider>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> {
    vec![
        Arc::new(push_down_joins::PushDownJoins),
    ]
}
