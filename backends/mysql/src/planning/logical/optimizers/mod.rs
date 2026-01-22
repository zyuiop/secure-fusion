mod push_down_sort;

use datafusion::optimizer::OptimizerRule;
use std::sync::Arc;

pub fn get_optimizers() -> Vec<Arc<dyn OptimizerRule + Sync + Send>> {
    vec![Arc::new(push_down_sort::PushDownSort)]
}
