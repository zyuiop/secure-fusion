mod push_down_sort;

use datafusion::optimizer::OptimizerRule;
use std::sync::Arc;

pub fn get_optimizers() -> Vec<Arc<dyn OptimizerRule + Sync + Send>> {
    vec![
        // Disabled: this only makes sense in the (not interesting) case where the sort column is
        // Arc::new(push_down_sort::PushDownSort)
    ]
}
