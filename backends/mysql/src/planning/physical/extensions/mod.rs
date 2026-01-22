use crate::planning::physical::extensions::custom_ddl::CustomDdlExtensionPlanner;
use crate::planning::physical::extensions::custom_forward_statement::CustomForwardStatementPlanner;
use crate::planning::physical::extensions::custom_locking_scan::CustomLockingScanPlanner;
use crate::planning::physical::extensions::custom_scan::CustomScanPlanner;
use crate::planning::physical::extensions::custom_txcontrol::CustomTxControlPlanner;
use datafusion::physical_planner::ExtensionPlanner;
use std::sync::Arc;

mod custom_ddl;
mod custom_forward_statement;
mod custom_locking_scan;
mod custom_scan;
mod custom_txcontrol;

pub fn get_extensions() -> Vec<Arc<dyn ExtensionPlanner + Sync + Send>> {
    vec![
        Arc::new(CustomDdlExtensionPlanner),
        Arc::new(CustomForwardStatementPlanner),
        Arc::new(CustomTxControlPlanner),
        Arc::new(CustomLockingScanPlanner),
        Arc::new(CustomScanPlanner),
    ]
}
