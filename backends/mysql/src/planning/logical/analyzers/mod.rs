mod eliminate_dummy_table;
mod rewrite_exists;
mod rewrite_forward_function;

use datafusion::optimizer::Analyzer;
use std::sync::Arc;

pub fn insert_analyzer_rules(analizer: &mut Analyzer) {
    analizer.rules.extend_from_slice(&[
        Arc::new(rewrite_exists::RewriteExists::default()),
        Arc::new(rewrite_forward_function::RewriteForwardedFunctions),
        Arc::new(eliminate_dummy_table::EliminateDummyTable),
    ]);
}
