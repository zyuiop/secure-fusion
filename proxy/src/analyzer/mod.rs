mod add_decryption_rule;

use crate::analyzer::add_decryption_rule::AddDecryptionRule;
use datafusion::execution::SessionStateBuilder;
use std::sync::Arc;

pub(crate) fn register_rules(ssb: SessionStateBuilder) -> SessionStateBuilder {
    ssb.with_analyzer_rule(Arc::new(AddDecryptionRule))
}
