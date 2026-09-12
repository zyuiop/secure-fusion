use crate::udf::bitwise_not::BitwiseNotUdf;
use crate::udf::database::DatabaseUdf;
use crate::udf::group_concat::GroupConcatUdf;
use crate::udf::length::LengthUdf;
use crate::udf::mysql_forward_functions::FORWARDED_FUNCTIONS;
use crate::udf::substr::{SubstrPlanner, SubstrUdf};
use datafusion::execution::{FunctionRegistry, SessionState, SessionStateDefaults};
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use std::sync::Arc;

mod bitwise_not;
mod database;
mod func_if;
mod group_concat;
mod length;
pub(crate) mod mysql_forward_functions;
mod substr;

pub fn register_udfs(target: &mut Vec<Arc<ScalarUDF>>) {
    target.push(Arc::new(ScalarUDF::from(DatabaseUdf)));
    target.push(Arc::new(ScalarUDF::from(BitwiseNotUdf::default())));
    target.push(Arc::new(ScalarUDF::from(func_if::IfUdf::default())));

    FORWARDED_FUNCTIONS
        .iter()
        .for_each(|f| target.push(Arc::new(ScalarUDF::from(f.clone()))))
}

pub fn register_udfs_late(session: &mut SessionState) {
    let _ = session.register_udf(Arc::new(ScalarUDF::from(LengthUdf::default())));
    let _ = session.register_udf(Arc::new(ScalarUDF::from(SubstrUdf::default())));
}

pub fn expr_planners() -> Vec<Arc<dyn ExprPlanner>> {
    let mut default_planners = SessionStateDefaults::default_expr_planners();
    default_planners.insert(
        0,
        SubstrPlanner::new(Arc::new(ScalarUDF::from(SubstrUdf::default()))),
    );

    default_planners
}

pub fn register_aggregates(target: &mut Vec<Arc<AggregateUDF>>) {
    target.push(Arc::new(AggregateUDF::new_from_impl(
        GroupConcatUdf::default(),
    )));
}
