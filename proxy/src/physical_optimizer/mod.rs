#[cfg(feature = "df-trace")]
use common::profile;
#[cfg(feature = "df-trace")]
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::pushdown_sort::PushdownSort;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
#[cfg(feature = "df-trace")]
use datafusion::physical_plan::ExecutionPlan;
use log::warn;
use std::sync::Arc;

#[cfg(feature = "df-trace")]
#[derive(Debug)]
struct TimedPhysicalOptimizer(Arc<dyn PhysicalOptimizerRule + Sync + Send>);

#[cfg(feature = "df-trace")]
impl PhysicalOptimizerRule for TimedPhysicalOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        profile!(self.name(), self.0.optimize(plan, config))
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn schema_check(&self) -> bool {
        self.0.schema_check()
    }
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> {
    let ret: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
        // If there is a output requirement of the query, make sure that
        // this information is not lost across different rules during optimization.
        Arc::new(OutputRequirements::new_add_mode()),
        Arc::new(AggregateStatistics::new()),
        // Statistics-based join selection will change the Auto mode to a real join implementation,
        // like collect left, or hash join, or future sort merge join, which will influence the
        // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
        // repartitioning and local sorting steps to meet distribution and ordering requirements.
        // Therefore, it should run before EnforceDistribution and EnforceSorting.
        Arc::new(JoinSelection::new()),
        // The LimitedDistinctAggregation rule should be applied before the EnforceDistribution rule,
        // as that rule may inject other operations in between the different AggregateExecs.
        // Applying the rule early means only directly-connected AggregateExecs must be examined.
        Arc::new(LimitedDistinctAggregation::new()),
        // The FilterPushdown rule tries to push down filters as far as it can.
        // For example, it will push down filtering from a `FilterExec` to `DataSourceExec`.
        // Note that this does not push down dynamic filters (such as those created by a `SortExec` operator in TopK mode),
        // those are handled by the later `FilterPushdown` rule.
        // See `FilterPushdownPhase` for more details.
        Arc::new(FilterPushdown::new()),
        // The EnforceDistribution rule is for adding essential repartitioning to satisfy distribution
        // requirements. Please make sure that the whole plan tree is determined before this rule.
        // This rule increases parallelism if doing so is beneficial to the physical plan; i.e. at
        // least one of the operators in the plan benefits from increased parallelism.
        // Arc::new(EnforceDistribution::new()),
        // The CombinePartialFinalAggregate rule should be applied after the EnforceDistribution rule
        Arc::new(CombinePartialFinalAggregate::new()),
        // The EnforceSorting rule is for adding essential local sorting to satisfy the required
        // ordering. Please make sure that the whole plan tree is determined before this rule.
        // Note that one should always run this rule after running the EnforceDistribution rule
        // as the latter may break local sorting requirements.
        Arc::new(EnforceSorting::new()),
        // Run once after the local sorting requirement is changed
        Arc::new(OptimizeAggregateOrder::new()),
        // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
        Arc::new(ProjectionPushdown::new()),
        // Remove the ancillary output requirement operator since we are done with the planning
        // phase.
        Arc::new(OutputRequirements::new_remove_mode()),
        // The aggregation limiter will try to find situations where the accumulator count
        // is not tied to the cardinality, i.e. when the output of the aggregation is passed
        // into an `order by max(x) limit y`. In this case it will copy the limit value down
        // to the aggregation, allowing it to use only y number of accumulators.
        Arc::new(TopKAggregation::new()),
        // Tries to push limits down through window functions, growing as appropriate
        // This can possibly be combined with [LimitPushdown]
        // It needs to come after [EnforceSorting]
        // Arc::new(LimitPushPastWindows::new()),
        // The LimitPushdown rule tries to push limits down as far as possible,
        // replacing operators with fetching variants, or adding limits
        // past operators that support limit pushdown.
        Arc::new(LimitPushdown::new()),
        // The ProjectionPushdown rule tries to push projections towards
        // the sources in the execution plan. As a result of this process,
        // a projection can disappear if it reaches the source providers, and
        // sequential projections can merge into one. Even if these two cases
        // are not present, the load of executors such as join or union will be
        // reduced by narrowing their input tables.
        Arc::new(ProjectionPushdown::new()),
        // PushdownSort: Detect sorts that can be pushed down to data sources.
        Arc::new(PushdownSort::new()),
        // Arc::new(EnsureCooperative::new()),
        // This FilterPushdown handles dynamic filters that may have references to the source ExecutionPlan.
        // Therefore it should be run at the end of the optimization process since any changes to the plan may break the dynamic filter's references.
        // See `FilterPushdownPhase` for more details.
        Arc::new(FilterPushdown::new_post_optimization()),
        // The SanityCheckPlan rule checks whether the order and
        // distribution requirements of each node in the plan
        // is satisfied. It will also reject non-runnable query
        // plans that use pipeline-breaking operators on infinite
        // input(s). The rule generates a diagnostic error
        // message for invalid plans. It makes no changes to the
        // given query plan; i.e. it only acts as a final
        // gatekeeping rule.
        Arc::new(SanityCheckPlan::new()),
    ];

    // Uncomment line below to disable all optimizations
    // let ret = vec![];

    #[cfg(feature = "df-trace")]
    {
        ret.into_iter()
            .map(|rule| {
                Arc::new(TimedPhysicalOptimizer(rule))
                    as Arc<dyn PhysicalOptimizerRule + Sync + Send>
            })
            .collect()
    }

    #[cfg(not(feature = "df-trace"))]
    ret
}

pub(crate) fn register_rules(mut ssb: SessionStateBuilder) -> SessionStateBuilder {
    let optim = ssb.physical_optimizers();
    if optim.is_none() {
        // unlikely: no optimizations are present yet
        warn!("initialize default optimizer");
        *optim = Some(PhysicalOptimizer::with_rules(default_optimizer_rules()));
    }

    ssb
}
