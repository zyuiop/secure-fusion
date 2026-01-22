use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt, stream};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct PhysicalTracer(Arc<dyn ExecutionPlan>);

impl PhysicalTracer {
    pub fn apply_recursive(
        root: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let new_nodes = root.transform_up(|node| {
            if let Some(_) = node.as_any().downcast_ref::<PhysicalTracer>() {
                return Ok(Transformed::no(node));
            }

            Ok(Transformed::yes(Arc::new(PhysicalTracer(node))))
        })?;

        Ok(new_nodes.data)
    }
}

impl Display for PhysicalTracer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhysicalTracer")
    }
}

impl DisplayAs for PhysicalTracer {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("PhysicalTracer")
    }
}

impl ExecutionPlan for PhysicalTracer {
    fn name(&self) -> &str {
        PhysicalTracer::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.0.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(children.pop().unwrap())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let parent = self.0.clone();
        let task_start = context.session_config().get_extension::<Instant>().unwrap();
        let parent_result = parent.execute(partition, context)?;
        let step_name = format!("<{}>", self.0.name());

        let func = async move {
            let start = Instant::now();
            log::info!(
                "+{:?} {step_name}: (down)",
                start.duration_since(Arc::unwrap_or_clone(task_start.clone()))
            );

            let collected = parent_result.collect::<Vec<_>>().await;

            let end_time = task_start.elapsed();
            let duration = start.elapsed();
            log::info!("+{end_time:?} {step_name}: total {duration:?}");

            // Returns all the values
            Result::<_, DataFusionError>::Ok(stream::iter(collected))
        };

        let stream = stream::once(func).try_flatten();

        let schema = self.schema();
        let result = RecordBatchStreamAdapter::new(schema, stream);

        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.0.metrics()
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        self.0.statistics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        self.0.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}
