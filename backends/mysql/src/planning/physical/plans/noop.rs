use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream::once;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct NoOpDdlPlan {
    props: PlanProperties,
}

impl NoOpDdlPlan {
    pub fn new() -> Arc<Self> {
        let schema_ref = SchemaRef::new(Schema::empty());
        Arc::new(NoOpDdlPlan {
            props: PlanProperties::new(
                EquivalenceProperties::new(schema_ref),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }
}

impl DisplayAs for NoOpDdlPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "no_op")
    }
}

impl ExecutionPlan for NoOpDdlPlan {
    fn name(&self) -> &str {
        "no_op"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let schema = self.schema().clone();
        let result =
            async move { Result::<_, DataFusionError>::Ok(RecordBatch::new_empty(schema)) };

        let result = once(result);
        let schema = self.schema();
        let result = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(result))
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        Ok(Statistics::default().with_num_rows(Precision::Exact(0)))
    }
}
