use common::dml::{DML_SCHEMA, DmlResult};
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
    props: Arc<PlanProperties>,
}

impl NoOpDdlPlan {
    pub fn new() -> Arc<Self> {
        Arc::new(NoOpDdlPlan {
            props: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&DML_SCHEMA)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }
}

impl DisplayAs for NoOpDdlPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NoOpDdlPlan")
    }
}

impl ExecutionPlan for NoOpDdlPlan {
    fn name(&self) -> &str {
        "NoOpDdlPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let result = async move { Result::<_, DataFusionError>::Ok(DmlResult::empty().into()) };

        let result = once(result);
        let schema = self.schema();
        let result = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(result))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        if partition.is_some() {
            Ok(Statistics::new_unknown(self.schema().as_ref()))
        } else {
            Ok(Statistics::default().with_num_rows(Precision::Exact(0)))
        }
    }
}
