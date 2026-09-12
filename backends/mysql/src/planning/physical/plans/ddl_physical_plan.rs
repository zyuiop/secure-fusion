use crate::planning::physical::plans::mysql_raw_exec_plan::MySqlRawExecPlan;
use common::dml::DmlResult;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::TryStreamExt;
use futures_util::stream::once;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait::async_trait]
pub(super) trait DdlPlanImpl: Debug {
    fn name(&self) -> &'static str;

    async fn execute_extra(&self, context: Arc<TaskContext>) -> datafusion::common::Result<()>;
}

#[derive(Debug)]
pub(super) struct DdlPlan {
    base: Arc<dyn ExecutionPlan>,
    extra_implementation: Arc<dyn DdlPlanImpl + Send + Sync>,
}

impl DdlPlan {
    pub(super) fn new(query: ast::Statement, executor: Arc<dyn DdlPlanImpl + Send + Sync>) -> Self {
        Self {
            base: Arc::new(MySqlRawExecPlan::new(query)),
            extra_implementation: executor,
        }
    }
}

impl DisplayAs for DdlPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("DdlPlan[")?;
        f.write_str(self.name())?;
        f.write_str("]")
    }
}

impl ExecutionPlan for DdlPlan {
    fn name(&self) -> &str {
        self.extra_implementation.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.base.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            plan_err!("Invalid number of children for ddl_physical_plan")?
        }

        let child = Arc::clone(&children[0]);
        if !child.as_any().is::<MySqlRawExecPlan>() {
            plan_err!("Invalid child {child:?} for ddl_physical_plan")?;
        }

        Ok(Arc::new(Self {
            extra_implementation: Arc::clone(&self.extra_implementation),
            base: child,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let parent = self.base.execute(partition, context.clone())?;

        // This closure must never use `self`, otherwise it introduces lifetime problems.
        // All needed values must be cloned upfront.
        let extra_impl = self.extra_implementation.clone();
        let context = context.clone();

        let result = async move {
            let _ = parent.try_collect::<Vec<_>>().await?;
            extra_impl.execute_extra(context).await?;
            Result::<RecordBatch, DataFusionError>::Ok(DmlResult::empty().into())
        };

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
