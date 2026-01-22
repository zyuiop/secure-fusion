pub use super::super::super::logical::custom_txcontrol::TransactionControl;
use crate::transaction_control::TxControlGetter;
use common::dml::{DML_SCHEMA, DmlResult};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream::once;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

impl TransactionControl {
    pub fn to_plan(self) -> Arc<dyn ExecutionPlan> {
        let schema_ref = Arc::clone(&DML_SCHEMA);
        Arc::new(TransactionControlPlan {
            inner: self,
            props: PlanProperties::new(
                EquivalenceProperties::new(schema_ref),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }
}

#[derive(Debug)]
pub struct TransactionControlPlan {
    props: PlanProperties,
    inner: TransactionControl,
}

impl DisplayAs for TransactionControlPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxControl {:?}", &self.inner)
    }
}

impl ExecutionPlan for TransactionControlPlan {
    fn name(&self) -> &str {
        "transaction_control"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let control = context.get_tx_control();
        let context = context.clone();
        let inner = self.inner;

        let do_fut = async move {
            match inner {
                TransactionControl::Commit => control.commit(&context).await,
                TransactionControl::Rollback => control.rollback(&context).await,
                TransactionControl::SetAutocommit(ac) => control.set_autocommit(ac, &context).await,
                TransactionControl::SetIsolationLevel(il) => {
                    control.set_isolation_level(il, &context).await
                }
                TransactionControl::Start => control.start_transaction(&context).await,
            };

            Result::<_, DataFusionError>::Ok(DmlResult::empty().into())
        };

        let result = once(do_fut);
        let schema = self.schema();
        let result = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(result))
    }
}
