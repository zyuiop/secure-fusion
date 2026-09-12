use crate::transaction_control::TxControlGetter;
use async_trait::async_trait;
use common::dml::{DML_SCHEMA, DmlResult};
use common::profile;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::EvaluationType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures_util::TryStreamExt;
use futures_util::stream::once;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait]
pub trait RecordBatchSink: Send + Sync + DisplayAs + Debug {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult>;

    fn input_schema(&self) -> &SchemaRef;
}

/// A custom DataSinkExec with a custom schema
#[derive(Debug, Clone)]
pub struct RecordBatchSinkExec {
    input: Arc<dyn ExecutionPlan>,
    sink: Arc<dyn RecordBatchSink>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RecordBatchSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn RecordBatchSink>,
    ) -> RecordBatchSinkExec {
        let output_schema = Arc::clone(&DML_SCHEMA);
        let props = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_evaluation_type(EvaluationType::Eager);

        Self {
            input,
            properties: Arc::new(props),
            output_schema,
            sink,
        }
    }
}

impl DisplayAs for RecordBatchSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "RecordBatchSinkExec: sink=")?;
        self.sink.fmt_as(t, f)
    }
}

impl ExecutionPlan for RecordBatchSinkExec {
    fn name(&self) -> &str {
        "RecordBatchSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(children.remove(0), self.sink.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let sink = self.sink.clone();
        let context = context.clone();
        let schema = self.output_schema.clone();

        // This function should not do anything yet, operations normally only happen in the async
        // block which is only executed on pull.
        // If that turns out to be false, move inside the async block, after the tx_control part.
        let underlying_result = execute_input_stream(
            self.input.clone(),
            self.sink.input_schema().clone(),
            partition,
            context.clone(),
        )?;

        let future = async move {
            let tx_control = context.get_tx_control();
            tx_control.weak_start_transaction(&context).await;

            // We must pull all the data, because the MySQL connection is likely locked by the input stream
            let underlying_result = match profile!(
                "collect data for sink",
                underlying_result.try_collect::<Vec<_>>().await
            ) {
                Ok(result) => result,
                Err(e) => {
                    tx_control.weak_rollback(&context).await;
                    return Err(e);
                }
            };

            let result = match profile!(
                "push data to sink",
                sink.handle_batches(underlying_result, &context).await
            ) {
                Ok(result) => result,
                Err(e) => {
                    tx_control.weak_rollback(&context).await;
                    return Err(e);
                }
            };

            // Make the output record batch
            let result = RecordBatch::from(result);
            tx_control.weak_commit(&context).await;

            Ok(result)
        };

        let result = once(future);
        let stream_adapter = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(stream_adapter))
    }
}
