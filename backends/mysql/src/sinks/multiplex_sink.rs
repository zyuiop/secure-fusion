use super::sink::RecordBatchSink;
use async_trait::async_trait;
use common::dml::DmlResult;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use std::fmt::Formatter;
use std::sync::Arc;

#[allow(unused)]
#[derive(Debug)]
pub struct MultiplexSink {
    schema: SchemaRef,
    sinks: Vec<Arc<dyn RecordBatchSink>>,
}

impl MultiplexSink {
    pub fn empty() -> Self {
        Self {
            schema: SchemaRef::new(Schema::empty()),
            sinks: vec![],
        }
    }

    pub fn one(sink: Arc<dyn RecordBatchSink>) -> Self {
        let mut new = Self::empty();
        new.add_sink(sink);
        new
    }

    /// Adds a sink to this multiplexing sink.
    pub fn add_sink(&mut self, sink: Arc<dyn RecordBatchSink>) {
        let schema = Schema::new(
            self.schema
                .fields()
                .iter()
                .chain(sink.input_schema().fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        );

        self.schema = SchemaRef::new(schema);
        self.sinks.push(sink);
    }
}

impl DisplayAs for MultiplexSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("MultiplexSink[")?;
        for sink in &self.sinks {
            sink.fmt_as(t, f)?;
            f.write_str("; ")?;
        }
        f.write_str("]")
    }
}

#[async_trait]
impl RecordBatchSink for MultiplexSink {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult> {
        let mut splited_batches: Vec<Vec<RecordBatch>> =
            vec![Vec::with_capacity(data.len()); self.sinks.len()];

        for mut batch in data.into_iter() {
            for (pos, sink) in self.sinks.iter().enumerate() {
                let mut new_batch = Vec::with_capacity(sink.input_schema().fields().len());

                for _ in 0..sink.input_schema().fields().len() {
                    new_batch.push(batch.remove_column(0))
                }

                let new_batch = RecordBatch::try_new(Arc::clone(sink.input_schema()), new_batch)?;
                splited_batches[pos].push(new_batch);
            }
        }

        let zipped_sinks = self.sinks.iter().zip(splited_batches.into_iter());

        // Do all sinks in order
        let mut dml_result = DmlResult::empty();
        for (sink, batches) in zipped_sinks {
            let local_result = sink.handle_batches(batches, context).await?;
            dml_result = dml_result.combine_max(local_result);
        }

        Ok(dml_result)
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.schema
    }
}
