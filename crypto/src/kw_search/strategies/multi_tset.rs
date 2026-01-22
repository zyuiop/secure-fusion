//! This index strategy, formerly known as "one-step", queries the TSet for *all* the keywords in the
//! query at once, and computes the intersection locally.

use crate::kw_search::building_blocks::tset_wrapper::TSetWrapper;
use crate::kw_search::{KWSearchQuery, TSetQuery};
use async_trait::async_trait;
use common::profile;
use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use log::info;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct MultiTsetExec<TSet: TSetQuery + 'static> {
    query: Arc<KWSearchQuery>,
    // keyword_inputs: Vec<Arc<dyn ExecutionPlan>>,
    tset_impl: Arc<TSet>,
    schema: SchemaRef,
    tset_name: Arc<String>,
    props: PlanProperties,
}

impl<TSet: TSetQuery> DisplayAs for MultiTsetExec<TSet> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} query={:?} tset_name={}",
            self.name(),
            self.query,
            self.tset_name
        )
    }
}

impl<TSet: TSetQuery> MultiTsetExec<TSet> {
    pub fn new(tset_impl: Arc<TSet>, query: KWSearchQuery, tset_name: String) -> Self {
        let schema = Arc::new(Schema::new(vec![FieldRef::new(Field::new(
            // TODO: changeable index type
            "document_id",
            DataType::Int32,
            false,
        ))]));
        Self {
            query: Arc::new(query),
            tset_impl,
            schema: schema.clone(),
            tset_name: Arc::new(tset_name),
            props: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        }
    }
}

async fn execute_async(
    tset_name: Arc<String>,
    query: Arc<KWSearchQuery>,
    tset_impl: Arc<impl TSetQuery>,
    schema: SchemaRef,
    context: Arc<TaskContext>,
) -> datafusion::common::Result<RecordBatch> {
    // let keywords = self.keywords(partition, context.clone()).await?;
    let keywords = query.iter().flatten().cloned().collect();
    let tset_result = profile!(
        "Query TSet pages",
        tset_impl.query(context.clone(), &tset_name, keywords,)
    )
    .await?;

    let mut matched_ids = HashSet::new();
    for conjunction in query.iter() {
        if conjunction.is_empty() {
            // Already handled with the already_ok
            continue;
        }

        let matching_documents = conjunction
            .iter()
            .map(|kw| {
                tset_result
                    .get(kw)
                    .iter()
                    // TODO: changeable index type
                    .flat_map(|vec| vec.iter().map(|v| v.document_id as i32))
                    .collect::<HashSet<_>>()
            })
            .reduce(|a, b| a.intersection(&b).cloned().collect())
            .unwrap_or_default();

        matched_ids.extend(matching_documents);
    }

    info!("MultiTSetQuery Result: {:?}", &matched_ids);

    // TODO: changeable index type
    let array = Int32Array::from_iter_values(matched_ids.iter().cloned());
    let array: ArrayRef = Arc::new(array);

    RecordBatch::try_new(schema, vec![array])
        .map_err(|arrow_err| datafusion::error::DataFusionError::External(Box::new(arrow_err)))
}

#[async_trait]
impl<TSet: TSetQuery> ExecutionPlan for MultiTsetExec<TSet> {
    fn name(&self) -> &str {
        "multi_tset_exec"
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
        let result = execute_async(
            self.tset_name.clone(),
            self.query.clone(),
            self.tset_impl.clone(),
            self.schema.clone(),
            context,
        );
        let stream = futures_util::stream::once(result);
        let schema = self.schema.clone();
        let stream = RecordBatchStreamAdapter::new(schema, stream);

        Ok(Box::pin(stream))
    }
}
