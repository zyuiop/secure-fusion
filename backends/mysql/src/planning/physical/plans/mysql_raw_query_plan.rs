use crate::errors::MySqlBackendErrorInner;
use crate::get_conn::ConnGetter;
use common::profile;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MySqlRawQueryPlan {
    query: ast::Statement,
    project_schema: SchemaRef,
    props: PlanProperties,
}

impl DisplayAs for MySqlRawQueryPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} schema={} query={}",
            self.name(),
            self.project_schema,
            self.query,
        )
    }
}

impl MySqlRawQueryPlan {
    pub(crate) fn new(stmt: ast::Statement, schema_ref: SchemaRef) -> Self {
        Self {
            query: stmt,
            props: PlanProperties::new(
                EquivalenceProperties::new(schema_ref.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            project_schema: schema_ref.clone(),
        }
    }
}

impl ExecutionPlan for MySqlRawQueryPlan {
    fn name(&self) -> &str {
        MySqlRawQueryPlan::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.project_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This plan is a source for rows, it can never have child plans
        vec![]
    }

    fn supports_limit_pushdown(&self) -> bool {
        // logical planning pushes the limit already, but if we don't report pushdown compatibility physical planning re-adds a limit step on top
        // true
        false // TODO
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: limit pushdown will require this
        if _children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::External(Box::new(
                MySqlBackendErrorInner::PhysicalPlanningError(
                    "MySQL raw plan cannot have children".to_string(),
                ),
            )))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let conn = context.get_conn();
        let schema = self.schema();
        let query = self.query.clone();

        profile!(
            "query_arrow",
            crate::arrow_helper::query_arrow(
                conn,
                &query.to_string(),
                schema,
                context.session_config().batch_size()
            )
        )
    }
}
