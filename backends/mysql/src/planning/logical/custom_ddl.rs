use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::sqlparser::ast::AlterTableOperation;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::ResolvedTableReference;
use datafusion::sql::sqlparser::ast::CreateTable;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum DdlOperation {
    CreateTable(ResolvedTableReference, Box<CreateTable>),
    AlterTable {
        table: ResolvedTableReference,
        operations: Vec<AlterTableOperation>,
        if_exists: bool,
    },
    CreateDatabase {
        db_name: String,
        if_not_exists: bool,
    },
    DropDatabase {
        db_name: String,
        if_exists: bool,
    },
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct CustomDdlLogicalPlan {
    pub operation: DdlOperation,

    schema: DFSchemaRef,
}

impl CustomDdlLogicalPlan {
    pub fn new(operation: DdlOperation) -> CustomDdlLogicalPlan {
        Self {
            operation,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }
    }

    pub fn plan_for(operation: DdlOperation) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Self::new(operation)),
        })
    }
}

impl PartialOrd for CustomDdlLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.operation.partial_cmp(&other.operation)
    }
}

impl UserDefinedLogicalNodeCore for CustomDdlLogicalPlan {
    fn name(&self) -> &str {
        match self.operation {
            DdlOperation::CreateTable(..) => "CreateTable",
            DdlOperation::CreateDatabase { .. } => "CreateDatabase",
            DdlOperation::DropDatabase { .. } => "DropDatabase",
            DdlOperation::AlterTable { .. } => "AlterTable",
        }
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(UserDefinedLogicalNodeCore::name(self))
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(CustomDdlLogicalPlan::new(self.operation.clone()))
    }
}
