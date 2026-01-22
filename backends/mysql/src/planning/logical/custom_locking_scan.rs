use datafusion::common::{DFSchemaRef, DataFusionError, plan_err};
use datafusion::logical_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::sqlparser::ast;
use std::fmt::Formatter;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum LockType {
    Update,
}

impl TryFrom<ast::LockType> for LockType {
    type Error = DataFusionError;

    fn try_from(value: ast::LockType) -> Result<Self, Self::Error> {
        match value {
            ast::LockType::Update => Ok(LockType::Update),
            other => plan_err!("unhandled lock type {other:?}")?,
        }
    }
}

impl From<LockType> for ast::LockType {
    fn from(value: LockType) -> Self {
        match value {
            LockType::Update => ast::LockType::Update,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CustomLockingScan(pub LogicalPlan, pub LockType);

impl CustomLockingScan {
    pub fn lock_type(&self) -> ast::LockType {
        self.1.into()
    }
}

impl UserDefinedLogicalNodeCore for CustomLockingScan {
    fn name(&self) -> &str {
        "CustomLockingScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.0]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.0.schema()
    }

    fn check_invariants(&self, _check: InvariantLevel) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    #[allow(clippy::disallowed_types)]
    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        Default::default() // Everything can be pushed below this node
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CustomLockingScan({}, {:?})", self.0, self.1)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        assert_eq!(inputs.len(), 1);
        Ok(Self(inputs.remove(0), self.1))
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}
