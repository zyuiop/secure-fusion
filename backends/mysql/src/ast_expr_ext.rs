use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{BinaryOperator, UnaryOperator};

pub trait AstExprExt: Sized {
    fn binop(self, other: Self, binary_operator: BinaryOperator) -> Self;

    #[inline(always)]
    fn and(self, other: Self) -> Self {
        self.binop(other, BinaryOperator::And)
    }

    #[inline(always)]
    fn or(self, other: Self) -> Self {
        self.binop(other, BinaryOperator::Or)
    }

    #[inline(always)]
    fn eq(self, other: Self) -> Self {
        self.binop(other, BinaryOperator::Eq)
    }
    fn not(self) -> Self;
}

impl AstExprExt for ast::Expr {
    #[inline(always)]
    fn binop(self, other: Self, op: BinaryOperator) -> Self {
        ast::Expr::BinaryOp {
            left: Box::new(self),
            right: Box::new(other),
            op,
        }
    }

    #[inline(always)]
    fn not(self) -> Self {
        ast::Expr::UnaryOp {
            expr: Box::new(self),
            op: UnaryOperator::Not,
        }
    }
}
