use crate::providers::table_provider::TableStatistics;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::stats::Precision;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::sqlparser::ast::CastKind;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{BinaryOperator, Ident};
use datafusion::sql::unparser;
use datafusion::sql::unparser::Unparser;
use rustc_hash::FxHashSet;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum EqualityOperator {
    Eq,
    Gt,
    Lt,
    LtEq,
    GtEq,
}

impl EqualityOperator {
    pub fn inverse_order(self) -> Self {
        match self {
            EqualityOperator::Eq => EqualityOperator::Eq,
            EqualityOperator::Gt => EqualityOperator::Lt,
            EqualityOperator::Lt => EqualityOperator::Gt,
            EqualityOperator::LtEq => EqualityOperator::GtEq,
            EqualityOperator::GtEq => EqualityOperator::LtEq,
        }
    }
}

impl From<EqualityOperator> for BinaryOperator {
    fn from(value: EqualityOperator) -> Self {
        match value {
            EqualityOperator::Eq => Self::Eq,
            EqualityOperator::Gt => Self::Gt,
            EqualityOperator::Lt => Self::Lt,
            EqualityOperator::LtEq => Self::LtEq,
            EqualityOperator::GtEq => Self::GtEq,
        }
    }
}

impl<'a> TryFrom<&'a Operator> for EqualityOperator {
    type Error = &'a Operator;

    fn try_from(value: &'a Operator) -> Result<Self, Self::Error> {
        match value {
            Operator::Eq => Ok(EqualityOperator::Eq),
            Operator::Lt => Ok(EqualityOperator::Lt),
            Operator::LtEq => Ok(EqualityOperator::LtEq),
            Operator::Gt => Ok(EqualityOperator::Gt),
            Operator::GtEq => Ok(EqualityOperator::GtEq),
            _ => return Err(value),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum IndexableFilterExpr<E> {
    // Logical operators
    And(Box<IndexableFilterExpr<E>>, Box<IndexableFilterExpr<E>>),
    Or(Box<IndexableFilterExpr<E>>, Box<IndexableFilterExpr<E>>),
    Not(Box<IndexableFilterExpr<E>>),

    // Common operations that are used when building a tree
    /// Equality between a column and a value
    Eq(ColumnWithCast, EqualityOperator, ScalarValue),

    Between(ColumnWithCast, ScalarValue, ScalarValue),

    /// Equality between a column and one of many values
    InList(ColumnOrTuple, FxHashSet<ScalarValue>),

    /// <column (0)> LIKE <pattern (1)>
    KwSearchLike(ColumnWithCast, String),

    /// Kw Search function or MATCH ... AGAINST ... syntax
    KwMatch {
        columns: Vec<Column>,
        search_string: String,
    },

    /// Other operations
    Other(E),
}

macro_rules! recombine {
    ($left: expr, $right: expr, $combinator: expr) => {
        match ($left, $right) {
            (Some(l), Some(r)) => Some($combinator(Box::new(l), Box::new(r))),
            (Some(a), _) | (_, Some(a)) => Some(a),
            (None, None) => None,
        }
    };
}

pub type IndexSelectivity = Precision<usize>;

pub struct SupportOptions<'a, E: Clone> {
    /// If true, the filter support `NOT` expressions
    pub supports_not: bool,
    /// If true, the filter supports `OR` expressions
    pub supports_or: bool,

    /// A function that returns None if the index does not support the filter, and Some(IndexSelectivity) if it does.
    /// Note that the index may choose not to report the selectivity of the index, by reporting Some(IndexSelectivity::Absent)
    pub is_supported:
        Box<dyn Fn(&IndexableFilterExpr<E>, &TableStatistics) -> Option<IndexSelectivity> + 'a>,
}

trait ScalarOp: Sized {
    fn min<'a>(&'a self, other: &'a Self) -> Option<&'a Self>;
}

impl ScalarOp for ScalarValue {
    fn min<'a>(&'a self, other: &'a Self) -> Option<&'a Self> {
        Some(match (&self, &other) {
            (ScalarValue::Float16(Some(v)), ScalarValue::Float16(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Float32(Some(v)), ScalarValue::Float32(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Float64(Some(v)), ScalarValue::Float64(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Decimal32(Some(v), _, _), ScalarValue::Decimal32(Some(v2), _, _)) => {
                if v > v2 { other } else { self }
            }
            (ScalarValue::Decimal64(Some(v), _, _), ScalarValue::Decimal64(Some(v2), _, _)) => {
                if v > v2 { other } else { self }
            }
            (ScalarValue::Decimal128(Some(v), _, _), ScalarValue::Decimal128(Some(v2), _, _)) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Decimal256(Some(v), _, _), ScalarValue::Decimal256(Some(v2), _, _)) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Int8(Some(v)), ScalarValue::Int8(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Int16(Some(v)), ScalarValue::Int16(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Int32(Some(v)), ScalarValue::Int32(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Int64(Some(v)), ScalarValue::Int64(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::UInt8(Some(v)), ScalarValue::UInt8(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::UInt16(Some(v)), ScalarValue::UInt16(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::UInt32(Some(v)), ScalarValue::UInt32(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::UInt64(Some(v)), ScalarValue::UInt64(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Date32(Some(v)), ScalarValue::Date32(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Date64(Some(v)), ScalarValue::Date64(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Time32Second(Some(v)), ScalarValue::Time32Second(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Time32Millisecond(Some(v)), ScalarValue::Time32Millisecond(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Time64Microsecond(Some(v)), ScalarValue::Time64Microsecond(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (ScalarValue::Time64Nanosecond(Some(v)), ScalarValue::Time64Nanosecond(Some(v2))) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (
                ScalarValue::TimestampSecond(Some(v), _),
                ScalarValue::TimestampSecond(Some(v2), _),
            ) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (
                ScalarValue::TimestampMillisecond(Some(v), _),
                ScalarValue::TimestampMillisecond(Some(v2), _),
            ) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (
                ScalarValue::TimestampMicrosecond(Some(v), _),
                ScalarValue::TimestampMicrosecond(Some(v2), _),
            ) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            (
                ScalarValue::TimestampNanosecond(Some(v), _),
                ScalarValue::TimestampNanosecond(Some(v2), _),
            ) => {
                if v > v2 {
                    other
                } else {
                    self
                }
            }
            _ => return None,
        })
    }
}

impl<E: Clone> IndexableFilterExpr<E> {
    pub fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// Rewrites A > x AND A < y as A BETWEEN (x, y)
    pub fn optimize_cmp(self) -> Self {
        match self {
            IndexableFilterExpr::And(left, right) => {
                if let IndexableFilterExpr::Eq(
                    left_col,
                    EqualityOperator::LtEq
                    | EqualityOperator::Lt
                    | EqualityOperator::GtEq
                    | EqualityOperator::Gt,
                    _,
                ) = left.as_ref()
                    && let IndexableFilterExpr::Eq(
                        right_col,
                        EqualityOperator::LtEq
                        | EqualityOperator::Lt
                        | EqualityOperator::GtEq
                        | EqualityOperator::Gt,
                        _,
                    ) = right.as_ref()
                    && left_col == right_col
                {
                    let IndexableFilterExpr::Eq(left_col, left_operator, left_value) = *left else {
                        unreachable!()
                    };

                    let IndexableFilterExpr::Eq(right_col, right_operator, right_value) = *right
                    else {
                        unreachable!()
                    };

                    let Some(minimum) = left_value.min(&right_value) else {
                        // Recursion is not needed here, we know values cannot have values
                        return IndexableFilterExpr::Eq(left_col, left_operator, left_value).and(
                            IndexableFilterExpr::Eq(right_col, right_operator, right_value),
                        );
                    };
                    let min_is_left = minimum == &left_value;

                    match (&left_operator, &right_operator) {
                        // Same operator case <
                        (
                            EqualityOperator::Lt | EqualityOperator::LtEq,
                            EqualityOperator::Lt | EqualityOperator::LtEq,
                        ) => IndexableFilterExpr::Eq(
                            left_col,
                            if min_is_left {
                                left_operator
                            } else {
                                right_operator
                            },
                            if min_is_left { left_value } else { right_value },
                        ),
                        // Same operator case >
                        (
                            EqualityOperator::Gt | EqualityOperator::GtEq,
                            EqualityOperator::Gt | EqualityOperator::GtEq,
                        ) => IndexableFilterExpr::Eq(
                            left_col,
                            if min_is_left {
                                right_operator
                            } else {
                                left_operator
                            },
                            if min_is_left { right_value } else { left_value },
                        ),
                        // Different operator -- doable cases
                        (
                            EqualityOperator::Gt | EqualityOperator::GtEq, // x > LEFT
                            EqualityOperator::Lt | EqualityOperator::LtEq, // x < RIGHT ==> x BETWEEN (LEFT, RIGHT)
                        ) if min_is_left => {
                            IndexableFilterExpr::Between(left_col, left_value, right_value)
                        }
                        (
                            EqualityOperator::Lt | EqualityOperator::LtEq, // x < LEFT ==> x BETWEEN (RIGHT, LEFT)
                            EqualityOperator::Gt | EqualityOperator::GtEq, // x > RIGHT
                        ) if !min_is_left => {
                            IndexableFilterExpr::Between(left_col, right_value, left_value)
                        }
                        _ => IndexableFilterExpr::Eq(left_col, left_operator, left_value).and(
                            IndexableFilterExpr::Eq(right_col, right_operator, right_value),
                        ),
                    }
                } else {
                    left.optimize_cmp().and(right.optimize_cmp())
                }
            }
            IndexableFilterExpr::Or(left, right) => left.optimize_cmp().or(right.optimize_cmp()),
            other => other,
        }
    }

    pub fn pushup_not(self) -> Self {
        match self {
            Self::And(left, right) => {
                match ((*left).pushup_not(), (*right).pushup_not()) {
                    (Self::Not(left), Self::Not(right)) => {
                        // (NOT A) AND (NOT B) == NOT (A OR B)
                        left.or(*right).not()
                    }
                    (left, right) => left.and(right),
                }
            }
            Self::Or(left, right) => {
                match ((*left).pushup_not(), (*right).pushup_not()) {
                    (Self::Not(left), Self::Not(right)) => {
                        // (NOT A) OR (NOT B) == NOT (A AND B)
                        left.and(*right).not()
                    }
                    (left, right) => left.and(right),
                }
            }
            Self::Not(not) => match not.pushup_not() {
                Self::Not(not) => *not,
                other => other.not(),
            },
            other => other,
        }
    }

    pub fn pushdown_not(self) -> Self {
        match self {
            Self::Not(not) => {
                match not.pushdown_not() {
                    Self::And(left, right) => {
                        // NOT (A AND B) = NOT A OR NOT B
                        left.not().or(right.not())
                    }
                    Self::Or(left, right) => {
                        // NOT (A OR B) = NOT A AND NOT B
                        left.not().and(right.not())
                    }
                    Self::Not(not) => *not,
                    other => other.not(),
                }
            }
            Self::And(left, right) => left.pushdown_not().and(right.pushdown_not()),
            Self::Or(left, right) => left.pushdown_not().or(right.pushdown_not()),
            other => other,
        }
    }

    /// Splits a tree into a conjunction of two trees, one of which is composed only of supported
    /// operations, and the other which contains the rest of the tree.
    ///
    /// The right tree is going to be discarded after all indices have been applied.
    /// However, it is important that (SUPPORTED AND UNSUPPORTED) remains valid, so that we don't
    /// exclude valid rows using the supported filters.
    /// e.g. supported filters must always return a superset of the valid rows.
    pub fn split_into_supported_unsupported(
        self,
        support_options: &SupportOptions<E>,
        table_stats: &TableStatistics,
    ) -> (Option<Self>, Option<Self>, IndexSelectivity) {
        let supported = (support_options.is_supported)(&self, table_stats);
        if let Some(selectivity) = supported {
            // Per function contract: if the index returns a selectivity (even if it's "Absent") then it supports the filter
            return (Some(self), None, selectivity);
        }

        match self {
            IndexableFilterExpr::And(left, right) => {
                let (l_supp, l_unsupp, l_sel) =
                    left.split_into_supported_unsupported(support_options, table_stats);
                let (r_supp, r_unsupp, r_sel) =
                    right.split_into_supported_unsupported(support_options, table_stats);

                (
                    recombine!(l_supp, r_supp, Self::And),
                    recombine!(l_unsupp, r_unsupp, Self::And),
                    l_sel.min(&r_sel),
                )
            }
            IndexableFilterExpr::Or(left, right) if support_options.supports_or => {
                // OR is annoying
                let (l_supp, l_unsupp, l_sel) =
                    left.split_into_supported_unsupported(support_options, table_stats);
                let (r_supp, r_unsupp, r_sel) =
                    right.split_into_supported_unsupported(support_options, table_stats);

                // Naming:
                // Supported: filter entirely supported (unsupp is empty)
                // Mixed: supported and unsupported parts (both are defined)
                // Unsupported: nothing supported (supp is empty)

                // 1. Both filters are entirely supported | Both filters are entirely unsupported
                //  Trivial -> forward
                if (l_supp.is_some()
                    && r_supp.is_some()
                    && l_unsupp.is_none()
                    && r_unsupp.is_none())
                    || (l_supp.is_none()
                        && r_supp.is_none()
                        && l_unsupp.is_some()
                        && r_unsupp.is_some())
                {
                    return (
                        recombine!(l_supp, r_supp, Self::Or),
                        recombine!(l_unsupp, r_unsupp, Self::Or),
                        l_sel.add(&r_sel),
                    );
                } else {
                }

                // We have (S1 AND U1) OR (S2 AND U2)
                // We can rewrite it as:
                // A- ((S1 AND U1) OR S2) AND ((S1 AND U1) OR U2)
                // B- (S1 OR (S2 AND U2)) AND (U1 OR (S2 AND U2))

                // ASSUMING U1 IS UNDEFINED (resp. U2)
                // A[U1=1]- (S1 OR S2) AND (S1 OR U2)
                // B[U2=1]- (S1 OR S2) AND (U1 OR S2)
                if l_unsupp.is_none() {
                    // No unsupported filters on the left side
                    return (
                        recombine!(l_supp.clone(), r_supp, Self::Or),
                        // Not a typo - intentional: the supported filter also makes it to the unsupported side
                        // (although it likely won't matter)
                        recombine!(l_supp, r_unsupp, Self::Or),
                        l_sel.add(&r_sel),
                    );
                } else if r_unsupp.is_none() {
                    // No unsupported filters on the right side
                    return (
                        recombine!(l_supp, r_supp.clone(), Self::Or),
                        // Not a typo - intentional: the supported filter also makes it to the unsupported side
                        // (although it likely won't matter)
                        recombine!(l_unsupp, r_supp, Self::Or),
                        l_sel.add(&r_sel),
                    );
                }

                // ASSUMING S1 IS UNDEFINED (resp. S2)
                // A[S1=1]- (U1 OR S2) AND (U1 OR U2)
                // B[S2=1]- (S1 OR U2) AND (U1 OR U2)
                // In all these cases, we spill unsupported filters on both sides, so we cannot support anything
                (
                    None,
                    recombine!(
                        // Recombine supported & unsupported parts in an AND
                        // (method contract: both returns are assumed to be a conjunction)
                        recombine!(l_supp, l_unsupp, Self::And),
                        recombine!(r_supp, r_unsupp, Self::And),
                        Self::Or
                    ),
                    table_stats.num_rows(), // return the full original selectivity as we have no filter
                )
            }
            IndexableFilterExpr::Not(not) if support_options.supports_not => {
                let (supp, unsupp, sel) =
                    not.split_into_supported_unsupported(support_options, table_stats);

                (
                    supp.map(|v| Self::Not(Box::new(v))),
                    unsupp.map(|v| Self::Not(Box::new(v))),
                    table_stats.num_rows().sub(&sel),
                )
            }
            // Other expressions are like "leaves" and do not allow any kind of logic
            other => (None, Some(other), table_stats.num_rows().clone()),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ColumnWithCast {
    pub column: Column,
    pub cast_to: Option<DataType>,
    /// Returns a null value if the cast fails
    pub or_null: bool,
}

impl From<ColumnWithCast> for ast::Expr {
    fn from(column: ColumnWithCast) -> ast::Expr {
        let base = Ident::new(column.column.name);
        let base = if let Some(rel) = column.column.relation {
            ast::Expr::CompoundIdentifier(vec![Ident::new(rel.table()), base])
        } else {
            ast::Expr::Identifier(base)
        };

        if let Some(cast) = column.cast_to {
            let dt = unparser().arrow_dtype_to_ast_dtype(&cast).unwrap();
            ast::Expr::Cast {
                expr: Box::new(base),
                data_type: dt,
                kind: if column.or_null {
                    CastKind::TryCast
                } else {
                    CastKind::Cast
                },
                array: false,
                format: None,
            }
        } else {
            base
        }
    }
}

impl ColumnWithCast {
    pub(super) fn column(col: &Column) -> Self {
        Self {
            column: col.clone(),
            cast_to: None,
            or_null: false,
        }
    }

    pub(super) fn raw(col_name: &str) -> Self {
        Self {
            column: Column::from_name(col_name),
            cast_to: None,
            or_null: false,
        }
    }

    pub(super) fn cast(self, typ: &DataType) -> Self {
        Self {
            column: self.column,
            cast_to: Some(typ.clone()),
            or_null: false,
        }
    }

    pub(super) fn try_cast(self, typ: &DataType) -> Self {
        Self {
            column: self.column,
            cast_to: Some(typ.clone()),
            or_null: true,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ColumnOrTuple {
    Column(ColumnWithCast),
    Tuple(Vec<ColumnWithCast>),
}

impl From<ColumnOrTuple> for ast::Expr {
    fn from(column: ColumnOrTuple) -> ast::Expr {
        match column {
            ColumnOrTuple::Column(v) => v.into(),
            ColumnOrTuple::Tuple(v) => {
                let values_in_tuple = v.into_iter().map(ast::Expr::from).collect::<Vec<_>>();
                ast::Expr::Tuple(values_in_tuple)
            }
        }
    }
}

const DIALECT: unparser::dialect::MySqlDialect = unparser::dialect::MySqlDialect {};

#[inline]
pub(super) fn unparser() -> Unparser<'static> {
    Unparser::new(&DIALECT)
}
