use crate::get_catalog::CatalogGetter;
use crate::get_conn::ConnGetter;
use crate::metadata::indices::inverted_index::db_inverted_index::{
    CreateDbInvertedIndexPlan, IndexedDocumentExtensions, IndexedDocumentId, InvertedIndexGetter,
    RawIndexQuery, RawIndexTermRef, RawInvertedIndex,
};
use crate::metadata::kw_search_func::{KwSearchArgs, KwSearchUdf, SearchMode};
use crate::metadata::{
    ColumnName, EncryptedIndex, EncryptedIndexConfigurationVariant, IndexConfig,
    IndexInsertStrategy, IndexQueryStrategy, IndexSink, SerializableEncryptedTableMeta,
};
use crate::planning::logical::{CURRENT_VALUE_PREFIX, FILTER_PREFIX};
use crate::planning::physical::plans::mysql_scan_plan::DynamicFilter;
use crate::providers::schema_provider::MySqlSchemaProvider;
use crate::providers::table_provider::{IndexableColumn, IndexableColumnSize, MySqlTableProvider};
use crate::sinks::sink::RecordBatchSink;
use crate::store::StoreGetter;
use async_trait::async_trait;
use bitflags::bitflags;
use common::dml::DmlResult;
use crypto::LongTermKeyManager;
use crypto::planning::physical::to_binary::ToBinaryExpr;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericByteBuilder, GenericListBuilder, OffsetSizeTrait,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::GenericBinaryType;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{
    Column, DataFusionError, ResolvedTableReference, ScalarValue, exec_err, plan_err,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::functions_nested::concat::ArrayConcat;
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{VisitMut, VisitorMut};
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDFImpl,
};
use datafusion::physical_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Ident, Value};
use datafusion::sql::sqlparser::tokenizer::Span;
use futures_util::{TryStreamExt, stream};
use log::{info, trace, warn};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::{fmt, iter};

const MAX_KW_LEN: usize = 32;

bitflags! {
    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    pub struct Collation: u8 {
        const CASE_INSENSITIVE = 1 << 0;

        /// If defined, no length limit for keywords in this index
        /// If left to 0, keywords longer than `MAX_KW_LEN` will be dropped
        const LENGTH_UNLIMITED = 1 << 1;

        /// Allows alphanumeric keywords - if unset, only alphabetic kw are accepted
        const ALLOW_ALPHANUMERIC = 1 << 2;
    }
}

#[derive(Debug, Clone)]
pub struct InvertedIndex<const KeySize: usize> {
    inner: Arc<RawInvertedIndex<KeySize>>,

    watched_string_columns: BTreeSet<ColumnName>,

    /// A vec (column_name, term_if_true, term_if_false)
    watched_boolean_columns: BTreeMap<ColumnName, (RawIndexTermRef, RawIndexTermRef)>,

    /// A vec (condition, term_if_true, term_if_false)
    // watched_conditions: Vec<(Expr, RawIndexTermRef, RawIndexTermRef)>,
    collation: Collation,

    all_tracked_columns: FxHashSet<ColumnName>,
    ordered_tracked_columns: Vec<ColumnName>,
}

const TERM_PREFIX_BOOLEAN_FALSE: u8 = 0x10;
const TERM_PREFIX_BOOLEAN_TRUE: u8 = 0x11;

// const TERM_PREFIX_CONDITION_FALSE: u8 = 0x20;
// const TERM_PREFIX_CONDITION_TRUE: u8 = 0x21;

const TERM_PREFIX_KEYWORD: u8 = 0x30;

impl<const KeySize: usize> InvertedIndex<KeySize> {
    /// This function returns all the columns that the index tracks.
    /// The index should be notified of changes to these columns.
    pub fn tracked_columns(&self) -> &FxHashSet<ColumnName> {
        &self.all_tracked_columns
    }

    pub fn supports_query(&self, filter_expr: &Expr) -> bool {
        let parsed = self
            .parse_expr(filter_expr)
            .eliminate_unsupported_branches();
        !matches!(parsed, FilterTree::UnsupportedFilter(_))
    }

    fn parse_expr(&self, expr: &Expr) -> FilterTree {
        match expr {
            Expr::Alias(inner) => self.parse_expr(&inner.expr),
            Expr::Column(c) => {
                // Only if it's a boolean column!
                self.get_filter_for_column(c)
                    .unwrap_or_else(|| FilterTree::UnsupportedFilter(Box::new(expr.clone())))
            }

            Expr::ScalarVariable(_, _) | Expr::Literal(_, _) |
            Expr::IsUnknown(_) | Expr::Unnest(_) | Expr::Case(_) | Expr::InSubquery(_) |
            Expr::ScalarSubquery(_) | Expr::OuterReferenceColumn(_, _) | Expr::Wildcard { .. } |
            Expr::Exists(_) | Expr::WindowFunction(_) | Expr::AggregateFunction(_) |
            Expr::GroupingSet(_) |

            // Acceptable only as a value
            Expr::Placeholder(_)
            => {
                FilterTree::UnsupportedFilter(Box::new(expr.clone()))
            }

            expr @ Expr::BinaryExpr(bin) => {
                // One of the two arms may be a scalar value

                match bin.op {
                    Operator::And => FilterTree::And(
                        Box::new(self.parse_expr(&bin.left)),
                        Box::new(self.parse_expr(&bin.right)),
                    ),
                    Operator::Or => FilterTree::Or(
                        Box::new(self.parse_expr(&bin.left)),
                        Box::new(self.parse_expr(&bin.right)),
                    ),

                    Operator::Eq | Operator::NotEq => {
                        let (left_v, right_v) = (Self::find_value(&bin.left), Self::find_value(&bin.right));

                        let (value, expr) = if let Some(left_v) = left_v {
                            (left_v, &bin.right)
                        } else if let Some(right_v) = right_v {
                            (right_v, &bin.left)
                        } else {
                            return self.try_find_tested_expression(expr);
                        };

                        if value.data_type() == DataType::Boolean {
                            let inner = self.parse_expr(&expr);

                            if value.eq(&ScalarValue::Boolean(Some(true))) {
                                inner
                            } else if value.eq(&ScalarValue::Boolean(Some(false))) {
                                FilterTree::Not(Box::new(inner))
                            } else {
                                // IS NULL
                                self.try_find_tested_expression(expr)
                            }
                        } else {
                            self.try_find_tested_expression(expr)
                        }
                    }

                    _ => self.try_find_tested_expression(expr)
                }
            }

            // TODO: here we should detect keywords and emit the correct tree
            Expr::Like(v) => {
                let Some(column) = v.expr.try_as_col() else {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                };
                if !self.tracked_columns().contains(column.name()) {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                }

                let Some(pattern) = v.pattern.as_literal() else {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                };

                let ScalarValue::Utf8(Some(pattern)) = pattern else {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                };

                let pattern = pattern.replace(['%'], " "); // TODO: better variant here, any special symbol should be excluded
                let pattern = self.parse_string(&pattern)
                    .into_iter()
                    .map(|term| FilterTree::KwSearch(term))
                    .reduce(|l, r| FilterTree::And(Box::new(l), Box::new(r)));

                pattern.unwrap_or_else(|| FilterTree::UnsupportedFilter(Box::new(expr.clone())))
            },
            Expr::InList(_) => todo!("kw detection"),
            Expr::SimilarTo(_) => todo!("kw detection"),

            Expr::ScalarFunction(sf) if sf.func.name() == KwSearchUdf::KW_SEARCH_UDF_NAME => {
                let Ok(KwSearchArgs { search_columns, search_string, search_mode }) = KwSearchUdf::split_args(&sf.args) else {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                };

                // Are the columns supported?
                let supported_columns = search_columns.iter().all(|column| self.tracked_columns().contains(column.name()));
                if !supported_columns {
                    return FilterTree::UnsupportedFilter(Box::new(expr.clone()))
                }

                match search_mode {
                    SearchMode::Boolean => {
                        let search_string = if self.collation.contains(Collation::CASE_INSENSITIVE) {
                            search_string.to_lowercase()
                        } else {
                            search_string
                        };

                        let split_q = search_string.trim().split(' ')
                            .fold(FilterTree::Empty, |filter, keyword| {
                                // https://dev.mysql.com/doc/refman/8.4/en/fulltext-boolean.html
                                if let Some(str) = keyword.strip_prefix('+') {
                                    FilterTree::And(Box::new(filter), Box::new(FilterTree::KwSearch(Self::keyword_to_term(str))))
                                } else if let Some(str) = keyword.strip_prefix('-') {
                                    let base = Box::new(FilterTree::KwSearch(Self::keyword_to_term(str)));
                                    FilterTree::And(Box::new(filter), Box::new(FilterTree::Not(base)))
                                } else {
                                    FilterTree::Or(Box::new(filter), Box::new(FilterTree::KwSearch(Self::keyword_to_term(keyword))))
                                }
                            });

                        split_q
                    }
                }
            }

            Expr::Not(e) | Expr::IsFalse(e) | Expr::IsNotTrue(e) => {
                // TODO: in practice, these are different semantics (re. null values), but let's for now ignore this
                let inner = self.parse_expr(&e);
                FilterTree::Not(Box::new(inner))
            }

            Expr::IsTrue(e) | Expr::IsNotFalse(e) => {
                // TODO: in practice, these are different semantics (re. null values), but let's for now ignore this
                self.parse_expr(&e)
            }

            expr @ (Expr::IsNotNull(_) | Expr::IsNull(_) | Expr::Negative(_) | Expr::Between(_) | Expr::IsNotUnknown(_) | Expr::ScalarFunction(_)) => {
                self.try_find_tested_expression(expr)
            }

            Expr::Cast(inner) => self.parse_expr(&inner.expr),
            Expr::TryCast(inner) => self.parse_expr(&inner.expr),
        }
    }

    fn find_value(expr: &Expr) -> Option<ScalarValue> {
        match expr {
            Expr::Literal(lit, _) => Some(lit.clone()),

            Expr::Alias(e) => Self::find_value(&e.expr),

            Expr::Cast(e) => {
                let v = Self::find_value(&e.expr)?;
                v.cast_to(&e.data_type).ok()
            }
            Expr::TryCast(e) => {
                let v = Self::find_value(&e.expr)?;
                v.cast_to(&e.data_type).ok()
            }
            Expr::BinaryExpr(binary) => {
                let left = Self::find_value(&binary.left)?;
                let right = Self::find_value(&binary.right)?;

                match binary.op {
                    Operator::Eq => Some(ScalarValue::Boolean(Some(left == right))),
                    Operator::NotEq => Some(ScalarValue::Boolean(Some(left != right))),
                    Operator::Lt => Some(ScalarValue::Boolean(Some(left < right))),
                    Operator::LtEq => Some(ScalarValue::Boolean(Some(left <= right))),
                    Operator::Gt => Some(ScalarValue::Boolean(Some(left > right))),
                    Operator::GtEq => Some(ScalarValue::Boolean(Some(left >= right))),

                    _ => todo!("non trivial operations on literals"),
                }
            }

            // TODO: binary operators between scalar values...
            _ => None,
        }
    }

    fn get_filter_for_column(&self, c: &Column) -> Option<FilterTree> {
        self.watched_boolean_columns
            .get(&c.name)
            .map(|(if_true, if_false)| FilterTree::BooleanExpr {
                if_true: if_true.clone(),
                if_false: if_false.clone(),
            })
    }

    fn find_boolean_column(&self, expr: &Expr) -> Option<FilterTree> {
        match expr {
            Expr::Alias(alias) => self.find_boolean_column(&alias.expr),
            Expr::Column(c) => self.get_filter_for_column(c),
            _ => None,
        }
    }

    fn try_find_tested_expression(&self, expr: &Expr) -> FilterTree {
        // Is the expr. a simple column?
        if let Some(e) = self.find_boolean_column(expr) {
            return e;
        }

        // TODO: We may add some heuristics here, for example if we have expr = `col > 12` and we
        // have a known tested expression `col > 10`, we may insert it (even though it is an imperfect filter)

        /* let found = self.watched_conditions
        .iter()
        .find(|(watched_cond, _, _)| Self::is_similar(watched_cond, expr)); */

        /* if let Some((_, if_true, if_false)) = found {
            FilterTree::BooleanExpr { if_true: if_true.clone(), if_false: if_false.clone() }
        } else { */
        FilterTree::UnsupportedFilter(Box::new(expr.clone()))
        /* } */
    }

    /* fn is_similar(expr: &Expr, compare_against: &Expr) -> bool {
        expr == compare_against
    } */

    pub fn query(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        filter_exprs: &[Expr],
    ) -> datafusion::common::Result<Option<IndexQueryStrategy>> {
        trace!("Query index with expressions: {:?}", filter_exprs);

        let mut forward_expressions = Vec::new();
        let mut executable_expressions = None;
        let placeholder_pfx = format!(":_idx_{}_", self.inner.index_name());

        for expr in filter_exprs {
            let parsed = self.parse_expr(expr).simplify();

            info!("Parsed expr: {:?}", parsed);

            if parsed.is_entirely_unsuported() {
                forward_expressions.push(Box::new(expr.clone()))
            } else {
                let parsed = Box::new(parsed);
                if let Some(executable) = executable_expressions {
                    executable_expressions = Some(Box::new(FilterTree::And(parsed, executable)))
                } else {
                    executable_expressions = Some(parsed)
                }
            }
        }

        let replacer = if let Some(executable_expressions) = executable_expressions {
            let mut visitor = FilterTreeVisitor {
                queries: Vec::new(),
                placeholder_pfx,
            };
            let executable_expr = executable_expressions
                .simplify()
                .to_executable()
                .simplify()
                .visit(&mut visitor);

            forward_expressions.push(Box::new(executable_expr));

            let indexable_column = table_ref.get_indexable_column().unwrap();

            let replacer = QueryReplacer::<KeySize> {
                query: visitor.queries,
                placeholder_pfx: visitor.placeholder_pfx,
                id_column: Ident::new(indexable_column.name()),
                id_column_type: indexable_column.data_type().clone(),
                index: self.inner.clone(),
            };

            Some(replacer)
        } else {
            None
        };

        let Some(e) = forward_expressions.into_iter().reduce(|left, right| {
            Box::new(Expr::BinaryExpr(BinaryExpr {
                left,
                right,
                op: Operator::And,
            }))
        }) else {
            return Ok(None);
        };

        Ok(Some(match replacer {
            None => IndexQueryStrategy::AddFilterExpression(*e),
            Some(replacer) => {
                IndexQueryStrategy::AddDynamicFilterExpression(*e, Arc::new(replacer))
            }
        }))
    }

    fn parse_string(&self, string: &str) -> Vec<RawIndexTermRef> {
        let string = string.trim();
        let string = if self.collation.contains(Collation::CASE_INSENSITIVE) {
            string.to_lowercase()
        } else {
            string.to_owned()
        };

        let len_unlimited = self.collation.contains(Collation::LENGTH_UNLIMITED);
        let allow_alphanum = self.collation.contains(Collation::ALLOW_ALPHANUMERIC);

        let words = string
            // TODO: heuristics to detect strings that are not keywords (e.g. hashes, ...)
            .split(|c: char| c.is_ascii_punctuation() || c.is_ascii_control() || c.is_whitespace())
            .filter(|word| {
                !word.is_empty()
                    && (len_unlimited || word.len() <= MAX_KW_LEN)
                    && (allow_alphanum || word.chars().all(char::is_alphabetic))
            })
            .map(|word| Self::keyword_to_term(word));

        words.collect()
    }

    fn keyword_to_term(word: &str) -> RawIndexTermRef {
        let word = word.as_bytes();
        let mut vec = Vec::with_capacity(word.len() + 1);
        vec.push(TERM_PREFIX_KEYWORD);
        vec.extend_from_slice(word);
        RawIndexTermRef::new(vec)
    }
}

struct QueryReplacer<const KS: usize> {
    query: Vec<RawIndexQuery>,
    placeholder_pfx: String,
    id_column: Ident,
    id_column_type: DataType,
    index: Arc<RawInvertedIndex<KS>>,
}

impl<const KS: usize> Debug for QueryReplacer<KS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueryReplacer {{ query: {:?}, placeholder_pfx: {}, id_column: {:?}, id_column_type: {:?}, index: [abridged] }}",
            self.query, self.placeholder_pfx, self.id_column, self.id_column_type
        )
    }
}

#[async_trait]
impl<const KS: usize> DynamicFilter for QueryReplacer<KS> {
    async fn execute_filter(
        &self,
        mut filter: ast::Expr,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<ast::Expr> {
        let results = self
            .index
            .query(context.clone(), self.query.clone())
            .await?;
        let mut visitor = ResultReplacer::<KS> {
            placeholder_pfx: self.placeholder_pfx.clone(),
            id_column: self.id_column.clone(),
            id_column_type: self.id_column_type.clone(),
            query_result: results,
        };

        let _ = VisitMut::visit(&mut filter, &mut visitor);
        Ok(filter)
    }
}

struct ResultReplacer<const KS: usize> {
    placeholder_pfx: String,
    id_column: ast::Ident,
    id_column_type: DataType,
    query_result: Vec<FxHashSet<IndexedDocumentId<KS>>>,
}

impl<const N: usize> VisitorMut for ResultReplacer<N> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        let ast::Expr::Value(value) = expr else {
            return ControlFlow::Continue(());
        };
        let ast::Value::Placeholder(pl) = &value.value else {
            return ControlFlow::Continue(());
        };

        if let Some(suffix) = pl.strip_prefix(self.placeholder_pfx.as_str()) {
            let Ok(suffix) = suffix.parse::<usize>() else {
                warn!("Invalid suffix `{suffix}` for placeholder: {pl}");
                return ControlFlow::Continue(());
            };
            let Some(query_result) = self.query_result.get(suffix) else {
                warn!("No query result for placeholder: {pl}");
                return ControlFlow::Continue(());
            };

            let value_is_number = matches!(
                self.id_column_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            );

            // Encode as expressions
            if query_result.len() == 0 {
                value.value = Value::Boolean(false);
            } else if query_result.len() == 1 {
                let result = query_result.into_iter().next().unwrap();

                value.value = if value_is_number {
                    result.as_number_value().unwrap()
                } else {
                    Value::HexStringLiteral(hex::encode(result))
                };

                *expr = ast::Expr::BinaryOp {
                    op: BinaryOperator::Eq,
                    left: Box::new(ast::Expr::Identifier(self.id_column.clone())),
                    right: Box::new(ast::Expr::Value(value.clone())),
                };
            } else {
                let expr_list = query_result
                    .into_iter()
                    .map(|v| {
                        let result = if value_is_number {
                            v.as_number_value().unwrap()
                        } else {
                            Value::HexStringLiteral(hex::encode(v))
                        };

                        let value = ast::ValueWithSpan {
                            value: result,
                            span: Span::empty(),
                        };
                        ast::Expr::Value(value)
                    })
                    .collect();

                *expr = ast::Expr::InList {
                    expr: Box::new(ast::Expr::Identifier(self.id_column.clone())),
                    negated: false,
                    list: expr_list,
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
enum FilterTree {
    And(Box<FilterTree>, Box<FilterTree>),
    Or(Box<FilterTree>, Box<FilterTree>),
    Not(Box<FilterTree>),

    UnsupportedFilter(Box<Expr>),

    // QUESTION: we have two different ways to handle the same problem here
    // Is it interesting to have an `if_true` term, or in the end, can we handle everything as `NOT IN (...)` ?
    // Is the added flexibility interesting or not
    BooleanExpr {
        if_true: RawIndexTermRef,
        if_false: RawIndexTermRef,
    },
    KwSearch(RawIndexTermRef),
    Empty,
}

enum ExecutableFilterTree {
    And(Box<ExecutableFilterTree>, Box<ExecutableFilterTree>),
    Or(Box<ExecutableFilterTree>, Box<ExecutableFilterTree>),
    Not(Box<ExecutableFilterTree>),

    UnsupportedFilter(Box<Expr>),
    FilterExpr(RawIndexQuery),
}

struct FilterTreeVisitor {
    placeholder_pfx: String,
    queries: Vec<RawIndexQuery>,
}

impl ExecutableFilterTree {
    pub fn visit(self, visitor: &mut FilterTreeVisitor) -> Expr {
        match self {
            ExecutableFilterTree::And(a, b) => Expr::BinaryExpr(BinaryExpr {
                left: Box::new(a.visit(visitor)),
                right: Box::new(b.visit(visitor)),
                op: Operator::And,
            }),
            ExecutableFilterTree::Or(a, b) => Expr::BinaryExpr(BinaryExpr {
                left: Box::new(a.visit(visitor)),
                right: Box::new(b.visit(visitor)),
                op: Operator::Or,
            }),
            ExecutableFilterTree::Not(a) => Expr::Not(Box::new(a.visit(visitor))),
            ExecutableFilterTree::UnsupportedFilter(f) => *f,
            ExecutableFilterTree::FilterExpr(f) => {
                let query_number = visitor.queries.len();
                visitor.queries.push(f);
                Expr::Placeholder(Placeholder {
                    field: None,
                    id: format!("{}{query_number}", &visitor.placeholder_pfx),
                })
            }
        }
    }

    pub fn simplify(self) -> Self {
        match self {
            ExecutableFilterTree::And(a, b) => {
                let a = a.simplify();
                let b = b.simplify();

                match (a, b) {
                    (ExecutableFilterTree::FilterExpr(a), ExecutableFilterTree::FilterExpr(b)) => {
                        ExecutableFilterTree::FilterExpr(RawIndexQuery::Intersect(
                            Box::new(a),
                            Box::new(b),
                        ))
                    }
                    (ExecutableFilterTree::Not(a), ExecutableFilterTree::FilterExpr(b)) => {
                        if matches!(*a, ExecutableFilterTree::FilterExpr(_)) {
                            // NOT(A) AND B works: it's just B - A
                            let ExecutableFilterTree::FilterExpr(a) = *a else {
                                unreachable!()
                            };
                            ExecutableFilterTree::FilterExpr(RawIndexQuery::Difference(
                                Box::new(b),
                                Box::new(a),
                            ))
                        } else {
                            // Forward AND as is (no simplify)
                            ExecutableFilterTree::And(
                                Box::new(ExecutableFilterTree::Not(a)),
                                Box::new(ExecutableFilterTree::FilterExpr(b)),
                            )
                        }
                    }
                    (ExecutableFilterTree::FilterExpr(a), ExecutableFilterTree::Not(b)) => {
                        if matches!(*b, ExecutableFilterTree::FilterExpr(_)) {
                            // A AND NOT(B) works: it's just A - B
                            let ExecutableFilterTree::FilterExpr(b) = *b else {
                                unreachable!()
                            };
                            ExecutableFilterTree::FilterExpr(RawIndexQuery::Difference(
                                Box::new(a),
                                Box::new(b),
                            ))
                        } else {
                            // Forward AND as is (no simplify)
                            ExecutableFilterTree::And(
                                Box::new(ExecutableFilterTree::FilterExpr(a)),
                                Box::new(ExecutableFilterTree::Not(b)),
                            )
                        }
                    }
                    (ExecutableFilterTree::Not(a), ExecutableFilterTree::Not(b)) => {
                        // Lift "Not" up, only in some cases!
                        match (*a, *b) {
                            (
                                ExecutableFilterTree::FilterExpr(a),
                                ExecutableFilterTree::FilterExpr(b),
                            ) => {
                                // (NOT A) AND (NOT B) is NOT(A union B)
                                ExecutableFilterTree::Not(Box::new(
                                    ExecutableFilterTree::FilterExpr(RawIndexQuery::Union(
                                        Box::new(a),
                                        Box::new(b),
                                    )),
                                ))
                            }
                            (a, b) => {
                                // Forward AND as is (no simplify)
                                ExecutableFilterTree::And(
                                    Box::new(ExecutableFilterTree::Not(Box::new(a))),
                                    Box::new(ExecutableFilterTree::Not(Box::new(b))),
                                )
                            }
                        }
                    }
                    (a, b) => {
                        // Other cases, no simplification
                        ExecutableFilterTree::And(Box::new(a), Box::new(b))
                    }
                }
            }
            ExecutableFilterTree::Or(a, b) => {
                let a = a.simplify();
                let b = b.simplify();
                ExecutableFilterTree::Or(Box::new(a), Box::new(b))
            }
            ExecutableFilterTree::Not(a) => {
                let a = a.simplify();
                ExecutableFilterTree::Not(Box::new(a))
            }
            leaf => leaf,
        }
    }
}

impl FilterTree {
    fn is_unsupported(&self) -> bool {
        matches!(self, Self::UnsupportedFilter(_))
    }

    fn is_kwsearch(&self) -> bool {
        matches!(self, Self::KwSearch(_))
    }

    #[allow(unused)]
    #[warn(unused)]
    fn has_unsupported_child(&self) -> bool {
        match self {
            FilterTree::And(a, b) | FilterTree::Or(a, b) => {
                a.has_unsupported_child() || b.has_unsupported_child()
            }
            FilterTree::Not(a) => a.has_unsupported_child(),
            FilterTree::UnsupportedFilter(_) => true,
            FilterTree::BooleanExpr { .. } => false,
            FilterTree::KwSearch(_) => false,
            FilterTree::Empty => false,
        }
    }

    fn is_entirely_unsuported(&self) -> bool {
        match self {
            FilterTree::And(a, b) | FilterTree::Or(a, b) => {
                a.is_entirely_unsuported() && b.is_entirely_unsuported()
            }
            FilterTree::Not(a) => a.is_entirely_unsuported(),
            FilterTree::UnsupportedFilter(_) => true,
            FilterTree::BooleanExpr { .. } => false,
            FilterTree::KwSearch(_) => false,
            FilterTree::Empty => false,
        }
    }

    fn has_kwsearch_child(&self) -> bool {
        match self {
            FilterTree::And(a, b) | FilterTree::Or(a, b) => {
                a.has_kwsearch_child() || b.has_kwsearch_child()
            }
            FilterTree::Not(a) => a.has_kwsearch_child(),
            FilterTree::UnsupportedFilter(_) => false,
            FilterTree::BooleanExpr { .. } => false,
            FilterTree::KwSearch(_) => true,
            FilterTree::Empty => false,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        matches!(self, FilterTree::Empty)
    }

    pub fn simplify(self) -> Self {
        // We want to eliminate as many NOT as possible, and move them UP whenever we can
        // BUT we accept NOT as a leaf for some terms (it's just the KwSearch that we don't like actually)
        // SO: NOT(kwsearch) as a leaf: not great, want removed
        // NOT(boolean | unsupported) as leafs: okay, can be simplified

        // AND:
        // NOT(kwsearch) AND NOT(boolean) => NOT(kwsearch OR boolean) (single expr, preferred)
        // NOT(kwsearch) AND boolean => NOT(kwsearch OR NOT(boolean)) (single expr, preferred - NOT(boolean) will be simplified - should we simplify it immediately?)

        // NOT(kwsearch) AND unsupported => preferred form (separate unsupported)
        // NOT(kwsearch) AND kwsearch => sadly we cannot do better

        // kwsearch AND NOT(boolean) => preferred (single expr, convert?)
        // kwsearch AND kwsearch => preferred
        // kwsearch AND unsupported => preferred

        // OR:
        // NOT(kwsearch) OR NOT(boolean) => NOT(kwsearch AND boolean) (single expr, preferred)
        // NOT(kwsearch) OR boolean => NOT(kwsearch AND NOT(boolean)) (single expr, preferred)
        // NOT(kwsearch) OR unsupported => untouched
        // NOT(kwsearch) OR kwsearch => untouched
        // kwsearch OR NOT(boolean) => untouched - single expr
        // kwsearch OR kwsearch => untouched

        // Rule 0: simplify children first
        // Rule 1: distribute NOT(kwsearch) AND ... and NOT(kwsearch) OR
        // BASICALLY: only touch if we have a NOT(kwsearch) somewhere, both in OR and AND.
        // Rule 2: do not touch NOT directly, will be done when converting later on

        // How can we pull unsupported nodes?
        // 1. NOT(unsupported) is equiv to unsupported here
        // Trivially: we can at least pull unsupported nodes AND(AND(unsupp), supp), supp) <=> AND(AND(supp, supp), unsupp)
        // Identical for OR
        // Identical for NOT (but ignore it)

        match self {
            FilterTree::And(left, right) | FilterTree::Or(left, right) if left.is_empty() => {
                right.simplify()
            }
            FilterTree::And(left, right) | FilterTree::Or(left, right) if right.is_empty() => {
                left.simplify()
            }
            FilterTree::And(left, right) => {
                let left = left.simplify();
                let right = right.simplify();

                match (left, right) {
                    (a, b) if a.is_unsupported() || b.is_unsupported() => {
                        // We cannot touch nodes with unsupported components
                        FilterTree::And(Box::new(a), Box::new(b))
                    }
                    (FilterTree::Not(a), FilterTree::Not(b)) => {
                        // Two boolean expressions, we lift NOT up
                        FilterTree::Not(Box::new(FilterTree::Or(a, b)))
                    }
                    (FilterTree::Not(a), b) | (b, FilterTree::Not(a))
                        if a.is_kwsearch() && !b.has_kwsearch_child() =>
                    {
                        // we want to eliminate Not(kwsearch), without introducing a new one down below - would be bad
                        FilterTree::Not(Box::new(FilterTree::Or(
                            a,                                      // Eliminated not (moved up)
                            Box::new(FilterTree::Not(Box::new(b))), // introduced not (on a node that can take it)
                        )))
                    }
                    (a, b) => FilterTree::And(Box::new(a), Box::new(b)),
                }
            }
            FilterTree::Or(left, right) => {
                let left = left.simplify();
                let right = right.simplify();

                match (left, right) {
                    (a, b) if a.is_unsupported() || b.is_unsupported() => {
                        // We cannot touch nodes with unsupported components
                        FilterTree::Or(Box::new(a), Box::new(b))
                    }
                    (FilterTree::Not(a), FilterTree::Not(b)) => {
                        // Two boolean expressions, we lift NOT up
                        FilterTree::Not(Box::new(FilterTree::And(a, b)))
                    }
                    (FilterTree::Not(a), b) | (b, FilterTree::Not(a))
                        if a.is_kwsearch() && !b.has_kwsearch_child() =>
                    {
                        // we want to eliminate Not(kwsearch), without introducing a new one down below - would be bad
                        FilterTree::Not(Box::new(FilterTree::And(
                            a,                                      // Eliminated not (moved up)
                            Box::new(FilterTree::Not(Box::new(b))), // introduced not (on a node that can take it)
                        )))
                    }
                    (a, b) => FilterTree::Or(Box::new(a), Box::new(b)),
                }
            }
            other => other,
        }
    }

    pub fn eliminate_unsupported_branches(self) -> FilterTree {
        match self {
            FilterTree::And(left, right) => {
                let left = left.eliminate_unsupported_branches();
                let right = right.eliminate_unsupported_branches();

                if matches!(left, FilterTree::UnsupportedFilter(_)) {
                    right
                } else if matches!(right, FilterTree::UnsupportedFilter(_)) {
                    left
                } else {
                    FilterTree::And(Box::new(left), Box::new(right))
                }
            }
            FilterTree::Or(left, right) => {
                let left = left.eliminate_unsupported_branches();
                let right = right.eliminate_unsupported_branches();

                if matches!(left, FilterTree::UnsupportedFilter(_)) {
                    right
                } else if matches!(right, FilterTree::UnsupportedFilter(_)) {
                    left
                } else {
                    FilterTree::Or(Box::new(left), Box::new(right))
                }
            }
            FilterTree::Not(inner) => {
                let inner = inner.eliminate_unsupported_branches();
                if matches!(inner, FilterTree::UnsupportedFilter(_)) {
                    inner
                } else {
                    FilterTree::Not(Box::new(inner))
                }
            }
            leaf @ (FilterTree::BooleanExpr { .. }
            | FilterTree::KwSearch(_)
            | FilterTree::UnsupportedFilter(_)
            | FilterTree::Empty) => leaf,
        }
    }

    pub fn to_executable(self) -> ExecutableFilterTree {
        match self {
            FilterTree::And(l, r) => {
                ExecutableFilterTree::And(Box::new(l.to_executable()), Box::new(r.to_executable()))
            }
            FilterTree::Or(l, r) => {
                ExecutableFilterTree::Or(Box::new(l.to_executable()), Box::new(r.to_executable()))
            }
            FilterTree::Not(inner) => match *inner {
                FilterTree::BooleanExpr { if_false, .. } => {
                    ExecutableFilterTree::FilterExpr(RawIndexQuery::Term(if_false))
                }
                FilterTree::UnsupportedFilter(expr) => {
                    let new_expr = match *expr {
                        Expr::Not(inner) | Expr::IsFalse(inner) | Expr::IsNotTrue(inner) => inner,
                        other => Box::new(Expr::Not(Box::new(other))),
                    };
                    ExecutableFilterTree::UnsupportedFilter(new_expr)
                }
                other => ExecutableFilterTree::Not(Box::new(other.to_executable())),
            },
            FilterTree::BooleanExpr { if_true: s, .. } | FilterTree::KwSearch(s) => {
                ExecutableFilterTree::FilterExpr(RawIndexQuery::Term(s))
            }
            FilterTree::UnsupportedFilter(expr) => ExecutableFilterTree::UnsupportedFilter(expr),
            FilterTree::Empty => unreachable!("no empty node should remain in tree"),
        }
    }
}

#[async_trait]
impl<const N: usize> EncryptedIndex for InvertedIndex<N> {
    fn requires_external_storage(&self) -> bool {
        true
    }

    fn insert(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        _key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        let sink = InvertedIndexInsertSink::<N>::new(table_ref, self, schema)?;
        Ok(IndexInsertStrategy::IndexSink(Arc::new(sink)))
    }

    fn query(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        _key_manager: Arc<LongTermKeyManager>,
        filter: &[Expr],
    ) -> datafusion::common::Result<Option<IndexQueryStrategy>> {
        InvertedIndex::<N>::query(self, table_ref, filter)
    }

    fn is_column_hidden(&self, _column: &ColumnName) -> bool {
        false
    }

    fn tracked_columns(&self) -> FxHashSet<ColumnName> {
        InvertedIndex::<N>::tracked_columns(self).clone()
    }

    fn supports_expression(&self, filter: &Expr) -> bool {
        self.supports_query(filter)
    }

    async fn create_index_plan(
        &self,
        parent_table_ref: ResolvedTableReference,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = CreateInvertedIndexPlan::<N>::new(
            session_state,
            Arc::new(self.clone()),
            session_state
                .get_catalog()
                .mysql_schema(&parent_table_ref.schema)
                .unwrap(),
            parent_table_ref,
        )
        .await?;

        Ok(Arc::new(result))
    }

    fn to_config(&self) -> EncryptedIndexConfigurationVariant {
        EncryptedIndexConfigurationVariant::InvertedIndex(InvertedIndexConfig {
            collation: self.collation,
            index_name: self.inner.index_name().to_string(),
            watched_boolean_columns: self.watched_boolean_columns.clone(),
            watched_string_columns: self.watched_string_columns.clone(),
        })
    }

    fn update(
        self: Arc<Self>,
        table_ref: &MySqlTableProvider,
        _key_manager: Arc<LongTermKeyManager>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<IndexInsertStrategy> {
        let sink = InvertedIndexUpdateSink::<N>::new(table_ref, self, schema)?;
        Ok(IndexInsertStrategy::IndexSink(Arc::new(sink)))
    }

    fn linked_table_names(&self) -> Vec<String> {
        vec![self.inner.table_name().to_string()]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedIndexConfig {
    index_name: String,
    collation: Collation,

    watched_string_columns: BTreeSet<ColumnName>,

    /// A vec (column_name, term_if_true, term_if_false)
    watched_boolean_columns: BTreeMap<ColumnName, (RawIndexTermRef, RawIndexTermRef)>,
    // A vec (condition, term_if_true, term_if_false)
    // watched_conditions: Vec<(Expr, RawIndexTermRef, RawIndexTermRef)>,
}

impl IndexConfig for InvertedIndexConfig {
    fn into_index(
        self,
        table_name: &str,
        indexable_column: IndexableColumn,
    ) -> Arc<dyn EncryptedIndex> {
        match indexable_column.size() {
            IndexableColumnSize::B8 => self.into_sized_index::<1>(table_name),
            IndexableColumnSize::B16 => self.into_sized_index::<2>(table_name),
            IndexableColumnSize::B32 => self.into_sized_index::<4>(table_name),
            IndexableColumnSize::B64 => self.into_sized_index::<8>(table_name),
            IndexableColumnSize::B96 => self.into_sized_index::<12>(table_name),
            IndexableColumnSize::B128 => self.into_sized_index::<16>(table_name),
        }
    }
}

impl InvertedIndexConfig {
    fn into_sized_index<const N: usize>(self, table_name: &str) -> Arc<dyn EncryptedIndex> {
        let inner = RawInvertedIndex::<N>::new(table_name.to_string(), self.index_name.clone());
        let ordered_tracked_columns: Vec<_> = self
            .watched_string_columns
            .iter()
            .map(|v| v.clone())
            //.chain(
            //    self.watched_conditions.iter().flat_map(|(cnd, _, _)| cnd.column_refs().into_iter().map(|c| c.name.clone()))
            //)
            .chain(self.watched_boolean_columns.keys().cloned())
            .collect();

        let all_tracked_columns = ordered_tracked_columns.iter().cloned().collect();

        let index = InvertedIndex::<N> {
            inner: Arc::new(inner),

            collation: self.collation,
            watched_boolean_columns: self.watched_boolean_columns,
            // watched_conditions: self.watched_conditions,
            // watched_conditions: vec![],
            watched_string_columns: self.watched_string_columns,
            ordered_tracked_columns,
            all_tracked_columns,
        };

        Arc::new(index)
    }

    pub fn initialize_for_columns(
        name: String,
        table: Arc<MySqlTableProvider>,
        columns: Vec<ColumnName>,
    ) -> datafusion::common::Result<InvertedIndexConfig> {
        // Verify column types + expressions (later)
        let schema = TableProvider::schema(table.as_ref());
        let columns: Vec<Field> = columns
            .into_iter()
            .map(|col| schema.field_with_name(&col).cloned())
            .collect::<Result<_, _>>()?;

        let (boolean, other): (Vec<_>, Vec<_>) = columns
            .into_iter()
            .partition(|field| field.data_type() == &DataType::Boolean);

        let boolean_columns = boolean
            .into_iter()
            .map(|field| {
                let field_name_len = field.name().as_bytes().len();
                let (mut if_tru, mut if_fls) = (
                    Vec::with_capacity(1 + field_name_len),
                    Vec::with_capacity(1 + field_name_len),
                );
                if_tru.push(TERM_PREFIX_BOOLEAN_TRUE);
                if_fls.push(TERM_PREFIX_BOOLEAN_FALSE);
                if_tru.copy_from_slice(field.name().as_bytes());
                if_fls.copy_from_slice(field.name().as_bytes());

                (field.name().clone(), (Arc::new(if_tru), Arc::new(if_fls)))
            })
            .collect();

        let other_columns = other
            .into_iter()
            .map(|field| field.name().clone())
            .collect();

        let cfg = InvertedIndexConfig {
            watched_boolean_columns: boolean_columns,
            watched_string_columns: other_columns,
            collation: Collation::CASE_INSENSITIVE,
            index_name: name,
        };

        Ok(cfg)
    }
}

#[derive(Debug, Clone)]
struct CreateInvertedIndexPlan<const IdSize: usize> {
    create_base_index: Arc<dyn ExecutionPlan>,

    schema_provider: Arc<MySqlSchemaProvider>,
    index_config: Arc<InvertedIndex<IdSize>>,
    parent_table: ResolvedTableReference,
    properties: PlanProperties,
}

impl<const IdSize: usize> CreateInvertedIndexPlan<IdSize> {
    async fn new(
        session: &dyn Session,
        index: Arc<InvertedIndex<IdSize>>,
        schema_provider: Arc<MySqlSchemaProvider>,
        table: ResolvedTableReference,
    ) -> datafusion::common::Result<Self> {
        let indexable_column = {
            let table_provider = schema_provider.mysql_table(&table.table).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CreateInvertedIndexPlan: Table not found {}",
                    table.table
                ))
            })?;

            table_provider.get_indexable_column()
        };

        let indexable_column = if let Some(indexable_column) = indexable_column {
            indexable_column
        } else {
            let conn = session.config().get_conn();
            let mut conn = conn
                .try_lock()
                .expect("could not lock connection to create index column");

            schema_provider
                .force_create_indexable_column(table.table.as_ref(), &mut conn)
                .await?
        };

        // Create the plan to select whole table for index construction
        let table_provider = schema_provider
            .mysql_table(&table.table)
            .expect("race condition: table vanished");
        let schema = table_provider.schema();

        let tracked_columns = index
            .all_tracked_columns
            .iter()
            .filter_map(|col| schema.field_with_name(col).ok().cloned());

        let fetched_columns: Vec<Field> = iter::once(indexable_column.clone())
            .chain(tracked_columns)
            .collect();

        let scan_plan = table_provider
            .scan_and_decrypt_all(session, Schema::new(fetched_columns), &[], None)
            .await?;

        let indexable_column_select = Arc::new(
            datafusion::physical_expr::expressions::Column::new(indexable_column.name(), 0),
        );

        let project_plan = ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(
                    Arc::new(ToBinaryExpr::new(indexable_column_select)),
                    CreateDbInvertedIndexPlan::<IdSize>::COLUMN_NAME_ID.to_string(),
                ),
                ProjectionExpr::new(
                    Arc::new(ProjectTransformIntoTerms::new_for_found_columns(
                        index.clone(),
                        scan_plan.schema().as_ref(),
                    )?),
                    CreateDbInvertedIndexPlan::<IdSize>::COLUMN_NAME_TERMS.to_string(),
                ),
            ],
            scan_plan,
        )?;

        // Create the plan to create the base index
        let base_plan = CreateDbInvertedIndexPlan::<IdSize>::new_from_data(
            table.table.to_string(),
            index.inner.index_name().to_string(),
            Arc::new(project_plan),
        )?;

        Ok(Self {
            properties: base_plan.properties().clone(),
            create_base_index: Arc::new(base_plan),
            schema_provider,
            index_config: index,
            parent_table: table,
        })
    }
}

impl<const IdSize: usize> DisplayAs for CreateInvertedIndexPlan<IdSize> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateInvertedIndex table={}.{}",
            self.parent_table.schema, self.parent_table.table
        )
    }
}

impl<const IdSize: usize> ExecutionPlan for CreateInvertedIndexPlan<IdSize> {
    fn name(&self) -> &str {
        "create_inverted_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.create_base_index]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            plan_err!("CreateInvertedIndexPlan must have one child")?;
        }

        let child = children.remove(0);

        if !child
            .as_ref()
            .as_any()
            .is::<CreateDbInvertedIndexPlan<IdSize>>()
        {
            warn!("CreateInvertedIndexPlan should have a CreateDbInvertedIndexPlan child");
        }

        let mut self_unwrapped = Arc::unwrap_or_clone(self);
        self_unwrapped.create_base_index = child;

        Ok(Arc::new(self_unwrapped))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let create_base_index = self.create_base_index.clone();
        let schema_provider = self.schema_provider.clone();
        let table_name = self.parent_table.table.clone();
        let table_schema = self.parent_table.schema.clone();
        let index_config = self.index_config.clone();

        let parent = create_base_index.execute(partition, context.clone())?;
        let result = async move {
            let _ = parent.try_collect::<Vec<_>>().await?;

            // Update the metadata
            let table = schema_provider.mysql_table(&table_name).ok_or_else(|| {
                DataFusionError::Execution(format!("table not found {table_name}"))
            })?;

            let mut table = Arc::unwrap_or_clone(table);
            let meta = table.encryption_metadata_mut();
            meta.add_index(index_config);

            let serialized_meta: SerializableEncryptedTableMeta =
                SerializableEncryptedTableMeta::from(&*meta);

            context
                .get_store()
                .update_metadata(&table_schema, |meta| {
                    meta.encrypted_tables
                        .insert(table_name.to_string(), serialized_meta);
                    Ok(())
                })
                .await?;

            let _ = schema_provider.replace_mysql_table(table_name.to_string(), Arc::new(table))?;
            Ok(DmlResult::empty().into())
        };

        let result = stream::once(result);
        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), result);

        Ok(Box::pin(stream_adapter))
    }
}

#[derive(Debug, Clone)]
struct ProjectTransformIntoTerms<const N: usize> {
    return_field: FieldRef,
    children: Vec<Arc<dyn PhysicalExpr>>,
    parent_index: Arc<InvertedIndex<N>>,
}

impl<const N: usize> ProjectTransformIntoTerms<N> {
    pub fn new_for_found_columns(
        parent_index: Arc<InvertedIndex<N>>,
        parent_schema: &Schema,
    ) -> datafusion::common::Result<Self> {
        Self::new_for_found_columns_prefixed(parent_index, parent_schema, None)
    }

    pub fn new_for_found_columns_prefixed(
        parent_index: Arc<InvertedIndex<N>>,
        parent_schema: &Schema,
        prefix: Option<&str>,
    ) -> datafusion::common::Result<Self> {
        let data_type = DataType::List(FieldRef::new(
            // We don't have nulls, but the concat function returns a nullable type, so we must declare it
            Field::new_list_field(DataType::Binary, true),
        ));
        let return_field = Arc::new(Field::new("project_transform_into_terms", data_type, false));

        let children = parent_index
            .ordered_tracked_columns
            .iter()
            .map(|column_name| {
                let column_name = if let Some(pfx) = prefix {
                    format!("{}{}", pfx, column_name)
                } else {
                    column_name.clone()
                };

                let arc: Arc<dyn PhysicalExpr> =
                    if let Some((position, _)) = parent_schema.column_with_name(&column_name) {
                        Arc::new(physical_expr::expressions::Column::new(
                            &column_name,
                            position,
                        ))
                    } else {
                        // Column not found! Null value
                        Arc::new(physical_expr::expressions::Literal::new(ScalarValue::Null))
                    };

                Ok(arc)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        Ok(Self {
            return_field,
            parent_index,
            children,
        })
    }

    fn process_boolean_column(
        column: ArrayRef,
        (if_true, if_false): &(RawIndexTermRef, RawIndexTermRef),
    ) -> datafusion::common::Result<ColumnarValue> {
        let boolean_column = if column.data_type() == &DataType::Boolean {
            column
        } else {
            cast(column.as_ref(), &DataType::Boolean)?
        };

        let boolean_column = boolean_column.as_boolean();
        let values_builder = GenericByteBuilder::<GenericBinaryType<i32>>::new();
        let mut builder =
            GenericListBuilder::<i32, _>::with_capacity(values_builder, boolean_column.len());

        for v in boolean_column {
            if let Some(v) = v {
                if v {
                    builder.append_value([Some(if_true.as_slice())])
                } else {
                    builder.append_value([Some(if_false.as_slice())])
                }
            } else {
                builder.append_value::<_, Vec<u8>>([]);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn process_string_column(&self, column: ArrayRef) -> datafusion::common::Result<ColumnarValue> {
        if column.data_type() == &DataType::Utf8 {
            self.process_string_column_sized::<i32>(column)
        } else if column.data_type() == &DataType::LargeUtf8 {
            self.process_string_column_sized::<i64>(column)
        } else {
            let cast = cast(column.as_ref(), &DataType::Utf8)?;
            self.process_string_column_sized::<i32>(cast)
        }
    }

    fn process_string_column_sized<T: OffsetSizeTrait>(
        &self,
        column: ArrayRef,
    ) -> datafusion::common::Result<ColumnarValue> {
        let string_column = column.as_string::<T>();

        let values_builder = GenericByteBuilder::<GenericBinaryType<i32>>::new();
        let mut builder =
            GenericListBuilder::<i32, _>::with_capacity(values_builder, string_column.len());

        for v in string_column {
            if let Some(v) = v {
                for sub_value in self.parent_index.parse_string(v) {
                    builder.values().append_value(sub_value.as_slice());
                }
                builder.append(true);
            } else {
                builder.append(true);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl<const N: usize> PartialEq for ProjectTransformIntoTerms<N> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&other.parent_index, &self.parent_index)
            && self.return_field == other.return_field
            && self.children == other.children
    }
}

impl<const N: usize> Eq for ProjectTransformIntoTerms<N> {}

impl<const N: usize> Display for ProjectTransformIntoTerms<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProjectTransformIntoTerms(")?;
        for child in self.children.iter() {
            Display::fmt(child, f)?;
        }
        f.write_str(")")
    }
}

impl<const N: usize> Hash for ProjectTransformIntoTerms<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.return_field.hash(state);
        Arc::as_ptr(&self.parent_index).hash(state);
        self.children.hash(state);
    }
}

impl<const N: usize> PhysicalExpr for ProjectTransformIntoTerms<N> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        // This function takes no inputs as it always operates on the entire record batch (maybe a bad idea?)
        // We may need to modify it to handle updates...

        // Strategy: process columns one by one, and concatenate results at the end
        let mut intermediate_values = Vec::new();

        for (column_data, tracked_column_name) in self
            .children
            .iter()
            .zip(self.parent_index.ordered_tracked_columns.iter())
        {
            let column = column_data.evaluate(batch)?;
            let column = match column {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(value) => {
                    if value == ScalarValue::Null {
                        continue;
                    }

                    exec_err!("Operator returned constant value {value:?}")?
                }
            };

            let boolean_column = self
                .parent_index
                .watched_boolean_columns
                .get(tracked_column_name);
            if let Some(terms) = boolean_column {
                intermediate_values.push(Self::process_boolean_column(column, terms)?);
            } else if self
                .parent_index
                .watched_string_columns
                .contains(tracked_column_name)
            {
                intermediate_values.push(self.process_string_column(column)?)
            }
        }

        ArrayConcat::new().invoke_with_args(ScalarFunctionArgs {
            args: intermediate_values,
            arg_fields: vec![],
            number_rows: batch.num_rows(),
            return_field: self.return_field.clone(),
            config_options: Arc::new(Default::default()), // Ignored by concat
        })
    }

    fn return_field(&self, _input_schema: &Schema) -> datafusion::common::Result<FieldRef> {
        Ok(self.return_field.clone())
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.parent_index.ordered_tracked_columns.len() {
            plan_err!(
                "ProjectTransformIntoTerms must have all tracked column as arguments, optionnally replaced by null"
            )?
        }

        Ok(Arc::new(Self {
            children,
            parent_index: self.parent_index.clone(),
            return_field: self.return_field.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Debug, Clone)]
struct InvertedIndexInsertSink<const N: usize> {
    index: Arc<InvertedIndex<N>>,

    input_schema: SchemaRef,
    index_project_name: String,
    terms_project_name: String,

    project: [ProjectionExpr; 2],
}

impl<const N: usize> InvertedIndexInsertSink<N> {
    pub fn new(
        table: &MySqlTableProvider,
        index: Arc<InvertedIndex<N>>,
        input_schema: SchemaRef,
    ) -> datafusion::common::Result<Self> {
        let index_project_name = format!("_idx_{}_id", index.inner.index_name());
        let terms_project_name = format!("_idx_{}_terms", index.inner.index_name());

        // Find ID column
        let indexable_column = table.get_indexable_column().ok_or_else(|| {
            DataFusionError::Plan("table must have an indexable column".to_string())
        })?;
        let indexable_column_select = Arc::new(physical_expr::expressions::Column::new(
            indexable_column.name(),
            input_schema.index_of(&indexable_column.name())?,
        ));
        let id_extractor = Arc::new(ToBinaryExpr::new(indexable_column_select));

        // Find terms columns
        let terms_extractor = Arc::new(ProjectTransformIntoTerms::new_for_found_columns(
            index.clone(),
            input_schema.as_ref(),
        )?);

        let input_schema = Schema::new(vec![
            Field::new(&index_project_name, DataType::Binary, false),
            Field::new(
                &terms_project_name.clone(),
                terms_extractor.return_field.data_type().clone(),
                false,
            ),
        ]);

        let project = [
            ProjectionExpr::new(id_extractor, index_project_name.clone()),
            ProjectionExpr::new(terms_extractor, terms_project_name.clone()),
        ];

        Ok(Self {
            project,
            index,
            index_project_name,
            terms_project_name,
            input_schema: Arc::new(input_schema),
        })
    }
}

impl<const N: usize> IndexSink for InvertedIndexInsertSink<N> {
    fn input(&self) -> &[ProjectionExpr] {
        &self.project
    }

    fn into_record_batch_sink(self: Arc<Self>) -> Arc<dyn RecordBatchSink> {
        self
    }
}
impl<const N: usize> DisplayAs for InvertedIndexInsertSink<N> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "InsertInvertedIndex")
    }
}

macro_rules! extract_terms_column {
    ($batch:expr, $terms_column_name:expr) => {{
        let terms_column = $batch.column_by_name($terms_column_name).unwrap();

        terms_column.as_list::<i32>().iter().filter_map(|arr| {
            Some(
                arr?.as_binary::<i32>()
                    .iter()
                    .filter_map(|v| Some(Arc::new(v?.to_vec())))
                    .collect::<FxHashSet<RawIndexTermRef>>(),
            )
        })
    }};
}

macro_rules! extract_id_column {
    ($batch:expr, $id_column_name:expr) => {{
        let index_column = $batch.column_by_name(&$id_column_name).unwrap();
        index_column
            .as_binary::<i32>()
            .into_iter()
            .filter_map(|value| <[u8; N]>::try_from(value?).ok())
    }};
}

#[async_trait]
impl<const N: usize> RecordBatchSink for InvertedIndexInsertSink<N> {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult> {
        for batch in data.into_iter() {
            let terms_column = extract_terms_column!(batch, &self.terms_project_name);
            let index_column = extract_id_column!(batch, &self.index_project_name);

            let iterator = index_column.zip(terms_column);

            self.index
                .inner
                .insert_many(Arc::clone(context), iterator)
                .await?;
        }

        Ok(DmlResult::empty())
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

#[derive(Debug, Clone)]
struct InvertedIndexUpdateSink<const N: usize> {
    index: Arc<InvertedIndex<N>>,

    input_schema: SchemaRef,
    index_project_name: String,
    terms_project_name: String,
    old_terms_project_name: String,

    project: [ProjectionExpr; 3],
}

impl<const N: usize> InvertedIndexUpdateSink<N> {
    fn get_id_column(
        table: &MySqlTableProvider,
        input_schema: &SchemaRef,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let indexable_column = table.get_indexable_column().ok_or_else(|| {
            DataFusionError::Plan("table must have an indexable column".to_string())
        })?;

        let indexable_column_name = if table.get_indexable_column_type().is_generated() {
            format!("{CURRENT_VALUE_PREFIX}{}", indexable_column.name())
        } else {
            // The column is the primary key and should therefore be present as a filter
            format!("{FILTER_PREFIX}{}", indexable_column.name())
        };

        let indexable_column_select = Arc::new(physical_expr::expressions::Column::new(
            &indexable_column_name,
            input_schema.index_of(&indexable_column_name)?,
        ));

        Ok(Arc::new(ToBinaryExpr::new(indexable_column_select)) as _)
    }

    pub fn new(
        table: &MySqlTableProvider,
        index: Arc<InvertedIndex<N>>,
        input_schema: SchemaRef,
    ) -> datafusion::common::Result<Self> {
        let index_project_name = format!("_idx_{}_id", index.inner.index_name());
        let terms_project_name = format!("_idx_{}_terms", index.inner.index_name());
        let old_terms_project_name = format!("_idx_{}_old_terms", index.inner.index_name());

        // Find ID column
        let id_extractor = Self::get_id_column(table, &input_schema)?;

        // Find new columns
        let new_terms_extractor = Arc::new(ProjectTransformIntoTerms::new_for_found_columns(
            index.clone(),
            input_schema.as_ref(),
        )?);
        let old_terms_extractor =
            Arc::new(ProjectTransformIntoTerms::new_for_found_columns_prefixed(
                index.clone(),
                input_schema.as_ref(),
                Some(CURRENT_VALUE_PREFIX),
            )?);

        let input_schema = Schema::new(vec![
            Field::new(&index_project_name, DataType::Binary, false),
            Field::new(
                &terms_project_name.clone(),
                new_terms_extractor.return_field.data_type().clone(),
                false,
            ),
            Field::new(
                &old_terms_project_name.clone(),
                old_terms_extractor.return_field.data_type().clone(),
                false,
            ),
        ]);

        let project = [
            ProjectionExpr::new(id_extractor, index_project_name.clone()),
            ProjectionExpr::new(new_terms_extractor, terms_project_name.clone()),
            ProjectionExpr::new(old_terms_extractor, old_terms_project_name.clone()),
        ];

        Ok(Self {
            project,
            index,
            index_project_name,
            terms_project_name,
            old_terms_project_name,
            input_schema: Arc::new(input_schema),
        })
    }
}

impl<const N: usize> IndexSink for InvertedIndexUpdateSink<N> {
    fn input(&self) -> &[ProjectionExpr] {
        &self.project
    }

    fn into_record_batch_sink(self: Arc<Self>) -> Arc<dyn RecordBatchSink> {
        self
    }
}
impl<const N: usize> DisplayAs for InvertedIndexUpdateSink<N> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "UpdateInvertedIndex")
    }
}

#[async_trait]
impl<const N: usize> RecordBatchSink for InvertedIndexUpdateSink<N> {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult> {
        for batch in data.into_iter() {
            let terms_column = extract_terms_column!(batch, &self.terms_project_name);
            let old_terms_column = extract_terms_column!(batch, &self.old_terms_project_name);

            let diff_array = terms_column.zip(old_terms_column).map(|(new, old)| {
                let added = new
                    .difference(&old)
                    .cloned()
                    .collect::<FxHashSet<RawIndexTermRef>>();
                let removed = old
                    .difference(&new)
                    .cloned()
                    .collect::<FxHashSet<RawIndexTermRef>>();

                (added, removed)
            });

            let index_column = extract_id_column!(batch, &self.index_project_name);

            let iterator = index_column.zip(diff_array);

            self.index
                .inner
                .update_many(Arc::clone(context), iterator)
                .await?;
        }

        Ok(DmlResult::empty())
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}
