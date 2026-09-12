use crate::ast_expr_ext::AstExprExt;
use crate::filtering::indexable_filter::{IndexSelectivity, IndexableFilterExpr, SupportOptions};
use crate::filtering::logical::IndexableLogicalExpr;
use crate::get_catalog::CatalogGetter;
use crate::metadata::indices::inverted_index::db_inverted_index::{
    CreateDbInvertedIndexPlan, IndexedDocumentExtensions, IndexedDocumentId, InvertedIndexGetter,
    RawIndexQuery, RawIndexTermRef, RawInvertedIndex,
};
use crate::metadata::{
    ColumnName, DynamicFilter, EncryptedIndex, EncryptedIndexConfigurationVariant, IndexConfig,
    IndexInsertStrategy, IndexQueryStrategy, IndexSink, SerializableEncryptedTableMeta,
};
use crate::planning::logical::{CURRENT_VALUE_PREFIX, FILTER_PREFIX};
use crate::providers::schema_provider::MySqlSchemaProvider;
use crate::providers::table_provider::MySqlTableProvider;
use crate::sinks::sink::RecordBatchSink;
use crate::store::StoreGetter;
use async_trait::async_trait;
use bitflags::bitflags;
use common::dml::DmlResult;
use crypto::LongTermKeyManager;
use crypto::planning::physical::to_binary::ToBinaryExpr;
use crypto::row_id::{RowIdColumn, RowIdColumnSize};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericByteBuilder, GenericListBuilder, OffsetSizeTrait,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::GenericBinaryType;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{
    DataFusionError, ResolvedTableReference, ScalarValue, exec_datafusion_err, exec_err,
    plan_datafusion_err, plan_err,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::functions_nested::concat::ArrayConcat;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Ident, Value};
use futures_util::{TryStreamExt, stream};
use log::{trace, warn};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
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
    /// TODO: Ignored for now
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

    /// Checks if a tree "leaf" is supported by this filter
    fn is_supported<T: Clone + Debug>(
        &self,
        tree: &IndexableFilterExpr<T>,
    ) -> Option<IndexSelectivity> {
        match tree {
            IndexableFilterExpr::KwSearchLike(column, _)
                if self.tracked_columns().contains(column.column.name()) =>
            {
                Some(IndexSelectivity::Absent)
            }
            IndexableFilterExpr::KwMatch { columns, .. }
                if columns
                    .iter()
                    .all(|column| self.tracked_columns().contains(column.name())) =>
            {
                Some(IndexSelectivity::Absent)
            }
            _ => None,
        }
    }

    /// Converts an entirely supported tree to an executable filter tree
    fn parse_filter_tree<T: Clone + Debug>(
        &self,
        tree: IndexableFilterExpr<T>,
    ) -> datafusion::common::Result<ExecutableFilterTree> {
        match tree {
            IndexableFilterExpr::And(a, b) => {
                Ok(self.parse_filter_tree(*a)?.and(self.parse_filter_tree(*b)?))
            }
            IndexableFilterExpr::Or(a, b) => {
                Ok(self.parse_filter_tree(*a)?.or(self.parse_filter_tree(*b)?))
            }
            IndexableFilterExpr::Not(other) => {
                // TODO: when we support boolean expressions again
                /*
                FilterTree::BooleanExpr { if_false, .. } => {
                    ExecutableFilterTree::FilterExpr(RawIndexQuery::Term(if_false))
                }
                 */
                Ok(ExecutableFilterTree::Not(Box::new(
                    self.parse_filter_tree(*other)?,
                )))
            }
            IndexableFilterExpr::KwSearchLike(column, pattern) => {
                if !self.tracked_columns().contains(column.column.name()) {
                    plan_err!("unsupported column for inverted index: {column:?}")?
                };

                let pattern = pattern.replace(['%'], " "); // TODO: better variant here, any special symbol should be excluded
                let pattern = self
                    .parse_like_string(&pattern)
                    .into_iter()
                    .map(|term| RawIndexQuery::Term(term))
                    .reduce(|l, r| RawIndexQuery::Intersect(Box::new(l), Box::new(r)))
                    .ok_or(plan_datafusion_err!(
                        "invalid LIKE expression with no keyword {pattern}"
                    ))?;

                Ok(ExecutableFilterTree::FilterExpr(pattern))
            }
            IndexableFilterExpr::KwMatch {
                columns,
                search_string,
            } => {
                if let Some(unsupported_column) = columns
                    .iter()
                    .find(|column| !self.tracked_columns().contains(column.name()))
                {
                    plan_err!("unsupported column for inverted index: {unsupported_column:?}")?
                };

                let search_string = if self.collation.contains(Collation::CASE_INSENSITIVE) {
                    search_string.to_lowercase()
                } else {
                    search_string
                };

                /*
                A leading or trailing plus sign indicates that this word must be present in each row that is returned. InnoDB only supports leading plus signs.
                A leading or trailing minus sign indicates that this word must not be present in any of the rows that are returned. InnoDB only supports leading minus signs.
                Note: The - operator acts only to exclude rows that are otherwise matched by other search terms. Thus, a boolean-mode search that contains only terms preceded by - returns an empty result. It does not return “all rows except those containing any of the excluded terms.”
                 */
                let mut excluded = None;
                let mut must_be_included = None;
                let mut can_be_included = None;

                for keyword in search_string.trim().split(' ') {
                    if let Some(str) = keyword.strip_prefix('-') {
                        let term = RawIndexQuery::Term(Self::keyword_to_term(str));
                        excluded = if let Some(initial) = excluded.take() {
                            Some(RawIndexQuery::Intersect(Box::new(initial), Box::new(term)))
                        } else {
                            Some(term)
                        }
                    } else if let Some(str) = keyword.strip_prefix('+') {
                        let term = RawIndexQuery::Term(Self::keyword_to_term(str));
                        must_be_included = if let Some(initial) = must_be_included.take() {
                            Some(RawIndexQuery::Intersect(Box::new(initial), Box::new(term)))
                        } else {
                            Some(term)
                        }
                    } else {
                        let term = RawIndexQuery::Term(Self::keyword_to_term(keyword));
                        can_be_included = if let Some(initial) = can_be_included.take() {
                            Some(RawIndexQuery::Union(Box::new(initial), Box::new(term)))
                        } else {
                            Some(term)
                        }
                    }
                }

                if must_be_included.is_none() && can_be_included.is_none() {
                    // We must have AT LEAST one keyword
                    plan_err!(
                        "MATCH ... AGAINST ... must have at least one mandatory (+) or optional ( ) keyword"
                    )?
                }

                let base = if let Some(must_be_included) = must_be_included {
                    if let Some(can_be_included) = can_be_included {
                        // This is not exactly correct but that will do...
                        RawIndexQuery::Intersect(
                            Box::new(must_be_included),
                            Box::new(can_be_included),
                        )
                    } else {
                        must_be_included
                    }
                } else {
                    can_be_included.expect(
                        "unreachable: given previous twoconditions, can_be_included is defined",
                    )
                };

                Ok(ExecutableFilterTree::FilterExpr(
                    if let Some(excluded) = excluded {
                        RawIndexQuery::Difference(Box::new(base), Box::new(excluded))
                    } else {
                        base
                    },
                ))
            }
            unsupported => plan_err!("unsupported plan node for inverted index: {unsupported:?}"),
        }
    }

    pub fn query<T: Clone + Debug>(
        self: Arc<Self>,
        indexable_column: &RowIdColumn,
        supported_filter: IndexableFilterExpr<T>,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        trace!("Query index with expressions: {:?}", supported_filter);
        let executable = self.parse_filter_tree(supported_filter)?.simplify();
        let replacer = InvertedIndexExec::<KeySize> {
            query: executable,
            id_column: indexable_column.field(),
            index: self.inner.clone(),
        };
        Ok(IndexQueryStrategy::Dynamic(Arc::new(replacer)))
    }

    fn parse_like_string(&self, string: &str) -> Vec<RawIndexTermRef> {
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

struct InvertedIndexExec<const KS: usize> {
    query: ExecutableFilterTree,
    id_column: Field,
    index: Arc<RawInvertedIndex<KS>>,
}

impl<const KS: usize> Debug for InvertedIndexExec<KS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InvertedIndexExec {{ query: {:?}, id_column: {:?}, index: [abridged] }}",
            self.query, self.id_column
        )
    }
}

#[async_trait]
impl<const KS: usize> DynamicFilter for InvertedIndexExec<KS> {
    async fn execute_filter(
        &self,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<ast::Expr> {
        let mut queries = vec![];
        self.query.extract_queries(&mut queries);

        let mut results = self.index.query(context.clone(), queries).await?;

        self.query.to_expr(&self.id_column, &mut results)
    }
}

#[derive(Debug)]
enum ExecutableFilterTree {
    And(Box<ExecutableFilterTree>, Box<ExecutableFilterTree>),
    Or(Box<ExecutableFilterTree>, Box<ExecutableFilterTree>),

    Not(Box<ExecutableFilterTree>),

    FilterExpr(RawIndexQuery),
}

impl ExecutableFilterTree {
    pub fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    pub fn extract_queries(&self, queries: &mut Vec<RawIndexQuery>) {
        match self {
            ExecutableFilterTree::And(a, b) | ExecutableFilterTree::Or(a, b) => {
                a.extract_queries(queries);
                b.extract_queries(queries);
            }
            ExecutableFilterTree::Not(a) => {
                a.extract_queries(queries);
            }
            ExecutableFilterTree::FilterExpr(f) => {
                queries.push(f.clone());
            }
        }
    }

    pub fn to_expr<const KS: usize>(
        &self,
        id_column: &Field,
        queries_results: &mut Vec<FxHashSet<IndexedDocumentId<KS>>>,
    ) -> datafusion::common::Result<ast::Expr> {
        match self {
            ExecutableFilterTree::And(a, b) => Ok(a
                .to_expr(id_column, queries_results)?
                .and(b.to_expr(id_column, queries_results)?)),
            ExecutableFilterTree::Or(a, b) => Ok(a
                .to_expr(id_column, queries_results)?
                .or(b.to_expr(id_column, queries_results)?)),
            ExecutableFilterTree::Not(a) => Ok(a.to_expr(id_column, queries_results)?.not()),
            ExecutableFilterTree::FilterExpr(_) => {
                let query_result = queries_results.pop().ok_or(exec_datafusion_err!(
                    "not enough queries results in Inverted Index!"
                ))?;
                let value_is_number = matches!(
                    id_column.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                );

                let mut values: Vec<ast::Expr> = query_result
                    .into_iter()
                    .map(|v| {
                        ast::Expr::Value(
                            if value_is_number {
                                v.as_number_value().unwrap()
                            } else {
                                Value::HexStringLiteral(hex::encode(v))
                            }
                            .into(),
                        )
                    })
                    .collect();

                if values.len() == 0 {
                    Ok(ast::Expr::Value(Value::Boolean(false).into()))
                } else if values.len() == 1 {
                    let result = values.pop().unwrap();
                    Ok(ast::Expr::BinaryOp {
                        op: BinaryOperator::Eq,
                        left: Box::new(ast::Expr::Identifier(Ident::new(id_column.name()))),
                        right: Box::new(result),
                    })
                } else {
                    Ok(ast::Expr::InList {
                        expr: Box::new(ast::Expr::Identifier(Ident::new(id_column.name()))),
                        negated: false,
                        list: values,
                    })
                }
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
                            // TODO: this branch should never be reached?
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
                if let ExecutableFilterTree::Not(a) = a {
                    *a // simplify double not
                } else {
                    ExecutableFilterTree::Not(Box::new(a))
                }
            }
            leaf => leaf,
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

    fn is_column_hidden(&self, _column: &ColumnName) -> bool {
        false
    }

    fn tracked_columns(&self) -> FxHashSet<ColumnName> {
        InvertedIndex::<N>::tracked_columns(self).clone()
    }

    async fn create_index_plan(
        self: Arc<Self>,
        parent_table_ref: &ResolvedTableReference,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = CreateInvertedIndexPlan::<N>::new(
            session_state,
            self,
            session_state
                .get_catalog()
                .mysql_schema(&parent_table_ref.schema)
                .unwrap(),
            parent_table_ref.clone(),
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

    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn logical_support_options<'s, 'b>(&'s self) -> SupportOptions<'s, &'b Expr> {
        SupportOptions {
            supports_not: true,
            supports_or: true,
            is_supported: Box::new(|v, _| self.is_supported(v)),
        }
    }

    fn logical_query(
        self: Arc<Self>,
        _table_reference: &ResolvedTableReference,
        _indexable_column: Option<&RowIdColumn>,
        _key_manager: &Arc<LongTermKeyManager>,
        _filter: IndexableLogicalExpr,
    ) -> datafusion::common::Result<IndexQueryStrategy> {
        self.query(
            _indexable_column.ok_or(plan_datafusion_err!(
                "missing indexable column on table (required by inverted index query)"
            ))?,
            _filter,
        )
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
        table_name: &ResolvedTableReference,
        indexable_column: Option<&RowIdColumn>,
    ) -> Arc<dyn EncryptedIndex> {
        match indexable_column
            .expect("failed to parse inverted index: row_id_column is not defined")
            .size()
        {
            RowIdColumnSize::Size4Bytes => self.into_sized_index::<4>(table_name),
            RowIdColumnSize::Size8Bytes => self.into_sized_index::<8>(table_name),
            RowIdColumnSize::Size12Bytes => self.into_sized_index::<12>(table_name),
            RowIdColumnSize::Size16Bytes => self.into_sized_index::<16>(table_name),
        }
    }
}

impl InvertedIndexConfig {
    fn into_sized_index<const N: usize>(
        self,
        table_name: &ResolvedTableReference,
    ) -> Arc<dyn EncryptedIndex> {
        let inner = RawInvertedIndex::<N>::new(table_name.clone(), self.index_name.clone());
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
    properties: Arc<PlanProperties>,
}

impl<const IdSize: usize> CreateInvertedIndexPlan<IdSize> {
    async fn new(
        session: &dyn Session,
        index: Arc<InvertedIndex<IdSize>>,
        schema_provider: Arc<MySqlSchemaProvider>,
        table: ResolvedTableReference,
    ) -> datafusion::common::Result<Self> {
        let table_provider = schema_provider
            .mysql_table(&table.table)
            .ok_or_else(|| plan_datafusion_err!("table not found: {}", table))?;
        let indexable_column = table_provider.try_get_row_id_column()?;
        let schema = table_provider.schema();

        let tracked_columns = index
            .all_tracked_columns
            .iter()
            .filter_map(|col| schema.field_with_name(col).ok().cloned());

        let fetched_columns: Vec<Field> = iter::once(indexable_column.field())
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
            table.clone(),
            index.inner.index_name().clone(),
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
                for sub_value in self.parent_index.parse_like_string(v) {
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
        let id_extractor = table
            .try_get_row_id_column()?
            .rowid_bin_physical(input_schema.as_ref())?;

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
        let row_id = table
            .get_row_id_column()
            .ok_or_else(|| plan_datafusion_err!("table must have an indexable column"))?;

        if row_id.is_hidden() {
            row_id.rowid_bin_physical_prefixed(CURRENT_VALUE_PREFIX, input_schema.as_ref())
        } else {
            // The column is the primary key and should therefore be present as a filter
            row_id.rowid_bin_physical_prefixed(FILTER_PREFIX, input_schema.as_ref())
        }
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
