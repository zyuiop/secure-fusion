use crypto::planning::physical::decrypt::DecryptUdf;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::{
    Expr as SqlExpr, FunctionArguments, ObjectNamePart,
};
use datafusion::logical_expr::{
    ColumnarValue, Expr as LogicalExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{
    FunctionArg, FunctionArgExpr, FunctionArgumentList, Ident, ObjectName, Query, SearchModifier,
    ValueWithSpan, VisitMut, VisitorMut,
};
use std::any::Any;
use std::iter::once;
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KwSearchUdf(Signature);

pub struct KwSearchArgs {
    pub search_string: String,
    pub search_mode: SearchMode,
    pub search_columns: Vec<Column>,
}

pub enum SearchMode {
    Boolean,
}

impl TryFrom<SearchModifier> for SearchMode {
    type Error = DataFusionError;

    fn try_from(value: SearchModifier) -> Result<Self, Self::Error> {
        match value {
            SearchModifier::InBooleanMode => Ok(Self::Boolean),
            other => Err(DataFusionError::NotImplemented(format!(
                "KW search not implemented for mode: {other}"
            ))),
        }
    }
}

impl TryFrom<&'_ str> for SearchMode {
    type Error = DataFusionError;

    fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
        match value {
            "BOOLEAN" => Ok(Self::Boolean),
            other => Err(DataFusionError::Plan(format!(
                "Invalid KW search mode: {other}"
            ))),
        }
    }
}

impl From<SearchMode> for String {
    fn from(value: SearchMode) -> Self {
        match value {
            SearchMode::Boolean => "BOOLEAN".to_string(),
        }
    }
}

impl KwSearchUdf {
    pub const KW_SEARCH_UDF_NAME: &str = "kw_search";

    pub fn new() -> Self {
        Self(Signature::variadic_any(Volatility::Stable))
    }

    pub fn split_args(args: &[LogicalExpr]) -> datafusion::common::Result<KwSearchArgs> {
        let [columns @ .., search_string, mode] = args else {
            plan_err!("Invalid KW_SEARCH arguments")?
        };

        if columns.is_empty() {
            plan_err!("Invalid KW_SEARCH arguments")?
        }

        let search_string = search_string
            .as_literal()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "KW_SEARCH second to last argument must be a pattern string".to_string(),
                )
            })?
            .try_as_str()
            .flatten()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "KW_SEARCH second to last argument must be a pattern string".to_string(),
                )
            })?
            .to_string();

        let search_mode = mode
            .as_literal()
            .ok_or_else(|| {
                DataFusionError::Plan("KW_SEARCH last argument must be a mode string".to_string())
            })?
            .try_as_str()
            .flatten()
            .ok_or_else(|| {
                DataFusionError::Plan("KW_SEARCH last argument must be a mode string".to_string())
            })?;

        let search_mode = SearchMode::try_from(search_mode)?;

        let search_columns: Vec<_> = columns
            .iter()
            .map(|expr| {
                DecryptUdf::eliminate_decrypt_in_expr(expr)?
                    .try_as_col()
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "KW_SEARCH first arguments must be columns".to_string(),
                        )
                    })
            })
            .collect::<Result<_, _>>()?;

        // Ensure all columns are in the same table
        search_columns.iter().try_fold(None, |acc, elem| {
            if let Some(table) = acc {
                if elem
                    .relation
                    .as_ref()
                    .is_none_or(|elem_table| elem_table == &table)
                {
                    Ok(Some(table))
                } else {
                    Err(DataFusionError::Plan(
                        "Multiple tables are not supported within a single KW_SEARCH call"
                            .to_string(),
                    ))
                }
            } else {
                Ok(elem.relation.clone())
            }
        })?;

        Ok(KwSearchArgs {
            search_mode,
            search_columns,
            search_string,
        })
    }
}

impl ScalarUDFImpl for KwSearchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::KW_SEARCH_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        /* Err(DataFusionError::Execution(
            "KW_SEARCH function should not reach execution phase".to_string(),
        ))*/

        // TODO: optimize away filters that contain this function (should not reach execution)
        // Quite easy to do: the table filter should report 100% success for that filter?
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
    }
}

pub fn register_kw_search(ctx: &mut Vec<Arc<ScalarUDF>>) {
    ctx.push(Arc::new(ScalarUDF::from(KwSearchUdf::new())))
}

pub fn rewrite_kw_search(query: &mut Query) {
    let _ = query.visit(&mut KwSearchReplacer);
}

struct KwSearchReplacer;

impl VisitorMut for KwSearchReplacer {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => {
                let search_modifier = opt_search_modifier
                    .as_ref()
                    .cloned()
                    .unwrap_or(SearchModifier::InBooleanMode);
                let Ok(search_modifier) = SearchMode::try_from(search_modifier) else {
                    return ControlFlow::Break(()); // Invalid!
                };
                let search_modifier: String = search_modifier.into();

                let args = columns
                    .iter()
                    .map(|on| {
                        let mut ident: Vec<Ident> =
                            on.0.iter()
                                .filter_map(|onp| onp.as_ident())
                                .cloned()
                                .collect();

                        if ident.len() == 1 {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(
                                ident.remove(0),
                            )))
                        } else {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                SqlExpr::CompoundIdentifier(ident),
                            ))
                        }
                    })
                    .chain(once(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        SqlExpr::Value(ValueWithSpan::from(match_value.clone())),
                    ))))
                    .chain(once(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        SqlExpr::Value(ValueWithSpan::from(ast::Value::SingleQuotedString(
                            search_modifier,
                        ))),
                    ))));

                *expr = SqlExpr::Function(ast::Function {
                    filter: None,
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                        KwSearchUdf::KW_SEARCH_UDF_NAME,
                    ))]),
                    parameters: FunctionArguments::None,
                    over: None,
                    uses_odbc_syntax: false,
                    within_group: vec![],
                    null_treatment: None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        clauses: vec![],
                        duplicate_treatment: None,
                        args: args.collect(),
                    }),
                })
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
