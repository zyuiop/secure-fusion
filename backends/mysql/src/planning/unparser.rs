use datafusion::common::ResolvedTableReference;
use datafusion::logical_expr::sqlparser::ast::ObjectName;
use datafusion::logical_expr::sqlparser::tokenizer::Span;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::Ident;

fn quote_ident(ident: String) -> Ident {
    // let quote_style = MySqlDialect{}.identifier_quote_style(&ident);
    Ident {
        value: ident,
        quote_style: Some('`'),
        span: Span::empty(),
    }
}

pub fn object_name_from_resolved(table_ref: &ResolvedTableReference) -> ObjectName {
    let table_parts = vec![
        quote_ident(table_ref.schema.to_string()),
        quote_ident(table_ref.table.to_string()),
    ];

    ObjectName::from(table_parts)
}

pub fn object_name_matches_resolved(
    object_name: &ObjectName,
    table_ref: &ResolvedTableReference,
) -> bool {
    match &object_name.0[..] {
        [.., schema, table] => {
            schema.as_ident().unwrap().value == table_ref.schema.as_ref()
                && table.as_ident().unwrap().value == table_ref.table.as_ref()
        }
        [table] => table.as_ident().unwrap().value == table_ref.table.as_ref(),
        _ => false,
    }
}

pub fn table_name_to_ref(object_name: &ObjectName) -> TableReference {
    match &object_name.0[..] {
        [catalog, schema, table] => TableReference::Full {
            catalog: catalog.as_ident().unwrap().value.clone().into(),
            schema: schema.as_ident().unwrap().value.clone().into(),
            table: table.as_ident().unwrap().value.clone().into(),
        },
        [schema, table] => TableReference::Partial {
            schema: schema.as_ident().unwrap().value.clone().into(),
            table: table.as_ident().unwrap().value.clone().into(),
        },
        [table] => TableReference::Bare {
            table: table.as_ident().unwrap().value.clone().into(),
        },
        _ => panic!("invalid object name"),
    }
}
