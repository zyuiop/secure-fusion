use crate::planning::logical::{CURRENT_VALUE_PREFIX, FILTER_PREFIX};
use crate::sinks::mysql_dml_sink::MySqlDmlSink;
use datafusion::arrow::datatypes::FieldRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::WriteOp;
use datafusion::sql::ResolvedTableReference;

impl MySqlDmlSink {
    fn build_query_string_and_projection(
        target_table: ResolvedTableReference,
        filters: Vec<&FieldRef>,
        updates: Vec<&FieldRef>,
        source_schema: &SchemaRef,
    ) -> (String, Vec<usize>) {
        let update = updates
            .iter()
            .map(|field| format!("{} = ?", field.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let filter = filters
            .iter()
            .map(|field| {
                let (_, suffix) = field.name().split_at(FILTER_PREFIX.len());
                format!("{suffix} = ?")
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let query = format!(
            r#"UPDATE {}.{} SET {update} WHERE {filter}"#,
            target_table.schema, target_table.table
        );

        let projection = updates
            .iter()
            .chain(filters.iter())
            .map(|field| source_schema.index_of(&field.name()).unwrap())
            .collect();

        (query, projection)
    }

    pub(crate) fn update(
        target_table: ResolvedTableReference,
        update_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let (filters, updates) = update_schema
            .fields
            .iter()
            .filter(|field| !field.name().starts_with(CURRENT_VALUE_PREFIX))
            .partition::<Vec<_>, _>(|field| field.name().starts_with(FILTER_PREFIX));

        let (query_string, columns_projection) =
            Self::build_query_string_and_projection(target_table, filters, updates, &update_schema);

        Ok(Self {
            operation: WriteOp::Update,
            query_string,
            columns_projection,
            expected_input_schema: update_schema,
            report_last_insert: false,
        })
    }
}
