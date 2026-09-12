use crate::planning::logical::DUPLICATE_VALUE_PFX;
use crate::sinks::mysql_dml_sink::MySqlDmlSink;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ResolvedTableReference;
use datafusion::logical_expr::WriteOp;
use datafusion::logical_expr::dml::InsertOp;

impl MySqlDmlSink {
    pub(crate) fn insert(
        target_table: ResolvedTableReference,
        insert_op: InsertOp,
        insert_schema: SchemaRef,
    ) -> Self {
        let verb = match insert_op {
            InsertOp::Append => "INSERT INTO",
            InsertOp::Overwrite => unimplemented!(),
            InsertOp::Replace => unimplemented!(),
        };

        // Separate "ON DUPLICATE KEY SET ..." columns from regular columns
        let (duplicate_updates, columns): (Vec<_>, Vec<_>) = insert_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(pos, field)| (field.name().clone(), pos))
            .partition(|(field_name, _)| field_name.starts_with(DUPLICATE_VALUE_PFX));

        // We need the column positions as nothing guarantees that the ON DUPLICATE ... columns are put at the end of the schema!
        let (columns, mut columns_positions): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let (duplicate_updates, mut duplicate_updates_positions): (Vec<_>, Vec<_>) =
            duplicate_updates.into_iter().unzip();
        columns_positions.append(&mut duplicate_updates_positions);

        // Build the SET part of the query
        // Since we're using MySQL we can use the INSERT INTO <table> SET k = v syntax.
        let insert_set = columns
            .into_iter()
            .map(|col| format!("{col}=?"))
            .collect::<Vec<_>>()
            .join(",");

        let on_duplicate_set = duplicate_updates
            .into_iter()
            .map(|col| format!("{}=?", col.strip_prefix(DUPLICATE_VALUE_PFX).unwrap()))
            .collect::<Vec<_>>()
            .join(",");

        let on_duplicate = if on_duplicate_set.is_empty() {
            String::new()
        } else {
            format!(" ON DUPLICATE KEY UPDATE {on_duplicate_set}")
        };

        let query = format!(
            r#"{verb} {}.{} SET {insert_set}{on_duplicate}"#,
            target_table.schema, target_table.table
        );
        log::trace!("Insert query: {query}");

        Self {
            operation: WriteOp::Insert(insert_op),
            expected_input_schema: insert_schema,
            query_string: query,
            columns_projection: columns_positions,
            report_last_insert: true,
        }
    }
}
