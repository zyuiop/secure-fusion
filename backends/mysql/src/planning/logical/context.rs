/*
pub(crate) struct ParserContext<'a> {
    resolved_tables: HashMap<TableReference, Arc<dyn TableSource>>,
    state: &'a SessionState,
}

impl<'a> ParserContext<'a> {
    fn resolve_reference(&self, reference: TableReference) -> TableReference {
        match reference {
            TableReference::Bare { table } => TableReference::Partial { table, schema: self.state.default_schema().into() },
            tp @ TableReference::Partial { .. } => tp,
            TableReference::Full { table, schema, .. } => TableReference::Partial { table, schema },
        }
    }
}


impl<'a> ContextProvider for ParserContext<'a> {
    fn get_table_source(&self, name: TableReference) -> datafusion::common::Result<Arc<dyn TableSource>> {
        let name = self.resolve_reference(name);
        self.resolved_tables.get(&name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        self.state.expr_planners()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {

    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        todo!()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        todo!()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        todo!()
    }

    fn options(&self) -> &ConfigOptions {
        todo!()
    }

    fn udf_names(&self) -> Vec<String> {
        todo!()
    }

    fn udaf_names(&self) -> Vec<String> {
        todo!()
    }

    fn udwf_names(&self) -> Vec<String> {
        todo!()
    }
}
 */
