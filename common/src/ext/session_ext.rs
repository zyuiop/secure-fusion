use async_trait::async_trait;
use datafusion::prelude::SessionContext;

#[async_trait]
pub trait SessionExt {
    fn get_current_schema(&self) -> String;

    fn list_tables(&self, schema: &str) -> Vec<String>;
}

#[async_trait]
impl SessionExt for &SessionContext {
    fn get_current_schema(&self) -> String {
        self.state().config_options().catalog.default_schema.clone()
    }

    fn list_tables(&self, database: &str) -> Vec<String> {
        let state = self.state();
        let catalog = &state.config_options().catalog.default_catalog;
        let Some(catalog) = self.catalog(catalog) else {
            return vec![];
        };
        let Some(schema) = catalog.schema(database) else {
            return vec![];
        };

        schema.table_names()
    }

    /*
    fn get_variables(&self, variable_types: &[VarType]) -> HashMap<String, String> {
        if variable_types.is_empty() {
            return HashMap::new();
        }

        let state = self.state();

        for vt in variable_types {
            let Some(var_provider) = state.execution_props().get_var_provider(*vt) else { continue; };
            var_provider.
        }
    }
     */
}
