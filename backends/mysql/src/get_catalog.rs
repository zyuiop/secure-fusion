use crate::MySqlCatalogProviderList;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use crate::providers::table_provider::MySqlTableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion::logical_expr::TableSource;
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use std::sync::Arc;

pub trait CatalogGetter {
    fn try_get_catalog(&self) -> Option<Arc<MySqlCatalogProvider>>;

    fn default_schema(&self) -> &str;

    fn get_catalog(&self) -> Arc<MySqlCatalogProvider> {
        self.try_get_catalog().expect("server runtime state is invalid: catalog list is not an instance of MySqlCatalogProviderList")
    }

    fn mysql_schema_for_ref(
        &self,
        table_reference: &TableReference,
    ) -> Option<Arc<MySqlTableProvider>> {
        let schema = self
            .get_catalog()
            .mysql_schema(table_reference.schema().unwrap_or(self.default_schema()))?;

        schema.mysql_table(table_reference.table())
    }
}

impl CatalogGetter for SessionState {
    fn try_get_catalog(&self) -> Option<Arc<MySqlCatalogProvider>> {
        let catalog: &MySqlCatalogProviderList = self.catalog_list().as_any().downcast_ref()?;

        Some(catalog.default_catalog())
    }

    fn default_schema(&self) -> &str {
        &self.config_options().catalog.default_schema
    }
}

impl CatalogGetter for SessionConfig {
    fn try_get_catalog(&self) -> Option<Arc<MySqlCatalogProvider>> {
        let catalog: Arc<MySqlCatalogProviderList> = self.get_extension()?;

        Some(catalog.default_catalog())
    }

    fn default_schema(&self) -> &str {
        &self.options().catalog.default_schema
    }
}

pub trait TableGetter {
    fn as_mysql_opt(&self) -> Option<&MySqlTableProvider>;

    #[allow(unused)]
    fn as_mysql(&self) -> &MySqlTableProvider {
        self.as_mysql_opt().unwrap()
    }
}

impl TableGetter for DefaultTableSource {
    fn as_mysql_opt(&self) -> Option<&MySqlTableProvider> {
        self.table_provider.as_any().downcast_ref()
    }
}

impl TableGetter for Arc<dyn TableSource> {
    fn as_mysql_opt(&self) -> Option<&MySqlTableProvider> {
        let myself: &DefaultTableSource = self.as_any().downcast_ref()?;
        myself.as_mysql_opt()
    }
}
