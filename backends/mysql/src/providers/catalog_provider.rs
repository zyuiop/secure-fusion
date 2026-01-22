#![allow(clippy::disallowed_types)]

use crate::errors::{ColumnError, MySqlBackendError, MySqlBackendErrorInner};
use crate::metadata::{ColumnName, EncryptedSchemaMeta, EncryptedTableMeta};
use crate::providers::parser::{parse_column_type, parse_default_value_to_expr};
use crate::providers::schema_provider::MySqlSchemaProvider;
use crate::providers::table_provider::MySqlTableProvider;
use crate::store::MetadataStore;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::logical_expr::sqlparser::tokenizer::Token;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident};
use log::{info, trace};
use mysql_async::prelude::{FromRow, Queryable};
use mysql_async::{Conn, FromValueError, Value};
use rustc_hash::FxHashMap;
use std::any::Any;
use std::mem;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct MySqlCatalogProvider {
    databases: RwLock<FxHashMap<String, Arc<MySqlSchemaProvider>>>,
}

#[derive(Debug, FromRow)]
#[mysql(rename_all = "UPPERCASE")]
struct MySqlColumnDescription {
    table_schema: String,
    table_name: String,
    column_name: String,

    column_default: Option<String>,
    /// The base column type (e.g. "int")
    // data_type: String,
    /// The extended column type (e.g. "int unsigned")
    column_type: String,
    // character_maximum_length: Option<i32>,
    // character_octet_length: Option<String>,
    // numeric_precision: Option<String>,
    // numeric_scale: Option<String>,
    // datetime_precision: Option<String>,
    // character_set_name: Option<String>,
    // collation_name: Option<String>,
    column_key: String,

    extra: String,

    #[mysql(deserialize_with = "deser_boolean")]
    is_nullable: bool,
}

fn deser_boolean(v: Value) -> Result<bool, FromValueError> {
    if let Value::Bytes(b) = v {
        let str = String::from_utf8(b).map_err(|e| FromValueError(Value::Bytes(e.into_bytes())))?;
        if str == "NO" {
            Ok(false)
        } else if str == "YES" {
            Ok(true)
        } else {
            Err(FromValueError(Value::Bytes(str.into_bytes())))
        }
    } else {
        Err(FromValueError(v))
    }
}

impl TryFrom<MySqlColumnDescription> for ColumnDef {
    type Error = MySqlBackendError;

    fn try_from(value: MySqlColumnDescription) -> Result<Self, Self::Error> {
        let data_type = match parse_column_type(&value.column_type) {
            Err(error) => {
                return Err(Box::new(MySqlBackendErrorInner::ColumnError {
                    schema: value.table_schema,
                    table: value.table_name,
                    column: value.column_name,
                    error: ColumnError::CannotParseColumnType {
                        col_type: value.column_type,
                        error,
                    },
                }));
            }
            Ok(dt) => dt,
        };

        let mut options = vec![];
        if value.is_nullable {
            options.push(ColumnOptionDef {
                name: None,
                option: ColumnOption::Null,
            })
        }
        if value.column_key == "PRI" {
            options.push(ColumnOptionDef {
                name: None,
                option: ColumnOption::Unique {
                    is_primary: true,
                    characteristics: None,
                },
            })
        }
        if value.extra.contains("auto_increment") {
            options.push(ColumnOptionDef {
                name: None,
                option: ColumnOption::DialectSpecific(vec![Token::make_keyword("auto_increment")]),
            })
        }

        let column_default = value
            .column_default
            .as_ref()
            .filter(|s| !s.is_empty())
            .and_then(|expr| match parse_default_value_to_expr(expr, &data_type) {
                Err(error) => {
                    log::warn!(
                        "Failed to parse the default value for column {} in table {}.{}: {:?}",
                        &value.column_name,
                        &value.table_schema,
                        &value.table_name,
                        error
                    );
                    None
                }
                Ok(dt) => Some(dt),
            });

        if let Some(default_value) = column_default {
            options.push(ColumnOptionDef {
                name: None,
                option: ColumnOption::Default(default_value),
            })
        }

        Ok(ColumnDef {
            name: Ident::new(value.column_name),
            data_type,
            options,
        })
    }
}

struct TableBuilder {
    table_schema: String,
    table_name: String,
    columns: Vec<ColumnDef>,
    primary_key_members: Vec<usize>, // TODO: check relevance
    // defaults: HashMap<String, Expr>,
    encryption_metadata: Option<EncryptedTableMeta>,
}

impl TableBuilder {
    fn new(
        table_schema: String,
        table_name: String,
        encryption_metadata: Option<EncryptedTableMeta>,
    ) -> Self {
        trace!("New table builder {table_schema}:{table_name}, {encryption_metadata:?}");

        Self {
            table_schema,
            table_name,
            columns: Vec::new(),
            primary_key_members: Vec::new(),
            // defaults: HashMap::new(),
            encryption_metadata,
        }
    }

    fn is_empty(&self) -> bool {
        self.table_name.is_empty() && self.columns.is_empty()
    }

    #[inline]
    fn is_same_table(&self, column: &MySqlColumnDescription) -> bool {
        self.table_name == column.table_name && self.is_same_schema(column)
    }

    #[inline]
    fn is_same_schema(&self, column: &MySqlColumnDescription) -> bool {
        self.table_schema == column.table_schema
    }

    fn add_column(&mut self, column: MySqlColumnDescription) -> Result<(), MySqlBackendError> {
        if column.column_key == "PRI" {
            self.primary_key_members.push(self.columns.len());
        }

        self.columns.push(column.try_into()?);
        Ok(())
    }

    fn build(self) -> Arc<MySqlTableProvider> {
        let primary_key = self
            .primary_key_members
            .into_iter()
            .map(|index| self.columns[index].name.value.clone())
            .collect::<Vec<ColumnName>>();

        Arc::new(MySqlTableProvider::from_columns(
            TableReference::partial(self.table_schema.clone(), self.table_name.clone()),
            self.columns,
            primary_key,
            self.encryption_metadata,
        ))
    }
}

impl MySqlCatalogProvider {
    // const EXCLUDED_SCHEMAS: &str = "'information_schema', 'mysql', 'performance_schema', 'sys'";
    // Exposing information_schema and mysql directly is misleading, but easier at this point
    // Later on we can implement shims in the frontend?
    const EXCLUDED_SCHEMAS: &str = "'performance_schema', 'sys'";

    pub(crate) async fn introspect(
        mut conn: Conn,
        metadata_store: &MetadataStore,
        ident_normalization: bool,
    ) -> Result<Arc<MySqlCatalogProvider>, MySqlBackendError> {
        info!("Begin introspection of all databases on server...");

        let schemas = conn
            .query::<String, _>(format!(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ({})",
                Self::EXCLUDED_SCHEMAS
            ))
            .await?;

        let mut encryption_metas: FxHashMap<String, EncryptedSchemaMeta> = FxHashMap::default();
        for schema in schemas.iter() {
            let metadata = metadata_store.read_metadata(schema).await?;
            encryption_metas.insert(schema.clone(), metadata.deserialize());
        }

        let mut schemas = schemas
            .into_iter()
            .map(|name| (name, MySqlSchemaProvider::new(FxHashMap::default())))
            .collect::<FxHashMap<_, _>>();

        let results = conn.query::<MySqlColumnDescription, _>(format!("
            SELECT table_schema, table_name, column_name, column_default, data_type, column_type, character_octet_length,
                numeric_precision, numeric_scale, datetime_precision, character_set_name, collation_name, column_key, extra, is_nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema NOT IN ({})
            ORDER BY table_schema, table_name, ordinal_position", Self::EXCLUDED_SCHEMAS)).await?;

        let mut builder = TableBuilder::new(String::new(), String::new(), None); // initial instance will get ignored

        let mut insert_schema = |table_builder: TableBuilder| {
            schemas
                .entry(table_builder.table_schema.clone())
                .or_default()
                .register_mysql_table(table_builder.table_name.clone(), table_builder.build())
                .map_err(|e| Box::new(MySqlBackendErrorInner::IntrospectionError(e)))?;

            Ok::<(), MySqlBackendError>(())
        };

        for mut col_desc in results {
            if ident_normalization {
                col_desc.column_name = col_desc.column_name.to_lowercase();
                col_desc.table_name = col_desc.table_name.to_lowercase();
            }

            let encryption_meta = encryption_metas.get(&col_desc.table_schema);

            if !builder.is_same_table(&col_desc) {
                let new_builder = TableBuilder::new(
                    col_desc.table_schema.clone(),
                    col_desc.table_name.clone(),
                    encryption_meta
                        .and_then(|meta| meta.table(&col_desc.table_name))
                        .cloned(),
                );
                let old_builder = mem::replace(&mut builder, new_builder);

                if !old_builder.is_empty() {
                    info!(
                        "Found table {}:{}",
                        &old_builder.table_schema, &old_builder.table_name
                    );

                    insert_schema(old_builder)?;
                }
            }

            builder.add_column(col_desc)?;
        }

        // Add the last column
        if !builder.is_empty() {
            info!(
                "Found table {}:{}",
                &builder.table_schema, &builder.table_name
            );

            insert_schema(builder)?;
        }

        // Check that mandatory indexing columns are present
        // TODO: maybe we want to do this optimistically later?

        // TODO: for views, https://dev.mysql.com/doc/refman/8.4/en/information-schema-views-table.html
        info!("Introspection complete");

        Ok(Arc::new(Self {
            databases: RwLock::new(schemas),
        }))
    }

    pub fn mysql_schema(&self, name: &str) -> Option<Arc<MySqlSchemaProvider>> {
        let rw_lock = self
            .databases
            .try_read()
            .expect("Failed to lock database, locked for writing?");
        rw_lock.get(name).cloned()
    }

    pub fn register_schema(&self, name: String, schema: Arc<MySqlSchemaProvider>) {
        let mut rw_lock = self
            .databases
            .try_write()
            .expect("Failed to lock database, locked for writing?");
        rw_lock.insert(name, schema);
    }
}

impl CatalogProvider for MySqlCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let rw_lock = self
            .databases
            .try_read()
            .expect("Failed to lock database, locked for writing?");
        rw_lock.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.mysql_schema(name).map(|arc| arc as _)
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }

    fn deregister_schema(
        &self,
        name: &str,
        _cascade: bool,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        let mut rw_lock = self
            .databases
            .try_write()
            .expect("Failed to lock database, locked for writing?");
        rw_lock.remove(name);
        Ok(None)
    }
}
