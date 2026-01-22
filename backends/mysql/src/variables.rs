#![allow(clippy::disallowed_types)]

use crate::MySqlBackend;
use crate::errors::MySqlBackendError;
use mysql_async::Conn;
use mysql_async::prelude::{FromRow, Queryable};
use rustc_hash::FxHashMap;

#[derive(Debug, FromRow)]
#[mysql]
struct MySqlColumnDescription {
    #[mysql(rename = "Variable_name")]
    variable_name: String,
    #[mysql(rename = "Value")]
    value: String,
}

impl MySqlBackend {
    pub async fn get_session_variables(
        &self,
        conn: &mut Conn,
    ) -> Result<FxHashMap<String, String>, MySqlBackendError> {
        let res = conn
            .query::<MySqlColumnDescription, _>("SHOW SESSION VARIABLES")
            .await?;
        Ok(res
            .into_iter()
            .map(|desc| (desc.variable_name, desc.value))
            .collect())
    }
}
