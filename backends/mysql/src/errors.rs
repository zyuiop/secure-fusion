use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use std::fmt::{Display, Formatter};
use toml::ser::Error;

#[derive(Debug)]
pub enum ColumnError {
    UnknownColumnType {
        col_type: String,
        extended_type: String,
    },
    BadlySpecifiedColumnType {
        col_type: String,
        error: String,
    },
    CannotTokenizeColumnType {
        col_type: String,
        error: ParserError,
    },
    CannotParseColumnType {
        col_type: String,
        error: ParserError,
    },
    CannotParseDefault {
        default: String,
        error: ParserError,
    },
}

impl Display for ColumnError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            ColumnError::UnknownColumnType {
                col_type,
                extended_type,
            } => write!(f, "unknown column type '{col_type}' ('{extended_type}')"),
            ColumnError::BadlySpecifiedColumnType { col_type, error } => {
                write!(f, "column type '{col_type}' is badly specified ({error})")
            }
            ColumnError::CannotTokenizeColumnType { col_type, error } => {
                write!(
                    f,
                    "column type '{col_type}' cannot be tokenized ({error:?})"
                )
            }
            ColumnError::CannotParseColumnType { col_type, error } => {
                write!(f, "column type '{col_type}' cannot be parsed ({error:?})")
            }
            ColumnError::CannotParseDefault { default, error } => {
                write!(f, "column default '{default}' cannot be parsed ({error:?})")
            }
        }
    }
}

pub type MySqlBackendError = Box<MySqlBackendErrorInner>;

#[derive(Debug)]
pub enum MySqlBackendErrorInner {
    PhysicalPlanningError(String),
    ColumnError {
        schema: String,
        table: String,
        column: String,
        error: ColumnError,
    },
    DriverError(mysql_async::Error),
    IntrospectionError(DataFusionError),
    ConfigErrog(toml::ser::Error),
}

impl From<mysql_async::Error> for MySqlBackendError {
    fn from(value: mysql_async::Error) -> Self {
        Box::new(MySqlBackendErrorInner::DriverError(value))
    }
}

impl From<toml::ser::Error> for MySqlBackendError {
    fn from(value: Error) -> Self {
        Box::new(MySqlBackendErrorInner::ConfigErrog(value))
    }
}

impl Display for MySqlBackendErrorInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::PhysicalPlanningError(s) => {
                write!(f, "MySQL - Physical Planning Error: {s}")
            }
            Self::ColumnError {
                schema,
                table,
                column,
                error,
            } => write!(
                f,
                "MySQL - Schema error for column {column} of table {schema}.{table}: {error}"
            ),
            Self::DriverError(e) => write!(f, "MySQL Driver error: {e}"),
            Self::IntrospectionError(e) => write!(f, "MySQL Introspection error: {e}"),
            Self::ConfigErrog(e) => write!(f, "Configuration error: {e}"),
        }
    }
}

impl std::error::Error for MySqlBackendErrorInner {}

pub type MySqlResult<T> = Result<T, MySqlBackendError>;

impl From<MySqlBackendError> for datafusion::error::DataFusionError {
    fn from(value: MySqlBackendError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
