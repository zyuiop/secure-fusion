use bincode::error::{DecodeError, EncodeError};
use crypto::error::CryptoError;
use datafusion::error::DataFusionError;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum IndexError {
    /// CryptoErrors are opaque
    CryptoError,

    DecodeError(DecodeError),
    EncodeError(EncodeError),

    DatabaseError(mysql_async::Error),

    DataFusionError(DataFusionError),

    UnknownError(String),
}

impl Display for IndexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::CryptoError => f.write_str("IndexError: cryptographic error"),
            IndexError::DecodeError(err) => write!(f, "IndexError: failed to decode page {}", err),
            IndexError::EncodeError(err) => write!(f, "IndexError: failed to encode page {}", err),
            IndexError::DatabaseError(err) => {
                write!(f, "IndexError: failed to perform database query {:?}", err)
            }
            IndexError::UnknownError(other) => write!(f, "IndexError: unknown error {}", other),
            IndexError::DataFusionError(df) => write!(f, "IndexError: datafusion error {:?}", df),
        }
    }
}

impl From<IndexError> for DataFusionError {
    fn from(value: IndexError) -> Self {
        match value {
            IndexError::DataFusionError(df) => df,
            value => DataFusionError::Execution(value.to_string()),
        }
    }
}
pub type IndexResult<T> = Result<T, IndexError>;

impl From<DecodeError> for IndexError {
    fn from(e: DecodeError) -> IndexError {
        IndexError::DecodeError(e)
    }
}

impl From<EncodeError> for IndexError {
    fn from(e: EncodeError) -> IndexError {
        IndexError::EncodeError(e)
    }
}

impl From<DataFusionError> for IndexError {
    fn from(e: DataFusionError) -> IndexError {
        IndexError::DataFusionError(e)
    }
}

impl From<CryptoError> for IndexError {
    fn from(_: CryptoError) -> IndexError {
        IndexError::CryptoError
    }
}

impl From<mysql_async::Error> for IndexError {
    fn from(db: mysql_async::Error) -> IndexError {
        IndexError::DatabaseError(db)
    }
}
