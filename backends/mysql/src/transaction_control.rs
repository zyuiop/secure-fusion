use crate::get_conn::ConnGetter;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use mysql_async::IsolationLevel;
use mysql_async::prelude::Queryable;
use std::sync::Arc;
use tokio::sync::RwLock;

struct TransactionControlInner {
    autocommit: bool,
    current_transaction: Option<TransactionStatus>,
    isolation_level: IsolationLevel,
}

enum TransactionStatus {
    Forced,
    Weak(u32),
}

impl Default for TransactionControlInner {
    fn default() -> Self {
        Self {
            autocommit: true,
            current_transaction: None,
            isolation_level: IsolationLevel::RepeatableRead,
        }
    }
}

pub struct TransactionControl(RwLock<TransactionControlInner>);

impl Default for TransactionControl {
    fn default() -> Self {
        TransactionControl(RwLock::new(TransactionControlInner::default()))
    }
}

impl TransactionControl {
    pub async fn set_autocommit(&self, autocommit: bool, conn: &(dyn ConnGetter + Send + Sync)) {
        if autocommit {
            // "set autocommit=1" implicitly commits
            // https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html
            self.commit(conn).await;
        }

        let mut guard = self.0.try_write().unwrap();
        guard.autocommit = autocommit;
        guard.current_transaction = None;

        conn.set_autocommit(guard.autocommit).await.unwrap();
    }

    /// Commit the current operation if autocommit is true and no transaction is in progress
    pub async fn weak_commit(&self, conn: &(dyn ConnGetter + Send + Sync)) {
        let mut guard = self.0.try_write().unwrap();

        if guard.autocommit {
            if let Some(TransactionStatus::Weak(ref mut depth)) = guard.current_transaction {
                if *depth > 0 {
                    *depth -= 1;

                    if *depth == 0 {
                        conn.commit().await.unwrap();
                        guard.current_transaction = None;
                    }
                }
            }
        }
    }

    /// Open a transaction if autocommit is true and no transaction is in progress
    pub async fn weak_start_transaction(&self, conn: &(dyn ConnGetter + Send + Sync)) {
        let mut guard = self.0.try_write().unwrap();

        if guard.autocommit {
            if let Some(TransactionStatus::Weak(ref mut depth)) = guard.current_transaction {
                *depth += 1;
            } else if guard.current_transaction.is_none() {
                conn.start_transaction().await.unwrap();
                guard.current_transaction = Some(TransactionStatus::Weak(1));
            }
        }
    }

    pub async fn commit(&self, conn: &(dyn ConnGetter + Send + Sync)) {
        let mut guard = self.0.try_write().unwrap();
        conn.commit().await.unwrap();
        guard.current_transaction = None;
    }

    pub async fn rollback(&self, conn: &(dyn ConnGetter + Send + Sync)) {
        let mut guard = self.0.try_write().unwrap();
        conn.rollback().await.unwrap();
        guard.current_transaction = None;
    }

    pub async fn start_transaction(&self, conn: &(dyn ConnGetter + Send + Sync)) {
        self.commit(conn).await;
        conn.start_transaction().await.unwrap();

        let mut guard = self.0.try_write().unwrap();
        guard.current_transaction = Some(TransactionStatus::Forced)
    }

    pub async fn set_isolation_level(
        &self,
        isolation_level: IsolationLevel,
        conn: &(dyn ConnGetter + Send + Sync),
    ) {
        let mut guard = self.0.try_write().unwrap();
        guard.isolation_level = isolation_level;

        ConnGetterExt::set_isolation_level(conn, isolation_level)
            .await
            .unwrap();
    }
}

#[async_trait]
pub trait ConnGetterExt {
    async fn commit(&self) -> Result<(), DataFusionError>;
    async fn rollback(&self) -> Result<(), DataFusionError>;
    async fn start_transaction(&self) -> Result<(), DataFusionError>;
    async fn set_autocommit(&self, autocommit: bool) -> Result<(), DataFusionError>;
    async fn set_isolation_level(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<(), DataFusionError>;
}

#[async_trait]
impl<T> ConnGetterExt for T
where
    T: ConnGetter + Sync + Send + ?Sized,
{
    async fn commit(&self) -> Result<(), DataFusionError> {
        let conn = self.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop("COMMIT")
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        Ok(())
    }

    async fn set_autocommit(&self, autocommit: bool) -> Result<(), DataFusionError> {
        let conn = self.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop(format!("SET autocommit={}", if autocommit { 1 } else { 0 }))
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        Ok(())
    }

    async fn start_transaction(&self) -> Result<(), DataFusionError> {
        let conn = self.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        Ok(())
    }

    async fn rollback(&self) -> Result<(), DataFusionError> {
        let conn = self.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop("ROLLBACK")
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        Ok(())
    }

    async fn set_isolation_level(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<(), DataFusionError> {
        let conn = self.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop(format!(
            "SET TRANSACTION ISOLATION LEVEL {}",
            isolation_level
        ))
        .await
        .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        Ok(())
    }
}

pub trait TxControlGetter {
    fn get_tx_control(&self) -> Arc<TransactionControl>;
}

impl TxControlGetter for &SessionConfig {
    fn get_tx_control(&self) -> Arc<TransactionControl> {
        self.get_extension()
            .expect("no MySQL transaction control configured!")
    }
}

impl TxControlGetter for &SessionState {
    fn get_tx_control(&self) -> Arc<TransactionControl> {
        self.config().get_tx_control()
    }
}

impl TxControlGetter for Arc<TaskContext> {
    fn get_tx_control(&self) -> Arc<TransactionControl> {
        self.session_config().get_tx_control()
    }
}
