use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

#[derive(Debug, Clone)]
pub enum ParsedStatement {
    Statement(SqlStatement),

    #[deprecated]
    Placeholder([u8; 2688]), // An INSERT IGNORE statement
                             // The IGNORE part is omitted from the underlying statement, as it is not compatible with DataFusion
                             // It should be added back by the backend implementation if supported. If the backend does not support
                             // it, it can raise an error when processing the statement.
                             // InsertIgnore(SqlStatement),
                             // ProxyCommand(ProxyCommand)
}
