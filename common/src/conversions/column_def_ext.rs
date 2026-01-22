use crate::parser::{
    has_decrypted_option, has_encrypted_option, is_decrypted_option, is_encrypted_option,
};
use datafusion::sql::sqlparser::ast::{ColumnDef, ColumnOption, Expr};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;
use std::mem;

pub trait ColumnDefExt {
    fn is_primary_key(&self) -> bool;

    fn is_nullable(&self) -> bool;

    fn is_encrypted_column(&self) -> bool;

    fn is_decrypted_column(&self) -> bool;

    fn clear_encryption_flag(&mut self);

    fn pop_default_value(&mut self) -> Option<Expr>;

    fn get_default_value(&self) -> Option<Expr>;

    fn get_collation(&self) -> Option<String>;
    fn is_auto_increment(&self) -> bool;
}

impl ColumnDefExt for ColumnDef {
    fn is_primary_key(&self) -> bool {
        self.options.iter().any(|opt| match opt.option {
            ColumnOption::Unique { is_primary, .. } => is_primary,
            _ => false,
        })
    }

    fn get_default_value(&self) -> Option<Expr> {
        self.options.iter().find_map(|opt| match &opt.option {
            ColumnOption::Default(expr) => Some(expr.clone()),
            _ => None,
        })
    }

    fn get_collation(&self) -> Option<String> {
        self.options.iter().find_map(|opt| match &opt.option {
            ColumnOption::Collation(name) => Some(name.to_string()),
            _ => None,
        })
    }

    fn is_encrypted_column(&self) -> bool {
        has_encrypted_option(self)
    }

    fn is_auto_increment(&self) -> bool {
        self.options.iter().any(|opt| match &opt.option {
            ColumnOption::DialectSpecific(tokens) => tokens.iter().any(|token| match token {
                Token::Word(kw) if kw.keyword == Keyword::AUTO_INCREMENT => true,
                _ => false,
            }),
            _ => false,
        })
    }

    fn clear_encryption_flag(&mut self) {
        let options = mem::take(&mut self.options);
        self.options = options
            .into_iter()
            .filter(|opt| !is_encrypted_option(opt) && !is_decrypted_option(opt))
            .collect();
    }

    fn is_nullable(&self) -> bool {
        !self
            .options
            .iter()
            .any(|x| x.option == ColumnOption::NotNull)
    }

    fn is_decrypted_column(&self) -> bool {
        has_decrypted_option(self)
    }

    fn pop_default_value(&mut self) -> Option<Expr> {
        self.options
            .extract_if(.., |opt| match &opt.option {
                ColumnOption::Default(_) => true,
                _ => false,
            })
            .map(|opt| {
                let ColumnOption::Default(expr) = opt.option else {
                    unreachable!()
                };
                expr
            })
            .next()
    }
}
