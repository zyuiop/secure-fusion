// This module is disabled as it is not used currently.
// We keep it around for now as in spirit we will likely want to have a similar approach in the
// future to refactor the code?

pub(crate) mod building_blocks;
pub mod strategies;
pub(crate) mod tset;

use async_trait::async_trait;
use datafusion::execution::TaskContext;
use hybrid_array::Array;
use hybrid_array::sizes::U16;
use std::fmt::Debug;

pub type EncryptedTSetPage = Vec<u8>;
pub type STagSize = U16;
pub type STag = Array<u8, STagSize>;

#[async_trait]
pub trait TSetQuery: Debug + Send + Sync {
    /// Queries the requested TSet pages from the database.
    ///
    /// ## Arguments
    ///
    /// * `tset_name`: the name of the TSet to query. Will typically correspond to a file or table.
    /// * `identifier`: a single TSet may be used in different contexts, for example a user ID. The
    /// identifier selects the context used for this query.
    /// * `pages`: an iterator for the STags of the pages to query
    /// * `blinding_bits`: optionally, a number of bits. If not None and if supported by the
    /// implementation, STags will be sent to the underlying database truncated to `blinding_bits`
    /// bits. The implementation must still ensure that only the requested STags are returned by
    /// this function.
    async fn query_pages<'a, T: Iterator<Item = &'a STag> + Send + Sync>(
        &self,
        context: Arc<TaskContext>,
        identifier: &str,
        pages: T,
    ) -> datafusion::common::Result<HashMap<STag, EncryptedTSetPage>>;
}

#[async_trait]
pub trait TSetUpdate {
    // TODO, functions to update the index
}

pub type KWSearchKeyword = String;
pub type KWSearchQueryAnd = Vec<String>;
pub type KWSearchQueryOr = Vec<KWSearchQueryAnd>;
pub type KWSearchQuery = KWSearchQueryOr;
