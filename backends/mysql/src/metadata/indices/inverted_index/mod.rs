//! Goal: separate the inverted index table in two layers to reduce complexity
//! The database layer should only be parameterized by the size of the table key, expressed in bytes
//! Keys should not be converted and should be processed directly as binary
//! Indexed terms should also be processed as raw bytes
//! A second layer should take care of converting query rows and pre-processing them

mod db_inverted_index;
mod index_error;
pub mod inverted_index;
