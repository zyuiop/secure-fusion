mod arrow_helper;
mod char_utils;
pub mod client_side_wrapper;
mod commands;
mod error;
mod parser;
mod placeholders;
mod protocol;
mod rewrites;
mod row_handlers;
mod statements;

pub enum HandleResult {
    HandledPacket,
    ConnectionClosed { reason: String },
}
