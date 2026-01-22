#![deny(
    unused_must_use,
    unreachable_code,
    unreachable_patterns,
    unused_imports,
    dead_code,
    irrefutable_let_patterns,
    unused_unsafe,
    unused_mut,
    unused_variables
)]
#![warn(unused_lifetimes, redundant_lifetimes)]
#![deny(clippy::perf)]

use bytes::Buf;
use mysql_common::proto::MySerialize;
use std::fmt::Debug;

pub mod connection;
pub mod constants;
pub mod sql_error;

pub trait ConnectionWrapper {
    fn send_packet(&mut self, packet: &(dyn MySerialize + Send + Sync)) {
        self.connection()
            .write_ref(packet)
            .expect("failed to send packet")
    }

    fn send_packet_raw<B: Buf + Debug>(&mut self, mut packet: B) {
        self.connection()
            .write_buf(&mut packet)
            .expect("failed to send packet")
    }

    fn connection(&mut self) -> &mut connection::MySQLConnection;
}
