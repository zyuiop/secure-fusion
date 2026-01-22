use bytes::{Buf, Bytes, BytesMut};
use mysql_common::proto::MySerialize;
use mysql_common::proto::codec::error::PacketCodecError;
use mysql_common::proto::sync_framed::MySyncFramed;
use std::fmt::Debug;
use std::net::{SocketAddr, TcpStream};
use std::ops::Deref;

#[derive(Debug)]
pub struct MySQLConnection {
    stream: MySyncFramed<TcpStream>,
    addr: SocketAddr,
    write_buffer: Vec<u8>,

    /// The read buffer
    /// It should always be present
    /// This approach only works if the wrapped buffer is not persisted in any way
    /// This is somewhat ensured by not allowing to clone the bytes
    read_buffer: Option<Bytes>,
}

#[repr(transparent)]
pub struct WrappedBuffer(Bytes);

impl AsRef<[u8]> for WrappedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for WrappedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MySQLConnection {
    // todo(zerocopy): implement an easier "forward" mode that prevents copies between MySyncFramed instances? (peek next packet, then take decision)
    pub fn new(stream: TcpStream) -> MySQLConnection {
        let addr = stream.peer_addr().unwrap();
        let framed = MySyncFramed::new(stream);
        MySQLConnection {
            stream: framed,
            addr,
            write_buffer: Vec::with_capacity(8192),
            read_buffer: Some(BytesMut::with_capacity(8192).freeze()),
        }
    }

    pub fn peer_addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn read(&mut self) -> Option<Result<WrappedBuffer, PacketCodecError>> {
        // todo(zerocopy): reduce allocation by reusing a buffer?
        let buf = self.read_buffer.take().unwrap();
        let mut buf = buf.try_into_mut().unwrap();
        unsafe {
            // Clear the vector without freeing the elements.
            // This may be slightly faster than using `clear` and does not leak memory.
            buf.set_len(0);
        }

        let result = self.stream.next_packet(&mut buf);
        let buf = buf.freeze();
        let _ = self.read_buffer.insert(buf.clone());

        match result {
            Ok(p) if p => Some(Ok(WrappedBuffer(buf))),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    }

    pub fn write_ref(
        &mut self,
        packet: &(dyn MySerialize + Send + Sync),
    ) -> Result<(), PacketCodecError> {
        packet.serialize(&mut self.write_buffer);
        self.stream.send(&mut self.write_buffer.as_slice())?;

        unsafe {
            // Clear the vector without freeing the elements.
            // This may be slightly faster than using `clear` and does not leak memory.
            self.write_buffer.set_len(0);
        }
        Ok(())
    }

    pub fn write_buf<B: Buf + Debug>(&mut self, packet: &mut B) -> Result<(), PacketCodecError> {
        self.stream.send(packet)
    }

    pub fn reset_seqno(&mut self) {
        self.stream.codec_mut().reset_seq_id();
    }
}
