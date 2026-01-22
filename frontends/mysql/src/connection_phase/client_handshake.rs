use mysql_common::constants::CapabilityFlags;
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::bytes::{NullBytes, U8Bytes};
use mysql_common::misc::raw::int::{LeU32, LenEnc};
use mysql_common::misc::raw::{Const, RawBytes, RawConst, RawInt, Skip};
use mysql_common::packets::AuthPlugin;
use mysql_common::proto::MyDeserialize;
use rustc_hash::FxHashMap;
use std::io;

#[derive(Debug, Clone)]
pub struct CustomClientHandshake<'a> {
    pub capabilities: Const<CapabilityFlags, LeU32>,
    pub max_packet_size: RawInt<LeU32>,
    #[allow(unused)]
    pub collation: RawInt<u8>,
    pub scramble_buf: ScrambleBuf<'a>,
    pub user: RawBytes<'a, NullBytes>,
    pub db_name: Option<RawBytes<'a, NullBytes>>,
    pub auth_plugin: Option<AuthPlugin<'a>>,
    #[allow(unused)]
    pub connect_attributes: Option<FxHashMap<RawBytes<'a, LenEnc>, RawBytes<'a, LenEnc>>>,
}

#[derive(Debug, Clone)]
pub enum ScrambleBuf<'a> {
    NullTerminated(RawBytes<'a, NullBytes>),
    LenEncoded(RawBytes<'a, LenEnc>),
    FixedLength(RawBytes<'a, U8Bytes>),
}

impl<'a> CustomClientHandshake<'a> {
    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities.0
    }

    pub fn scramble_buf(&self) -> &[u8] {
        match &self.scramble_buf {
            ScrambleBuf::NullTerminated(inner) => inner.as_bytes(),
            ScrambleBuf::LenEncoded(inner) => inner.as_bytes(),
            ScrambleBuf::FixedLength(inner) => inner.as_bytes(),
        }
    }

    pub fn user(&self) -> &[u8] {
        self.user.as_bytes()
    }

    pub fn db_name(&self) -> Option<&[u8]> {
        self.db_name.as_ref().map(|x| x.as_bytes())
    }

    #[allow(unused)]
    pub fn auth_plugin(&self) -> Option<&AuthPlugin<'a>> {
        self.auth_plugin.as_ref()
    }
}

type ServerCapabilities = CapabilityFlags;

impl<'de> MyDeserialize<'de> for CustomClientHandshake<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ServerCapabilities;

    fn deserialize(server_cap: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(4 + 4 + 1 + 23)?;
        let client_flags: RawConst<LeU32, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let max_packet_size: RawInt<LeU32> = sbuf.parse_unchecked(())?;
        let collation = sbuf.parse_unchecked(())?;
        sbuf.parse_unchecked::<Skip<23>>(())?;

        let client_flags = client_flags.get().unwrap();
        if !client_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        let shared_flags = client_flags.intersection(server_cap);

        let user = buf.parse(())?;
        let scramble_buf =
            if server_cap.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
                ScrambleBuf::LenEncoded(buf.parse(())?)
            } else if server_cap.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
                ScrambleBuf::FixedLength(buf.parse(())?)
            } else {
                ScrambleBuf::NullTerminated(buf.parse(())?)
            };

        let mut db_name = None;
        if shared_flags.contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB) {
            db_name = buf.parse(()).map(Some)?;
        }

        let mut auth_plugin = None;
        if server_cap.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
            let auth_plugin_name = buf.eat_null_str();
            auth_plugin = Some(AuthPlugin::from_bytes(auth_plugin_name));
        }

        let mut connect_attributes = None;
        if server_cap.contains(CapabilityFlags::CLIENT_CONNECT_ATTRS) {
            connect_attributes = Some(deserialize_connect_attrs(&mut *buf)?);
        }

        Ok(Self {
            capabilities: Const::new(client_flags),
            max_packet_size,
            collation,
            scramble_buf,
            user,
            db_name,
            auth_plugin,
            connect_attributes,
        })
    }
}

// Helper that deserializes connect attributes.
fn deserialize_connect_attrs<'de>(
    buf: &mut ParseBuf<'de>,
) -> io::Result<FxHashMap<RawBytes<'de, LenEnc>, RawBytes<'de, LenEnc>>> {
    let data_len = buf.parse::<RawInt<LenEnc>>(())?;

    let mut data: ParseBuf = buf.parse(data_len.0 as usize)?;
    let mut attrs = FxHashMap::default();
    while !data.is_empty() {
        let key = data.parse::<RawBytes<LenEnc>>(())?;
        let value = data.parse::<RawBytes<LenEnc>>(())?;
        attrs.insert(key, value);
    }
    Ok(attrs)
}
