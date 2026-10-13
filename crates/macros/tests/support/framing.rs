//! Minimal version and empty-trailer framing for compiled codec consumers.

use {
    crate::error::{BrokerError, take},
    bytes::{Bytes, BytesMut},
};

pub(crate) fn encode_value<W>(write: W) -> Bytes
where
    W: FnOnce(&mut BytesMut),
{
    let mut framed = BytesMut::from(&b"\0\0"[..]);
    write(&mut framed);
    framed.extend_from_slice(&[0]);
    framed.freeze()
}

pub(crate) fn decode_value<T, R>(
    mut framed: &[u8],
    unknown_version: &'static str,
    read: R,
) -> Result<T, BrokerError>
where
    R: FnOnce(&mut &[u8]) -> Result<T, BrokerError>,
{
    match take::<2>(&mut framed)? {
        [0, 0] => {
            let decoded = read(&mut framed)?;
            match take::<1>(&mut framed)? {
                [0] => Ok(decoded),
                _ => Err("bad trailer"),
            }
        }
        _ => Err(unknown_version),
    }
}

pub(crate) mod coordinator {
    pub(crate) mod unified {
        pub(crate) mod persistence {
            pub(crate) mod flex {
                pub(crate) use crate::framing::{decode_value, encode_value};
            }
        }
    }
}
