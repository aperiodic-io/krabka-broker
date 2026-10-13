//! The public decoder shared by flexible field and array codecs.

use moxy::token::{LitStr, TokenStream};

pub(crate) fn decode_method(record: &LitStr, read: &TokenStream) -> TokenStream {
    moxy::template! {
        /// # Errors
        /// Returns an error when the value version is not 0, or when a
        /// field or the tagged-field trailer is truncated or invalid.
        pub fn decode(buf: &[u8]) -> Result<Self, crate::error::BrokerError> {
            crate::coordinator::unified::persistence::flex::decode_value(
                buf, concat!("unknown ", {{ record }}, " version"), {{ read }},
            )
        }
    }
}
