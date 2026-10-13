//! The reason a signed freeze record was refused, and the answer it becomes.
//!
//! All six checks answer with one wire code, so the reason a response carries
//! is a message and never a code. Holding the reason as a value keeps the two
//! apart: a test can name the check that fired while the response still tells
//! a caller nothing about which one it was.

use crate::codes;

/// Why the broker refused a signed freeze record.
///
/// Every variant answers with the same wire code. The variant decides the
/// message alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignatureRefusal {
    /// No `[[operator_keys]]` entry carries the `key_id` the record names.
    UnknownKeyId,
    /// The record claims an author that is not the principal bound to the key.
    AuthorIsNotTheKeyPrincipal,
    /// The record claims an author that is not the principal on the
    /// connection.
    AuthorIsNotTheConnectionPrincipal,
    /// `set_at_ms` sits further from this broker's clock than
    /// `freeze.signature_max_skew` allows.
    TimestampOutsideSkewWindow,
    /// `set_at_ms` is not newer than the entry this record replaces.
    TimestampNotNewerThanTheEntryItReplaces,
    /// The signature does not verify over the canonical bytes.
    SignatureDidNotVerify,
}

/// A [`SignatureRefusal`] as a response carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WireRefusal {
    pub(crate) code: i16,
    pub(crate) message: &'static str,
}

impl SignatureRefusal {
    /// The wire error code of every refusal.
    ///
    /// It is a constant and not a method on purpose. One code covers all six
    /// checks, because a code that separated them would tell an attacker which
    /// check they got past. [`Self::message`] is the only thing that varies.
    pub(crate) const CODE: i16 = codes::OPERATOR_SIGNATURE_INVALID;

    /// The `error_message` that names the check that failed.
    pub(crate) fn message(self) -> &'static str {
        match self {
            SignatureRefusal::UnknownKeyId => "no operator key is configured under that key_id",
            SignatureRefusal::AuthorIsNotTheKeyPrincipal => {
                "the record names an author that the signing key does not speak for"
            }
            SignatureRefusal::AuthorIsNotTheConnectionPrincipal => {
                "the record names an author that is not the principal on this connection"
            }
            SignatureRefusal::TimestampOutsideSkewWindow => {
                "set_at_ms is outside the signature skew window of this broker"
            }
            SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces => {
                "set_at_ms is not newer than the entry this record replaces"
            }
            SignatureRefusal::SignatureDidNotVerify => {
                "the signature does not verify over the canonical bytes of this record"
            }
        }
    }

    /// The refusal as a response carries it: the code and the text.
    ///
    /// The code is [`Self::CODE`] for every variant, and the text is the only
    /// part that separates the six checks.
    pub(crate) fn wire(self) -> WireRefusal {
        WireRefusal {
            code: Self::CODE,
            message: self.message(),
        }
    }
}
