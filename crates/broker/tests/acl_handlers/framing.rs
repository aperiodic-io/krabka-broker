//! The SASL/PLAIN authentication exchange and the length-prefixed
//! request/response framing primitive. Every driver in this suite opens its
//! connection and moves its bytes through the typed exchange, which binds the
//! shared [`kafka_wire`] helpers to this suite's client id.

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-acl-test";
