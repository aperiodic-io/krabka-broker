//! The wire-level integer aliases that the dispatch path and the handlers
//! share.
//!
//! Each alias names one field of a Kafka request or response header. The
//! alias says what the number means, which a bare `i16` does not.

/// Raw wire `api_key` (i16) that selects the RPC.
///
/// This is the numeric form of a [`krabka_protocol::api_key::ApiKey`] variant.
/// It stays an `i16` because it arrives off the wire and may name an API that
/// this broker does not know.
pub type ApiKeyCode = i16;

/// Negotiated Kafka request/response schema version for a single RPC.
pub type ApiVersion = i16;

/// Client-chosen request correlation id. The response header echoes it exactly.
pub type CorrelationId = i32;
