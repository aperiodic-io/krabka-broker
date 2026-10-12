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

/// Construct an exhaustive wire value with an explicitly empty tagged-field set.
/// Every other field remains required at the call site, including expected responses.
macro_rules! tagged_wire {
    ($($ty:ident)::+ { $($field:ident $(: $value:expr)?),* $(,)? }) => {
        $($ty)::+ {
            $($field $(: $value)?,)*
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }
    };
}

/// Exhaustive responses that report no throttle delay and no tagged fields.
macro_rules! unthrottled_wire {
    ($($ty:ident)::+ { $($field:ident $(: $value:expr)?),* $(,)? }) => {
        tagged_wire!($($ty)::+ { throttle_time_ms: 0, $($field $(: $value)?,)* })
    };
}

/// Define the typed entry point consumed by the dispatch registry. Every
/// operation supplies its request, response, parameter names and unchanged
/// body; the broker, negotiated version, caller context and error contract are
/// shared by all typed handlers.
macro_rules! context_handler {
    ($(#[$attr:meta])* $request:ty => $response:ty,
        ($broker:ident, $request_binding:pat_param, $version:ident, $ctx:ident), $body:block) => {
        $(#[$attr])*
        pub(crate) async fn handle(
            $broker: &crate::broker::Broker,
            $request_binding: $request,
            $version: crate::handlers::ApiVersion,
            $ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<$response, crate::error::BrokerError> $body
    };
}

/// Define the raw-body entry point used by wire-dispatched contextual APIs.
/// Bodies retain their own decoding, refusal and forwarding order.
macro_rules! wire_handler {
    ($(#[$attr:meta])* $($mode:ident)? ($broker:ident, $version:ident, $bytes:ident, $ctx:ident), $body:block) => {
        wire_handler!(@define $(#[$attr])* $($mode)? ($broker, $version, $bytes, $ctx),
            crate::handlers::RequestContext<'_>, [], $body);
    };
    ($(#[$attr:meta])* $($mode:ident)? ($broker:ident, $version:ident, $correlation:ident, $bytes:ident, $ctx:ident: $context:ty), $body:block) => {
        wire_handler!(@define $(#[$attr])* $($mode)? ($broker, $version, $bytes, $ctx),
            $context, [$correlation], $body);
    };
    (@define $(#[$attr:meta])* $($mode:ident)? ($broker:ident, $version:ident, $bytes:ident, $ctx:ident),
        $context:ty, [$($correlation:ident)?], $body:block) => {
        $(#[$attr])*
        pub(crate) $($mode)? fn handle(
            $broker: &crate::broker::Broker,
            $version: crate::handlers::ApiVersion,
            $($correlation: crate::handlers::CorrelationId,)?
            $bytes: &[u8],
            $ctx: &$context,
        ) -> Result<bytes::Bytes, crate::error::BrokerError> $body
    };
}

/// Define a raw dispatch entry point that first decodes its request body.
macro_rules! raw_dispatch {
    ($(#[$attr:meta])* mut $request:ty, ($broker:ident, $version:ident, $bytes:ident, $ctx:ident, $req:ident), $body:block) => {
        raw_dispatch!(@define $(#[$attr])* [mut] $request,
            ($broker, $version, $bytes, $ctx, $req), $body);
    };
    ($(#[$attr:meta])* $request:ty, ($broker:ident, $version:ident, $bytes:ident, $ctx:ident, $req:ident), $body:block) => {
        raw_dispatch!(@define $(#[$attr])* [] $request,
            ($broker, $version, $bytes, $ctx, $req), $body);
    };
    (@define $(#[$attr:meta])* [$($mutability:ident)?] $request:ty,
        ($broker:ident, $version:ident, $bytes:ident, $ctx:ident, $req:ident), $body:block) => {
        $(#[$attr])*
        pub(crate) async fn dispatch(
            $broker: &$crate::broker::Broker,
            $version: $crate::handlers::ApiVersion,
            $bytes: &[u8],
            $ctx: &$crate::handlers::RequestContext<'_>,
        ) -> $crate::handlers::DispatchResult {
            let $($mutability)? $req =
                $crate::handlers::decode_request::<$request>($bytes, $version)?;
            $body
        }
    };
}
