//! TCP listener, per-connection task, and Kafka framing helpers.

use {crate::handlers::ApiKeyCode, krabka_protocol::api_key::ApiKey};

pub(crate) mod auth;
/// Outbound inter-broker client, with TLS and SASL. It is public so peer
/// crates inside this workspace can `use krabka_broker::network::client::*`.
pub mod client;
pub(crate) mod codec;
pub(crate) mod dispatch;
/// Zero-copy fetch response write-plan, with a vectored or sendfile drain.
pub(crate) mod fetch_writer;
/// Linux kTLS support probe (Increment F). It runs once at startup and decides
/// whether TLS fetch connections route through kernel-offloaded sendfile.
pub(crate) mod ktls_probe;
pub(crate) mod listener;
pub(crate) mod request;

#[cfg(test)]
pub(crate) mod test_support;

/// Whether the response header for `api_key` carries the v1 empty
/// tagged-fields byte after the correlation id.
#[must_use]
pub fn response_header_v1(api_key: ApiKeyCode, body_flexible: bool) -> bool {
    body_flexible && api_key != ApiKey::ApiVersions as i16
}

/// Bytes the response header occupies for `api_key` at a flexible or
/// non-flexible body.
#[must_use]
pub fn response_header_len(api_key: ApiKeyCode, body_flexible: bool) -> usize {
    if response_header_v1(api_key, body_flexible) {
        5
    } else {
        4
    }
}
