//! SASL/GSSAPI on `wasm32-wasip1`, where it is unavailable.
//!
//! The `sspi` Kerberos stack does not build for this target. The types here
//! keep the native names and the handler keeps its signature, so the
//! configuration and the SASL dispatch build unchanged. [`GssapiConfig`] has
//! no values, so `BrokerConfig::gssapi` is always `None` and a listener that
//! enables GSSAPI fails validation with `BrokerError::GssapiConfigMissing`.

use {
    super::state::ConnectionAuth,
    krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    },
    krabka_units::Time,
};

/// The GSSAPI accept-path configuration, which this platform cannot hold.
#[derive(Debug, Clone)]
pub enum GssapiConfig {}

/// The SASL/GSSAPI `SaslAuthenticate` handler. No [`GssapiConfig`] exists, so
/// no call reaches it.
pub fn handle_authenticate_gssapi(
    _req: &SaslAuthenticateRequest,
    _auth: &mut ConnectionAuth,
    config: &GssapiConfig,
    _max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    match *config {}
}
