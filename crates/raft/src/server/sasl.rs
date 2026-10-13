//! `SaslHandshake` and `SaslAuthenticate` after the connection reaches the
//! listener.
//!
//! Kafka tags both schemas with the `controller` listener, so a controller
//! advertises them on every controller listener. A SASL listener runs the
//! exchange in the handshake, before this listener gets the connection. A
//! request that arrives after that, or on a listener with no SASL, gets the
//! answer of Kafka's `ControllerApis.handleSaslHandshakeRequest` and
//! `handleSaslAuthenticateRequest`: `ILLEGAL_SASL_STATE`.

use {
    crate::error::RaftError,
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            sasl_authenticate_request::{self, SaslAuthenticateRequest},
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request::{self, SaslHandshakeRequest},
            sasl_handshake_response::SaslHandshakeResponse,
        },
    },
};

/// Kafka's `ILLEGAL_SASL_STATE`.
const ILLEGAL_SASL_STATE: i16 = 34;

/// The message Kafka's `ControllerApis.handleSaslAuthenticateRequest` sends.
const AUTHENTICATE_AFTER_AUTHENTICATION: &str =
    "SaslAuthenticate request received after successful authentication";

/// Whether the listener answers `api_key` with [`sasl_response`].
pub(super) fn is_sasl_api(api_key: i16) -> bool {
    api_key == sasl_handshake_request::API_KEY || api_key == sasl_authenticate_request::API_KEY
}

/// The `ILLEGAL_SASL_STATE` answer to a `SaslHandshake` or `SaslAuthenticate`.
///
/// # Errors
/// Returns the decode error when the body does not decode at `version`, as
/// Kafka's `RequestContext.parseRequest` fails such a request.
pub(super) fn sasl_response(api_key: i16, version: i16, body: &[u8]) -> Result<Bytes, RaftError> {
    let mut out = BytesMut::new();
    if api_key == sasl_handshake_request::API_KEY {
        SaslHandshakeRequest::decode(&mut &body[..], version)?;
        SaslHandshakeResponse {
            error_code: ILLEGAL_SASL_STATE,
            ..Default::default()
        }
        .encode(&mut out, version)?;
    } else {
        SaslAuthenticateRequest::decode(&mut &body[..], version)?;
        SaslAuthenticateResponse {
            error_code: ILLEGAL_SASL_STATE,
            error_message: Some(AUTHENTICATE_AFTER_AUTHENTICATION.into()),
            ..Default::default()
        }
        .encode(&mut out, version)?;
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn is_sasl_api_matches_handshake_and_authenticate() {
        check!(is_sasl_api(sasl_handshake_request::API_KEY));
        check!(is_sasl_api(sasl_authenticate_request::API_KEY));
        check!(!is_sasl_api(0));
        check!(!is_sasl_api(-1));
    }

    #[test]
    fn sasl_requests_get_illegal_sasl_state() {
        for version in sasl_handshake_request::MIN_VERSION..=sasl_handshake_request::MAX_VERSION {
            let mut body = BytesMut::new();
            SaslHandshakeRequest {
                mechanism: "PLAIN".into(),
                ..Default::default()
            }
            .encode(&mut body, version)
            .unwrap();
            let response = sasl_response(17, version, &body).unwrap();
            check!(
                SaslHandshakeResponse::decode(&mut &response[..], version).unwrap()
                    == SaslHandshakeResponse {
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    },
                "SaslHandshake v{version}"
            );
        }
        for version in
            sasl_authenticate_request::MIN_VERSION..=sasl_authenticate_request::MAX_VERSION
        {
            let mut body = BytesMut::new();
            SaslAuthenticateRequest::default()
                .encode(&mut body, version)
                .unwrap();
            let response = sasl_response(36, version, &body).unwrap();
            check!(
                SaslAuthenticateResponse::decode(&mut &response[..], version).unwrap()
                    == SaslAuthenticateResponse {
                        error_code: ILLEGAL_SASL_STATE,
                        error_message: Some(AUTHENTICATE_AFTER_AUTHENTICATION.into()),
                        ..Default::default()
                    },
                "SaslAuthenticate v{version}"
            );
        }
        check!(sasl_response(17, 1, &[0xff]).is_err());
    }
}
