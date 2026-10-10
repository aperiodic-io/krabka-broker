//! HTTP transport for RFC 7662 introspection and OIDC userinfo.
//!
//! It lives here instead of in `crates/security`, so that the security crate
//! stays free of I/O. This mirrors the JWKS-refresher pattern.

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use krabka_security::{
    IntrospectionClient, IntrospectionError, JwksTrustError, build_client_config_from_pem,
};
use krabka_units::{Time, convert::TimeExt as _};

/// reqwest-backed RFC 7662 introspection client.
///
/// It uses HTTP Basic Auth with the operator-configured `client_id` and
/// `client_secret`. It can also call a userinfo endpoint after a successful
/// introspection.
#[derive(Debug)]
pub struct ReqwestIntrospectionClient {
    client: reqwest::Client,
    introspection_endpoint: String,
    userinfo_endpoint: Option<String>,
    client_id: String,
    client_secret: String,
}

/// Errors that can occur while the broker builds the introspection client at
/// startup.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("tls trust: {0}")]
    Tls(#[from] JwksTrustError),
    #[error("reqwest build: {0}")]
    Reqwest(String),
}

impl ReqwestIntrospectionClient {
    /// Builds a new client.
    ///
    /// When `tls_trust` is `Some`, this method builds the rustls
    /// `ClientConfig` with
    /// `krabka_security::build_client_config_from_pem`. The same trust bundle
    /// then covers JWKS, introspection, and userinfo. When `tls_trust` is
    /// `None`, reqwest's default webpki-roots apply.
    // Returns Arc<dyn IntrospectionClient> to fit the validator's trait-object
    // slot; constructing the concrete type would just be unwrapped immediately
    // into the Arc.
    pub fn build(
        introspection_endpoint: String,
        userinfo_endpoint: Option<String>,
        client_id: String,
        client_secret: String,
        tls_trust: Option<&Path>,
        timeout: Time,
    ) -> Result<Arc<dyn IntrospectionClient>, BuildError> {
        // `file_config::FileConfig::apply_to` constructs this client
        // before `Broker::start` runs, so the rustls CryptoProvider that
        // `Broker::start` installs at line ~783 isn't there yet when
        // `build_client_config_from_pem` reaches into
        // `rustls::ClientConfig::builder()` → `CryptoProvider::get_default()`
        // and panics with "Could not automatically determine the
        // process-level CryptoProvider from Rustls crate features."
        // (Both the `aws-lc-rs` and `ring` rustls features end up in the
        // dep graph, so the `auto-determine` heuristic refuses to pick
        // one.) Install idempotently here so the introspection client
        // works from any entry point — the `let _ =` swallows
        // `AlreadySet` when a sibling caller (notably `Broker::start`)
        // won the race.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut builder = reqwest::Client::builder().timeout(timeout.to_std());
        if let Some(path) = tls_trust {
            let cfg = build_client_config_from_pem(path)?;
            builder = builder.use_preconfigured_tls((*cfg).clone());
        }
        let client = builder
            .build()
            .map_err(|e| BuildError::Reqwest(e.to_string()))?;
        Ok(Arc::new(Self {
            client,
            introspection_endpoint,
            userinfo_endpoint,
            client_id,
            client_secret,
        }))
    }
}

#[async_trait]
impl IntrospectionClient for ReqwestIntrospectionClient {
    async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError> {
        request_json(
            self.client
                .post(&self.introspection_endpoint)
                .basic_auth(&self.client_id, Some(&self.client_secret))
                .form(&[("token", token)]),
        )
        .await
    }

    async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError> {
        let Some(endpoint) = &self.userinfo_endpoint else {
            return Ok(None);
        };
        request_json(self.client.get(endpoint).bearer_auth(token))
            .await
            .map(Some)
    }
}

/// Both identity-provider endpoints use the same transport/status/JSON error mapping.
async fn request_json(
    request: reqwest::RequestBuilder,
) -> Result<serde_json::Value, IntrospectionError> {
    let response = request
        .send()
        .await
        .map_err(|error| IntrospectionError::Transport(error.to_string()))?;
    if !response.status().is_success() {
        return Err(IntrospectionError::Status(response.status().as_u16()));
    }
    response.json().await.map_err(|_| IntrospectionError::Parse)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Mutex};

    use assert2::assert;
    use krabka_units::{millis, secs};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Records observed requests so tests can assert on the bytes.
    #[derive(Debug, Default)]
    struct ObservedRequests {
        introspect_bodies: Mutex<Vec<String>>,
        introspect_auths: Mutex<Vec<String>>,
        userinfo_auths: Mutex<Vec<String>>,
    }

    /// Starts an HTTPS test server with these routes:
    ///   POST /introspect  -> `introspect_body` with `introspect_status`
    ///   GET  /userinfo    -> `userinfo_body` with 200, or 404 when it is None
    ///
    /// Returns (addr, shutdown, `ca_pem_path`, observed).
    // TLS handshake + HTTP/1.1 framing + dual-route dispatch sit naturally in one
    // function for the test fixture; extraction would just scatter mock state.
    #[derive(Clone, Copy)]
    struct HttpStatusCode(u16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct HttpsResponseSetup {
        #[default("{}")]
        introspect_body: &'static str,
        #[default(HttpStatusCode(200))]
        introspect_status: HttpStatusCode,
        userinfo_body: Option<&'static str>,
    }

    async fn serve_https(
        setup: HttpsResponseSetup,
    ) -> (
        SocketAddr,
        CancellationToken,
        std::path::PathBuf,
        Arc<ObservedRequests>,
    ) {
        let HttpsResponseSetup {
            introspect_body,
            introspect_status,
            userinfo_body,
        } = setup;
        let introspect_status = introspect_status.0;
        let observed = Arc::new(ObservedRequests::default());
        let observed_in_task = observed.clone();
        let (addr, shutdown, cert_path) = crate::test_support::serve_loopback_tls(move |mut tls| {
            let observed = observed_in_task.clone();
            async move {
                            let mut buf = vec![0u8; 8192];
                            let n = tls.read(&mut buf).await.unwrap_or(0);
                            let req = String::from_utf8_lossy(&buf[..n]).to_string();
                            let body = req
                                .split("\r\n\r\n")
                                .nth(1)
                                .unwrap_or("")
                                .to_string();
                            let auth_header = req
                                .lines()
                                .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
                                .map(|l| {
                                    l.trim_start_matches(|c: char| c != ':')
                                        .trim_start_matches(':')
                                        .trim()
                                        .to_string()
                                })
                                .unwrap_or_default();
                            let (status, body_out) = if req.starts_with("POST /introspect") {
                                observed
                                    .introspect_bodies
                                    .lock()
                                    .unwrap()
                                    .push(body.clone());
                                observed
                                    .introspect_auths
                                    .lock()
                                    .unwrap()
                                    .push(auth_header.clone());
                                (introspect_status, introspect_body)
                            } else if req.starts_with("GET /userinfo") {
                                observed
                                    .userinfo_auths
                                    .lock()
                                    .unwrap()
                                    .push(auth_header.clone());
                                match userinfo_body {
                                    Some(b) => (200u16, b),
                                    None => (404, "{}"),
                                }
                            } else {
                                (404, "{}")
                            };
                            let status_text = match status {
                                200 => "OK",
                                401 => "Unauthorized",
                                404 => "Not Found",
                                500 => "Internal Server Error",
                                _ => "Error",
                            };
                            let header = format!(
                                "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                body_out.len(),
                            );
                            let _ = tls.write_all(header.as_bytes()).await;
                            let _ = tls.write_all(body_out.as_bytes()).await;
                            let _ = tls.shutdown().await;
            }
        }).await;

        (addr, shutdown, cert_path, observed)
    }

    /// A trusted HTTPS introspection client without a user-info endpoint.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct HttpsClientSetup<'a> {
        #[default("id")]
        client_id: &'a str,
        #[default("s")]
        secret: &'a str,
        #[default(secs(5))]
        timeout: krabka_units::Time,
    }

    fn https_client(
        addr: SocketAddr,
        ca: &std::path::Path,
        setup: HttpsClientSetup<'_>,
    ) -> Arc<dyn IntrospectionClient> {
        let HttpsClientSetup {
            client_id,
            secret,
            timeout,
        } = setup;
        ReqwestIntrospectionClient::build(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            client_id.into(),
            secret.into(),
            Some(ca),
            timeout,
        )
        .unwrap()
    }

    /// A server and trusted client, retaining the request observations and shutdown handle.
    async fn https_fixture(
        response: HttpsResponseSetup,
        client: HttpsClientSetup<'_>,
    ) -> (
        Arc<dyn IntrospectionClient>,
        CancellationToken,
        Arc<ObservedRequests>,
    ) {
        let (addr, shutdown, ca, observed) = serve_https(response).await;
        (https_client(addr, &ca, client), shutdown, observed)
    }

    #[tokio::test]
    async fn introspection_fetches_active_token_over_https_with_custom_trust() {
        let body = r#"{"active":true,"sub":"alice"}"#;
        let (client, srv_shutdown, _observed) = https_fixture(
            HttpsResponseSetup {
                introspect_body: body,
                ..Default::default()
            },
            HttpsClientSetup {
                client_id: "kafka-broker",
                secret: "secret",
                ..Default::default()
            },
        )
        .await;
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("active").and_then(serde_json::Value::as_bool) == Some(true));
        assert!(resp.get("sub").and_then(|v| v.as_str()) == Some("alice"));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_returns_inactive_when_idp_says_inactive() {
        let (client, srv_shutdown, _) = https_fixture(
            HttpsResponseSetup {
                introspect_body: r#"{"active":false}"#,
                ..Default::default()
            },
            HttpsClientSetup::default(),
        )
        .await;
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("active").and_then(serde_json::Value::as_bool) == Some(false));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_returns_transport_error_on_non_2xx() {
        let (client, srv_shutdown, _) = https_fixture(
            HttpsResponseSetup {
                introspect_body: r#"{"error":"x"}"#,
                introspect_status: HttpStatusCode(500),
                ..Default::default()
            },
            HttpsClientSetup::default(),
        )
        .await;
        let err = client.introspect("tok").await.unwrap_err();
        assert!(
            matches!(err, IntrospectionError::Status(500)),
            "got {err:?}"
        );
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_userinfo_endpoint_is_called_after_active_introspection() {
        let (addr, srv_shutdown, ca, observed) = serve_https(HttpsResponseSetup {
            introspect_body: r#"{"active":true,"sub":"alice"}"#,
            userinfo_body: Some(r#"{"preferred_username":"alice","email":"a@b.c"}"#),
            ..Default::default()
        })
        .await;
        let client = ReqwestIntrospectionClient::build(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            Some(format!("https://127.0.0.1:{}/userinfo", addr.port())),
            "id".into(),
            "s".into(),
            Some(&ca),
            secs(5),
        )
        .unwrap();
        client.introspect("tok").await.unwrap();
        let ui = client.userinfo("tok").await.unwrap().unwrap();
        assert!(ui.get("preferred_username").and_then(|v| v.as_str()) == Some("alice"));
        assert!(observed.userinfo_auths.lock().unwrap().len() == 1);
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_userinfo_endpoint_is_not_called_when_endpoint_unset() {
        let (client, srv_shutdown, _) = https_fixture(
            HttpsResponseSetup {
                introspect_body: r#"{"active":true,"sub":"a"}"#,
                ..Default::default()
            },
            HttpsClientSetup::default(),
        )
        .await;
        let ui = client.userinfo("tok").await.unwrap();
        assert!(ui.is_none());
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_handles_keycloak_response_shape() {
        let body = r#"{"active":true,"sub":"svc-account-kafka-client","client_id":"kafka-client","scope":"kafka.write profile","exp":9999999999}"#;
        let (client, srv_shutdown, _) = https_fixture(
            HttpsResponseSetup {
                introspect_body: body,
                ..Default::default()
            },
            HttpsClientSetup::default(),
        )
        .await;
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("client_id").and_then(|v| v.as_str()) == Some("kafka-client"));
        assert!(resp.get("scope").and_then(|v| v.as_str()) == Some("kafka.write profile"));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_basic_auth_sent_with_configured_client_id_and_secret() {
        let (client, srv_shutdown, observed) = https_fixture(
            HttpsResponseSetup {
                introspect_body: r#"{"active":true,"sub":"a"}"#,
                ..Default::default()
            },
            HttpsClientSetup {
                client_id: "kafka-broker",
                secret: "shh",
                ..Default::default()
            },
        )
        .await;
        client.introspect("tok").await.unwrap();
        let auths = observed.introspect_auths.lock().unwrap();
        assert!(auths.len() == 1);
        // base64("kafka-broker:shh") = "a2Fma2EtYnJva2VyOnNoaA=="
        assert!(auths[0] == "Basic a2Fma2EtYnJva2VyOnNoaA==");
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_form_body_token_field() {
        let (client, srv_shutdown, observed) = https_fixture(
            HttpsResponseSetup {
                introspect_body: r#"{"active":true,"sub":"a"}"#,
                ..Default::default()
            },
            HttpsClientSetup::default(),
        )
        .await;
        client.introspect("opaque-abc").await.unwrap();
        let bodies = observed.introspect_bodies.lock().unwrap();
        assert!(bodies.len() == 1);
        assert!(bodies[0] == "token=opaque-abc");
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_respects_http_timeout() {
        // Bind a port, immediately drop the listener — connect attempts
        // hit a closed port and reqwest's transport layer fails fast.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (cert, _key) = crate::test_support::ecdsa_pair_for_host("127.0.0.1");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();
        drop(listener);
        let client = https_client(
            addr,
            &ca_path,
            HttpsClientSetup {
                timeout: millis(200),
                ..Default::default()
            },
        );
        let err = client.introspect("tok").await.unwrap_err();
        assert!(
            matches!(err, IntrospectionError::Transport(_)),
            "got {err:?}"
        );
    }
}
