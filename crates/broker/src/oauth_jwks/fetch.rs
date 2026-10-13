//! Fetching and parsing a JWKS document from the identity provider's endpoint.
//!
//! This is the module's only outbound HTTP call. It is separate from the
//! refresher loop so that the transport concern -- build a request, check the
//! status, hand the body to the parser -- stays independent of the scheduling
//! and swap logic that drives it.

use krabka_security::Jwks;

/// A JWKS fetch failure, reported for logging and tests. The refresher keeps
/// the previous key set on an error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FetchError {
    #[error("jwks http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jwks document was not a valid key set")]
    Parse,
}

/// Fetches and parses a JWKS document from `endpoint`, over HTTP or HTTPS.
///
/// The client's configured timeout bounds a hung identity provider. A non-2xx
/// response is an error. `ignore_key_use` passes through to the JWKS parser.
/// When it is false, the parser filters out `use=enc` keys.
pub(crate) async fn fetch_jwks(
    client: &reqwest::Client,
    endpoint: &str,
    ignore_key_use: bool,
) -> Result<Jwks, FetchError> {
    let body = client
        .get(endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Jwks::from_json(&body, ignore_key_use).map_err(|_| FetchError::Parse)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::oauth_jwks::test_support::{JWKS_BODY, serve_jwks},
        assert2::assert,
        std::time::Duration,
    };

    #[tokio::test]
    async fn fetch_jwks_parses_served_keyset() {
        let (addr, shutdown) = serve_jwks(JWKS_BODY).await;
        let client = reqwest::Client::new();
        let jwks = fetch_jwks(&client, &format!("http://{addr}/jwks"), false)
            .await
            .unwrap();
        assert!(jwks.len() == 1);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn fetch_jwks_errors_on_dead_endpoint() {
        // Nothing is listening on this port.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        let err = fetch_jwks(&client, "http://127.0.0.1:1/jwks", false).await;
        assert!(err.is_err());
    }
}
