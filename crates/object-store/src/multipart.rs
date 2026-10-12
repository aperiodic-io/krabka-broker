//! S3's incomplete-multipart listing, which `object_store` 0.13 does not expose,
//! and the signed bucket `GET` it shares with the WORM check.

use object_store::{
    RetryConfig,
    aws::{AwsAuthorizer, AwsCredential},
    client::HttpRequestBody,
};
use serde::{Deserialize, de::DeserializeOwned};

use crate::{ObjectStoreError, S3Config, build::build_s3_store};

/// One upload that S3 has initiated but not completed or aborted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteMultipartUpload {
    /// Object key the upload would create.
    pub key: String,
    /// Backend upload identifier.
    pub upload_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListResponse {
    #[serde(default, rename = "Upload")]
    uploads: Vec<IncompleteUpload>,
    #[serde(default)]
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct IncompleteUpload {
    key: String,
    upload_id: String,
}

/// Lists every incomplete S3 multipart upload under `prefix`.
///
/// `object_store` exposes multipart creation and abort but not S3's
/// `ListMultipartUploads` operation, so the verifier makes this one signed S3
/// request directly with the same credential provider.
///
/// # Errors
///
/// Returns an error when the S3 configuration, credentials, request, or XML
/// response is invalid.
pub async fn list_s3_multipart_uploads(
    cfg: &S3Config,
    prefix: Option<&str>,
) -> Result<Vec<IncompleteMultipartUpload>, ObjectStoreError> {
    let store = build_s3_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let client = s3_http_client(cfg)?;
    let bucket_url = s3_bucket_url(cfg)?;
    let mut key_marker = None;
    let mut upload_id_marker = None;
    let mut found = Vec::new();

    loop {
        let mut url = bucket_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("uploads", "");
            if let Some(prefix) = prefix {
                query.append_pair("prefix", prefix);
            }
            if let Some(marker) = key_marker.as_deref() {
                query.append_pair("key-marker", marker);
            }
            if let Some(marker) = upload_id_marker.as_deref() {
                query.append_pair("upload-id-marker", marker);
            }
        }
        let page: ListResponse =
            signed_s3_get_xml(&client, &credential, cfg, url, "ListMultipartUploads").await?;
        found.extend(
            page.uploads
                .into_iter()
                .map(|upload| IncompleteMultipartUpload {
                    key: upload.key,
                    upload_id: upload.upload_id,
                }),
        );
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        upload_id_marker = page.next_upload_id_marker;
        if key_marker.is_none() && upload_id_marker.is_none() {
            return Err(ObjectStoreError::Backend(
                "truncated ListMultipartUploads response has no continuation marker".into(),
            ));
        }
    }
    Ok(found)
}

/// The path-style URL of `cfg`'s bucket, on its custom endpoint if it has one.
pub(crate) fn s3_bucket_url(cfg: &S3Config) -> Result<reqwest::Url, ObjectStoreError> {
    let endpoint = cfg.endpoint.as_ref().map_or_else(
        || format!("https://s3.{}.amazonaws.com/{}", cfg.region, cfg.bucket),
        |endpoint| format!("{}/{}", endpoint.trim_end_matches('/'), cfg.bucket),
    );
    reqwest::Url::parse(&endpoint)
        .map_err(|error| ObjectStoreError::InvalidConfig(format!("S3 endpoint: {error}")))
}

/// The HTTP client for direct S3 bucket requests.
///
/// A custom endpoint bypasses the system proxy, the way a local or in-cluster
/// S3 implementation needs.
pub(crate) fn s3_http_client(cfg: &S3Config) -> Result<reqwest::Client, ObjectStoreError> {
    let mut client = reqwest::Client::builder();
    client = client
        .timeout(cfg.request_timeout)
        .connect_timeout(cfg.connect_timeout);
    if cfg.endpoint.is_some() {
        client = client.no_proxy();
    }
    client.build().map_err(backend)
}

/// An [`ObjectStoreError::Backend`] carrying `error`'s message.
pub(crate) fn backend(error: impl std::fmt::Display) -> ObjectStoreError {
    ObjectStoreError::Backend(error.to_string())
}

/// Sends `request` and returns the response body.
///
/// A non-success status is an error naming `operation`, the status and the
/// body. Transient transport failures, 408, 429, and server errors retry
/// within `retry`'s attempt and wall-clock bounds.
pub(crate) async fn read_ok_body(
    request: reqwest::RequestBuilder,
    operation: &str,
    retry: &RetryConfig,
) -> Result<bytes::Bytes, ObjectStoreError> {
    let attempts = async move {
        let mut retries = 0;
        let mut backoff = retry.backoff.init_backoff;
        loop {
            let attempt = request.try_clone().ok_or_else(|| {
                backend(format!("{operation}: control-plane GET cannot be cloned"))
            })?;
            let (result, retryable) = match attempt.send().await {
                Ok(response) => {
                    let status = response.status();
                    let retryable_status = status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::REQUEST_TIMEOUT;
                    match response.bytes().await {
                        Ok(body) if status.is_success() => return Ok(body),
                        Ok(body) => (
                            Err(backend(format!(
                                "{operation} returned {status}: {}",
                                String::from_utf8_lossy(&body)
                            ))),
                            retryable_status,
                        ),
                        Err(error) => (
                            Err(backend(format!(
                                "{operation} returned {status}, but its body could not be read: {error}"
                            ))),
                            status.is_success() || retryable_status,
                        ),
                    }
                }
                Err(error) => {
                    let retryable = !error.is_builder() && !error.is_redirect();
                    (Err(backend(error)), retryable)
                }
            };
            if !retryable || retries >= retry.max_retries {
                return result;
            }
            retries += 1;
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(retry.backoff.max_backoff);
        }
    };
    tokio::time::timeout(retry.retry_timeout, attempts)
        .await
        .map_err(|_| backend(format!("{operation}: retry deadline exceeded")))?
}

/// Sends a SigV4-signed `GET` for `url` and decodes the XML response body.
///
/// `operation` names the S3 API call in the errors.
pub(crate) async fn signed_s3_get_xml<T: DeserializeOwned>(
    client: &reqwest::Client,
    credential: &AwsCredential,
    cfg: &S3Config,
    url: reqwest::Url,
    operation: &str,
) -> Result<T, ObjectStoreError> {
    let mut signed = http::Request::get(url.as_str())
        .body(HttpRequestBody::empty())
        .map_err(backend)?;
    AwsAuthorizer::new(credential, "s3", &cfg.region)
        .try_authorize(&mut signed, None)
        .map_err(backend)?;
    let body = read_ok_body(
        client.get(url).headers(signed.into_parts().0.headers),
        operation,
        &RetryConfig {
            max_retries: cfg.max_retries,
            retry_timeout: cfg.retry_timeout,
            ..RetryConfig::default()
        },
    )
    .await?;
    quick_xml::de::from_reader(body.as_ref())
        .map_err(|error| ObjectStoreError::Backend(format!("{operation}: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    };

    use assert2::check;
    use tokio::io::AsyncWriteExt as _;

    use super::*;
    use crate::test_support::{http_listener, read_request, respond, s3_config, serve_http_pages};

    async fn with_listing<T>(
        pages: Vec<(&'static str, &'static str, &'static str)>,
        prefix: Option<&str>,
        extract: impl FnOnce(Result<Vec<IncompleteMultipartUpload>, ObjectStoreError>) -> T,
    ) -> T {
        let pages = pages
            .into_iter()
            .map(|(request, status, body)| (request.into(), status, body))
            .collect();
        let (endpoint, server) = serve_http_pages(pages, true).await;
        let result = extract(list_s3_multipart_uploads(&config(endpoint), prefix).await);
        server.await.unwrap();
        result
    }

    async fn check_listing_error(
        pages: Vec<(&'static str, &'static str, &'static str)>,
        reason: &str,
    ) {
        let error = with_listing(pages, None, Result::unwrap_err).await;
        check!(error.to_string().contains(reason));
    }

    fn config(endpoint: String) -> S3Config {
        s3_config("bucket", endpoint)
    }

    #[test]
    fn parses_incomplete_uploads_and_pagination() {
        let page: ListResponse = quick_xml::de::from_str(
            r"<ListMultipartUploadsResult>
                <IsTruncated>true</IsTruncated>
                <NextKeyMarker>worm/b</NextKeyMarker>
                <NextUploadIdMarker>next</NextUploadIdMarker>
                <Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload>
                <Upload><Key>worm/b</Key><UploadId>two</UploadId></Upload>
            </ListMultipartUploadsResult>",
        )
        .unwrap();

        check!(page.is_truncated);
        check!(page.next_key_marker.as_deref() == Some("worm/b"));
        check!(page.next_upload_id_marker.as_deref() == Some("next"));
        check!(page.uploads.len() == 2);
        check!(page.uploads[0].key == "worm/a");
        check!(page.uploads[0].upload_id == "one");
    }

    #[tokio::test]
    async fn lists_incomplete_uploads_under_the_prefix() {
        let body = "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload></ListMultipartUploadsResult>";
        let uploads = with_listing(
            vec![(
                "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
                "200 OK",
                body,
            )],
            Some("worm/"),
            Result::unwrap,
        )
        .await;

        check!(
            uploads
                == vec![IncompleteMultipartUpload {
                    key: "worm/a".into(),
                    upload_id: "one".into(),
                }]
        );
    }

    #[tokio::test]
    async fn follows_multipart_pagination_markers() {
        let first = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>worm/a</NextKeyMarker><NextUploadIdMarker>one</NextUploadIdMarker><Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload></ListMultipartUploadsResult>";
        let second = "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>worm/b</Key><UploadId>two</UploadId></Upload></ListMultipartUploadsResult>";
        let uploads = with_listing(vec![
            (
                "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
                "200 OK",
                first,
            ),
            (
                "GET /bucket?uploads=&prefix=worm%2F&key-marker=worm%2Fa&upload-id-marker=one HTTP/1.1",
                "200 OK",
                second,
            ),
        ], Some("worm/"), Result::unwrap).await;

        check!(uploads.len() == 2);
        check!(uploads[1].key == "worm/b");
        check!(uploads[1].upload_id == "two");
    }

    #[tokio::test]
    async fn rejects_truncated_page_without_markers() {
        let body = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>";
        check_listing_error(
            vec![("GET /bucket?uploads= HTTP/1.1", "200 OK", body)],
            "no continuation marker",
        )
        .await;
    }

    #[tokio::test]
    async fn reports_unsuccessful_multipart_listing() {
        check_listing_error(
            vec![("GET /bucket?uploads= HTTP/1.1", "403 Forbidden", "denied")],
            "403 Forbidden: denied",
        )
        .await;
    }

    #[tokio::test]
    async fn a_request_that_never_reaches_the_store_is_a_backend_error() {
        let (listener, endpoint) = http_listener().await;
        drop(listener);

        let client = s3_http_client(&config(endpoint.clone())).unwrap();
        let error = read_ok_body(
            client.get(endpoint),
            "ListMultipartUploads",
            &RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert2::assert!(let ObjectStoreError::Backend(message) = error);
        check!(message.contains("error sending request"), "{message}");
    }

    #[tokio::test]
    async fn control_plane_retries_transient_statuses_only_within_the_attempt_budget() {
        for (max_retries, statuses, succeeds) in [
            (0, vec!["503 Service Unavailable"], false),
            (1, vec!["503 Service Unavailable", "200 OK"], true),
            (2, vec!["403 Forbidden"], false),
            (1, vec!["429 Too Many Requests", "200 OK"], true),
        ] {
            let pages = statuses
                .into_iter()
                .map(|status| ("GET / HTTP/1.1".into(), status, "result"))
                .collect();
            let (endpoint, server) = serve_http_pages(pages, false).await;
            let result = read_ok_body(
                reqwest::Client::new().get(endpoint),
                "test control-plane",
                &RetryConfig {
                    max_retries,
                    ..RetryConfig::default()
                },
            )
            .await;
            check!(result.is_ok() == succeeds);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_persistently_failing_control_plane_stops_at_the_retry_count() {
        let (listener, endpoint) = http_listener().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                observed.fetch_add(1, SeqCst);
                respond(&mut socket, "503 Service Unavailable", "retry").await;
            }
        });
        let result = read_ok_body(
            reqwest::Client::new().get(endpoint),
            "failing control-plane",
            &RetryConfig {
                max_retries: 1,
                ..RetryConfig::default()
            },
        )
        .await;
        server.abort();
        check!(result.is_err());
        check!(attempts.load(SeqCst) == 2);
    }

    #[tokio::test]
    async fn a_truncated_permission_error_preserves_status_and_does_not_retry() {
        let (listener, endpoint) = http_listener().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\ncontent-length: 100\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let error = read_ok_body(
            reqwest::Client::new().get(endpoint),
            "permission check",
            &RetryConfig {
                max_retries: 2,
                ..RetryConfig::default()
            },
        )
        .await
        .unwrap_err();
        server.await.unwrap();
        check!(
            error
                .to_string()
                .contains("permission check returned 403 Forbidden")
        );
    }
}
