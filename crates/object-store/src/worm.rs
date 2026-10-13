//! S3 control-plane checks required before enabling a WORM writer.

use {
    crate::{
        GcsConfig, ObjectStoreError, S3Config,
        build::{build_gcs_store, build_s3_store},
        multipart::{backend, read_ok_body, s3_bucket_url, s3_http_client, signed_s3_get_xml},
    },
    object_store::RetryConfig,
    serde::{Deserialize, de::DeserializeOwned},
};

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VersioningConfiguration {
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectLockConfiguration {
    object_lock_enabled: Option<String>,
    rule: Option<ObjectLockRule>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectLockRule {
    default_retention: Option<DefaultRetention>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DefaultRetention {
    mode: Option<String>,
    days: Option<u64>,
    years: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsBucketPolicy {
    versioning: Option<GcsVersioning>,
    retention_policy: Option<GcsRetentionPolicy>,
}

#[derive(Deserialize)]
struct GcsVersioning {
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsRetentionPolicy {
    retention_period: Option<String>,
    is_locked: Option<bool>,
}

/// Confirms that an S3 bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Names the failed control-plane request or the concrete missing policy.
pub async fn verify_s3_worm_bucket(cfg: &S3Config) -> Result<(), ObjectStoreError> {
    let versioning: VersioningConfiguration = get_bucket_xml(cfg, "versioning").await?;
    let lock: ObjectLockConfiguration = get_bucket_xml(cfg, "object-lock").await?;
    validate_s3_policy(cfg, &versioning, lock)
}

fn validate_s3_policy(
    cfg: &S3Config,
    versioning: &VersioningConfiguration,
    lock: ObjectLockConfiguration,
) -> Result<(), ObjectStoreError> {
    if versioning.status.as_deref() != Some("Enabled") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires versioning Enabled, found {}",
            cfg.bucket,
            versioning.status.as_deref().unwrap_or("unset")
        )));
    }

    if lock.object_lock_enabled.as_deref() != Some("Enabled") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires Object Lock Enabled",
            cfg.bucket
        )));
    }
    let retention = lock
        .rule
        .and_then(|rule| rule.default_retention)
        .ok_or_else(|| {
            ObjectStoreError::InvalidConfig(format!(
                "WORM bucket `{}` has no Object Lock default retention",
                cfg.bucket
            ))
        })?;
    if retention.mode.as_deref() != Some("COMPLIANCE") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` default retention must use COMPLIANCE mode, found {}",
            cfg.bucket,
            retention.mode.as_deref().unwrap_or("unset")
        )));
    }
    if retention.days.unwrap_or(0) == 0 && retention.years.unwrap_or(0) == 0 {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` default retention has no positive Days or Years",
            cfg.bucket
        )));
    }
    Ok(())
}

/// Confirms that a GCS bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Names the failed control-plane request or the concrete missing policy.
pub async fn verify_gcs_worm_bucket(cfg: &GcsConfig) -> Result<(), ObjectStoreError> {
    let store = build_gcs_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let endpoint = cfg
        .endpoint
        .as_deref()
        .unwrap_or("https://storage.googleapis.com");
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|error| ObjectStoreError::InvalidConfig(format!("GCS endpoint: {error}")))?;
    url.path_segments_mut()
        .map_err(|()| ObjectStoreError::InvalidConfig("GCS endpoint cannot be a base URL".into()))?
        .pop_if_empty()
        .extend(["storage", "v1", "b", &cfg.bucket]);
    url.query_pairs_mut()
        .append_pair("fields", "versioning,retentionPolicy");
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .connect_timeout(cfg.connect_timeout)
        .build()
        .map_err(backend)?;
    let mut request = client.get(url);
    if !credential.bearer.is_empty() {
        request = request.bearer_auth(&credential.bearer);
    }
    let body = read_ok_body(request, "GCS Buckets.get", &RetryConfig {
        max_retries: cfg.max_retries,
        retry_timeout: cfg.retry_timeout,
        ..RetryConfig::default()
    })
    .await?;
    let policy: GcsBucketPolicy = serde_json::from_slice(&body)
        .map_err(|error| ObjectStoreError::Backend(format!("GCS Buckets.get: {error}")))?;
    validate_gcs_policy(cfg, policy)
}

fn validate_gcs_policy(cfg: &GcsConfig, policy: GcsBucketPolicy) -> Result<(), ObjectStoreError> {
    if policy.versioning.and_then(|v| v.enabled) != Some(true) {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires GCS versioning enabled",
            cfg.bucket
        )));
    }
    let retention = policy.retention_policy.ok_or_else(|| {
        ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` has no GCS retention policy",
            cfg.bucket
        ))
    })?;
    if retention.is_locked != Some(true) {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires a locked GCS retention policy",
            cfg.bucket
        )));
    }
    let seconds = retention
        .retention_period
        .as_deref()
        .and_then(|period| period.parse::<u64>().ok())
        .unwrap_or(0);
    if seconds == 0 {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` GCS retention policy has no positive retention period",
            cfg.bucket
        )));
    }
    Ok(())
}

async fn get_bucket_xml<T: DeserializeOwned>(
    cfg: &S3Config,
    api: &str,
) -> Result<T, ObjectStoreError> {
    let operation = match api {
        "versioning" => "GetBucketVersioning",
        "object-lock" => "GetObjectLockConfiguration",
        _ => "GetBucketConfiguration",
    };
    let store = build_s3_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let mut url = s3_bucket_url(cfg)?;
    url.query_pairs_mut().append_pair(api, "");
    signed_s3_get_xml(&s3_http_client(cfg)?, &credential, cfg, url, operation).await
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{accept_http_request, http_listener, s3_config, serve_http_pages},
        assert2::check,
        std::time::Duration,
    };

    const GCS_TEST_KEY: &str = r#"{
        "private_key":"unused",
        "private_key_id":"unused",
        "client_email":"unused",
        "disable_oauth":true
    }"#;

    #[tokio::test]
    async fn requires_versioning_and_positive_compliance_retention() {
        let pages = [
            (
                "versioning",
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            ),
            (
                "object-lock",
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>",
            ),
        ].into_iter().map(|(query, body)| (format!("GET /bucket?{query}= HTTP/1.1"), "200 OK", body)).collect();
        let (endpoint, server) = serve_http_pages(pages, true).await;
        let cfg = s3_config("bucket", endpoint);

        verify_s3_worm_bucket(&cfg).await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn s3_policy_rejects_each_missing_protection() {
        let cfg = S3Config {
            bucket: "bucket".into(),
            ..Default::default()
        };
        let versioning = |status: Option<&str>| VersioningConfiguration {
            status: status.map(str::to_string),
        };
        let lock = |enabled: Option<&str>, mode: Option<&str>, days| ObjectLockConfiguration {
            object_lock_enabled: enabled.map(str::to_string),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: mode.map(str::to_string),
                    days,
                    years: None,
                }),
            }),
        };

        for (versioning, lock, reason) in [
            (
                versioning(Some("Suspended")),
                lock(Some("Enabled"), Some("COMPLIANCE"), Some(1)),
                "versioning Enabled",
            ),
            (
                versioning(Some("Enabled")),
                lock(None, Some("COMPLIANCE"), Some(1)),
                "Object Lock Enabled",
            ),
            (
                versioning(Some("Enabled")),
                ObjectLockConfiguration {
                    object_lock_enabled: Some("Enabled".into()),
                    rule: None,
                },
                "no Object Lock default retention",
            ),
            (
                versioning(Some("Enabled")),
                lock(Some("Enabled"), Some("GOVERNANCE"), Some(1)),
                "COMPLIANCE",
            ),
            (
                versioning(Some("Enabled")),
                lock(Some("Enabled"), Some("COMPLIANCE"), Some(0)),
                "no positive Days or Years",
            ),
        ] {
            let error = validate_s3_policy(&cfg, &versioning, lock).unwrap_err();
            check!(error.to_string().contains(reason), "{reason}: {error}");
        }
    }

    #[test]
    fn gcs_policy_rejects_each_missing_protection() {
        let cfg = GcsConfig {
            bucket: "bucket".into(),
            ..Default::default()
        };
        for (json, reason) in [
            (
                r#"{"versioning":{"enabled":false},"retentionPolicy":{"retentionPeriod":"1","isLocked":true}}"#,
                "versioning enabled",
            ),
            (
                r#"{"versioning":{"enabled":true}}"#,
                "no GCS retention policy",
            ),
            (
                r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"1","isLocked":false}}"#,
                "locked GCS retention policy",
            ),
            (
                r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"0","isLocked":true}}"#,
                "no positive retention period",
            ),
        ] {
            let policy = serde_json::from_str(json).unwrap();
            let error = validate_gcs_policy(&cfg, policy).unwrap_err();
            check!(error.to_string().contains(reason), "{reason}: {error}");
        }
    }

    /// Serves one canned `Buckets.get` body to one client, checking on the
    /// way past that the request is the control-plane call this module claims
    /// to make. Returns the endpoint to point a `GcsConfig` at, and the task
    /// to join once the check has run.
    async fn serve_bucket_policy(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        serve_http_pages(
            vec![("GET /storage/v1/b/bucket?fields=".into(), "200 OK", body)],
            false,
        )
        .await
    }

    fn gcs_cfg(endpoint: String) -> GcsConfig {
        GcsConfig {
            bucket: "bucket".into(),
            endpoint: Some(endpoint),
            service_account_key: Some(GCS_TEST_KEY.into()),
            allow_http: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn confirms_gcs_versioning_and_locked_retention() {
        let (endpoint, server) = serve_bucket_policy(
            r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"86400","isLocked":true}}"#,
        )
        .await;

        verify_gcs_worm_bucket(&gcs_cfg(endpoint)).await.unwrap();
        server.await.unwrap();
    }

    /// The refusal half of the same control-plane call. `validate_gcs_policy`
    /// covers the decision; this covers the whole `Buckets.get` round trip, so
    /// a policy a real bucket reports as non-compliant fails archive startup
    /// rather than being lost between the request and the check.
    #[tokio::test]
    async fn refuses_a_gcs_bucket_whose_policy_is_not_worm() {
        for (body, reason) in [
            (
                r#"{"versioning":{"enabled":false},"retentionPolicy":{"retentionPeriod":"86400","isLocked":true}}"#,
                "versioning enabled",
            ),
            (
                r#"{"versioning":{"enabled":true}}"#,
                "no GCS retention policy",
            ),
            (
                r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"86400","isLocked":false}}"#,
                "locked GCS retention policy",
            ),
        ] {
            let (endpoint, server) = serve_bucket_policy(body).await;

            let error = verify_gcs_worm_bucket(&gcs_cfg(endpoint))
                .await
                .unwrap_err();
            check!(error.to_string().contains(reason), "{reason}: {error}");
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn bucket_checks_obey_request_and_retry_deadlines() {
        for (gcs, retry_deadline) in [(false, false), (true, false), (false, true), (true, true)] {
            let (listener, endpoint) = http_listener().await;
            let server = tokio::spawn(async move {
                let _socket = accept_http_request(listener).await;
                std::future::pending::<()>().await;
            });
            let request_timeout = if retry_deadline {
                Duration::from_secs(2)
            } else {
                Duration::from_millis(25)
            };
            let retry_timeout = if retry_deadline {
                Duration::from_millis(25)
            } else {
                Duration::from_secs(2)
            };
            let result = tokio::time::timeout(Duration::from_secs(1), async {
                if gcs {
                    let mut cfg = gcs_cfg(endpoint);
                    cfg.request_timeout = request_timeout;
                    cfg.retry_timeout = retry_timeout;
                    cfg.max_retries = 0;
                    verify_gcs_worm_bucket(&cfg).await
                } else {
                    let mut cfg = s3_config("bucket", endpoint);
                    cfg.request_timeout = request_timeout;
                    cfg.retry_timeout = retry_timeout;
                    cfg.max_retries = 0;
                    verify_s3_worm_bucket(&cfg).await
                }
            })
            .await;
            server.abort();
            let error = result
                .expect("configured deadline must expire before the test guard")
                .unwrap_err();
            check!(matches!(error, ObjectStoreError::Backend(_)));
            if retry_deadline {
                check!(error.to_string().contains("retry deadline exceeded"));
            }
        }
    }
}
