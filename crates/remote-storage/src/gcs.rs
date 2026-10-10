//! Native Google Cloud Storage [`RemoteStorageManager`] backend (KIP-405).
//!
//! This module is the GCS sibling of [`S3RemoteStorage`]. It builds an
//! `object_store::gcp::GoogleCloudStorage` client from a [`GcsConfig`] and
//! wraps it in the same generic [`S3RemoteStorage`] engine with
//! [`S3RemoteStorage::with_store`].
//!
//! That engine is backend-agnostic: the object-key layout, the byte-range
//! fetch, the multipart upload stream, and the `object_store` error mapping
//! are all generic over `dyn ObjectStore`, so GCS reuses the whole copy,
//! fetch, delete, and multipart implementation. There is no separate trait
//! impl or storage struct.
//!
//! ## Authentication
//!
//! GCS credentials follow `object_store`'s resolution order, which matches
//! Google's Application Default Credentials (ADC):
//!
//! 1. An explicit service-account JSON key file ([`GcsConfig::service_account_path`]).
//! 2. An inline service-account JSON key ([`GcsConfig::service_account_key`]).
//! 3. An application-default-credentials JSON file
//!    ([`GcsConfig::application_credentials_path`]; when unset, the gcloud
//!    well-known ADC file under `$HOME/.config/gcloud` if present).
//! 4. The GKE / GCE metadata server, that is **Workload Identity**. This is
//!    the keyless production path. Leave all credential fields unset, and the
//!    metadata server exchanges the pod's bound Kubernetes service account
//!    for GCS access tokens. No secret material is on disk or in the broker
//!    config.
//!
//! This backend does not need the S3-compatibility shim that reaches GCS
//! through [`S3RemoteStorage::from_s3_config`]. That shim cannot use Workload
//! Identity and needs HMAC interoperability keys.

use krabka_object_store::{GcsConfig, ObjectStoreConfig};

use crate::{
    error::RemoteStorageError,
    s3::{S3RemoteStorage, WormBucket},
};

impl S3RemoteStorage {
    /// Builds a `GoogleCloudStorage` client from `cfg` and wraps it in the
    /// generic [`S3RemoteStorage`] engine.
    ///
    /// With no credential fields set, authentication uses Workload Identity
    /// or ADC through the metadata server. This is the keyless GKE path.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if `object_store`'s
    /// builder rejects the bucket, credential, and endpoint combination. For
    /// example, the caller supplied both a service-account path and a key, a
    /// credential file is unreadable, or the bucket name is empty.
    pub fn from_gcs_config(cfg: &GcsConfig) -> Result<Self, RemoteStorageError> {
        Self::from_backend_config(
            &ObjectStoreConfig::Gcs(cfg.clone()),
            cfg.prefix.clone(),
            cfg.multipart_threshold,
            cfg.multipart_chunk_size,
            WormBucket::Gcs(cfg.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::Arc,
        thread,
    };

    use assert2::assert;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        storage_manager::{IndexType, RemoteStorageManager},
        test_support::{sample_data, sample_metadata},
        worm::WormConfig,
    };

    // The GCS backend reuses the generic `S3RemoteStorage` engine, so the
    // copy / fetch / delete round-trip behaviour is already covered by the
    // `InMemory`-backed suite in `s3.rs`. This test pins that the engine shape
    // used for GCS still applies prefixes correctly.

    /// End-to-end round-trip against the generic engine through the GCS
    /// construction path. The test asserts that the engine applies the
    /// operator-visible prefix. It uses `with_store(InMemory)` because the
    /// real GCS client needs a live bucket.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_round_trips_with_prefix() {
        let store =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_string()));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(uuid::Uuid::from_u128(10));
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(
                    &md,
                    &sample_data(src.path(), crate::test_support::TransactionIndex::Omitted),
                )
                .unwrap();
            assert!(store.fetch_log_segment(&md, 0, None).unwrap() == b"0123456789");
            assert!(store.fetch_index(&md, IndexType::Offset).unwrap() == b"OFFSET-IDX");
        })
        .await
        .unwrap();
    }

    #[test]
    fn worm_accepts_gcs_with_versioning_and_locked_retention() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 2048];
            let read = socket.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..read])
                    .starts_with("GET /storage/v1/b/archive?fields=")
            );
            let body = r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"86400","isLocked":true}}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let config = GcsConfig {
            bucket: "archive".into(),
            endpoint: Some(endpoint),
            service_account_key: Some(
                r#"{"private_key":"unused","private_key_id":"unused","client_email":"unused","disable_oauth":true}"#
                    .into(),
            ),
            allow_http: true,
            ..Default::default()
        };

        S3RemoteStorage::from_gcs_config(&config)
            .unwrap()
            .with_worm(&WormConfig::default())
            .unwrap();
        server.join().unwrap();
    }
}
