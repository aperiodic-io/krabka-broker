//! Unified object-store construction for Krabka.
//!
//! Krabka's KIP-405 tiered storage, `krabka-remote-storage`, and the
//! observability blockstore, `krabka-blockstore`, share `krabka-object-store`.
//!
//! The scope is the object-store access and plumbing layer only. The crate
//! turns a typed `ObjectStoreConfig` into an `object_store::ObjectStore`
//! handle. The data representation stays in the respective consumer crates.
//! That representation is verbatim Kafka segment bytes or Parquet blocks.
//!
//! On `wasm32-wasip1` the crate has only the in-memory backend. The S3, GCS
//! and local-filesystem backends need an HTTP client stack or a filesystem
//! walk that `object_store` does not build for that target, so their
//! configurations fail with [`ObjectStoreError::InvalidConfig`] there, and so
//! do the WORM bucket checks. The multipart-upload listing is not exported.

use object_store as object_store_api;

#[cfg(all(feature = "object-store-013", not(target_family = "wasm")))]
pub mod v013;

#[cfg(feature = "clap")]
mod archive_args;

mod build;
mod config;
mod error;
pub mod fault;
#[cfg(not(target_family = "wasm"))]
mod multipart;
mod ops;
mod read;
#[cfg(target_family = "wasm")]
mod unavailable;
#[cfg(not(target_family = "wasm"))]
mod worm;

#[cfg(feature = "clap")]
pub use archive_args::{
    ArchiveArgs, ArchiveArgsError, DEFAULT_S3_REGION, normalize_prefix, prefixed_key,
};
#[cfg(not(target_family = "wasm"))]
pub use multipart::{IncompleteMultipartUpload, list_s3_multipart_uploads};
#[cfg(target_family = "wasm")]
pub use unavailable::{verify_gcs_worm_bucket, verify_s3_worm_bucket};
#[cfg(not(target_family = "wasm"))]
pub use worm::{verify_gcs_worm_bucket, verify_s3_worm_bucket};
pub use {
    build::build_object_store,
    config::{
        DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_MULTIPART_CHUNK_SIZE,
        DEFAULT_MULTIPART_THRESHOLD, DEFAULT_REQUEST_TIMEOUT, DEFAULT_RETRY_TIMEOUT, GcsConfig,
        ObjectStoreConfig, S3Config,
    },
    error::ObjectStoreError,
    ops::{ObjectOps, ObjectStoreClient, PutMode, PutOutcome, PutRequest},
    read::read_capped,
};

#[cfg(all(test, not(target_family = "wasm")))]
mod test_support;
