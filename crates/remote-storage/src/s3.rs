//! [`S3RemoteStorage`] is an S3-compatible object-store
//! [`RemoteStorageManager`](crate::RemoteStorageManager), the KIP-405
//! production backend.
//!
//! This backend is built on the `object_store` crate, so it works against
//! any `S3-API` endpoint: AWS S3, `MinIO`, and Cloudflare R2. Google Cloud
//! Storage has a dedicated native backend. See
//! [`from_gcs_config`](S3RemoteStorage::from_gcs_config) in [`crate::gcs`],
//! which supports keyless GKE Workload Identity instead of the legacy
//! S3-compatibility/HMAC shim.
//!
//! The trait method bodies are synchronous, and they mirror Kafka's blocking
//! `RemoteStorageManager`. The broker drives them from `spawn_blocking`.
//! Inside them this crate blocks on the async `object_store` calls with the
//! current Tokio runtime handle. That handle is always present inside a
//! `spawn_blocking` worker that Tokio spawned.
//!
//! ## Object-key layout
//!
//! ```text
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.log
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.index
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.timeindex
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.snapshot
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.leader_epoch_checkpoint
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.txnindex
//! ```
//!
//! Keys mirror [`LocalTieredStorage`](crate::LocalTieredStorage)'s
//! directory layout so the two backends are observationally equivalent.
//!
//! ## Module layout
//!
//! This root holds the backend's state and its `Debug` rendering. Each child
//! module holds one concern: `client` builds and configures the store and
//! bridges its async calls, `keys` derives every object key, `copy`, `fetch`,
//! and `delete` hold one storage operation each, and `manager` binds those
//! three to the trait.

use krabka_object_store::{GcsConfig, ObjectStoreClient, S3Config};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

mod client;
mod copy;
mod delete;
mod fetch;
mod keys;
mod manager;

#[cfg(test)]
mod test_support;

use crate::worm::WormArchiver;

/// A [`RemoteStorageManager`](crate::RemoteStorageManager) backed by any
/// S3-compatible object store.
///
/// Construct it with [`S3RemoteStorage::with_store`], which accepts any
/// `ObjectStore` impl, for in-process tests. Use
/// [`S3RemoteStorage::from_s3_config`] for the production path, which builds
/// an `AmazonS3` client from credentials, endpoint, and bucket.
pub struct S3RemoteStorage {
    ops: ObjectStoreClient,
    /// Optional key prefix. The store joins it to every object key with
    /// `/`. This lets multiple Krabka clusters share a bucket safely.
    prefix: Option<String>,
    /// File-size threshold above which uploads switch to S3 multipart.
    multipart_threshold: ByteSize,
    /// Per-part size used by the multipart path.
    multipart_chunk_size: ByteSize,
    /// `Some` when this backend is a write-once archive.
    worm: Option<WormMode>,
    pub(crate) worm_bucket: WormBucket,
}

pub(crate) enum WormBucket {
    S3(S3Config),
    Gcs(GcsConfig),
    Unverified,
}

/// Resolved WORM state: the archiver (which owns the loaded key) plus the
/// credential posture.
struct WormMode {
    archiver: WormArchiver,
    write_only: bool,
    require_version_id: bool,
}

/// Lifts a raw byte count from [`krabka_object_store`]'s config layer into
/// the dimensioned domain. That config layer still uses primitive types.
///
/// The conversion saturates and does not wrap. A `usize` above `u64::MAX`
/// cannot occur on any target Krabka builds for.
pub(crate) fn size_from_usize(bytes: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(bytes).unwrap_or(u64::MAX))
}

impl std::fmt::Debug for S3RemoteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RemoteStorage")
            .field("prefix", &self.prefix)
            // Mode only. The archiver holds live private-key material, so it
            // never reaches a formatter.
            .field("worm", &self.worm.is_some())
            .field(
                "write_only",
                &self.worm.as_ref().is_some_and(|worm| worm.write_only),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_units::prelude::ByteSizeExt as _;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::{
        S3RemoteStorage, size_from_usize,
        test_support::{rsm, worm_config},
    };

    /// `usize::MAX` has no `u64` image on a hypothetical 128-bit target. The
    /// lift saturates and does not wrap to a tiny chunk size.
    #[test]
    fn size_from_usize_saturates_instead_of_wrapping() {
        check!(size_from_usize(0).bytes_usize() == 0);
        check!(size_from_usize(usize::MAX).bytes_usize() == usize::MAX);
    }

    #[test]
    fn storage_debug_is_nonempty() {
        // The S3RemoteStorage Debug impl must render something (a `fmt`
        // replaced with `Ok(())` would print nothing).
        let dbg = format!("{:?}", rsm(None));
        assert!(dbg.contains("S3RemoteStorage"));
    }

    #[test]
    fn worm_debug_reports_mode_without_leaking_the_key() {
        let keys = TempDir::new().unwrap();
        let cfg = worm_config(keys.path(), crate::s3::test_support::WormAccess::WriteOnly);
        let key_path = cfg.signing_key_path.clone().unwrap();
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_worm_unchecked(&cfg)
            .unwrap();

        let rendered = format!("{store:?}");
        check!(rendered.contains("worm: true"));
        check!(rendered.contains("write_only: true"));
        // Neither the key nor the path to it reaches a log line.
        check!(!rendered.contains(&key_path.display().to_string()));
        check!(!rendered.contains(&hex::encode(std::fs::read(&key_path).unwrap())));

        let plain = format!("{:?}", rsm(None));
        check!(plain.contains("worm: false"));
        check!(plain.contains("write_only: false"));
    }
}
