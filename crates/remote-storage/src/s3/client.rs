//! Construction and configuration of an [`S3RemoteStorage`], and the bridge
//! that drives the backend's async object-store calls from a synchronous
//! trait method.
//!
//! A backend comes either from an arbitrary `ObjectStore` handle through
//! [`S3RemoteStorage::with_store`] or from an [`S3Config`] through
//! [`S3RemoteStorage::from_s3_config`]. The chained builders
//! [`S3RemoteStorage::with_multipart_tuning`] and
//! [`S3RemoteStorage::with_worm`] then set the upload tuning and the archive
//! posture.

use std::sync::Arc;

use krabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectStoreClient,
    ObjectStoreConfig, ObjectStoreError, S3Config, build_object_store, verify_gcs_worm_bucket,
    verify_s3_worm_bucket,
};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};
use object_store::ObjectStore;

use super::{S3RemoteStorage, WormBucket, WormMode, size_from_usize};
use crate::{
    error::RemoteStorageError,
    worm::{WormArchiver, WormConfig},
};

impl S3RemoteStorage {
    /// Wraps an arbitrary `ObjectStore`, for example
    /// `object_store::memory::InMemory` for tests. Use
    /// [`Self::from_s3_config`] for the production S3 path. Multipart
    /// tuning falls back to the [`DEFAULT_MULTIPART_THRESHOLD`] and
    /// [`DEFAULT_MULTIPART_CHUNK_SIZE`] constants. Call
    /// [`Self::with_multipart_tuning`] to override them in tests.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self {
            ops: ObjectStoreClient::new(store),
            prefix,
            multipart_threshold: ByteSize::from_bytes(DEFAULT_MULTIPART_THRESHOLD),
            multipart_chunk_size: size_from_usize(DEFAULT_MULTIPART_CHUNK_SIZE),
            worm: None,
            worm_bucket: WormBucket::Unverified,
        }
    }

    /// Puts this backend into WORM archive mode.
    ///
    /// Every copy then seals a signed, chained `.manifest` beside the segment,
    /// every delete is refused, and a `write_only` archive refuses remote
    /// fetches as well.
    ///
    /// # Errors
    ///
    /// Returns an error when the bucket policy cannot be confirmed or `cfg`'s
    /// signing key cannot be loaded.
    pub fn with_worm(self, cfg: &WormConfig) -> Result<Self, RemoteStorageError> {
        match &self.worm_bucket {
            WormBucket::S3(s3) => {
                if !s3.conditional_put {
                    return Err(RemoteStorageError::InvalidArgument(
                        "WORM mode requires remote_storage.s3.conditional_put = true".into(),
                    ));
                }
            }
            WormBucket::Gcs(_) => {}
            WormBucket::Unverified => {
                return Err(RemoteStorageError::InvalidArgument(
                    "WORM mode requires an object-store configuration so bucket versioning and \
                     retention can be confirmed"
                        .into(),
                ));
            }
        }
        verify_worm_bucket(&self.worm_bucket)?;
        self.enable_worm(cfg, true)
    }

    fn enable_worm(
        mut self,
        cfg: &WormConfig,
        require_version_id: bool,
    ) -> Result<Self, RemoteStorageError> {
        self.worm = Some(WormMode {
            archiver: WormArchiver::from_config(cfg)?,
            write_only: cfg.write_only,
            require_version_id,
        });
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn with_worm_unchecked(self, cfg: &WormConfig) -> Result<Self, RemoteStorageError> {
        self.enable_worm(cfg, false)
    }

    /// Overrides the multipart threshold and chunk size. Returns `self` for
    /// chained calls. Tests use this to force the multipart path on small
    /// fixtures. Production usually keeps the defaults.
    #[must_use]
    pub fn with_multipart_tuning(mut self, threshold: ByteSize, chunk_size: ByteSize) -> Self {
        self.multipart_threshold = threshold;
        self.multipart_chunk_size = chunk_size;
        self
    }

    pub(crate) fn from_backend_config(
        config: &ObjectStoreConfig,
        prefix: Option<String>,
        threshold: u64,
        chunk_size: usize,
        worm_bucket: WormBucket,
    ) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(config)
            .map_err(|error| RemoteStorageError::InvalidArgument(error.to_string()))?;
        let mut storage = Self::with_store(store, prefix)
            .with_multipart_tuning(ByteSize::from_bytes(threshold), size_from_usize(chunk_size));
        storage.worm_bucket = worm_bucket;
        Ok(storage)
    }

    /// Builds an `AmazonS3` client from `cfg` and wraps it.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if `object_store`'s
    /// builder rejects the bucket, region, and endpoint combination.
    pub fn from_s3_config(cfg: &S3Config) -> Result<Self, RemoteStorageError> {
        Self::from_backend_config(
            &ObjectStoreConfig::S3(cfg.clone()),
            cfg.prefix.clone(),
            cfg.multipart_threshold,
            cfg.multipart_chunk_size,
            WormBucket::S3(cfg.clone()),
        )
    }

    /// Runs an async [`ObjectOps`](krabka_object_store::ObjectOps) call to
    /// completion on the current Tokio runtime. Sync trait callers reach this
    /// through `spawn_blocking`, where `Handle::current()` is always
    /// available. The `block_on` bridge lives here, never in the substrate.
    pub(super) fn block_os<T, F>(fut: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ObjectStoreError::Backend(
                "S3RemoteStorage requires an active Tokio runtime; call from spawn_blocking".into(),
            )
        })?;
        tokio::task::block_in_place(|| handle.block_on(fut))
    }
}

fn verify_worm_bucket(bucket: &WormBucket) -> Result<(), RemoteStorageError> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(ObjectStoreError::from)?
                    .block_on(async {
                        match bucket {
                            WormBucket::S3(cfg) => verify_s3_worm_bucket(cfg).await,
                            WormBucket::Gcs(cfg) => verify_gcs_worm_bucket(cfg).await,
                            WormBucket::Unverified => unreachable!("checked by with_worm"),
                        }
                    })
            })
            .join()
            .map_err(|_| ObjectStoreError::Backend("WORM bucket check panicked".into()))?
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_object_store::{
        DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, S3Config,
    };
    use krabka_units::prelude::{ByteSizeExt as _, kibibytes, mebibytes};
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::S3RemoteStorage;
    use crate::{s3::test_support::worm_config, worm::WormConfig};

    /// The multipart tunables cross two seams: in from
    /// [`krabka_object_store`]'s primitive config, and back out to the
    /// primitive-typed `ObjectOps` substrate. Both must be lossless for every
    /// size the config can express, so a mis-scaled conversion, such as a
    /// stray `* 1024`, cannot silently change when a segment switches to
    /// multipart.
    #[test]
    fn multipart_tuning_round_trips_through_the_primitive_seams() {
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None);
        check!(store.multipart_threshold.bytes_u64() == DEFAULT_MULTIPART_THRESHOLD);
        check!(store.multipart_chunk_size.bytes_usize() == DEFAULT_MULTIPART_CHUNK_SIZE);

        let tuned = store.with_multipart_tuning(mebibytes(64), kibibytes(512));
        check!(tuned.multipart_threshold.bytes_u64() == 64 * 1024 * 1024);
        check!(tuned.multipart_chunk_size.bytes_usize() == 512 * 1024);
    }

    #[test]
    fn worm_refuses_disabled_conditional_put_before_contacting_s3() {
        let keys = TempDir::new().unwrap();
        let s3 = S3Config {
            bucket: "archive".into(),
            region: "us-east-1".into(),
            conditional_put: false,
            ..Default::default()
        };

        let error = S3RemoteStorage::from_s3_config(&s3)
            .unwrap()
            .with_worm(&worm_config(
                keys.path(),
                crate::s3::test_support::WormAccess::ReadWrite,
            ))
            .unwrap_err();

        check!(error.to_string().contains("conditional_put = true"));
    }

    #[test]
    fn worm_refuses_an_opaque_store_without_policy_config() {
        let error = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_worm(&WormConfig::default())
            .unwrap_err();

        check!(error.to_string().contains("object-store configuration"));
    }
}
