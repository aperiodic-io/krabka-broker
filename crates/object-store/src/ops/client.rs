//! The concrete [`ObjectOps`] implementation over an `object_store` handle.
//!
//! [`ObjectStoreClient`] is the crate's only implementation of the trait, so
//! this file is where the operation logic actually lives: the
//! multipart-threshold branch in `put_from_path`, the optional whole-object
//! `SHA-256` digest, and the translation of every backend call into an
//! [`ObjectStoreError`].

use std::sync::Arc;

use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::{
    GetOptions, GetRange, MultipartUpload, ObjectMeta, ObjectStoreExt as _, PutOptions, PutPayload,
    PutResult, UploadPart, WriteMultipart, path::Path,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

#[cfg(test)]
mod tests;

use crate::{
    error::ObjectStoreError,
    ops::{ObjectOps, ObjectStream, PutOutcome, PutRequest},
    read::read_capped,
};

// Bound the payload retained by multipart tasks to eight chunks per upload.
const MULTIPART_MAX_CONCURRENCY: usize = 8;

/// The single concrete [`ObjectOps`] implementation.
///
/// It wraps any `object_store::ObjectStore` handle, for example a handle from
/// [`build_object_store`](crate::build_object_store), or an
/// `object_store::memory::InMemory` handle in tests.
#[derive(Clone)]
pub struct ObjectStoreClient {
    inner: Arc<dyn object_store::ObjectStore>,
}

#[derive(Debug)]
struct AbortOnDrop {
    upload: Option<Box<dyn MultipartUpload>>,
    key: Path,
}

impl AbortOnDrop {
    fn new(upload: Box<dyn MultipartUpload>, key: &Path) -> Self {
        Self {
            upload: Some(upload),
            key: key.clone(),
        }
    }

    fn upload(&mut self) -> &mut Box<dyn MultipartUpload> {
        self.upload.as_mut().expect("upload is still active")
    }
}

#[async_trait::async_trait]
impl MultipartUpload for AbortOnDrop {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload().put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.upload().complete().await;
        if result.is_ok() {
            self.upload.take();
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let Some(mut upload) = self.upload.take() else {
            return Ok(());
        };
        let key = self.key.clone();
        tokio::spawn(async move {
            let result = upload.abort().await;
            if let Err(error) = &result {
                tracing::warn!(%key, %error, "failed to abort multipart upload");
            }
            result
        })
        .await
        .map_err(|error| object_store::Error::Generic {
            store: "multipart upload",
            source: Box::new(error),
        })?
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let Some(mut upload) = self.upload.take() else {
            return;
        };
        let key = self.key.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(%key, "cannot abort multipart upload outside a Tokio runtime");
            return;
        };
        runtime.spawn(async move {
            if let Err(error) = upload.abort().await {
                tracing::warn!(%key, %error, "failed to abort multipart upload");
            }
        });
    }
}

impl ObjectStoreClient {
    /// Wrap an existing object-store handle.
    #[must_use]
    pub fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner }
    }
}

#[krabka_macros::object_ops]
#[async_trait::async_trait]
impl ObjectOps for ObjectStoreClient {
    async fn put(
        &self,
        key: &Path,
        bytes: Bytes,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError> {
        let create_precondition = matches!(req.mode, object_store::PutMode::Create);
        let size_bytes = bytes.len() as u64;
        let sha256 = req.digest.then(|| Sha256::digest(&bytes).into());
        let result = self
            .inner
            .put_opts(
                key,
                PutPayload::from_bytes(bytes),
                PutOptions {
                    mode: req.mode,
                    ..Default::default()
                },
            )
            .await?;
        Ok(PutOutcome {
            size_bytes,
            sha256,
            e_tag: result.e_tag,
            version_id: result.version,
            create_precondition,
        })
    }

    put_from_path!({
        if chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multipart chunk size must be greater than zero",
            )
            .into());
        }

        let len = tokio::fs::metadata(src).await?.len();
        if len < threshold {
            return self.put(key, tokio::fs::read(src).await?.into(), req).await;
        }
        // `req.mode` cannot reach multipart completion. Atomically reserve the
        // key with a retained empty version so only its winner may proceed.
        // A failed upload deliberately leaves the reservation behind: WORM
        // safety wins over retrying a key whose ownership is now ambiguous.
        if matches!(req.mode, object_store::PutMode::Create) {
            self.inner
                .put_opts(
                    key,
                    PutPayload::from_bytes(Bytes::new()),
                    PutOptions {
                        mode: object_store::PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await?;
        }
        let upload = Box::new(AbortOnDrop::new(self.inner.put_multipart(key).await?, key));
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
        let mut file = tokio::fs::File::open(src).await?;
        let mut buf = vec![0u8; chunk_size];
        let mut hasher = req.digest.then(Sha256::new);
        let mut size_bytes = 0u64;
        loop {
            writer.wait_for_capacity(MULTIPART_MAX_CONCURRENCY).await?;
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&buf[..n]);
            }
            size_bytes += n as u64;
            writer.write(&buf[..n]);
        }
        let result = writer.finish().await?;
        let expected_sha256 = hasher.map(|hasher| hasher.finalize().into());
        let mut e_tag = result.e_tag;
        let mut version_id = result.version;
        if matches!(req.mode, object_store::PutMode::Create) {
            let meta = self.inner.head(key).await?;
            if version_id.is_some() && meta.version != version_id {
                return Err(ObjectStoreError::Precondition {
                    key: key.clone(),
                    reason: format!(
                        "completed multipart version {:?} does not match HEAD version {:?}",
                        version_id, meta.version
                    ),
                });
            }
            version_id = meta.version;
            if let Some(expected) = expected_sha256 {
                let options = GetOptions {
                    version: version_id.clone(),
                    ..GetOptions::default()
                };
                let mut stored = self.inner.get_opts(key, options).await?.into_stream();
                let mut hasher = Sha256::new();
                while let Some(chunk) = stored.try_next().await? {
                    hasher.update(&chunk);
                }
                let actual: [u8; 32] = hasher.finalize().into();
                if expected != actual {
                    return Err(ObjectStoreError::Precondition {
                        key: key.clone(),
                        reason: "completed multipart object does not match the uploaded SHA-256"
                            .into(),
                    });
                }
            }
            e_tag = meta.e_tag;
        }
        Ok(PutOutcome {
            size_bytes,
            sha256: expected_sha256,
            e_tag,
            version_id,
            create_precondition: false,
        })
    });

    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError> {
        Ok(self.inner.get(key).await?.bytes().await?)
    }

    async fn get_capped(&self, key: &Path, max_bytes: u64) -> Result<Bytes, ObjectStoreError> {
        read_capped(&self.inner, key, max_bytes).await
    }

    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError> {
        let opts = GetOptions {
            range: Some(range),
            ..Default::default()
        };
        Ok(self.inner.get_opts(key, opts).await?.bytes().await?)
    }

    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(self.inner.head(key).await?)
    }

    async fn list(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError> {
        self.list_stream(prefix).try_collect::<Vec<_>>().await
    }

    fn list_stream(&self, prefix: Option<Path>) -> ObjectStream {
        // `object_store`'s listing owns its paging state rather than borrowing
        // the store, so the mapped stream is `'static` without cloning the
        // handle into it.
        self.inner
            .list(prefix.as_ref())
            .map(|result| result.map_err(ObjectStoreError::from))
            .boxed()
    }

    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await?;
        Ok(())
    }
}
