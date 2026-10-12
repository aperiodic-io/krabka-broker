//! The shared object-store operation surface.
//!
//! This module holds an async, mockable [`ObjectOps`] trait and its single
//! concrete implementation [`ObjectStoreClient`] over `object_store`. Consumers
//! route their put, get, delete, and multipart calls through it, so the
//! operation logic lives in one place. That logic includes the
//! multipart-threshold branch and the `object_store::Error` to
//! [`ObjectStoreError`] mapping.

use bytes::Bytes;
use futures_util::stream::BoxStream;
/// Precondition for a write, re-exported so callers need not depend on
/// `object_store` directly.
pub use object_store::PutMode;
use object_store::{GetRange, ObjectMeta, path::Path};

mod client;

#[cfg(test)]
mod tests;

pub use self::client::ObjectStoreClient;
use crate::error::ObjectStoreError;

/// A listing delivered one object at a time.
///
/// The stream is `'static` because the backend's own listing is: it owns the
/// paging state rather than borrowing the store, so a caller can hold the
/// stream past the borrow it was created from.
pub type ObjectStream = BoxStream<'static, Result<ObjectMeta, ObjectStoreError>>;

/// How one put should behave.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutRequest {
    /// Precondition for the write. Defaults to `PutMode::Overwrite`.
    pub mode: PutMode,
    /// Whether to compute a SHA-256 digest of the payload during upload.
    /// Defaults to `false`; only the WORM archive path needs it, and hashing
    /// every tiered byte is not free.
    pub digest: bool,
}

/// What a put produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// Bytes written.
    pub size_bytes: u64,
    /// Whole-object SHA-256, present only when `PutRequest::digest` was set.
    pub sha256: Option<[u8; 32]>,
    /// Backend entity tag, when the backend returned one.
    pub e_tag: Option<String>,
    /// Version id on a versioned bucket, when the backend returned one.
    pub version_id: Option<String>,
    /// Whether the backend applied an atomic create precondition to this put.
    /// Multipart uploads return `false`: an atomic empty-object reservation
    /// serializes creators, while bucket retention protects the completed
    /// version.
    pub create_precondition: bool,
}

/// Async object-store operations. The trait is `Send + Sync`, so tasks can
/// share it.
///
/// The trait stays dyn-safe and `#[automock]`-able. It expresses multipart
/// upload as [`ObjectOps::put_from_path`] over a filesystem path, not over a
/// generic reader. The trait thus mocks cleanly for mutation-testable IO
/// decision logic.
#[krabka_macros::object_ops]
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ObjectOps: Send + Sync {
    /// Single-PUT an in-memory payload under the preconditions in `req`.
    async fn put(
        &self,
        key: &Path,
        bytes: Bytes,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError>;

    /// Upload a local file. The method uses single-PUT below `threshold`
    /// bytes, and streaming multipart in `chunk_size` parts at or above
    /// `threshold`.
    put_from_path!();

    /// Fetch a whole object.
    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a whole object with a byte limit enforced before buffering its body.
    ///
    /// Bind the GET to the HEAD version or etag when the backend provides one,
    /// and reject an oversized response while streaming even without either.
    async fn get_capped(&self, key: &Path, max_bytes: u64) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError>;

    /// Fetch object metadata, such as the size and the etag.
    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError>;

    /// List objects under an optional prefix, collecting the whole listing.
    ///
    /// A prefix holding millions of keys costs that much memory here. Prefer
    /// [`ObjectOps::list_stream`] whenever the caller can consume the listing
    /// as it arrives.
    async fn list(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError>;

    /// List objects under an optional prefix as a stream, one object at a
    /// time, so a caller never holds the whole listing at once.
    fn list_stream(&self, prefix: Option<Path>) -> ObjectStream;

    /// Delete an object.
    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError>;
}
