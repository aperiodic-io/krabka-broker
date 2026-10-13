//! Error type shared by both tiered-storage SPIs.

use crate::{
    metadata::{
        RemoteLogSegmentId, RemoteLogSegmentState, RemotePartitionDeleteState, TopicIdPartition,
    },
    worm::WormError,
};

/// Errors raised by [`RemoteStorageManager`](crate::RemoteStorageManager)
/// and [`RemoteLogMetadataManager`](crate::RemoteLogMetadataManager)
/// implementations.
#[derive(Debug, thiserror::Error)]
pub enum RemoteStorageError {
    /// An I/O failure in the underlying store (filesystem, object store, …).
    #[error("remote storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A segment was referenced that the metadata store has never seen.
    #[error("no remote log segment metadata for {0:?}")]
    SegmentNotFound(RemoteLogSegmentId),

    /// A caller gave `add_remote_log_segment_metadata` a starting state
    /// other than [`RemoteLogSegmentState::CopySegmentStarted`], or a
    /// segment id that already exists.
    #[error("invalid add for {id:?}: {reason}")]
    InvalidAdd {
        /// The offending segment id.
        id: RemoteLogSegmentId,
        /// Why the add was rejected.
        reason: String,
    },

    /// A caller requested a lifecycle transition that the state machine
    /// forbids.
    #[error("invalid segment state transition for {id:?}: {from:?} -> {to:?}")]
    InvalidSegmentTransition {
        /// The segment whose transition was rejected.
        id: RemoteLogSegmentId,
        /// Current state.
        from: RemoteLogSegmentState,
        /// Requested state.
        to: RemoteLogSegmentState,
    },

    /// A caller requested a partition-delete lifecycle transition that the
    /// state machine forbids.
    #[error("invalid partition delete transition for {tp:?}: {from:?} -> {to:?}")]
    InvalidPartitionDeleteTransition {
        /// The partition whose transition was rejected.
        tp: TopicIdPartition,
        /// Current state (`None` when the partition was never marked).
        from: Option<RemotePartitionDeleteState>,
        /// Requested state.
        to: RemotePartitionDeleteState,
    },

    /// Constructor / argument validation failed.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A conditional create found an object already at the key.
    ///
    /// A write-once archive turns on `PutMode::Create` for every object it
    /// writes, so this is how it learns that a key is already taken rather
    /// than silently replacing what is there.
    #[error("object already exists: {key}")]
    ObjectExists {
        /// Object-store key that already holds an object.
        key: String,
    },

    /// A conditional write's precondition did not hold, for example an `ETag`
    /// that no longer matches.
    #[error("precondition failed for `{key}`: {reason}")]
    PreconditionFailed {
        /// Object-store key whose precondition was evaluated.
        key: String,
        /// Why the backend rejected the write.
        reason: String,
    },

    /// A scan of a remote segment's records met a compressed batch with a record
    /// whose body is larger than the caller's limit. This is Kafka trunk's
    /// `max.decompressed.message.bytes`, which `DefaultRecord.readFrom` enforces
    /// with an `InvalidRecordException`.
    ///
    /// The segment is fine and the answer is known: the lookup fails, and it
    /// fails the same way on every retry until the limit is raised.
    #[error("Invalid record size: {size} exceeds the configured maximum record size of {limit}.")]
    RecordTooLarge {
        /// The record's body size in bytes, Kafka's `sizeOfBodyInBytes`.
        size: usize,
        /// The configured maximum.
        limit: usize,
    },

    /// The write-once (WORM) archive layer refused or could not complete the
    /// operation.
    #[error("write-once archive: {0}")]
    Worm(#[from] WormError),

    /// A backend, for example an object store, raised an error. The error
    /// does not map cleanly to one of the structured variants above.
    #[error("remote storage backend error: {0}")]
    Backend(String),

    /// This broker holds the metadata partition that would answer this
    /// query, but its consumer has not yet caught up to the high-water mark
    /// that the broker observed when it took the assignment. The answer is
    /// unknown, not "no segment". Callers should retry rather than treat it
    /// as a definitive miss. `Ok(None)` is reserved for "caught up, no
    /// covering segment" and for partitions this broker does not consume at
    /// all.
    #[error("remote log metadata partition {partition} not ready (assigned but not caught up)")]
    NotReady {
        /// The `__remote_log_metadata` partition that is still catching up.
        partition: i32,
    },
}

impl From<krabka_object_store::ObjectStoreError> for RemoteStorageError {
    fn from(err: krabka_object_store::ObjectStoreError) -> Self {
        use krabka_object_store::ObjectStoreError as E;

        match err {
            E::Io(e) => Self::Io(e),
            E::InvalidConfig(m) => Self::InvalidArgument(m),
            // Engine methods with segment context match NotFound before converting.
            E::NotFound(p) => Self::Backend(format!("not found: {p}")),
            E::TooLarge {
                key,
                size,
                max_bytes,
            } => Self::Backend(format!(
                "object `{key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            E::AlreadyExists(p) => Self::ObjectExists { key: p.to_string() },
            E::Precondition { key, reason } => Self::PreconditionFailed {
                key: key.to_string(),
                reason,
            },
            E::Backend(m) => Self::Backend(m),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        object_store::path::Path,
    };

    /// The write-once archive branches on these two, so each must arrive as
    /// its own structured variant and not as an opaque `Backend` string.
    #[test]
    fn conditional_write_errors_convert_to_structured_variants() {
        let already = RemoteStorageError::from(
            krabka_object_store::ObjectStoreError::AlreadyExists(Path::from("worm/manifest")),
        );
        check!(
            matches!(&already, RemoteStorageError::ObjectExists { key } if key == "worm/manifest")
        );
        check!(already.to_string() == "object already exists: worm/manifest");

        let precondition =
            RemoteStorageError::from(krabka_object_store::ObjectStoreError::Precondition {
                key: Path::from("worm/manifest"),
                reason: "etag mismatch".to_owned(),
            });
        check!(matches!(
            &precondition,
            RemoteStorageError::PreconditionFailed { key, reason }
                if key == "worm/manifest" && reason == "etag mismatch"
        ));
        check!(
            precondition.to_string() == "precondition failed for `worm/manifest`: etag mismatch"
        );
    }

    /// `?` on a [`WormError`] must land in one variant that still renders the
    /// underlying refusal, so a log line names the key that was protected.
    #[test]
    fn worm_errors_convert_and_keep_their_message() {
        let err = RemoteStorageError::from(WormError::DeleteRefused {
            key: "archive/orders-0/0.log".to_owned(),
        });

        check!(matches!(
            err,
            RemoteStorageError::Worm(WormError::DeleteRefused { .. })
        ));
        check!(
            err.to_string()
                == "write-once archive: delete refused: `archive/orders-0/0.log` is in a write-once archive"
        );
    }

    #[test]
    fn object_store_too_large_converts_to_backend_message() {
        let err = krabka_object_store::ObjectStoreError::TooLarge {
            key: Path::from("index/snapshot.json"),
            size: 1000,
            max_bytes: 256,
        };

        let got = RemoteStorageError::from(err);

        assert!(matches!(
            got,
            RemoteStorageError::Backend(ref msg)
                if msg == "object `index/snapshot.json` is 1000 bytes, exceeds cap of 256 bytes"
        ));
    }
}
