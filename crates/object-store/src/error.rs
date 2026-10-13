//! Error taxonomy for object-store construction and access.

use {super::object_store_api as object_store, object_store::path::Path as ObjectPath};

/// Errors raised by the object-store substrate.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// A local-filesystem I/O failure, for example during a read of a segment
    /// file to upload.
    #[error("object store I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The backend builder rejected the config: a bad bucket, region,
    /// endpoint, or credential.
    #[error("invalid object store config: {0}")]
    InvalidConfig(String),
    /// A specific object was not found. The variant is structured, so
    /// consumers can upgrade it to their own domain error without string
    /// matching.
    #[error("object not found: {0}")]
    NotFound(ObjectPath),
    /// An object exceeded a caller-supplied size cap during a buffered read.
    /// This guards against OOM on a corrupt or malicious oversized object.
    #[error("object `{key}` is {size} bytes, exceeds cap of {max_bytes} bytes")]
    TooLarge {
        /// The object that breached the cap.
        key: object_store::path::Path,
        /// The metadata size or the bytes observed when streaming exceeded the cap.
        size: u64,
        /// The cap in bytes.
        max_bytes: u64,
    },
    /// A conditional create (`PutMode::Create`) found an object already at
    /// the key.
    #[error("object already exists: {0}")]
    AlreadyExists(ObjectPath),
    /// A conditional read or write's precondition did not hold.
    #[error("precondition failed for `{key}`: {reason}")]
    Precondition {
        /// The object whose precondition was evaluated.
        key: ObjectPath,
        /// Why the backend rejected the operation.
        reason: String,
    },
    /// Any other backend error. The error becomes a string, so the public
    /// surface stays stable.
    #[error("object store backend error: {0}")]
    Backend(String),
}

impl From<object_store::Error> for ObjectStoreError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => Self::NotFound(ObjectPath::from(path)),
            object_store::Error::AlreadyExists { path, .. } => {
                Self::AlreadyExists(ObjectPath::from(path))
            }
            object_store::Error::Precondition { path, source } => Self::Precondition {
                key: ObjectPath::from(path),
                reason: source.to_string(),
            },
            other => Self::Backend(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        assert2::assert,
    };

    #[test]
    fn io_error_converts_via_from() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "nope");
        let err: ObjectStoreError = io.into();
        assert!(matches!(err, ObjectStoreError::Io(_)));
    }

    #[test]
    fn not_found_maps_to_structured_variant() {
        let err = object_store::Error::NotFound {
            path: "tenant/block".to_string(),
            source: "missing".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::NotFound(p) if p.to_string() == "tenant/block")
        );
    }

    #[test]
    fn already_exists_maps_to_structured_variant() {
        let err = object_store::Error::AlreadyExists {
            path: "worm/manifest.json".to_string(),
            source: "exists".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::AlreadyExists(p) if p.to_string() == "worm/manifest.json")
        );
    }

    #[test]
    fn precondition_maps_to_structured_variant() {
        let err = object_store::Error::Precondition {
            path: "worm/manifest.json".to_string(),
            source: "etag mismatch".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::Precondition { key, reason }
                if key.to_string() == "worm/manifest.json" && reason == "etag mismatch")
        );
    }

    #[test]
    fn too_large_display_includes_sizes() {
        let err = ObjectStoreError::TooLarge {
            key: object_store::path::Path::from("index/snapshot.json"),
            size: 1000,
            max_bytes: 256,
        };
        let msg = err.to_string();
        assert!(msg.contains("1000"));
        assert!(msg.contains("256"));
    }

    #[test]
    fn other_errors_map_to_backend() {
        let err = object_store::Error::Generic {
            store: "s",
            source: "boom".into(),
        };
        assert!(matches!(
            ObjectStoreError::from(err),
            ObjectStoreError::Backend(_)
        ));
    }
}
