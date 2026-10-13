//! The one error type the tool reports, and the exit codes it maps onto.
//!
//! A runbook branches on `$?`, so the numbers are fixed here and stated in the
//! crate README and in `docs/operations/backup-restore.md`. They are chosen to
//! sit beside `krabka restore`'s own: `2` is a bad argument in both tools, `4`
//! is an unreadable archive in both, and `5` is an integrity failure in both.

/// Everything that can stop a capture, a verification, or an offset restore.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// A local file could not be read, or a directory could not be walked.
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// The archive could not be read or written.
    #[error("archive: {0}")]
    ObjectStore(#[from] krabka_object_store::ObjectStoreError),

    /// A manifest or an offsets file could not be decoded.
    #[error("{context}: {source}")]
    Json {
        /// What was being decoded, named by its object key.
        context: String,
        /// The decoder's own complaint.
        source: serde_json::Error,
    },

    /// A manifest or an offsets file has no `"version"`, as one written
    /// before krabka 1.0 does, or one this build does not read.
    #[error("{context}: {}", unsupported_version_reason(found.as_deref(), *expected))]
    UnsupportedVersion {
        /// What was being decoded, named by its object key.
        context: String,
        /// The `"version"` the document carries, as JSON text, or `None` when
        /// it has none.
        found: Option<String>,
        /// The version this build reads.
        expected: i16,
    },

    /// The flags contradict each other, or name something that cannot exist.
    #[error("{0}")]
    InvalidArgument(String),

    /// A captured artifact does not match the digest its manifest recorded.
    #[error("integrity failure: {0}")]
    Integrity(String),

    /// The archive holds no capture, or not the one that was named.
    #[error("{0}")]
    NoSuchCapture(String),

    /// The cluster could not be reached, or refused a request.
    #[error("cluster: {0}")]
    Cluster(String),
}

/// Exit code for a bad argument, or a flag set that contradicts itself.
pub const EXIT_BAD_ARGUMENT: i32 = 2;
/// Exit code for an archive or a log directory that cannot be read.
pub const EXIT_UNREADABLE: i32 = 4;
/// Exit code for a captured artifact that does not match its recorded digest.
pub const EXIT_INTEGRITY: i32 = 5;
/// Exit code for a cluster that could not be reached, or that refused.
pub const EXIT_CLUSTER: i32 = 6;

/// A contradictory `--archive-*` flag set is a bad argument like any other.
impl From<krabka_object_store::ArchiveArgsError> for BackupError {
    fn from(error: krabka_object_store::ArchiveArgsError) -> Self {
        Self::InvalidArgument(error.to_string())
    }
}

/// The reason half of [`BackupError::UnsupportedVersion`]'s message.
fn unsupported_version_reason(found: Option<&str>, expected: i16) -> String {
    match found {
        None => "no \"version\" field: the capture predates krabka 1.0, which this build \
                 does not read"
            .to_owned(),
        Some(found) => format!("unsupported version {found}: this build reads version {expected}"),
    }
}

impl BackupError {
    /// The process exit code this error reports.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::InvalidArgument(_) => EXIT_BAD_ARGUMENT,
            Self::Io(_)
            | Self::ObjectStore(_)
            | Self::Json { .. }
            | Self::UnsupportedVersion { .. }
            | Self::NoSuchCapture(_) => EXIT_UNREADABLE,
            Self::Integrity(_) => EXIT_INTEGRITY,
            Self::Cluster(_) => EXIT_CLUSTER,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{BackupError, EXIT_BAD_ARGUMENT, EXIT_CLUSTER, EXIT_INTEGRITY, EXIT_UNREADABLE},
        assert2::check,
    };

    #[test]
    fn every_variant_maps_onto_its_documented_exit_code() {
        let cases = [
            (
                BackupError::InvalidArgument("bad".to_owned()),
                EXIT_BAD_ARGUMENT,
            ),
            (
                BackupError::Io(std::io::Error::other("disk")),
                EXIT_UNREADABLE,
            ),
            (
                BackupError::NoSuchCapture("none".to_owned()),
                EXIT_UNREADABLE,
            ),
            (
                BackupError::UnsupportedVersion {
                    context: "manifest.json".to_owned(),
                    found: None,
                    expected: 0,
                },
                EXIT_UNREADABLE,
            ),
            (BackupError::Integrity("digest".to_owned()), EXIT_INTEGRITY),
            (BackupError::Cluster("refused".to_owned()), EXIT_CLUSTER),
        ];
        for (error, expected) in cases {
            check!(error.exit_code() == expected, "for {error}");
        }
    }
}
