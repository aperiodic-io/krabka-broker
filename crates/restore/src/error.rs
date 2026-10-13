//! The restore error taxonomy and the process exit codes it maps to.
//!
//! The library returns [`RestoreError`]. Only [`run`](crate::run) and the
//! binary turn one into an exit code and a line on stderr, so a caller that
//! embeds the restore keeps the structured error.

use {
    krabka_log::LogError, krabka_object_store::ObjectStoreError,
    krabka_protocol::records::RecordsError, krabka_remote_storage::RemoteStorageError,
    std::path::PathBuf, uuid::Uuid,
};

/// Exit codes:
/// - 0: success
/// - 2: bad arguments
/// - 3: target log directory not empty
/// - 4: archive unreadable or empty
/// - 5: integrity failure
/// - 6: materialization failure
pub const EXIT_OK: i32 = 0;
/// See [`EXIT_OK`].
pub const EXIT_BAD_ARGUMENTS: i32 = 2;
/// See [`EXIT_OK`].
pub const EXIT_DIRTY_LOG_DIR: i32 = 3;
/// See [`EXIT_OK`].
pub const EXIT_ARCHIVE_UNREADABLE: i32 = 4;
/// See [`EXIT_OK`].
pub const EXIT_INTEGRITY: i32 = 5;
/// See [`EXIT_OK`].
pub const EXIT_MATERIALIZE: i32 = 6;

/// Every way a point-in-time restore stops.
///
/// The variants keep the object key, the byte position, and the partition that
/// identify the failure, because an operator who runs a restore during an
/// incident has to decide whether to re-run with `--continue-on-corrupt` or to
/// go back to the archive. A message alone does not support that decision.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestoreError {
    /// A flag combination the parser accepts but the restore cannot act on.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// `--log-dir` names a directory that already holds entries. A restore
    /// writes a complete cluster, so it never merges into existing state.
    #[error("refusing to restore into non-empty log directory {0}")]
    LogDirNotEmpty(PathBuf),

    /// The scan reached the archive but found no segment for any selected
    /// topic. A wrong `--archive-prefix` reads exactly like an empty bucket,
    /// so the message carries the prefix that was searched.
    #[error("archive holds no segment under prefix {prefix:?}")]
    EmptyArchive {
        /// The key prefix the scan searched, empty when the whole bucket was
        /// scanned.
        prefix: String,
    },

    /// A batch header's CRC does not match the bytes that follow it.
    #[error(
        "checksum mismatch in {key} at byte {position}: header records {expected:#010x}, \
         body computes {computed:#010x}"
    )]
    ChecksumMismatch {
        /// The object key of the artifact that holds the corrupt batch.
        key: String,
        /// Byte position of the batch header within that object.
        position: u64,
        /// The CRC the batch header declares.
        expected: u32,
        /// The CRC re-computed over the batch body.
        computed: u32,
    },

    /// A segment ends part way through a batch. The archived copy is short of
    /// the length its last batch header declares.
    #[error(
        "segment {key} ends inside a batch at byte {position}: {declared} bytes declared, \
         {available} available"
    )]
    TruncatedSegment {
        /// The object key of the short artifact.
        key: String,
        /// Byte position of the batch header that overruns the object.
        position: u64,
        /// The batch length that header declares.
        declared: u64,
        /// The bytes that remain in the object from `position`.
        available: u64,
    },

    /// A segment copy stopped part way: the archive holds some of the
    /// segment's artifacts and not the rest.
    #[error("torn copy of segment {segment_id} of {topic}-{partition}: {artifact} is absent")]
    TornCopy {
        /// Topic of the incomplete segment.
        topic: String,
        /// Partition index of the incomplete segment.
        partition: i32,
        /// The segment id the archive names the copy with.
        segment_id: Uuid,
        /// The artifact that is absent, such as `.timeindex`.
        artifact: String,
    },

    /// Trusted WORM evidence did not authenticate an archive object or chain.
    #[error("WORM authentication failed: {reason}")]
    Authenticity {
        /// The failed trust check.
        reason: String,
    },

    /// Captured diskless-WAL state or a required object is inconsistent.
    #[error("integrity failure: {0}")]
    Integrity(String),

    /// The bucket scan and the `--rlmm-snapshot` do not agree about a
    /// partition. One of the two is stale, and a restore must not pick for the
    /// operator.
    #[error(
        "metadata disagreement for {topic}-{partition}: the bucket scan reports {scanned}, \
         the RLMM snapshot reports {snapshot}"
    )]
    MetadataDisagreement {
        /// Topic the two sources disagree about.
        topic: String,
        /// Partition index the two sources disagree about.
        partition: i32,
        /// What the bucket scan found.
        scanned: String,
        /// What the RLMM snapshot states.
        snapshot: String,
    },

    /// A controller metadata snapshot could not be decoded.
    #[error("cannot read metadata snapshot {path}: {reason}")]
    MetadataSnapshot {
        /// Snapshot path supplied by `--metadata-snapshot`.
        path: PathBuf,
        /// Decoder failure.
        reason: String,
    },

    /// A topic configuration belongs to a different incarnation of an
    /// archived topic, or cannot be tied to one at all.
    #[error(
        "metadata snapshot topic id mismatch for {topic}: archive has {archive_topic_id}, \
         snapshot has {snapshot_topic_id}"
    )]
    MetadataSnapshotTopicMismatch {
        /// Topic name shared by the archive and snapshot.
        topic: String,
        /// Topic id carried by the archived partition paths.
        archive_topic_id: Uuid,
        /// Topic id in the snapshot, or `absent` when its topic record is missing.
        snapshot_topic_id: String,
    },

    /// A bound names a topic partition the archive does not hold. Left
    /// unreported this silently restores the partition whole, which is the
    /// opposite of what the operator asked for.
    #[error("bound names {topic}-{partition}, which the archive does not hold")]
    UnknownPartition {
        /// Topic the bound names.
        topic: String,
        /// Partition index the bound names.
        partition: i32,
    },

    /// The formatter refused to format the target log directory.
    #[error("formatting the target log directory failed with exit code {code}")]
    Format {
        /// The exit code the formatter returned.
        code: i32,
    },

    /// The tiered-storage layer failed.
    #[error("remote storage: {0}")]
    RemoteStorage(#[from] RemoteStorageError),

    /// The object store failed, or its configuration was rejected.
    #[error("object store: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    /// The target log failed to accept a rehydrated segment.
    #[error("log: {0}")]
    Log(#[from] LogError),

    /// A record batch would not decode or re-encode.
    #[error("records: {0}")]
    Records(#[from] RecordsError),

    /// A local filesystem operation failed.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl RestoreError {
    /// The process exit code this error reports.
    ///
    /// The grouping is what an operator acts on: an integrity failure means
    /// the archive is damaged and a re-run needs `--continue-on-corrupt`,
    /// while a materialization failure means the target is at fault and the
    /// archive is intact.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::InvalidArgument(_) | Self::UnknownPartition { .. } => EXIT_BAD_ARGUMENTS,
            Self::LogDirNotEmpty(_) => EXIT_DIRTY_LOG_DIR,
            Self::EmptyArchive { .. } | Self::ObjectStore(_) | Self::RemoteStorage(_) => {
                EXIT_ARCHIVE_UNREADABLE
            }
            Self::ChecksumMismatch { .. }
            | Self::TruncatedSegment { .. }
            | Self::TornCopy { .. }
            | Self::Authenticity { .. }
            | Self::Integrity(_)
            | Self::MetadataDisagreement { .. }
            | Self::MetadataSnapshot { .. }
            | Self::MetadataSnapshotTopicMismatch { .. }
            | Self::Records(_) => EXIT_INTEGRITY,
            Self::Format { .. } | Self::Log(_) | Self::Io(_) => EXIT_MATERIALIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn exit_codes_are_distinct_and_documented() {
        let codes = [
            EXIT_OK,
            EXIT_BAD_ARGUMENTS,
            EXIT_DIRTY_LOG_DIR,
            EXIT_ARCHIVE_UNREADABLE,
            EXIT_INTEGRITY,
            EXIT_MATERIALIZE,
        ];
        let mut seen = codes.to_vec();
        seen.sort_unstable();
        seen.dedup();
        check!(seen.len() == codes.len());
        check!(codes == [0, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn each_failure_class_maps_to_its_exit_code() {
        let cases = vec![
            (
                RestoreError::InvalidArgument("bad".into()),
                EXIT_BAD_ARGUMENTS,
            ),
            (
                RestoreError::UnknownPartition {
                    topic: "orders".into(),
                    partition: 3,
                },
                EXIT_BAD_ARGUMENTS,
            ),
            (
                RestoreError::LogDirNotEmpty(PathBuf::from("/var/lib/krabka")),
                EXIT_DIRTY_LOG_DIR,
            ),
            (
                RestoreError::EmptyArchive {
                    prefix: "tier".into(),
                },
                EXIT_ARCHIVE_UNREADABLE,
            ),
            (
                RestoreError::ChecksumMismatch {
                    key: "orders-0-abc/000.log".into(),
                    position: 4096,
                    expected: 0xdead_beef,
                    computed: 0x0bad_f00d,
                },
                EXIT_INTEGRITY,
            ),
            (
                RestoreError::TruncatedSegment {
                    key: "orders-0-abc/000.log".into(),
                    position: 8192,
                    declared: 512,
                    available: 40,
                },
                EXIT_INTEGRITY,
            ),
            (
                RestoreError::TornCopy {
                    topic: "orders".into(),
                    partition: 0,
                    segment_id: Uuid::nil(),
                    artifact: ".timeindex".into(),
                },
                EXIT_INTEGRITY,
            ),
            (
                RestoreError::MetadataDisagreement {
                    topic: "orders".into(),
                    partition: 0,
                    scanned: "4 segments".into(),
                    snapshot: "3 segments".into(),
                },
                EXIT_INTEGRITY,
            ),
            (
                RestoreError::MetadataSnapshot {
                    path: "metadata.checkpoint".into(),
                    reason: "bad frame".into(),
                },
                EXIT_INTEGRITY,
            ),
            (
                RestoreError::MetadataSnapshotTopicMismatch {
                    topic: "orders".into(),
                    archive_topic_id: Uuid::nil(),
                    snapshot_topic_id: "absent".into(),
                },
                EXIT_INTEGRITY,
            ),
            (RestoreError::Format { code: 3 }, EXIT_MATERIALIZE),
            (
                RestoreError::Io(std::io::Error::other("disk full")),
                EXIT_MATERIALIZE,
            ),
        ];
        for (error, expected) in cases {
            check!(error.exit_code() == expected, "{error}");
        }
    }

    #[test]
    fn checksum_mismatch_names_the_key_and_the_byte_position() {
        let error = RestoreError::ChecksumMismatch {
            key: "tier/orders-0-abc/00000000000000000000-xyz.log".into(),
            position: 65_536,
            expected: 0x1234_5678,
            computed: 0x8765_4321,
        };
        let rendered = error.to_string();
        check!(rendered.contains("tier/orders-0-abc/00000000000000000000-xyz.log"));
        check!(rendered.contains("65536"));
        check!(rendered.contains("0x12345678"));
        check!(rendered.contains("0x87654321"));
    }

    #[test]
    fn io_errors_convert_without_a_string_hop() {
        let error: RestoreError = std::io::Error::other("boom").into();
        check!(matches!(error, RestoreError::Io(_)));
    }
}
