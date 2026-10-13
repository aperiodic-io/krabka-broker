use {crate::types::NodeId, std::path::PathBuf, thiserror::Error};

/// What is wrong with a node-local file whose layout carries a version
/// marker. [`RaftError::PersistedFormat`] names the file.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PersistedFormatError {
    /// The file has the layout a krabka build before 1.0 wrote, with no
    /// version marker. 1.0 reads no 0.x data, so the node is reformatted.
    #[error(
        "has no version marker, so a krabka build before 1.0 wrote it; reformat this node \
         with krabka-format"
    )]
    MissingVersion,
    /// The version marker names a version this build does not read, such as
    /// one a later build wrote.
    #[error("has version {found}, and this build reads versions {min} to {max}")]
    UnsupportedVersion { found: i64, min: i64, max: i64 },
    /// The file does not parse at a version this build reads.
    #[error("does not parse: {0}")]
    Malformed(String),
}

/// A committed metadata log record that a controller cannot replay. It stops
/// the controller, as Kafka's `QuorumController` hands a record it cannot
/// replay to `SharedServer.fatalQuorumControllerFaultHandler`, and a
/// controller-role node's `MetadataLoader` hands one it cannot load to
/// `SharedServer.metadataLoaderFaultHandler`, which is fatal on a node whose
/// `process.roles` include `controller`. Both handlers halt the process.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum MetadataReplayError {
    /// The record's bytes are not a metadata record this build reads: an
    /// unknown apiKey, a value version above the highest it supports, a frame
    /// version other than 1, or a krabka-private record that does not decode.
    #[error("the metadata record at offset {offset} does not decode: {error}")]
    UndecodableRecord {
        /// The record's offset in the metadata log.
        offset: i64,
        /// Why it does not decode.
        error: krabka_metadata::TranslateError,
    },
    /// A KIP-853 control record that does not decode, or that names a
    /// `kraft.version` or voter set Kafka would refuse. Kafka's
    /// `KRaftControlRecordStateMachine` throws while it reads such a record,
    /// and `KafkaRaftClientDriver` hands the throw to
    /// `SharedServer.raftManagerFaultHandler`, which is fatal on every role.
    #[error("the KRaft control record at offset {offset} is invalid: {reason}")]
    InvalidControlRecord {
        /// The record's offset in the metadata log.
        offset: i64,
        /// Why it is invalid.
        reason: String,
    },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RaftError {
    #[error("storage: {0}")]
    Storage(#[from] krabka_log::LogError),

    #[error("network: {0}")]
    Network(#[from] krabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] krabka_protocol::ProtocolError),

    #[error("controller admin dispatch: {0}")]
    ControllerAdmin(String),

    #[error("records: {0}")]
    Records(#[from] krabka_protocol::records::RecordsError),

    /// A record of a control batch lacks its key or value, or has an empty
    /// one. The message is the text of Kafka's
    /// `RecordsIterator.decodeControlRecord`.
    #[error("{0}")]
    MalformedControlRecord(&'static str),

    #[error("metadata: {0}")]
    Metadata(#[from] krabka_metadata::MetadataError),

    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    #[error("not leader; current leader: {current_leader:?}")]
    NotLeader { current_leader: Option<NodeId> },

    #[error("leader unknown (election in progress)")]
    LeaderUnknown,

    #[error("change rejected: {0}")]
    ChangeRejected(String),

    /// A leader-local mutation was planned from an image that has since changed.
    /// The caller must read a fresh image and repeat its admission checks.
    #[error("the metadata image changed before the mutation was submitted")]
    StaleMetadataImage,

    /// The leader refused a compare-and-set because its log holds records
    /// that are not committed yet.
    ///
    /// A break-glass consume and a topic-freeze replacement check the
    /// committed image, so the leader decides them only when its whole log is
    /// committed. A newly elected leader is in this state until it commits a
    /// record from its own epoch. Kafka's controller is not active in the same
    /// window, and it answers `NOT_CONTROLLER`. The refusal clears when the
    /// tail commits, so a caller can retry.
    ///
    /// A diskless offset reservation gets the same refusal until the leader
    /// has committed its own epoch. The leader reserves no offsets before
    /// that.
    #[error("the controller leader has uncommitted metadata records")]
    UncommittedTail,

    /// The controller refused a request because the principal of the
    /// connection lacks the cluster operation that the request needs.
    #[error("the controller denied the cluster operation to this node's principal")]
    ClusterAuthorizationFailed,

    #[error("reconfiguration rejected: {0}")]
    ReconfigRejected(String),

    #[error("a reconfiguration is already in progress")]
    ReconfigInProgress,

    #[error("voter {0} already exists")]
    DuplicateVoter(NodeId),

    #[error("voter {0} was not found")]
    VoterNotFound(NodeId),

    #[error("invalid voter update: {0}")]
    InvalidVoterUpdate(String),

    #[error("kraft.version {0} does not permit dynamic voter changes")]
    UnsupportedKraftVersion(u16),

    #[error("voter {id} is not a caught-up observer (lag {lag})")]
    VoterNotCaughtUp { id: NodeId, lag: u64 },

    #[error("serialization: {0}")]
    SerdeFailed(#[from] wincode::error::WriteError),

    #[error("deserialization: {0}")]
    SerdeFailedDecode(#[from] wincode::error::ReadError),

    /// A node-local file in the metadata partition directory has a version
    /// this build does not read, or does not parse. The node does not start
    /// on it.
    #[error("{artifact} file {}: {problem}", .path.display())]
    PersistedFormat {
        /// The file's name, such as `quorum-state`.
        artifact: &'static str,
        path: PathBuf,
        problem: PersistedFormatError,
    },

    /// A peer answered a krabka-private controller RPC with
    /// `UNSUPPORTED_VERSION`: it does not implement `version` of `api_key`.
    /// A mixed-version cluster meets it during a rolling upgrade.
    #[error("the peer does not implement version {version} of krabka-private api {api_key}")]
    UnsupportedPrivateVersion { api_key: i16, version: i16 },

    /// A committed metadata log record that this controller cannot replay.
    /// The controller stops.
    #[error("metadata replay: {0}")]
    MetadataReplay(#[from] MetadataReplayError),

    #[error("startup misconfiguration: {0}")]
    Startup(String),

    /// The controller met a fault that Kafka's `ProcessTerminatingFaultHandler`
    /// halts the process over, such as a replay of a `FeatureLevelRecord`
    /// above the range this controller supports. The text is the
    /// `Encountered fatal fault: ` prefix of that handler's log line, then the
    /// message of the exception that caused the fault. Kafka's own
    /// `failureMessage` between the two, such as `Unable to apply
    /// FeatureLevelRecord record at offset N on active controller, ...`, is
    /// not part of it.
    #[error("Encountered fatal fault: {0}")]
    FatalFault(String),

    #[error("controller shut down")]
    Shutdown,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_not_leader_with_id() {
        let e = RaftError::NotLeader {
            current_leader: Some(NodeId(7)),
        };
        assert2::assert!(e.to_string().contains("Some(NodeId(7))"));
    }

    #[test]
    fn display_not_leader_without_id() {
        let e = RaftError::NotLeader {
            current_leader: None,
        };
        assert2::assert!(e.to_string().contains("None"));
    }
}
