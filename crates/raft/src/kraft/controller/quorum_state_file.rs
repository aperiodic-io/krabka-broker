//! The node-local durable quorum-state file. Kafka's `QuorumStateData` JSON is
//! read by the JVM tools, so both schema versions keep their exact field set
//! and Kafka field order here.

use {
    super::QUORUM_STATE_FILE,
    crate::{
        error::{PersistedFormatError, RaftError},
        kraft::types::{NodeId, QuorumState, ReplicaKey},
    },
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    uuid::Uuid,
};

/// The lowest `data_version` of Kafka's `QuorumStateData` this build reads,
/// Kafka's `FileQuorumStateStore.LOWEST_SUPPORTED_VERSION`. It is part of the
/// 1.x on-disk contract.
pub(crate) const QUORUM_STATE_LOWEST_DATA_VERSION: i64 = 0;

/// The highest `data_version` of Kafka's `QuorumStateData` this build reads
/// and writes, Kafka's `FileQuorumStateStore.HIGHEST_SUPPORTED_VERSION`.
/// `kraft.version` 0 writes `data_version` 0, and `kraft.version` 1 writes
/// this one. It is part of the 1.x on-disk contract.
pub(crate) const QUORUM_STATE_HIGHEST_DATA_VERSION: i64 = 1;

/// The `data_version` field of Kafka's `QuorumStateData` JSON.
const DATA_VERSION_FIELD: &str = "data_version";

/// One entry of schema v0's `currentVoters` array.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuorumStateVoter {
    #[serde(rename = "voterId")]
    voter_id: i32,
}

/// Kafka's on-disk `QuorumStateData`. Schema v0 carries the cluster id, the
/// applied offset and the legacy voter ids; schema v1 carries the voted
/// directory id instead. The `skip_serializing_if` markers keep each version's
/// field set (and their Kafka field order) byte-exact.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuorumStateJson {
    #[serde(rename = "clusterId", skip_serializing_if = "Option::is_none")]
    cluster_id: Option<String>,
    #[serde(rename = "leaderId")]
    leader_id: i32,
    #[serde(rename = "leaderEpoch")]
    leader_epoch: i32,
    #[serde(rename = "votedId")]
    voted_id: i32,
    #[serde(rename = "votedDirectoryId", skip_serializing_if = "Option::is_none")]
    voted_directory_id: Option<String>,
    #[serde(rename = "appliedOffset", skip_serializing_if = "Option::is_none")]
    applied_offset: Option<i64>,
    #[serde(rename = "currentVoters", skip_serializing_if = "Option::is_none")]
    current_voters: Option<Vec<QuorumStateVoter>>,
    data_version: i32,
}

/// Write Kafka's JSON `QuorumStateData` atomically and durably, syncing the
/// directory after the rename as Kafka's `FileQuorumStateStore` does. Level 0 uses schema v0
/// (including the legacy voter ids); level 1 uses schema v1 with the voted
/// directory id and no embedded voter set.
pub fn save_quorum_state(dir: &std::path::Path, state: &QuorumState) -> Result<(), RaftError> {
    let voter_ids = state.voters.ids();
    let all_voter_ids_fit = voter_ids.iter().all(|id| i32::try_from(id.0).is_ok());
    let leader_id_value = state.leader_id.map_or(0, |id| id.0);
    let voted_id_value = state.voted_key.map_or(0, |key| key.id.0);
    if krabka_verified::quorum_state_write_decision(
        state.kraft_version,
        state.leader_epoch,
        state.leader_id.is_some(),
        leader_id_value,
        state.voted_key.is_some(),
        voted_id_value,
        all_voter_ids_fit,
    ) == krabka_verified::QuorumStateWriteDecision::Reject
    {
        return Err(RaftError::Storage(krabka_log::LogError::InvalidArgument(
            "quorum-state cannot be represented exactly by schema v0 or v1".into(),
        )));
    }
    let leader_id = state
        .leader_id
        .map_or(Ok(-1), |id| i32::try_from(id.0))
        .expect("write admission proves the leader id fits i32");
    let leader_epoch =
        i32::try_from(state.leader_epoch).expect("write admission proves the epoch fits i32");
    let voted_id = state
        .voted_key
        .map_or(Ok(-1), |key| i32::try_from(key.id.0))
        .expect("write admission proves the voted id fits i32");
    let data = if state.kraft_version == 0 {
        QuorumStateJson {
            cluster_id: Some(state.cluster_id.to_string()),
            leader_id,
            leader_epoch,
            voted_id,
            voted_directory_id: None,
            applied_offset: Some(0),
            current_voters: Some(
                voter_ids
                    .into_iter()
                    .map(|id| QuorumStateVoter {
                        voter_id: i32::try_from(id.0)
                            .expect("write admission proves every v0 voter id fits i32"),
                    })
                    .collect(),
            ),
            data_version: 0,
        }
    } else {
        let voted_directory_id = state
            .voted_key
            .map_or([0; 16], |key| *key.directory_id.as_bytes());
        QuorumStateJson {
            cluster_id: None,
            leader_id,
            leader_epoch,
            voted_id,
            voted_directory_id: Some(URL_SAFE_NO_PAD.encode(voted_directory_id)),
            applied_offset: None,
            current_voters: None,
            data_version: 1,
        }
    };
    let json = serde_json::to_string(&data).map_err(|e| {
        RaftError::Storage(krabka_log::LogError::Corrupt(format!(
            "serialize quorum-state: {e}"
        )))
    })?;
    let path = dir.join(QUORUM_STATE_FILE);
    krabka_log::write_file_atomic(&path.with_extension("tmp"), &path, json.as_bytes())
        .map_err(krabka_log::LogError::Io)?;
    Ok(())
}

/// Parses Kafka's `QuorumStateData` JSON as `FileQuorumStateStore.readStateFromFile`
/// does: the document must be an object with a `data_version` between
/// [`QUORUM_STATE_LOWEST_DATA_VERSION`] and
/// [`QUORUM_STATE_HIGHEST_DATA_VERSION`], and then parse at that version.
fn parse_quorum_state(bytes: &[u8]) -> Result<QuorumStateJson, PersistedFormatError> {
    let malformed = PersistedFormatError::Malformed;
    let document: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| malformed(error.to_string()))?;
    let serde_json::Value::Object(fields) = &document else {
        return Err(malformed(format!("{document} is not a JSON object")));
    };
    let data_version = fields
        .get(DATA_VERSION_FIELD)
        .ok_or_else(|| malformed(format!("it has no {DATA_VERSION_FIELD} field")))?
        .as_i64()
        .ok_or_else(|| malformed(format!("its {DATA_VERSION_FIELD} is not an integer")))?;
    if !(QUORUM_STATE_LOWEST_DATA_VERSION..=QUORUM_STATE_HIGHEST_DATA_VERSION)
        .contains(&data_version)
    {
        return Err(PersistedFormatError::UnsupportedVersion {
            found: data_version,
            min: QUORUM_STATE_LOWEST_DATA_VERSION,
            max: QUORUM_STATE_HIGHEST_DATA_VERSION,
        });
    }
    serde_json::from_value(document).map_err(|error| malformed(error.to_string()))
}

/// Load Kafka JSON `QuorumStateData`. The configured voter metadata supplies
/// endpoints at level 0; level-1 membership is recovered from snapshots/log.
///
/// A missing file is `None`, a node that has not voted yet. A file that is
/// there but that this build cannot read is an error, as Kafka's
/// `FileQuorumStateStore.readElectionState` throws on an unreadable file and
/// on a `data_version` outside its supported range. Treating it as absent
/// would drop the vote this node cast in its current epoch, and the node
/// could vote a second time in that epoch.
///
/// # Errors
/// [`RaftError::Storage`] when the file cannot be read, and
/// [`RaftError::PersistedFormat`] when it has an unsupported `data_version`,
/// does not parse, or does not match `cluster_id` and `voters`.
pub fn load_quorum_state(
    dir: &std::path::Path,
    cluster_id: Uuid,
    voters: &krabka_metadata::voters::VoterSet,
) -> Result<Option<QuorumState>, RaftError> {
    let path = dir.join(QUORUM_STATE_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(RaftError::Storage(krabka_log::LogError::Io(e))),
    };
    let refuse = |problem| RaftError::PersistedFormat {
        artifact: QUORUM_STATE_FILE,
        path: path.clone(),
        problem,
    };
    let data = parse_quorum_state(&bytes).map_err(refuse)?;
    let data_version = data.data_version;
    let voted_directory_id = data
        .voted_directory_id
        .as_deref()
        .and_then(|id| URL_SAFE_NO_PAD.decode(id).ok())
        .and_then(|raw| <[u8; 16]>::try_from(raw).ok())
        .map(Uuid::from_bytes);
    let configured_voter_ids = voters.ids();
    let v0_voters_match = data.current_voters.as_ref().is_some_and(|persisted| {
        persisted.len() == configured_voter_ids.len()
            && persisted
                .iter()
                .zip(&configured_voter_ids)
                .all(|(stored, configured)| {
                    stored.voter_id >= 0
                        && u64::try_from(stored.voter_id).ok() == Some(configured.0)
                })
    });
    let version_fields_valid = if data_version == 0 {
        data.cluster_id.as_deref() == Some(cluster_id.to_string().as_str())
            && data.voted_directory_id.is_none()
            && data.applied_offset == Some(0)
            && v0_voters_match
    } else if data_version == 1 {
        data.cluster_id.is_none()
            && data.voted_directory_id.is_some()
            && data.applied_offset.is_none()
            && data.current_voters.is_none()
    } else {
        false
    };
    let decision = krabka_verified::quorum_state_load_decision(
        data_version,
        data.leader_epoch,
        data.voted_id,
        version_fields_valid,
        voted_directory_id.is_some(),
        voted_directory_id == Some(Uuid::nil()),
    );
    let voted_key = match decision {
        krabka_verified::QuorumStateLoadDecision::Reject => {
            return Err(refuse(PersistedFormatError::Malformed(format!(
                "its fields are not a valid data_version {data_version} record for cluster \
                 {cluster_id} and the configured voters"
            ))));
        }
        krabka_verified::QuorumStateLoadDecision::RestoreNoVote => None,
        krabka_verified::QuorumStateLoadDecision::RestoreVote => Some(ReplicaKey {
            id: NodeId(
                u64::try_from(data.voted_id)
                    .expect("load admission proves the voted id is nonnegative"),
            ),
            directory_id: if data_version == 1 {
                voted_directory_id.expect("load admission proves the v1 directory id is valid")
            } else {
                Uuid::nil()
            },
        }),
    };
    let leader_epoch =
        u32::try_from(data.leader_epoch).expect("load admission proves the epoch is nonnegative");
    // Leadership is VOLATILE, not durable: Raft persists only currentTerm
    // (`leader_epoch`) and votedFor (`voted_key`), never the current leader. A
    // restarted node must NOT trust a persisted `leader_id` — especially an
    // ex-leader, which would otherwise come back believing it is still the
    // leader (stale `leader_id == self`), publish itself via `watch_leader`,
    // and never re-discover the real leader elected while it was down. Start
    // with no known leader; the node re-attaches via the current leader's
    // `BeginQuorumEpoch` heartbeat (higher epoch → Follower) or a re-election.
    Ok(Some(QuorumState {
        cluster_id,
        leader_epoch,
        leader_id: None,
        voted_key,
        voters: voters.clone(),
        kraft_version: u16::try_from(data_version)
            .expect("load admission proves the schema version is zero or one"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_and_load_quorum_state_with_no_leader() {
        let dir = tempfile::TempDir::new().unwrap();
        let cluster_id = Uuid::new_v4();
        let voter = krabka_metadata::voters::Voter {
            id: NodeId(1),
            directory_id: Uuid::nil(),
            endpoints: vec![krabka_metadata::voters::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
        };
        let state = QuorumState {
            cluster_id,
            leader_epoch: 3,
            leader_id: None,
            voted_key: None,
            voters: krabka_metadata::VoterSet::from_voters([voter]),
            kraft_version: 0,
        };
        save_quorum_state(dir.path(), &state).expect("save");
        let path = dir.path().join(QUORUM_STATE_FILE);
        let content = std::fs::read_to_string(&path).expect("read");
        assert2::assert!(content.contains("\"leaderId\":-1"));

        let loaded = load_quorum_state(dir.path(), cluster_id, &state.voters)
            .expect("load")
            .expect("some");
        assert2::assert!(loaded.leader_id == None);
        assert2::assert!(loaded.leader_epoch == 3);
    }
}
