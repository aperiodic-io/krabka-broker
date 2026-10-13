//! The two Krabka-private metadata RPCs the controller listener serves: the
//! follower-forwarded `SubmitChange` (1003) that applies records on the leader,
//! and the observer `MetadataFetch` (1004) that streams a committed
//! `__cluster_metadata` slice to a broker.

use {
    crate::{
        DelegationTokenMutation, NodeId, SubmitChangeResult,
        error::RaftError,
        kraft::{Epoch, KraftController, ReplicaKey},
        wire::{
            KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest,
            KrabkaSubmitChangeResponse, SUBMIT_CHANGE_UNCOMMITTED_TAIL,
        },
    },
    bytes::Bytes,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
};

/// `KrabkaSubmitChangeResponse::error_code`: the change was applied.
const SUBMIT_CHANGE_APPLIED: i16 = 0;
/// `KrabkaSubmitChangeResponse::error_code`: this node is not the leader;
/// consult `leader_hint`.
const SUBMIT_CHANGE_NOT_LEADER: i16 = 1;
/// `KrabkaSubmitChangeResponse::error_code`: metadata validation rejected the
/// records (also returned when the wincode body fails to decode).
const SUBMIT_CHANGE_REJECTED: i16 = 2;
/// `KrabkaSubmitChangeResponse::error_code`: any other engine failure.
const SUBMIT_CHANGE_FAILED: i16 = 3;

/// `leader_hint` sentinel meaning "the current leader is unknown".
const LEADER_HINT_UNKNOWN: i64 = -1;

/// Handle a follower-forwarded `submit_change` (1003). The forwarder wrapped a
/// wincode-encoded `Vec<MetadataRecord>`; we submit it to the local engine
/// (presumably the leader) and translate the result into the `error_code` enum:
/// `0` applied, `1` not leader (with `leader_hint`), `2` metadata-rejected.
pub(super) async fn dispatch_submit_change(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = KrabkaSubmitChangeRequest::decode_v0(&mut cur)?;
    let records: Vec<krabka_metadata::MetadataRecord> = match <serde_wincode::SerdeCompat<
        Vec<krabka_metadata::MetadataRecord>,
    > as wincode::Deserialize>::deserialize(
        &req.records
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "submit-change body decode failed");
            let resp = KrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_REJECTED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            };
            let mut out = Vec::with_capacity(16);
            resp.encode_v0(&mut out)?;
            return Ok(Bytes::from(out));
        }
    };
    let resp = match engine.submit_change(records).await {
        Ok(result) => applied_change(&result)?,
        Err(e) => submit_change_failure(&e),
    };
    let mut out = Vec::with_capacity(16);
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

fn applied_change(result: &SubmitChangeResult) -> Result<KrabkaSubmitChangeResponse, RaftError> {
    Ok(KrabkaSubmitChangeResponse {
        error_code: SUBMIT_CHANGE_APPLIED,
        leader_hint: LEADER_HINT_UNKNOWN,
        result: Bytes::from(
            <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Serialize>::serialize(
                result,
            )?,
        ),
    })
}

/// The forwarded `submit_change` response for a local submit that failed with
/// `error`.
fn submit_change_failure(error: &RaftError) -> KrabkaSubmitChangeResponse {
    let (error_code, leader_hint) = match error {
        RaftError::Metadata(_) => (SUBMIT_CHANGE_REJECTED, LEADER_HINT_UNKNOWN),
        RaftError::NotLeader { current_leader } => (
            SUBMIT_CHANGE_NOT_LEADER,
            current_leader
                .and_then(|l| i64::try_from(l.0).ok())
                .unwrap_or(LEADER_HINT_UNKNOWN),
        ),
        RaftError::UncommittedTail => (SUBMIT_CHANGE_UNCOMMITTED_TAIL, LEADER_HINT_UNKNOWN),
        other => {
            tracing::warn!(error = ?other, "submit-change failed");
            (SUBMIT_CHANGE_FAILED, LEADER_HINT_UNKNOWN)
        }
    };
    KrabkaSubmitChangeResponse {
        error_code,
        leader_hint,
        result: Bytes::new(),
    }
}

/// Handle a generation-bound delegation-token mutation forwarded to the
/// leader. The request reuses the private length-prefixed opaque payload and
/// response envelope, but deserializes a typed guarded mutation batch.
pub(super) async fn dispatch_delegation_token_mutation(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = KrabkaSubmitChangeRequest::decode_v0(&mut cur)?;
    let mutations: Vec<DelegationTokenMutation> = match <serde_wincode::SerdeCompat<
        Vec<DelegationTokenMutation>,
    > as wincode::Deserialize>::deserialize(
        &req.records
    ) {
        Ok(mutations) => mutations,
        Err(error) => {
            tracing::warn!(%error, "delegation-token mutation body decode failed");
            let response = KrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_REJECTED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            };
            let mut out = Vec::with_capacity(16);
            response.encode_v0(&mut out)?;
            return Ok(Bytes::from(out));
        }
    };
    let response = match engine.submit_delegation_token_mutations(mutations).await {
        Ok(result) => applied_change(&result)?,
        Err(RaftError::NotLeader { current_leader }) => KrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_NOT_LEADER,
            leader_hint: current_leader
                .and_then(|leader| i64::try_from(leader.0).ok())
                .unwrap_or(LEADER_HINT_UNKNOWN),
            result: Bytes::new(),
        },
        Err(error) => {
            tracing::warn!(?error, "delegation-token mutation failed");
            KrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_FAILED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            }
        }
    };
    let mut out = Vec::with_capacity(16);
    response.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

/// Serve a committed `__cluster_metadata` slice to a broker-only observer (1004)
/// from the engine's `KraftLog`.
fn quorum_metadata_leader(quorum: Option<(Option<NodeId>, Epoch)>) -> (i64, i32) {
    let leader_hint: i64 = quorum
        .and_then(|(leader_id, _)| leader_id)
        .and_then(|l| i64::try_from(l.0).ok())
        .unwrap_or(LEADER_HINT_UNKNOWN);
    let leader_epoch = quorum.map_or(-1, |(_, epoch)| i32::try_from(epoch).unwrap_or(i32::MAX));
    (leader_hint, leader_epoch)
}

pub(super) async fn dispatch_metadata_fetch(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = KrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let fetch_offset = req.fetch_offset.max(0);
    // The decoded `int32` enters the domain here; the codec itself stays raw so
    // the request stays byte-exact. A negative budget clamps to zero, as before.
    let max_size = ByteSize::from_bytes_i64(i64::from(req.max_bytes.max(0)));
    // A negative replica id is a fetcher that is not a replica, as in Kafka's
    // `FetchRequest`, and the leader tracks nothing for it.
    let replica = u64::try_from(req.replica_id).ok().map(|id| ReplicaKey {
        id: NodeId(id),
        directory_id: req.replica_directory_id,
    });
    let slice = engine
        .metadata_fetch(fetch_offset, max_size, replica)
        .await?;
    let quorum = engine.quorum_state().await.ok();
    let (leader_hint, leader_epoch) =
        quorum_metadata_leader(quorum.as_ref().map(|qs| (qs.leader_id, qs.leader_epoch)));

    let resp = KrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        leader_epoch,
        log_start_offset: slice.log_start_offset,
        high_watermark: slice.high_watermark,
        quorum_high_watermark: slice.quorum_high_watermark,
        snapshot_id: slice.snapshot_id,
        records: slice.records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            DelegationTokenMutation, NodeId,
            server::{
                dispatch::dispatch,
                test_support::{single_voter_engine, test_engine_with_voters, wait_for_leader},
            },
            test_support::single_partition_topic,
            wire::{
                API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
                PRIVATE_UNSUPPORTED_VERSION, private_api_highest_version,
            },
        },
        assert2::check,
        krabka_ids::ApiKey,
        krabka_metadata::MetadataRecord,
        uuid::Uuid,
    };

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(single_partition_topic(name, Uuid::new_v4()))
    }

    fn submit_change_body(records: &[MetadataRecord]) -> Bytes {
        let records = records.to_vec();
        let records =
            <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
                &records,
            )
            .expect("wincode");
        let req = KrabkaSubmitChangeRequest {
            records: Bytes::from(records),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).expect("submit request");
        Bytes::from(out)
    }

    fn metadata_fetch_body(fetch_offset: i64, max_bytes: i32) -> Bytes {
        let req = KrabkaMetadataFetchRequest {
            fetch_offset,
            max_bytes,
            replica_id: -1,
            replica_directory_id: Uuid::nil(),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        Bytes::from(out)
    }

    fn decode_submit_change_response(body: &[u8]) -> KrabkaSubmitChangeResponse {
        let mut cur = body;
        KrabkaSubmitChangeResponse::decode_v0(&mut cur).expect("submit response")
    }

    fn decode_metadata_fetch_response(body: &[u8]) -> KrabkaMetadataFetchResponse {
        let mut cur = body;
        KrabkaMetadataFetchResponse::decode_v0(&mut cur).expect("metadata fetch response")
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_success_and_decode_errors() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let ok_body = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            0,
            submit_change_body(&[topic_record("submit-ok")]),
            &engine,
        )
        .await
        .expect("submit dispatch");
        let ok = decode_submit_change_response(&ok_body);
        assert2::assert!(ok.error_code == 0);
        assert2::assert!(ok.leader_hint == -1);

        let bad_req = KrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"not-wincode"),
        };
        let mut bad_body = Vec::new();
        bad_req.encode_v0(&mut bad_body).unwrap();
        let err_body = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            0,
            Bytes::from(bad_body),
            &engine,
        )
        .await
        .expect("decode failure dispatch");
        let err = decode_submit_change_response(&err_body);
        assert2::assert!(err.error_code == 2);
        assert2::assert!(err.leader_hint == -1);
    }

    /// A private request one past the highest version this build's
    /// `krabka.version` table gives is answered with `UNSUPPORTED_VERSION` in
    /// the v0 shape, and the body is never decoded at the wrong version or
    /// applied.
    #[tokio::test]
    async fn dispatch_answers_an_unsupported_private_version() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        let past_max = |api_key| private_api_highest_version(api_key).expect("a private api") + 1;

        let submit = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            past_max(API_KEY_SUBMIT_CHANGE),
            submit_change_body(&[topic_record("from-a-later-build")]),
            &engine,
        )
        .await
        .expect("submit dispatch");
        check!(
            decode_submit_change_response(&submit)
                == KrabkaSubmitChangeResponse {
                    error_code: PRIVATE_UNSUPPORTED_VERSION,
                    leader_hint: -1,
                    result: Bytes::new(),
                }
        );
        check!(engine.current_image().topic("from-a-later-build").is_none());

        let fetch = dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            past_max(API_KEY_METADATA_FETCH),
            metadata_fetch_body(0, 1024),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");
        check!(
            decode_metadata_fetch_response(&fetch)
                == KrabkaMetadataFetchResponse {
                    error_code: PRIVATE_UNSUPPORTED_VERSION,
                    leader_hint: -1,
                    leader_epoch: -1,
                    log_start_offset: -1,
                    high_watermark: -1,
                    quorum_high_watermark: -1,
                    snapshot_id: None,
                    records: Bytes::new(),
                }
        );
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_metadata_rejection() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let topic = topic_record("duplicate");
        let first = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            0,
            submit_change_body(std::slice::from_ref(&topic)),
            &engine,
        )
        .await
        .expect("first submit");
        assert2::assert!(decode_submit_change_response(&first).error_code == 0);

        let duplicate = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            0,
            submit_change_body(&[topic]),
            &engine,
        )
        .await
        .expect("duplicate submit");
        let duplicate = decode_submit_change_response(&duplicate);
        assert2::assert!(duplicate.error_code == 2);
        assert2::assert!(duplicate.leader_hint == -1);
    }

    #[test]
    fn a_failed_submit_encodes_the_code_the_forwarding_node_translates() {
        for (case, error, error_code, leader_hint) in [
            (
                "metadata rejection",
                RaftError::Metadata(krabka_metadata::MetadataError::TopicExists("t".into())),
                SUBMIT_CHANGE_REJECTED,
                LEADER_HINT_UNKNOWN,
            ),
            (
                "not the leader",
                RaftError::NotLeader {
                    current_leader: Some(NodeId(3)),
                },
                SUBMIT_CHANGE_NOT_LEADER,
                3,
            ),
            (
                "uncommitted tail",
                RaftError::UncommittedTail,
                SUBMIT_CHANGE_UNCOMMITTED_TAIL,
                LEADER_HINT_UNKNOWN,
            ),
            (
                "other failure",
                RaftError::ChangeRejected("no".into()),
                SUBMIT_CHANGE_FAILED,
                LEADER_HINT_UNKNOWN,
            ),
        ] {
            check!(
                submit_change_failure(&error)
                    == KrabkaSubmitChangeResponse {
                        error_code,
                        leader_hint,
                        result: Bytes::new(),
                    },
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_clamps_negative_request_and_reports_unknown_leader() {
        let (engine, _dir) = test_engine_with_voters(1, std::iter::empty());

        let body = dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            0,
            metadata_fetch_body(-5, -1),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.high_watermark,
                resp.records.is_empty(),
            ) == (0, -1, 0, true)
        );
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_returns_committed_records_and_leader_hint() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        engine
            .submit_change(vec![topic_record("metadata-fetch")])
            .await
            .expect("submit");

        let body = dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            0,
            metadata_fetch_body(0, 1_048_576),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.leader_epoch,
                resp.high_watermark >= 1,
                resp.records.is_empty(),
            ) == (0, 1, 1, true, false)
        );
    }

    #[tokio::test]
    async fn dispatch_routes_delegation_token_mutation() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let mutation = DelegationTokenMutation::Delete {
            expected: krabka_metadata::DelegationTokenRecord {
                token_id: "token-1".into(),
                owner: krabka_security::KafkaPrincipal {
                    principal_type: "User".into(),
                    name: "alice".into(),
                },
                requester: krabka_security::KafkaPrincipal {
                    principal_type: "User".into(),
                    name: "alice".into(),
                },
                issue_timestamp_ms: 0,
                expiry_timestamp_ms: 0,
                max_timestamp_ms: 0,
                renewers: Vec::new(),
            },
        };
        let payload = <serde_wincode::SerdeCompat<Vec<DelegationTokenMutation>> as wincode::Serialize>::serialize(
            &vec![mutation],
        ).expect("serialize mutation");

        let req = KrabkaSubmitChangeRequest {
            records: Bytes::from(payload),
        };
        let mut req_body = Vec::new();
        req.encode_v0(&mut req_body).expect("encode req");

        let resp_bytes = dispatch(
            ApiKey(API_KEY_DELEGATION_TOKEN_MUTATION),
            0,
            Bytes::from(req_body),
            &engine,
        )
        .await
        .expect("dispatch delegation token mutation");

        let mut cur = &resp_bytes[..];
        let resp = KrabkaSubmitChangeResponse::decode_v0(&mut cur).expect("decode response");
        assert2::assert!(resp.error_code == SUBMIT_CHANGE_APPLIED);
    }

    #[test]
    fn quorum_metadata_leader_defaults_on_missing_or_overflow_quorum() {
        let (hint, epoch) = quorum_metadata_leader(None);
        check!(hint == -1);
        check!(epoch == -1);

        let (hint, epoch) = quorum_metadata_leader(Some((Some(NodeId(42)), 5)));
        check!(hint == 42);
        check!(epoch == 5);

        let (hint, epoch) = quorum_metadata_leader(Some((None, 0)));
        check!(hint == -1);
        check!(epoch == 0);

        let (hint, epoch) = quorum_metadata_leader(Some((Some(NodeId(1)), u32::MAX)));
        check!(hint == 1);
        check!(epoch == i32::MAX);
    }
}
