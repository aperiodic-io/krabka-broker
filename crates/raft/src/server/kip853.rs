//! The KIP-853 controller admin surface: the API keys the listener owns, the
//! router that picks a handler for one of them, the `DescribeQuorum` read, and
//! the per-API refusal an unauthorized caller receives.

use bytes::{Bytes, BytesMut};

use super::voter_admin::{
    add_raft_voter_response, remove_raft_voter_response, update_raft_voter_response,
};
use crate::{error::RaftError, kraft::KraftController};

pub(super) const API_KEY_DESCRIBE_QUORUM: i16 = 55;
pub(super) const API_KEY_ADD_RAFT_VOTER: i16 = 80;
pub(super) const API_KEY_REMOVE_RAFT_VOTER: i16 = 81;
pub(super) const API_KEY_UPDATE_RAFT_VOTER: i16 = 82;

pub(super) async fn kip853_admin_response(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key {
        API_KEY_DESCRIBE_QUORUM => describe_quorum_response(version, body, engine).await,
        API_KEY_ADD_RAFT_VOTER => add_raft_voter_response(version, body, engine).await,
        API_KEY_REMOVE_RAFT_VOTER => remove_raft_voter_response(version, body, engine).await,
        API_KEY_UPDATE_RAFT_VOTER => update_raft_voter_response(version, body, engine).await,
        _ => unreachable!("filtered KIP-853 admin API key"),
    }
}

async fn describe_quorum_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{Decode, Encode, owned::describe_quorum_request::DescribeQuorumRequest};

    let request = DescribeQuorumRequest::decode(&mut &body[..], version)?;
    let quorum = engine.quorum_state().await?;
    let mut output = BytesMut::new();
    super::describe_quorum::describe_quorum(&request, &quorum).encode(&mut output, version)?;
    Ok(output.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::Decode;
    use uuid::Uuid;

    use super::*;
    use crate::server::test_support::{
        activate_dynamic_membership, decoded, single_voter_engine, wait_for_leader,
    };

    /// The listener answers `DescribeQuorum` from the live engine: the
    /// elected leader describes the metadata partition, and a request for
    /// another partition gets `UNKNOWN_TOPIC_OR_PARTITION`. The response rows
    /// are covered as whole structs in `server::describe_quorum`.
    #[tokio::test]
    async fn describe_quorum_answers_only_for_the_metadata_partition() {
        use krabka_protocol::{
            Encode as _,
            owned::{describe_quorum_request, describe_quorum_response::DescribeQuorumResponse},
        };

        fn body(version: i16, topic: &str, partition: i32) -> Bytes {
            let request = crate::server::test_support::quorum_request(topic, partition);
            let mut buf = BytesMut::new();
            request.encode(&mut buf, version).expect("encode request");
            buf.freeze()
        }

        let version = describe_quorum_request::MAX_VERSION;
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        // (label, topic, partition, error code, leader id)
        for (label, topic, index, error_code, leader_id) in [
            ("the metadata partition", "__cluster_metadata", 0, 0, 1),
            ("another topic", "orders", 0, 3, 0),
            ("another partition", "__cluster_metadata", 7, 3, 0),
        ] {
            let answered = describe_quorum_response(version, &body(version, topic, index), &engine)
                .await
                .expect("an answer");
            let decoded =
                DescribeQuorumResponse::decode(&mut &answered[..], version).expect("decode");
            let partition = &decoded.topics[0].partitions[0];
            check!(
                (
                    partition.partition_index,
                    partition.error_code,
                    partition.leader_id
                ) == (index, error_code, leader_id),
                "{label}"
            );
        }

        // A voter of a two-voter quorum with no leader yet leads nothing.
        let (follower, _follower_dir) = crate::server::test_support::test_engine_with_voters(
            1,
            [
                crate::server::test_support::voter(1, vec![]),
                crate::server::test_support::voter(2, vec![]),
            ],
        );
        let answered =
            describe_quorum_response(version, &body(version, "__cluster_metadata", 0), &follower)
                .await
                .expect("an answer");
        let decoded = DescribeQuorumResponse::decode(&mut &answered[..], version).expect("decode");
        check!(decoded.topics[0].partitions[0].error_code == 6);
    }

    #[tokio::test]
    async fn kip853_controller_apis_describe_exact_identity_and_reject_last_removal() {
        use krabka_protocol::{
            Encode,
            owned::{
                add_raft_voter_request::AddRaftVoterRequest,
                add_raft_voter_response::AddRaftVoterResponse,
                describe_quorum_response::DescribeQuorumResponse,
                remove_raft_voter_request::RemoveRaftVoterRequest,
                remove_raft_voter_response::RemoveRaftVoterResponse,
                update_raft_voter_request::UpdateRaftVoterRequest,
                update_raft_voter_response::UpdateRaftVoterResponse,
            },
        };

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        activate_dynamic_membership(&engine).await;

        let describe = crate::server::test_support::quorum_request("__cluster_metadata", 0);
        let mut request_body = bytes::BytesMut::new();
        describe.encode(&mut request_body, 2).unwrap();
        let response_body =
            super::kip853_admin_response(API_KEY_DESCRIBE_QUORUM, 2, &request_body, &engine)
                .await
                .expect("DescribeQuorum");
        let response = decoded::<DescribeQuorumResponse>(&response_body, 2);
        let partition = &response.topics[0].partitions[0];
        check!(response.topics[0].topic_name == "__cluster_metadata");
        check!(partition.partition_index == 0);
        check!(partition.leader_id == 1);
        check!(partition.leader_epoch >= 1);
        check!(partition.high_watermark == engine.quorum_state().await.unwrap().high_watermark);
        check!(partition.current_voters[0].replica_id == 1);
        check!(
            partition.current_voters[0].replica_directory_id.0 == *Uuid::from_u128(1).as_bytes()
        );
        check!(partition.current_voters[0].log_end_offset >= 0);
        check!(partition.observers.is_empty());
        check!(response.nodes[0].node_id == 1);
        check!(response.nodes[0].listeners[0].name == "CONTROLLER");
        check!(response.nodes[0].listeners[0].host == "controller-1");
        check!(response.nodes[0].listeners[0].port == 9093);

        // Deliver Inbound::Fetch from observer so partition.observers is non-empty
        let req = crate::kraft::transport::wire::PeerRequest::Fetch {
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
            from: crate::NodeId(99),
            current_leader_epoch: partition.leader_epoch,
            fetch_epoch: partition.leader_epoch.cast_unsigned(),
            fetch_offset: 0,
            replica_directory_id: Uuid::from_u128(99),
        };
        let req_bytes = req.try_encode().expect("encode fetch");
        let (tx, rx) = tokio::sync::oneshot::channel();
        engine
            .deliver(crate::kraft::transport::Inbound::Fetch {
                version: crate::kraft::transport::wire::FETCH_VERSION,
                req: req_bytes,
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await;

        let describe_body = {
            let mut buf = bytes::BytesMut::new();
            describe.encode(&mut buf, 2).unwrap();
            buf
        };
        let response_body =
            super::kip853_admin_response(API_KEY_DESCRIBE_QUORUM, 2, &describe_body, &engine)
                .await
                .expect("DescribeQuorum with observer");
        let response = decoded::<DescribeQuorumResponse>(&response_body, 2);
        let partition = &response.topics[0].partitions[0];
        check!(partition.observers.len() == 1);
        check!(partition.observers[0].replica_id == 99);

        // Test API_KEY_ADD_RAFT_VOTER through kip853_admin_response
        let add = AddRaftVoterRequest {
            cluster_id: None,
            voter_id: -1,
            ..Default::default()
        };
        let mut add_body = bytes::BytesMut::new();
        add.encode(&mut add_body, 0).unwrap();
        let add_resp_body =
            super::kip853_admin_response(API_KEY_ADD_RAFT_VOTER, 0, &add_body, &engine)
                .await
                .expect("AddRaftVoter");
        let add_resp = decoded::<AddRaftVoterResponse>(&add_resp_body, 0);
        check!(add_resp.error_code == 42);

        // Test API_KEY_UPDATE_RAFT_VOTER through kip853_admin_response
        let update = UpdateRaftVoterRequest {
            cluster_id: Some("00000000-0000-0000-0000-0000000000ff".into()),
            ..Default::default()
        };
        let mut update_body = bytes::BytesMut::new();
        update.encode(&mut update_body, 0).unwrap();
        let update_resp_body =
            super::kip853_admin_response(API_KEY_UPDATE_RAFT_VOTER, 0, &update_body, &engine)
                .await
                .expect("UpdateRaftVoter");
        let update_resp = decoded::<UpdateRaftVoterResponse>(&update_resp_body, 0);
        check!(update_resp.error_code == 104);

        let remove = RemoveRaftVoterRequest {
            cluster_id: Some(engine.current_image().cluster_id().to_string()),
            voter_id: 1,
            voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                *Uuid::from_u128(1).as_bytes(),
            ),
            ..Default::default()
        };
        let mut request_body = bytes::BytesMut::new();
        remove.encode(&mut request_body, 0).unwrap();
        let response_body =
            super::kip853_admin_response(API_KEY_REMOVE_RAFT_VOTER, 0, &request_body, &engine)
                .await
                .expect("RemoveRaftVoter");
        let response = decoded::<RemoveRaftVoterResponse>(&response_body, 0);
        // `VoterSet.removeVoter` refuses to empty the set, and
        // `RemoveVoterHandler` answers that with VOTER_NOT_FOUND (127).
        check!(
            response
                == RemoveRaftVoterResponse {
                    error_code: crate::voter_requests::VOTER_NOT_FOUND,
                    error_message: Some(format!(
                        "Cannot remove voter {} from the set of voters",
                        crate::voter_requests::replica_key(1, remove.voter_directory_id)
                    )),
                    ..Default::default()
                }
        );

        engine.shutdown().await;
    }

    /// `describe_quorum_response` decodes the wire request, reads the live
    /// engine's snapshot and encodes the answer for the elected leader with a
    /// committed record. The response rows themselves, including a follower's
    /// refusal and an unmappable or never-fetched replica, are covered as
    /// whole structs in `server::describe_quorum` against a hand-built
    /// snapshot; this test only checks the glue that reads the real engine.
    #[tokio::test]
    async fn describe_quorum_response_answers_the_elected_leader() {
        use krabka_protocol::{Encode, owned::describe_quorum_response::DescribeQuorumResponse};

        use crate::server::test_support::topic_record;

        let describe = crate::server::test_support::quorum_request("__cluster_metadata", 0);

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        engine
            .submit_change(vec![topic_record("t1")])
            .await
            .expect("submit");

        let mut req_body = bytes::BytesMut::new();
        describe.encode(&mut req_body, 2).unwrap();
        let resp_body = super::describe_quorum_response(2, &req_body, &engine)
            .await
            .unwrap();
        let resp = DescribeQuorumResponse::decode(&mut resp_body.as_ref(), 2).unwrap();
        let partition = &resp.topics[0].partitions[0];
        assert2::assert!(partition.leader_id == 1);
        assert2::assert!(partition.current_voters[0].replica_id == 1);
        assert2::assert!(partition.current_voters[0].log_end_offset >= 1);
        assert2::assert!(resp.nodes[0].node_id == 1);
        engine.shutdown().await;
    }
}
