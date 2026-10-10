//! The write half of a broker-only node: a [`MetadataWriter`] that dials the
//! controller quorum and replays the batch as an `API_KEY_SUBMIT_CHANGE`
//! request. It is separate from the read path because the retry ordering and
//! the wire encoding are a concern of their own.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord, PrivateRpc};
use krabka_raft::{DelegationTokenMutation, NodeId, OutboundDialer, RaftError, SubmitChangeResult};
use tokio::sync::watch;

use super::MetadataWriter;
use crate::controller_endpoint::quorum_targets;

/// Forwards metadata writes from a broker-only node to the controller
/// quorum. Tries the leader hint first (from the observer), then walks the
/// voters of the image's voter set, the static voters, and the bootstrap
/// servers, in that order. Mirrors the `API_KEY_SUBMIT_CHANGE` request the
/// controller already serves.
pub struct QuorumForwarder {
    /// Static voter map `(id, "<host>:<port>")`, from
    /// `controller_quorum_voters`. The map carries the host verbatim. The
    /// dialer re-resolves it on each connect, so it reaches a rejoining peer's
    /// new pod IP.
    pub(crate) voters: Vec<(NodeId, String)>,
    /// `controller.quorum.bootstrap.servers`, which a node in a KIP-853
    /// dynamic quorum configures in place of `voters`.
    pub(crate) bootstrap_servers: Vec<String>,
    /// The observer's image, whose KIP-853 voter set names the endpoint of
    /// each voter, and so of the leader, in a dynamic quorum.
    pub(crate) image: watch::Receiver<Arc<MetadataImage>>,
    pub(crate) dialer: Arc<dyn OutboundDialer>,
    pub(crate) client_id: String,
    pub(crate) client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub(crate) client_frame_max: krabka_client_core::ClientFrameMax,
    pub(crate) leader: watch::Receiver<Option<NodeId>>,
}

impl QuorumForwarder {
    /// The controllers this node can address now: the voter set of the latest
    /// image, the static voters, and the bootstrap servers.
    fn targets(&self) -> Vec<(NodeId, String)> {
        quorum_targets(&self.image.borrow(), &self.voters, &self.bootstrap_servers)
    }

    /// The version of `rpc` to send: the one the `krabka.version` finalized
    /// in the observer's image gives. Before the first fetch the image is
    /// empty, which reads as level 0 and sends v0, the 1.0.0 baseline.
    fn request_version(&self, rpc: PrivateRpc) -> i16 {
        krabka_raft::private_request_version(rpc, Some(&self.image.borrow()))
    }

    async fn try_submit(
        &self,
        target: NodeId,
        addr: &str,
        rpc: PrivateRpc,
        body: &[u8],
    ) -> Result<krabka_raft::KrabkaSubmitChangeResponse, RaftError> {
        let version = self.request_version(rpc);
        let opts = krabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            frame_max: self.client_frame_max,
            ..krabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(target, addr, opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(rpc.api_key(), version, bytes::Bytes::copy_from_slice(body))
            .await
            .map_err(RaftError::Network)?;
        conn.close();
        let mut cur: &[u8] = &resp_body;
        krabka_raft::KrabkaSubmitChangeResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }
}

/// Order the voters to try when forwarding `submit_change`: the hinted leader
/// first when it is known and present in the set, then every OTHER voter as a
/// fallback. The function is pure, so a unit test can check the ordering
/// without a live quorum. That includes the "every voter except the hint"
/// fallback.
fn build_forward_order(voters: &[(NodeId, String)], hint: Option<NodeId>) -> Vec<(NodeId, String)> {
    let mut order: Vec<(NodeId, String)> = Vec::new();
    if let Some(l) = hint
        && let Some(t) = voters.iter().find(|(id, _)| *id == l)
    {
        order.push(t.clone());
    }
    for v in voters {
        if Some(v.0) != hint {
            order.push(v.clone());
        }
    }
    order
}

#[async_trait::async_trait]
impl MetadataWriter for QuorumForwarder {
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        let payload =
            <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
                &records,
            )
            .map_err(RaftError::from)?;
        let req = krabka_raft::KrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(payload),
        };
        // + 4 for the length-prefix encode_v0 writes ahead of the records.
        let mut body = Vec::with_capacity(req.records.len() + 4);
        req.encode_v0(&mut body).map_err(RaftError::Protocol)?;

        let hint = *self.leader.borrow();
        let order = build_forward_order(&self.targets(), hint);

        let mut last_err = RaftError::NotLeader {
            current_leader: hint,
        };
        for (target, addr) in order {
            match self
                .try_submit(target, &addr, PrivateRpc::SubmitChange, &body)
                .await
            {
                Ok(resp) if resp.error_code == 0 => {
                    return <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Deserialize>::deserialize(
                        &resp.result,
                    )
                    .map_err(RaftError::from);
                }
                // error_code 2 => leader rejected at apply-time. Match the
                // controller's own forward path (`forward_submit_to`), which
                // collapses the typed `MetadataError` into `TopicExists` since
                // the wire carries only an error code and the forwarded write
                // of record is CreateTopics (-> Kafka TOPIC_ALREADY_EXISTS).
                Ok(resp) if resp.error_code == 2 => {
                    return Err(RaftError::Metadata(
                        krabka_metadata::MetadataError::TopicExists(String::new()),
                    ));
                }
                // The leader answered, and it refused until its tail commits.
                // Another voter is not the leader, so stop here.
                Ok(resp) if resp.error_code == krabka_raft::SUBMIT_CHANGE_UNCOMMITTED_TAIL => {
                    return Err(RaftError::UncommittedTail);
                }
                // Every controller authorizes the same principal the same
                // way, so another voter would refuse too.
                Ok(resp)
                    if resp.error_code == krabka_raft::PRIVATE_CLUSTER_AUTHORIZATION_FAILED =>
                {
                    return Err(RaftError::ClusterAuthorizationFailed);
                }
                Ok(resp) => {
                    last_err = RaftError::NotLeader {
                        current_leader: (resp.leader_hint >= 0)
                            .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(0))),
                    };
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    async fn forward_raw(
        &self,
        api_key: i16,
        version: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        let hint = *self.leader.borrow();
        let order = build_forward_order(&self.targets(), hint);
        let mut last_err = RaftError::NotLeader {
            current_leader: hint,
        };
        for (target, addr) in &order {
            let opts = krabka_client_core::ConnectionOptions {
                client_id: self.client_id.clone(),
                dispatch_queue_capacity: self.client_dispatch_queue_capacity,
                frame_max: self.client_frame_max,
                ..krabka_client_core::ConnectionOptions::default()
            };
            let conn = match self.dialer.dial(*target, addr, opts).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = RaftError::Network(e);
                    continue;
                }
            };
            let resp = match conn.raw_request(api_key, version, body.clone()).await {
                Ok(r) => r,
                Err(e) => {
                    conn.close();
                    last_err = RaftError::Network(e);
                    continue;
                }
            };
            conn.close();
            return Ok(resp);
        }
        Err(last_err)
    }

    async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        let payload = <serde_wincode::SerdeCompat<Vec<DelegationTokenMutation>> as wincode::Serialize>::serialize(
            &mutations,
        )
        .map_err(RaftError::from)?;
        let request = krabka_raft::KrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(payload),
        };
        let mut body = Vec::with_capacity(request.records.len() + 4);
        request.encode_v0(&mut body).map_err(RaftError::Protocol)?;

        let hint = *self.leader.borrow();
        let mut last_error = RaftError::NotLeader {
            current_leader: hint,
        };
        for (target, addr) in build_forward_order(&self.targets(), hint) {
            match self
                .try_submit(target, &addr, PrivateRpc::DelegationTokenMutation, &body)
                .await
            {
                Ok(response) if response.error_code == 0 => {
                    return <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Deserialize>::deserialize(
                        &response.result,
                    )
                    .map_err(RaftError::from);
                }
                Ok(response)
                    if response.error_code == krabka_raft::PRIVATE_CLUSTER_AUTHORIZATION_FAILED =>
                {
                    return Err(RaftError::ClusterAuthorizationFailed);
                }
                Ok(response) => {
                    last_error = RaftError::NotLeader {
                        current_leader: (response.leader_hint >= 0)
                            .then(|| NodeId(u64::try_from(response.leader_hint).unwrap_or(0))),
                    };
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::{Bytes, BytesMut};
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };

    use super::*;
    use crate::metadata_source::test_support::topic_record;

    fn voters() -> Vec<(krabka_raft::NodeId, String)> {
        vec![
            (krabka_audit::NodeId(1), "h1:9093".to_string()),
            (krabka_audit::NodeId(2), "h2:9093".to_string()),
            (krabka_audit::NodeId(3), "h3:9093".to_string()),
        ]
    }

    fn api_versions_response_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn submit_change_response_body(error_code: i16, leader_hint: i64) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        let result =
            <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Serialize>::serialize(
                &SubmitChangeResult::default(),
            )
            .expect("serialize submit result");
        krabka_raft::KrabkaSubmitChangeResponse {
            error_code,
            leader_hint,
            result: Bytes::from(result),
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    use crate::test_support::RecordingDialer;

    fn forwarder(
        addr: SocketAddr,
        client_ids: Arc<Mutex<Vec<String>>>,
        leader_hint: Option<NodeId>,
    ) -> QuorumForwarder {
        let (_leader_tx, leader_rx) = watch::channel(leader_hint);
        let (_image_tx, image_rx) = watch::channel(Arc::new(MetadataImage::new(uuid::Uuid::nil())));
        QuorumForwarder {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            voters: vec![(NodeId(1), addr.to_string())],
            bootstrap_servers: vec![],
            image: image_rx,
            dialer: Arc::new(RecordingDialer { client_ids }),
            client_id: "forwarder-client".into(),
            leader: leader_rx,
        }
    }

    /// A node in a dynamic quorum configures bootstrap servers and no voters,
    /// and its writes reach the quorum through them before it has read the
    /// voter set.
    #[tokio::test]
    async fn quorum_forwarder_reaches_the_quorum_through_a_bootstrap_server() {
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_SUBMIT_CHANGE,
            move || submit_change_response_body(0, -1),
            api_versions_response_v0,
        )
        .await;
        let forwarder = QuorumForwarder {
            voters: vec![],
            bootstrap_servers: vec![mock.addr.to_string()],
            ..forwarder(
                mock.addr,
                Arc::new(Mutex::new(Vec::new())),
                Some(NodeId(3001)),
            )
        };

        let outcome = forwarder
            .submit_change(vec![topic_record("bootstrapped")])
            .await;

        assert2::assert!(
            matches!(&outcome, Ok(result) if *result == SubmitChangeResult::default()),
            "{outcome:?}"
        );
        mock.stop();
    }

    #[test]
    fn forward_order_hinted_leader_first_then_every_other_voter() {
        // Hint = 2 → try the leader first, then the OTHER voters as fallback.
        // A flipped `Some(v.0) != hint` (i.e. `== hint`) would re-push only the
        // hinted voter and drop the fallbacks, leaving no peer to retry when the
        // hint is stale.
        let order = build_forward_order(&voters(), Some(krabka_audit::NodeId(2)));
        assert2::assert!(
            (order)
                == (vec![
                    (krabka_raft::NodeId(2), "h2:9093".to_string()),
                    (krabka_raft::NodeId(1), "h1:9093".to_string()),
                    (krabka_raft::NodeId(3), "h3:9093".to_string()),
                ])
        );
    }

    #[test]
    fn forward_order_no_hint_tries_all_voters() {
        // No leader hint → fall back to trying every voter. A flipped predicate
        // (`== None`) would push nothing, so the forward could reach no peer.
        let order = build_forward_order(&voters(), None);
        assert2::assert!((order) == (voters()));
    }

    #[test]
    fn forward_order_unknown_hint_still_tries_all_voters() {
        // Hint names a voter not in the set → no leader-first entry, but every
        // voter is still tried (hint 9 != each id).
        let order = build_forward_order(&voters(), Some(krabka_audit::NodeId(9)));
        assert2::assert!((order) == (voters()));
    }

    #[tokio::test]
    async fn quorum_forwarder_applied_response_returns_ok_and_sends_client_id() {
        let submit_requests = Arc::new(AtomicUsize::new(0));
        let submit_requests_for_mock = submit_requests.clone();
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_SUBMIT_CHANGE,
            move || {
                submit_requests_for_mock.fetch_add(1, Ordering::SeqCst);
                submit_change_response_body(0, -1)
            },
            api_versions_response_v0,
        )
        .await;
        let client_ids = Arc::new(Mutex::new(Vec::new()));
        let forwarder = forwarder(mock.addr, client_ids.clone(), Some(NodeId(1)));

        forwarder
            .submit_change(vec![topic_record("applied")])
            .await
            .expect("applied");

        assert2::assert!((submit_requests.load(Ordering::SeqCst)) == (1));
        assert2::assert!(
            client_ids
                .lock()
                .unwrap()
                .iter()
                .any(|id| id == "forwarder-client")
        );
        mock.stop();
    }

    /// Both forwards send the request version the `krabka.version` finalized
    /// in the observer's image negotiates: v0 with the feature absent, which
    /// is the empty image a node holds before its first fetch, and at levels
    /// 0 and 1, which are the krabka-broker 1.0.0 formats.
    #[tokio::test]
    async fn quorum_forwarder_sends_the_negotiated_private_version() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_for_mock = sent.clone();
        let mock =
            krabka_client_core::MockBroker::start(move |api_key, version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                sent_for_mock.lock().unwrap().push((api_key, version));
                Some(submit_change_response_body(0, -1))
            })
            .await;
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for level in [None, Some(0), Some(1)] {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(
                    krabka_metadata::FeatureLevelRecord {
                        name: krabka_metadata::krabka_version::KRABKA_VERSION_FEATURE.into(),
                        level,
                    },
                ));
            }
            let (_image_tx, image_rx) = watch::channel(Arc::new(image));
            let forwarder = QuorumForwarder {
                image: image_rx,
                ..forwarder(mock.addr, Arc::new(Mutex::new(Vec::new())), Some(NodeId(1)))
            };
            forwarder
                .submit_change(vec![topic_record("negotiated")])
                .await
                .expect("applied");
            forwarder
                .submit_delegation_token_mutations(vec![])
                .await
                .expect("applied");
            actual.push((level, std::mem::take(&mut *sent.lock().unwrap())));
            expected.push((
                level,
                vec![
                    (krabka_raft::API_KEY_SUBMIT_CHANGE, 0),
                    (krabka_raft::API_KEY_DELEGATION_TOKEN_MUTATION, 0),
                ],
            ));
        }
        assert2::assert!(actual == expected);
        mock.stop();
    }

    async fn rejected_submission(code: i16, leader: i64, topic: &str) -> RaftError {
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_SUBMIT_CHANGE,
            move || submit_change_response_body(code, leader),
            api_versions_response_v0,
        )
        .await;
        let forwarder = forwarder(mock.addr, Arc::new(Mutex::new(Vec::new())), Some(NodeId(1)));
        let error = forwarder
            .submit_change(vec![topic_record(topic)])
            .await
            .expect_err("metadata submission rejected");
        mock.stop();
        error
    }

    #[tokio::test]
    async fn quorum_forwarder_error_code_two_maps_to_topic_exists() {
        let err = rejected_submission(2, -1, "already-exists").await;

        assert2::assert!(matches!(
            err,
            RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))
        ));
    }

    #[tokio::test]
    async fn quorum_forwarder_not_leader_response_preserves_positive_hint() {
        let err = rejected_submission(1, 7, "redirect").await;

        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(7))
            }
        ));
    }

    #[tokio::test]
    async fn quorum_forwarder_negative_leader_hint_is_unknown() {
        let err = rejected_submission(3, -1, "unknown-leader").await;

        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
    }
}
