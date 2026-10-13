//! `DescribeQuorum` forwarding carries the CALLER's own principal to the
//! active controller, not this node's inter-broker identity (review of
//! #1034).
//!
//! `krabka_raft::ControllerHandle::forward_raw` dials the leader's
//! controller listener as this node's own inter-broker identity -- on this
//! test's plaintext controller listener, `ANONYMOUS`. Before the fix,
//! `describe_quorum::handle` sent the bare `DescribeQuorumRequest` bytes
//! over that dial, so the leader's controller listener authorized `Describe`
//! against `ANONYMOUS`, not the caller who already passed
//! `cluster_describe_denied` on the follower -- a caller holding `Describe`
//! would be fenced out the moment it landed on a follower instead of the
//! leader. The fix wraps the forward in a KIP-590 `Envelope` carrying the
//! caller's own principal, so the leader authorizes and answers under the
//! identity that actually asked.

mod support;

use {
    crate::support::{
        quorum::{follower_config, two_node_leader},
        sasl_client, shutdown_cluster, start_n_node_with,
    },
    assert2::check,
    krabka_broker::{
        BrokerConfig,
        authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    },
    krabka_metadata::AclOperation,
    krabka_security::{ListenerProtocol, SaslMechanism},
};

const ALICE: &str = "alice";
const ALICE_PASSWORD: &str = "alice-secret";

/// Grants `ClusterAction` to EVERY principal -- raft consensus and the
/// KIP-590 envelope's own outer gate both need it from the inter-broker
/// identity (`ANONYMOUS` on this test's plaintext controller listener) just
/// to run at all -- and `Describe` to `alice` alone.
///
/// No ACL log entry is ever submitted: the rule is a static, config-time
/// decision, so it is in force from the very first bootstrap RPC. An
/// ACL-log-backed authorizer cannot grant anything before a leader is
/// elected to commit the grant, and granting the inter-broker identity a
/// blanket bypass (a super user, as other controller-listener authorization
/// tests do) would make it hold `Describe` too, which defeats this test:
/// the whole point is that the inter-broker identity must NOT be able to
/// answer `Describe` on the caller's behalf.
#[derive(Debug)]
struct ClusterActionForEveryoneDescribeForAlice;

impl Authorizer for ClusterActionForEveryoneDescribeForAlice {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        match req.operation {
            AclOperation::ClusterAction => AuthorizationResult::Allow,
            AclOperation::Describe if req.principal.name == ALICE => AuthorizationResult::Allow,
            _ => AuthorizationResult::Deny,
        }
    }
}

use crate::support::quorum::metadata_quorum_request as describe_quorum_request;

/// A caller holding `Describe` succeeds even when the node it asks is not the
/// active controller and has to forward the request: the forward carries
/// alice's own principal, which this test's authorizer grants `Describe`,
/// not the inter-broker identity, which it explicitly does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_describe_authorized_caller_succeeds_through_a_forwarding_follower() {
    let cluster = start_n_node_with(2, |_, cfg: &mut BrokerConfig| {
        // Replace the default plaintext data listener with SASL/PLAIN, bound
        // to the same address `start_n_node_with` already reserved, so alice
        // authenticates as herself rather than as `ANONYMOUS`.
        cfg.listeners = vec![crate::support::listeners::listener(
            "SASL_PLAINTEXT",
            cfg.listen_addr,
            ListenerProtocol::SaslPlaintext,
        )];
        cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_owned();
        cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        cfg.plain_credentials
            .insert(ALICE.to_owned(), ALICE_PASSWORD.to_owned());
        cfg.authorizer = std::sync::Arc::new(ClusterActionForEveryoneDescribeForAlice);
    })
    .await
    .expect("2-node cluster");

    // Ask node 0 first to learn the elected leader, whichever role it holds.
    let (_, first_cfg, _dir0) = &cluster[0];
    let first_client = sasl_client(&first_cfg.listen_addr.to_string(), ALICE, ALICE_PASSWORD).await;
    let first_resp = first_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from node 0");
    check!(first_resp.error_code == 0, "top-level error_code");
    let leader_id = two_node_leader(&first_resp, (1, 2));
    let follower_cfg = follower_config(&cluster, leader_id);
    let follower_client =
        sasl_client(&follower_cfg.listen_addr.to_string(), ALICE, ALICE_PASSWORD).await;
    let follower_resp = follower_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from the follower");

    check!(follower_resp.error_code == 0, "top-level error_code");
    let partition = &follower_resp.topics[0].partitions[0];
    check!(
        partition.error_code == 0,
        "the follower must forward under alice's own identity, which this \
         test's authorizer grants Describe, not its inter-broker one, which \
         it does not: {partition:?}"
    );
    check!(partition.leader_id == leader_id);

    shutdown_cluster(cluster).await;
}
