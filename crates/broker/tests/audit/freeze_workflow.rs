//! The KFC-9 freeze-and-thaw workflow, and the cluster that hosts it.
//!
//! One whole incident goes over the wire here: alice freezes `orders` under
//! her own operator key, opens a break-glass proposal for the thaw, bob and
//! carol approve it, and alice thaws. A second proposal is then withdrawn
//! rather than spent, because a withdrawal is the one path that reaches the
//! `consumed` phase with no Kafka transition behind it. The cases in
//! [`crate::freeze_evidence`] and [`crate::freeze_metrics`] read that run back,
//! off the audit topic and off the metric families.
//!
//! # Why this runs over SASL
//!
//! A two-person rule needs two people. A plaintext listener authenticates
//! every connection as one name, so a proposer over such a listener is also
//! the only available approver and the broker refuses them by design. This
//! cluster speaks `SASL_PLAINTEXT` so that alice, bob and carol are three real
//! principals and no broker-side shortcut stands in for a second person.

use assert2::assert;
use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::{
    krabka::{
        break_glass::{
            ApproveBreakGlassRequest, ApproveBreakGlassResponse, ProposeBreakGlassRequest,
            ProposeBreakGlassResponse,
        },
        freeze::{PATTERN_TYPE_LITERAL, SetTopicFreezeRequest, SetTopicFreezeResponse},
    },
    owned::metadata_request::MetadataRequest,
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    freeze_signing::{FreezeBytes, freeze_signing_bytes},
    support,
};

/// The proposer. She freezes the topic, opens the proposal, and thaws it.
pub(super) const ALICE: &str = "User:alice";
/// The first approver.
pub(super) const BOB: &str = "User:bob";
/// The second approver. Two are needed: `break_glass.required_approvals`
/// defaults to two, and the proposer may not approve her own proposal.
pub(super) const CAROL: &str = "User:carol";

/// The operator key alice signs a freeze under.
pub(super) const ALICE_KEY_ID: &str = "alice-yubi";

/// The scope alice freezes, and the scope the proposal names.
const FROZEN_SCOPE: &str = "orders";
/// The same scope as a break-glass target, which is `"<pattern>:<scope>"`.
pub(super) const FROZEN_TARGET: &str = "literal:orders";
/// The target of the second proposal, the one that is withdrawn rather than
/// spent. It names a different scope so that a case joining on the first
/// proposal cannot match it by accident.
const WITHDRAWN_TARGET: &str = "literal:invoices";

pub(super) const FREEZE_REASON: &str = "DR cutover: stop writes before the promotion";
pub(super) const PROPOSE_REASON: &str = "the promotion finished; hand the topic back";
pub(super) const THAW_REASON: &str = "promotion complete";
const WITHDRAW_REASON: &str = "raised against the wrong scope";

/// `BreakGlassAction::ThawTopicFreeze` as the wire spells it.
///
/// The broker keeps that mapping `pub(crate)`, so this is a copy of it. It is
/// not a copy taken on trust: the proposal's own audit record names the action
/// in words, and
/// [`crate::freeze_evidence::every_kfc9_phase_writes_one_chained_audit_record`]
/// asserts that this byte reaches the log as `thaw_topic_freeze`.
const ACTION_THAW_TOPIC_FREEZE: i8 = 1;

// Milliseconds since the Unix epoch, as the operator's own machine reads them.
krabka_macros::unix_millis_fixture!(
    pub(super) now_ms, "the clock is after the Unix epoch", "epoch milliseconds fit in an i64"
);

/// One `SetTopicFreeze` request as the operator's own machine builds it: the
/// signature is made here, from the private key that never reaches a broker.
pub(super) struct SignedFreeze<'a> {
    pub(super) key: &'a support::OperatorKey,
    pub(super) cluster_id: &'a str,
    pub(super) frozen: bool,
    pub(super) reason: &'a str,
    pub(super) proposal_id: uuid::Uuid,
    pub(super) set_at_ms: i64,
}

pub(super) async fn send_signed_freeze(
    client: &krabka_client_core::Client,
    freeze: &SignedFreeze<'_>,
) -> SetTopicFreezeResponse {
    let message = freeze_signing_bytes(&FreezeBytes {
        cluster_id: freeze.cluster_id,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: FROZEN_SCOPE,
        frozen: freeze.frozen,
        reason: freeze.reason,
        set_by: &freeze.key.principal,
        set_at_ms: freeze.set_at_ms,
        proposal_id: freeze.proposal_id,
    });
    client
        .send(SetTopicFreezeRequest {
            scope: FROZEN_SCOPE.into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: freeze.frozen,
            reason: freeze.reason.into(),
            proposal_id: WireUuid(*freeze.proposal_id.as_bytes()),
            set_at_ms: freeze.set_at_ms,
            key_id: freeze.key.key_id.clone(),
            signature: freeze.key.pair().sign(&message).as_ref().to_vec(),
            ..SetTopicFreezeRequest::default()
        })
        .await
        .expect("SetTopicFreeze")
}

/// Open a proposal to thaw `target`.
pub(super) async fn propose_thaw(
    client: &krabka_client_core::Client,
    target: &str,
    reason: &str,
) -> ProposeBreakGlassResponse {
    client
        .send(ProposeBreakGlassRequest {
            action: ACTION_THAW_TOPIC_FREEZE,
            target: target.into(),
            reason: reason.into(),
            // Zero asks for `break_glass.proposal_ttl`.
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass")
}

/// Approve a proposal, or withdraw it.
pub(super) async fn settle_proposal(
    client: &krabka_client_core::Client,
    proposal_id: WireUuid,
    withdraw: bool,
) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id,
            withdraw,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// The cluster id, as any client reads it.
///
/// The signed freeze bytes cover it, so an auditor needs it. It is a
/// client-visible fact about the cluster and not a reading of the metadata
/// image's freeze registry, which is the thing these cases refuse to consult.
pub(super) async fn cluster_id_of(client: &krabka_client_core::Client) -> String {
    client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata")
        .cluster_id
        .expect("the broker reports a cluster id")
}

/// One broker that trusts alice's operator key, takes all three principals as
/// its break-glass approvers, and holds a `PLAIN` credential for each.
///
/// alice is in the approver set because a proposer has to be: a proposer from
/// outside it could open a proposal that two approvers then sign, which turns a
/// rule about three people into a rule about two and a stranger. She still
/// cannot approve her own proposal, which is why bob and carol both do.
pub(super) struct Cluster {
    pub(super) broker: krabka_broker::BrokerHandle,
    pub(super) bootstrap: String,
    /// alice's key. Only the public half matters once the workflow has run:
    /// that is the half an auditor holds.
    pub(super) alice_key: support::OperatorKey,
    /// Where the audit partition's segment files live, for the offline reader.
    pub(super) log_dir: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

impl Cluster {
    pub(super) async fn workflow_clients(
        &self,
    ) -> (
        krabka_client_core::Client,
        krabka_client_core::Client,
        krabka_client_core::Client,
        String,
    ) {
        let alice = support::sasl_client(&self.bootstrap, "alice", "alice-pw").await;
        let bob = support::sasl_client(&self.bootstrap, "bob", "bob-pw").await;
        let carol = support::sasl_client(&self.bootstrap, "carol", "carol-pw").await;
        let cluster_id = cluster_id_of(&alice).await;
        (alice, bob, carol, cluster_id)
    }
}

pub(super) async fn boot_workflow_cluster() -> Cluster {
    let dir = tempfile::tempdir().expect("tempdir");
    // The keys live beside the log directory rather than inside it, so no
    // stray file lands where the broker enumerates partition directories.
    let keys = dir.path().join("keys");
    std::fs::create_dir_all(&keys).expect("create the operator key directory");
    let alice_key = support::mint_operator_key(&keys, ALICE_KEY_ID, ALICE);
    let log_dir = dir.path().join("data");

    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        &log_dir,
        crate::support::OperatorSaslSetup {
            trust: crate::support::OperatorKeysSetup {
                keys: &[&alice_key],
                approvers: &[ALICE, BOB, CAROL],
            },
            users: &[
                ("alice", "alice-pw"),
                ("bob", "bob-pw"),
                ("carol", "carol-pw"),
            ],
        },
    )
    .await;
    broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;
    Cluster {
        broker,
        bootstrap,
        alice_key,
        log_dir,
        _dir: dir,
    }
}

/// One complete freeze-and-thaw workflow, and the facts a case needs to join
/// its records back together.
pub(super) struct ThawWorkflow {
    pub(super) cluster: Cluster,
    /// alice's client. Every case reads the audit topic back through it.
    pub(super) alice: krabka_client_core::Client,
    /// The cluster id inside the signed freeze bytes.
    pub(super) cluster_id: String,
    /// The proposal that authorized the thaw.
    pub(super) proposal_id: uuid::Uuid,
    /// The timestamp alice signed into the freeze record.
    ///
    /// This is the one field of the signed bytes that the audit event does not
    /// carry; see
    /// [`crate::freeze_evidence::a_signed_freeze_reverifies_from_the_audit_topic_with_no_metadata_image`].
    pub(super) freeze_set_at_ms: i64,
}

/// Run every KFC-9 phase once, in the order an incident produces them.
///
/// alice freezes `orders` under her own signature, proposes the thaw, bob and
/// carol approve, and alice thaws — which spends the approval in the same raft
/// append that removes the entry. A second proposal is then withdrawn rather
/// than spent, because a withdrawal is the one path that records the
/// `consumed` phase without a Kafka transition behind it.
pub(super) async fn run_thaw_workflow() -> ThawWorkflow {
    let cluster = boot_workflow_cluster().await;
    let (alice, bob, carol, cluster_id) = cluster.workflow_clients().await;

    let freeze_set_at_ms = now_ms();
    let frozen = send_signed_freeze(
        &alice,
        &SignedFreeze {
            key: &cluster.alice_key,
            cluster_id: &cluster_id,
            frozen: true,
            reason: FREEZE_REASON,
            proposal_id: uuid::Uuid::nil(),
            set_at_ms: freeze_set_at_ms,
        },
    )
    .await;
    assert!(frozen.error_code == 0, "the freeze was refused: {frozen:?}");

    let proposed = propose_thaw(&alice, FROZEN_TARGET, PROPOSE_REASON).await;
    assert!(
        proposed.error_code == 0,
        "the proposal was refused: {proposed:?}"
    );
    for (label, client) in [("bob", &bob), ("carol", &carol)] {
        let approved = settle_proposal(client, proposed.proposal_id, false).await;
        assert!(
            approved.error_code == 0,
            "{label}'s approval was refused: {approved:?}"
        );
    }

    // A signed record's timestamp must beat the entry it replaces. The two
    // requests are several round trips apart, so wall time already does that;
    // the `max` makes it true whatever the clock's resolution.
    let thawed = send_signed_freeze(
        &alice,
        &SignedFreeze {
            key: &cluster.alice_key,
            cluster_id: &cluster_id,
            frozen: false,
            reason: THAW_REASON,
            proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
            set_at_ms: now_ms().max(freeze_set_at_ms + 1),
        },
    )
    .await;
    assert!(thawed.error_code == 0, "the thaw was refused: {thawed:?}");

    let withdrawn = propose_thaw(&alice, WITHDRAWN_TARGET, WITHDRAW_REASON).await;
    assert!(
        withdrawn.error_code == 0,
        "the second proposal was refused: {withdrawn:?}"
    );
    let settled = settle_proposal(&bob, withdrawn.proposal_id, true).await;
    assert!(
        settled.error_code == 0,
        "the withdrawal was refused: {settled:?}"
    );

    ThawWorkflow {
        cluster,
        alice,
        cluster_id,
        proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
        freeze_set_at_ms,
    }
}
