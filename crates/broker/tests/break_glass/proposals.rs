//! The KFC-9 break-glass wire itself: opening a proposal, reading one back,
//! approving one, and the canonical bytes an approval signature covers.
//!
//! Every case reaches the broker through these, so a change to the request
//! shapes lands in one place. The signing bytes are built here by hand rather
//! than borrowed from the broker's own encoder, which is what makes a verified
//! signature evidence that the two layouts agree.

use {
    crate::{
        cluster::Cluster,
        principals::{ALICE, BOB, CAROL},
        support,
    },
    assert2::assert,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::break_glass::{
            ApproveBreakGlassRequest, ApproveBreakGlassResponse, DescribeBreakGlassRequest,
            DescribedBreakGlassProposal, ProposeBreakGlassRequest,
        },
        primitives::uuid::Uuid as WireUuid,
    },
};

// KFC-9 pins these as the wire values of the broker's action enum. They are
// written out here rather than derived from the broker, so a renumbering that
// kept the broker self-consistent still fails this suite.

/// `unclean_elect_leaders`.
pub(super) const ACTION_UNCLEAN_ELECT_LEADERS: i8 = 2;
/// `unregister_broker`.
pub(super) const ACTION_UNREGISTER_BROKER: i8 = 4;
/// `cancel_reassignment`.
pub(super) const ACTION_CANCEL_REASSIGNMENT: i8 = 5;
/// `delete_topic`.
pub(super) const ACTION_DELETE_TOPIC: i8 = 6;
/// `delete_records`.
pub(super) const ACTION_DELETE_RECORDS: i8 = 7;

/// The `ttl_ms` value that asks the broker for its configured lifetime.
pub(super) const TTL_CONFIGURED: i64 = 0;

/// Open a proposal, and answer the whole response so a caller can assert on a
/// refusal as easily as on a success.
pub(super) async fn propose(
    client: &Client,
    action: i8,
    target: &str,
    ttl_ms: i64,
) -> krabka_protocol::krabka::break_glass::ProposeBreakGlassResponse {
    client
        .send(ProposeBreakGlassRequest {
            action,
            target: target.to_owned(),
            reason: "an integration case".to_owned(),
            ttl_ms,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass")
}

/// Open a proposal that the broker must accept, and answer its id.
pub(super) async fn open(client: &Client, action: i8, target: &str) -> WireUuid {
    let response = propose(client, action, target, TTL_CONFIGURED).await;
    assert!(
        response.error_code == codes::NONE,
        "propose {action} on {target}: {response:?}"
    );
    response.proposal_id
}

/// Read one stored proposal back.
pub(super) async fn stored(client: &Client, id: WireUuid) -> DescribedBreakGlassProposal {
    let response = client
        .send(DescribeBreakGlassRequest {
            proposal_id: id,
            ..DescribeBreakGlassRequest::default()
        })
        .await
        .expect("DescribeBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "describe {id:?}: {response:?}"
    );
    response
        .proposals
        .into_iter()
        .find(|proposal| proposal.proposal_id == id)
        .expect("the proposal the cluster holds")
}

/// Add one unsigned approval.
pub(super) async fn approve(client: &Client, id: WireUuid) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id: id,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// Add one approval signed by `key`, over the proposal the broker holds.
///
/// The signature covers the stored record and not one the caller made up, so
/// this reads the proposal back first, exactly as `krabka-guard` does.
pub(super) async fn approve_signed(
    client: &Client,
    id: WireUuid,
    key: &support::OperatorKey,
) -> ApproveBreakGlassResponse {
    let held = stored(client, id).await;
    let signature = key
        .pair()
        .sign(&approval_signing_bytes(&held))
        .as_ref()
        .to_vec();
    approve_with(client, id, &key.key_id, signature).await
}

/// Add one approval carrying `key_id` and `signature` verbatim.
pub(super) async fn approve_with(
    client: &Client,
    id: WireUuid,
    key_id: &str,
    signature: Vec<u8>,
) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id: id,
            key_id: key_id.to_owned(),
            signature,
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// The canonical bytes an approval signature covers.
///
/// This is the layout KFC-9 documents, built here by hand. The broker rebuilds
/// the same bytes from its own record, so a signature this file makes verifies
/// inside the broker only when the two agree. Deriving these bytes from the
/// broker's own encoder would make the check vacuous.
pub(super) fn approval_signing_bytes(proposal: &DescribedBreakGlassProposal) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"krabka-break-glass-v1\0");
    out.extend_from_slice(&proposal.proposal_id.0);
    out.extend_from_slice(&proposal.action.to_be_bytes());
    push_len_prefixed(&mut out, proposal.target.as_bytes());
    push_len_prefixed(&mut out, proposal.proposer.as_bytes());
    out.extend_from_slice(&proposal.created_at_ms.to_be_bytes());
    out.extend_from_slice(&proposal.expires_at_ms.to_be_bytes());
    out
}

/// Append `bytes` behind its `u32` big-endian length.
fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("a compact field");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Alice proposes, Bob and Carol approve. The proposal is then usable.
pub(super) async fn approved_proposal(cluster: &Cluster, action: i8, target: &str) -> WireUuid {
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    let carol = cluster.client(CAROL).await;
    let id = open(&alice, action, target).await;
    let first = approve(&bob, id).await;
    assert!(first.error_code == codes::NONE, "bob approves: {first:?}");
    let second = approve(&carol, id).await;
    assert!(
        second.error_code == codes::NONE,
        "carol approves: {second:?}"
    );
    id
}

// This process's wall clock in epoch milliseconds, which is the clock the
// broker stamps a proposal against.
krabka_macros::unix_millis_fixture!(
    pub(super) now_ms, "a clock after the epoch", "a clock inside i64 milliseconds"
);
