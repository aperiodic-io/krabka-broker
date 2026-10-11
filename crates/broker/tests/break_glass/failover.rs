//! An approved proposal outlives the controller that recorded it.
//!
//! The approver set is a per-node file value, so the design leans on the
//! proposal living in the metadata log for any node that becomes controller to
//! spend it. The case takes the controller away and asks the survivor.
//!
//! A `PLAINTEXT` listener has one principal, so the two approvals are written
//! straight into the log here. The wire path that produces them is proved on
//! the SASL broker elsewhere in the suite.

use assert2::check;
use krabka_broker::{codes, config::BackgroundUncleanRecovery};
use krabka_metadata::{
    BreakGlassApproval as StoredApproval, BreakGlassProposalRecord, MetadataRecord,
};

use crate::{
    cluster::{index_of, plain_client, start_gated_cluster, wait_for_new_leader},
    principals::{BOB, CAROL, principal},
    proposals::{ACTION_DELETE_TOPIC, now_ms, open, stored},
    support::{self, shutdown_cluster},
    topics::{create_topic, delete_topic_as_admin_client},
};

/// An approved proposal outlives the controller that recorded it.
///
/// The approver set is a per-node file value, and the design leans on the
/// proposal itself living in the metadata log so that any node which becomes
/// controller can spend it. If an approval only existed on the node that took
/// it, an incident response would die with the controller that a rolling
/// restart or a crash removed — which is exactly when a break-glass approval
/// matters most.
///
/// A `PLAINTEXT` listener has one principal, so the two approvals are written
/// straight into the metadata log here. What has to survive the failover is the
/// record; the wire path that produces it is proved on the SASL broker above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_approved_proposal_survives_a_controller_failover() {
    let mut cluster = start_gated_cluster(3, BackgroundUncleanRecovery::Off).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let leader = cluster[0].0.wait_until_controller_leader().await;
    let leader_index = index_of(&cluster, leader);
    let follower_index = (0..cluster.len())
        .find(|index| *index != leader_index)
        .expect("a follower");
    let client = plain_client(&cluster[follower_index].1.listen_addr.to_string()).await;

    create_topic(&client, "doomed", 3).await;
    let id = open(&client, ACTION_DELETE_TOPIC, "doomed").await;
    let wanted = uuid::Uuid::from_bytes(id.0);
    cluster[leader_index]
        .0
        .wait_for_image(|image| image.break_glass_proposal(wanted).is_some())
        .await;

    let held = cluster[leader_index]
        .0
        .controller_image_for_test()
        .break_glass_proposal(wanted)
        .expect("the proposal reached the leader's image")
        .clone();
    cluster[leader_index]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1BreakGlassProposal(
            BreakGlassProposalRecord {
                approvals: vec![approval_by(BOB), approval_by(CAROL)],
                ..held
            },
        ))
        .await
        .expect("the approvals commit");
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|image| {
                image
                    .break_glass_proposal(wanted)
                    .is_some_and(|proposal| proposal.approvals.len() == 2)
            })
            .await;
    }
    let before = stored(&client, id).await;

    let (old_controller, _config, _dir) = cluster.remove(leader_index);
    old_controller.shutdown().await;
    let elected = wait_for_new_leader(&cluster[0].0, leader).await;
    check!(elected != leader, "a different node holds the quorum now");

    let elected_index = index_of(&cluster, elected);
    let after_client = plain_client(&cluster[elected_index].1.listen_addr.to_string()).await;
    check!(
        stored(&after_client, id).await == before,
        "the proposal crossed the failover unchanged"
    );
    // The new leader refuses the consume with `NOT_CONTROLLER` until it
    // commits a record from its own epoch. A Kafka admin client retries that
    // code, so the case retries it the same way.
    check!(
        delete_topic_as_admin_client(&after_client, "doomed").await == codes::NONE,
        "the surviving controller spends the approval"
    );
    cluster[elected_index]
        .0
        .wait_for_image(|image| {
            image
                .break_glass_proposal(wanted)
                .is_some_and(|proposal| proposal.consumed_at_ms != 0)
        })
        .await;
    check!(stored(&after_client, id).await.consumed_at_ms != 0);

    shutdown_cluster(cluster).await;
}

/// One unsigned approval by `user`, in the metadata form.
fn approval_by(user: &str) -> StoredApproval {
    StoredApproval {
        principal: principal(user),
        approved_at_ms: now_ms(),
        key_id: String::new(),
        signature: Vec::new(),
    }
}
