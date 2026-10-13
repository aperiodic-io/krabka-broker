//! Losing a whole site by stopping its broker: which survivor leads
//! afterwards, what the in-sync set becomes, and whether `acks=all` keeps
//! committing.
//!
//! This is the site-loss half of the claim the witness role exists for. The
//! unreachable-site half, where every broker stays up, is in
//! `unreachable_site`.

use {
    crate::{
        NODE_A, NODE_B, NODE_C, SITE_B, WITNESS,
        cluster::cluster_with_topic,
        cluster_lock,
        produce::{client_at, produce_once, produce_until_committed},
        view::{PartitionView, partition_view, wait_for_leader_and_isr, witness_never_leads},
    },
    assert2::{assert, check},
    krabka_broker::codes,
    std::{collections::BTreeSet, time::Duration},
};

#[bon::builder]
fn preferred_partition_view(isr: BTreeSet<u64>) -> PartitionView {
    PartitionView {
        leader: 1,
        replicas: vec![1, 2, WITNESS],
        isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
    }
}

/// Placement pins the partition to the preferred site: `replicas[0]` is the
/// `site-a` broker and it is the leader, with the other data site and the
/// witness in the ISR behind it. `acks=all` commits with all three sites up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_site_holds_replicas_zero_and_the_leader() {
    let _guard = cluster_lock().lock().await;
    let (cluster, topic_id) = cluster_with_topic().await;

    check!(
        partition_view(cluster.handle(NODE_A))
            == Some(
                preferred_partition_view()
                    .isr(maplit::btreeset! {1, 2, WITNESS})
                    .call(),
            ),
        "replicas[0] and the leader are both the preferred site's broker"
    );

    check!(
        produce_until_committed(&cluster.addr(NODE_A), topic_id).await == codes::NONE,
        "acks=all commits with all three sites up"
    );

    cluster.shutdown().await;
}

/// **The headline claim.** Losing a whole *data* site leaves one data replica
/// and the witness. The witness is a full ISR member, so the in-sync set is
/// still two — `min.insync.replicas` — and `acks=all` keeps committing.
///
/// This is the property the witness role exists for. Without a data-bearing
/// witness the ISR would drop to one member here and every `acks=all` write
/// would be refused for as long as the site was down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_survives_the_loss_of_the_non_preferred_data_site() {
    let _guard = cluster_lock().lock().await;
    let (mut cluster, topic_id) = cluster_with_topic().await;

    cluster.stop(NODE_B).await;

    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the ISR shrinks to the preferred site plus the witness",
        1,
        &[1, WITNESS],
    )
    .await;
    check!(
        partition_view(cluster.handle(NODE_A))
            == Some(
                preferred_partition_view()
                    .isr(maplit::btreeset! {1, WITNESS})
                    .call(),
            ),
        "the witness keeps the ISR at two members after a data site is lost"
    );

    let code = produce_until_committed(&cluster.addr(NODE_A), topic_id).await;
    assert!(
        code == codes::NONE,
        "THE STRETCH-CLUSTER CLAIM FAILED: with data site {SITE_B} down, the surviving \
         data replica (node 1) and the witness (node {WITNESS}) are two in-sync replicas, \
         so an acks=all write MUST still commit under min.insync.replicas=2. \
         The leader refused it with error_code={code}. Either the witness left the ISR or \
         it stopped counting toward min.insync.replicas — the whole point of the role."
    );

    cluster.shutdown().await;
}

/// One case of the site-loss table: which site is lost, and what the survivors
/// must do afterwards.
struct SiteLossCase {
    /// What the case shows, used in the failure messages.
    claim: &'static str,
    /// The cluster index of the site that goes down.
    stop: usize,
    /// The node that must lead once the loss has settled.
    leader: u64,
    /// The ISR once the loss has settled.
    isr: &'static [u64],
    /// The cluster index that must accept `acks=all` afterwards.
    write_to: usize,
}

/// The remaining single-site losses, which differ only by which site goes down.
///
/// * The witness site is lost: the two data replicas are still in sync, so the
///   ISR is two and writes continue.
/// * The preferred data site is lost: leadership moves to the **other data
///   site**, never to the witness, and writes continue there against an ISR of
///   the surviving data replica plus the witness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_site_loss_keeps_acks_all_committing() {
    let _guard = cluster_lock().lock().await;

    for case in [
        SiteLossCase {
            claim: "the witness site is lost: the two data replicas carry the write",
            stop: NODE_C,
            leader: 1,
            isr: &[1, 2],
            write_to: NODE_A,
        },
        SiteLossCase {
            claim: "the preferred data site is lost: the other data site leads, \
                    with the witness in the ISR behind it",
            stop: NODE_A,
            leader: 2,
            isr: &[2, WITNESS],
            write_to: NODE_B,
        },
    ] {
        let (mut cluster, topic_id) = cluster_with_topic().await;
        cluster.stop(case.stop).await;

        let observer = cluster.handle(case.write_to);
        wait_for_leader_and_isr(observer, case.claim, case.leader, case.isr).await;
        check!(
            partition_view(observer)
                == Some(PartitionView {
                    leader: case.leader,
                    replicas: vec![1, 2, WITNESS],
                    isr: case.isr.iter().copied().collect(),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                }),
            "{}",
            case.claim
        );
        check!(
            produce_until_committed(&cluster.addr(case.write_to), topic_id).await == codes::NONE,
            "acks=all must still commit — {}",
            case.claim
        );

        cluster.shutdown().await;
    }
}

/// Losing **both** data sites is beyond what the topology promises. The
/// witness holds every committed record, but it serves no client and it never
/// leads: the partition has no leader a client can write to, and the witness
/// refuses the write outright rather than electing itself.
///
/// Refusing is the safe answer. A witness that took leadership here would be
/// serving clients from a node the deployment sized for neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_data_sites_down_leaves_no_leader_and_the_witness_refuses_writes() {
    let _guard = cluster_lock().lock().await;
    let (mut cluster, topic_id) = cluster_with_topic().await;

    cluster.stop(NODE_A).await;
    cluster.stop(NODE_B).await;

    // The witness is the only broker left. Watch it for long enough to cover
    // the failover path it must not take.
    witness_never_leads(cluster.handle(NODE_C), Duration::from_secs(5)).await;

    let witness = client_at(&cluster.addr(NODE_C)).await;
    let code = produce_once(&witness, topic_id, 3_000).await;
    check!(
        code == codes::NOT_LEADER_OR_FOLLOWER,
        "the witness must refuse an acks=all write when both data sites are down, \
         with the code that sends a client looking for another leader; got {code}"
    );

    let view = partition_view(cluster.handle(NODE_C));
    check!(
        view.as_ref().is_some_and(|view| view.leader != WITNESS),
        "no live broker leads the partition, and the witness is not it: {view:?}"
    );

    cluster.shutdown().await;
}
