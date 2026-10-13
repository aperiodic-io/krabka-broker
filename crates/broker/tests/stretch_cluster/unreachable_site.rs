//! A site that is unreachable rather than stopped: every broker stays up and
//! the network in front of one site is taken away instead.
//!
//! This is the half of the partition claim the relay harness can produce
//! honestly — what an isolated *leader* must stop doing — and it is written
//! against the one-way cut the crate documentation describes.

use {
    crate::{
        NODE_A, NODE_C, WITNESS, cluster_lock,
        linked::linked_cluster_with_topic,
        produce::{client_at, produce_once, produce_until_committed},
        view::{wait_for_leader_and_isr, witness_never_leads},
    },
    assert2::check,
    krabka_broker::codes,
    std::time::Duration,
};

/// A leader whose replicas can no longer reach it must stop acknowledging
/// `acks=all` writes, and must not hand leadership to the witness on the way.
///
/// This is the safety half of a partitioned leader site. Nothing here is
/// stopped: the leader is still running, still holds its log, and still
/// believes it leads. What it has lost is the ability to have a write copied
/// anywhere else. Its replicas stop fetching, so it drops them from the in-sync
/// set, and the in-sync set of one no longer satisfies
/// `min.insync.replicas=2` — `NOT_ENOUGH_REPLICAS`, before the record is even
/// appended.
///
/// Healing puts it back: the replicas catch up, the ISR returns to three, and
/// `acks=all` commits again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_leader_stops_acknowledging_acks_all_and_recovers_on_heal() {
    let _guard = cluster_lock().lock().await;
    let (cluster, topic_id) = linked_cluster_with_topic().await;

    let leader_addr = cluster.addr(NODE_A);
    check!(
        produce_until_committed(&leader_addr, topic_id).await == codes::NONE,
        "acks=all commits while every site is reachable"
    );

    cluster.cut(NODE_A);

    // A write after the cut lands in the leader's log and nowhere else, so it
    // must not be acknowledged. It also moves the leader's log end past the
    // replicas': Kafka keeps a follower whose log end equals the leader's in
    // sync however long ago it fetched (`ReplicaState.isCaughtUp`), so only
    // a record the replicas lack lets their lag clock run out.
    let leader = client_at(&leader_addr).await;
    let stranded = produce_once(&leader, topic_id, 3_000).await;
    check!(
        stranded != codes::NONE,
        "a write only the unreachable leader holds must not be acknowledged"
    );

    // The replicas stop fetching, so the leader drops them: the in-sync set
    // becomes itself alone. Waiting for that is what makes the refusal below
    // deterministic, and it is the observable proof that the cut reached the
    // replication path rather than only new connections.
    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the ISR shrinks to the unreachable leader alone",
        1,
        &[1],
    )
    .await;

    let code = produce_once(&leader, topic_id, 3_000).await;
    check!(
        code == codes::NOT_ENOUGH_REPLICAS,
        "a leader its replicas cannot reach must refuse an acks=all write \
         rather than acknowledge one nothing else holds; got error_code={code}"
    );
    witness_never_leads(cluster.handle(NODE_C), Duration::from_secs(3)).await;

    cluster.heal(NODE_A);

    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the ISR returns to three replicas after the heal",
        1,
        &[1, 2, WITNESS],
    )
    .await;
    check!(
        produce_until_committed(&leader_addr, topic_id).await == codes::NONE,
        "acks=all commits again once the site is reachable"
    );

    cluster.shutdown().await;
}
