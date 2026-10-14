//! A follower cut off from both of its peers.
//!
//! The leader drops it from the ISR and goes on committing with the one
//! replica it still reaches, which `min.insync.replicas=2` allows. The
//! follower misses every write of that time. Once healed it catches up, and
//! the catch-up must build its dedup index as well as its log: made leader,
//! it has to drop a duplicate of a key it only ever fetched.

use {
    crate::{
        harness::{
            Cluster, assert_indexes_match, at, await_isr, await_new_leader, converged, dedup_topic,
            dropped, offsets_and_keys, produce, replica_log,
        },
        support::{cluster_lock, produce::ProduceAcknowledgements::AllReplicas},
    },
    assert2::assert,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_follower_catches_up_its_index_and_deduplicates_once_it_leads() {
    let _guard = cluster_lock().lock().await;
    let cluster = Cluster::start().await;
    // Broker 3 is second in assignment order, so it is the replica a failover
    // away from broker 1 elects.
    let topic = dedup_topic()
        .cluster(&cluster)
        .name("isolated-follower")
        .replicas(&[1, 3, 2])
        .call()
        .await;
    assert!(produce(&cluster, 1, &topic, &["c0", "c1"], AllReplicas).await == 0);

    cluster.links().isolate(3);
    // The first write waits until the leader drops broker 3 from the ISR,
    // then commits on two replicas.
    let cut_off = [
        produce(&cluster, 1, &topic, &["w0", "w1", "w2"], AllReplicas).await,
        produce(&cluster, 1, &topic, &["w3", "c0"], AllReplicas).await,
    ];
    await_isr(&cluster, &topic.name, &[1, 2], &[1, 2]).await;
    let missed = offsets_and_keys(&replica_log(&cluster, 3, &topic.name));
    assert!((cut_off, missed) == ([2, 5], at(&[(0, "c0"), (1, "c1")])));

    cluster.links().heal(3);
    let caught_up = offsets_and_keys(&converged(&cluster, &topic.name).await);
    assert!(
        caught_up
            == at(&[
                (0, "c0"),
                (1, "c1"),
                (2, "w0"),
                (3, "w1"),
                (4, "w2"),
                (5, "w3"),
            ])
    );
    assert_indexes_match(&cluster, &topic.name, &caught_up).await;

    // Take broker 1 away so that broker 3 leads. Every key of the batch but
    // the last is in its log only because it fetched it after the heal.
    cluster.links().isolate(1);
    let new_leader = await_new_leader(&cluster, &topic.name, &[2, 3], 1).await;
    let before = dropped(&cluster, 3, &topic.name).await;
    let base = produce(
        &cluster,
        3,
        &topic,
        &["w0", "w1", "w2", "w3", "c1", "fresh"],
        AllReplicas,
    )
    .await;
    let dropped_by_new_leader = dropped(&cluster, 3, &topic.name).await - before;
    assert!((new_leader, base, dropped_by_new_leader) == (3, 7, 5.0));

    cluster.links().heal(1);
    let last = offsets_and_keys(&converged(&cluster, &topic.name).await);
    assert!(
        last == at(&[
            (0, "c0"),
            (1, "c1"),
            (2, "w0"),
            (3, "w1"),
            (4, "w2"),
            (5, "w3"),
            (12, "fresh"),
        ])
    );
    assert_indexes_match(&cluster, &topic.name, &last).await;
    cluster.finish().await;
}
