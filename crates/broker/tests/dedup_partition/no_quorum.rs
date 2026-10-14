//! Overlapping partitions: a second node is cut off before the first one
//! returns, which leaves no two nodes that can reach each other.
//!
//! Without a quorum no `acks=all` write may be acknowledged anywhere: not by
//! the stale leader on the first cut-off node, not by the current leader that
//! lost its last follower, and not by a follower. When the first node
//! returns, two nodes form a quorum again and the cluster recovers without
//! the third; when the third returns, every replica converges. Throughout,
//! no key is ever twice in the log, and the key whose only copy the stale
//! leader truncated is accepted again.
//!
//! A one-sided partition, where two nodes lose each other but both still
//! reach the third, is one of the faults the flapping scenario draws.

use {
    crate::{
        harness::{
            Cluster, STEP, Topic, acknowledged, assert_indexes_match, await_new_leader, converged,
            dedup_topic, keys, newest_leader, offsets_and_keys, produce, repeated_keys, send,
        },
        support::{cluster_lock, produce::ProduceAcknowledgements::AllReplicas},
    },
    assert2::assert,
    std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    },
};

/// Retry an `acks=all` write of `key` at whichever broker leads, until one
/// acknowledges it. Returns the base offset.
async fn produce_somewhere(cluster: &Cluster, topic: &Topic, key: &str) -> i64 {
    let deadline = Instant::now() + STEP;
    loop {
        if let Some(leader) = newest_leader(cluster, &topic.name) {
            let outcome = send(cluster, leader, topic, keys(&[key]), AllReplicas, 3_000).await;
            if let Ok(row) = &outcome
                && acknowledged(&outcome)
            {
                return row.base_offset;
            }
            tracing::debug!(?outcome, leader, "write not acknowledged yet");
        }
        assert!(
            Instant::now() < deadline,
            "no broker acknowledged {key} within {STEP:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_partitions_refuse_writes_without_a_quorum_and_recover_after() {
    let _guard = cluster_lock().lock().await;
    let cluster = Cluster::start().await;
    let topic = dedup_topic()
        .cluster(&cluster)
        .name("no-quorum")
        .replicas(&[1, 2, 3])
        .call()
        .await;
    assert!(produce(&cluster, 1, &topic, &["c0"], AllReplicas).await == 0);

    cluster.links().isolate(1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(await_new_leader(&cluster, &topic.name, &[2, 3], 1).await == 2);
    assert!(produce(&cluster, 2, &topic, &["a0"], AllReplicas).await == 1);

    // The overlap: broker 3 goes too before broker 1 comes back. Wait out the
    // replica lag and the broker session, so the refusal below is the
    // settled state and not the first moments of the cut.
    cluster.links().isolate(3);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (on_1, on_2, on_3) = tokio::join!(
        send(&cluster, 1, &topic, keys(&["nq-1"]), AllReplicas, 4_000),
        send(&cluster, 2, &topic, keys(&["nq-2"]), AllReplicas, 4_000),
        send(&cluster, 3, &topic, keys(&["nq-3"]), AllReplicas, 4_000),
    );
    assert!(
        [
            acknowledged(&on_1),
            acknowledged(&on_2),
            acknowledged(&on_3)
        ] == [false; 3],
        "without a quorum: {on_1:?} {on_2:?} {on_3:?}"
    );

    // Brokers 1 and 2 are a quorum again; broker 3 stays away.
    cluster.links().heal(1);
    let recovered = produce_somewhere(&cluster, &topic, "r0").await;
    cluster.links().heal(3);
    let healed = offsets_and_keys(&converged(&cluster, &topic.name).await);
    let at_of = |key: &str| {
        healed
            .iter()
            .filter(|(_, stored)| stored == key)
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>()
    };
    // `nq-2` timed out but may still be committed: once broker 2's ISR is
    // itself alone, its high watermark passes everything it holds. `nq-1`
    // was only ever on the deposed broker 1, and `nq-3` reached only a
    // follower, which refused it.
    assert!(
        (
            at_of("c0"),
            at_of("a0"),
            at_of("r0"),
            at_of("nq-1"),
            at_of("nq-3"),
            at_of("nq-2").len() <= 1,
            repeated_keys(&healed),
        ) == (
            vec![0],
            vec![1],
            vec![recovered],
            vec![],
            vec![],
            true,
            BTreeMap::new(),
        ),
        "{healed:?}"
    );

    // `nq-1` existed only in broker 1's truncated suffix: it is accepted.
    let again = produce_somewhere(&cluster, &topic, "nq-1").await;
    let last = offsets_and_keys(&converged(&cluster, &topic.name).await);
    assert!(
        (
            last.iter()
                .filter(|(_, key)| key == "nq-1")
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            repeated_keys(&last)
        ) == (vec![again], BTreeMap::new())
    );
    assert_indexes_match(&cluster, &topic.name, &last).await;
    cluster.finish().await;
}
