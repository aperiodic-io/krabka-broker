//! A partition leader cut off from both of its replicas while it keeps
//! running and keeps taking produces.
//!
//! The replicas fence it, elect broker 2 (the next replica in assignment
//! order) and go on. The isolated leader still believes it leads, so what it
//! appends meanwhile is a suffix no other replica has. On heal it learns the
//! newer leader epoch, truncates that suffix, and follows. Its dedup index
//! must lose the suffix's identities with it: a key whose only copy was
//! there has to be accepted when it is written again.

use {
    crate::{
        harness::{
            Cluster, Topic, acknowledged, assert_indexes_match, at, await_new_leader, converged,
            dedup_topic, dropped, elect_preferred, keys, offsets_and_keys, produce, replica_log,
            send,
        },
        support::{
            cluster_lock,
            produce::ProduceAcknowledgements::{self, AllReplicas, Leader},
        },
    },
    assert2::assert,
    std::time::Duration,
};

/// How long the isolated leader holds an `acks=all` write before it answers
/// that the write timed out.
const STRANDED_TIMEOUT_MS: i32 = 4_000;

/// Cut broker 1 off, and give the relays a moment to tear down the
/// connections that were open, so nothing it appends next can still leave.
async fn isolate_leader(cluster: &Cluster) {
    cluster.links().isolate(1);
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// The keys of broker `node`'s local log, as `(offset, key)` pairs.
fn local(cluster: &Cluster, node: u64, topic: &Topic) -> Vec<(i64, String)> {
    offsets_and_keys(&replica_log(cluster, node, &topic.name))
}

/// The log every replica agrees on, once each replica's index matches it.
async fn settled(cluster: &Cluster, topic: &Topic) -> Vec<(i64, String)> {
    let log = offsets_and_keys(&converged(cluster, &topic.name).await);
    assert_indexes_match(cluster, &topic.name, &log).await;
    log
}

/// Reconnect broker 1, and return the log once it has rejoined.
async fn heal_broker_1(cluster: &Cluster, topic: &Topic) -> Vec<(i64, String)> {
    cluster.links().heal(1);
    settled(cluster, topic).await
}

/// What broker 1 makes of a batch once a preferred election hands it the
/// leadership back.
#[derive(Debug, PartialEq)]
struct LeadAgain {
    base_offset: i64,
    /// Records of the batch it dropped as duplicates.
    dropped: f64,
    /// The log every replica agrees on afterwards.
    log: Vec<(i64, String)>,
}

async fn lead_again(
    cluster: &Cluster,
    topic: &Topic,
    keys: &[&str],
    acks: ProduceAcknowledgements,
) -> LeadAgain {
    converged(cluster, &topic.name).await;
    elect_preferred(cluster, &topic.name, 2, 1).await;
    let before = dropped(cluster, 1, &topic.name).await;
    let base_offset = produce(cluster, 1, topic, keys, acks).await;
    LeadAgain {
        base_offset,
        dropped: dropped(cluster, 1, &topic.name).await - before,
        log: settled(cluster, topic).await,
    }
}

/// What the `acks=all` scenario observes before the heal.
#[derive(Debug, PartialEq, Eq)]
struct WhileIsolated {
    /// Whether each of the two writes to the isolated leader was acknowledged.
    stranded_acknowledged: [bool; 2],
    new_leader: u64,
    /// What the isolated leader holds past the committed records.
    isolated_suffix: Vec<String>,
    /// What the two connected replicas hold.
    connected_logs: [Vec<(i64, String)>; 2],
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_acks_all_leader_acknowledges_nothing_and_forgets_its_truncated_suffix() {
    let _guard = cluster_lock().lock().await;
    let cluster = Cluster::start().await;
    let topic = dedup_topic()
        .cluster(&cluster)
        .name("isolated-leader-acks-all")
        .replicas(&[1, 2, 3])
        .call()
        .await;
    let committed = ["c0", "c1", "c2", "c3", "c4"];
    assert!(produce(&cluster, 1, &topic, &committed, AllReplicas).await == 0);

    isolate_leader(&cluster).await;
    // Two writes only the isolated leader takes: `lost-*` no producer ever
    // retries, `retry-a` its producer retries on the new leader.
    let stranded = async {
        tokio::join!(
            send(
                &cluster,
                1,
                &topic,
                keys(&["lost-a", "lost-b"]),
                AllReplicas,
                STRANDED_TIMEOUT_MS
            ),
            send(
                &cluster,
                1,
                &topic,
                keys(&["retry-a"]),
                AllReplicas,
                STRANDED_TIMEOUT_MS
            ),
        )
    };
    let failover = await_new_leader(&cluster, &topic.name, &[2, 3], 1);
    let ((lost, retried), new_leader) = tokio::join!(stranded, failover);
    let mut isolated_suffix: Vec<String> = local(&cluster, 1, &topic)
        .into_iter()
        .skip(committed.len())
        .map(|(_, key)| key)
        .collect();
    isolated_suffix.sort();
    let committed_log = at(&[(0, "c0"), (1, "c1"), (2, "c2"), (3, "c3"), (4, "c4")]);
    assert!(
        WhileIsolated {
            stranded_acknowledged: [acknowledged(&lost), acknowledged(&retried)],
            new_leader,
            isolated_suffix,
            connected_logs: [local(&cluster, 2, &topic), local(&cluster, 3, &topic)],
        } == WhileIsolated {
            stranded_acknowledged: [false, false],
            new_leader: 2,
            isolated_suffix: ["lost-a", "lost-b", "retry-a"].map(str::to_owned).into(),
            connected_logs: [committed_log.clone(), committed_log],
        },
        "stranded writes answered {lost:?} and {retried:?}"
    );

    // The producer retries `retry-a` on the new leader, next to a new key
    // and a duplicate of a committed one.
    assert!(
        produce(
            &cluster,
            2,
            &topic,
            &["retry-a", "new-a", "c0"],
            AllReplicas
        )
        .await
            == 5
    );
    let healed = heal_broker_1(&cluster, &topic).await;
    assert!(
        healed
            == at(&[
                (0, "c0"),
                (1, "c1"),
                (2, "c2"),
                (3, "c3"),
                (4, "c4"),
                (5, "retry-a"),
                (6, "new-a"),
            ])
    );

    // `lost-a` existed only in the truncated suffix: the new leader keeps it.
    assert!(produce(&cluster, 2, &topic, &["lost-a"], AllReplicas).await == 8);
    // Back on broker 1, which followed since the heal: it keeps `lost-b`,
    // whose only copy it truncated itself, and drops everything it
    // replicated as a follower.
    let back = ["lost-b", "c1", "new-a", "retry-a", "lost-a", "fresh"];
    assert!(
        lead_again(&cluster, &topic, &back, AllReplicas).await
            == LeadAgain {
                base_offset: 9,
                dropped: 4.0,
                log: at(&[
                    (0, "c0"),
                    (1, "c1"),
                    (2, "c2"),
                    (3, "c3"),
                    (4, "c4"),
                    (5, "retry-a"),
                    (6, "new-a"),
                    (8, "lost-a"),
                    (9, "lost-b"),
                    (14, "fresh"),
                ]),
            }
    );
    cluster.finish().await;
}

/// Kafka acknowledges an `acks=1` write once the leader appended it. A leader
/// that is deposed before any replica copied the write takes it with it:
/// that is the documented cost of `acks=1`, and these writes are lost here.
/// Deduplication must not add to it. The lost keys are accepted when written
/// again, on the new leader and on the old one, and no key is ever twice in
/// the converged log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_acks_1_leader_loses_what_it_alone_acknowledged_and_dedup_hides_none_of_it() {
    let _guard = cluster_lock().lock().await;
    let cluster = Cluster::start().await;
    let topic = dedup_topic()
        .cluster(&cluster)
        .name("isolated-leader-acks-1")
        .replicas(&[1, 2, 3])
        .min_insync(1)
        .call()
        .await;
    assert!(produce(&cluster, 1, &topic, &["c0", "c1", "c2"], AllReplicas).await == 0);

    isolate_leader(&cluster).await;
    // Acknowledged at once, though no replica will ever see them; the second
    // write is a duplicate the isolated leader drops.
    let alone = produce(&cluster, 1, &topic, &["un-a", "un-b"], Leader).await;
    let duplicate = produce(&cluster, 1, &topic, &["c0"], Leader).await;
    let isolated_log = local(&cluster, 1, &topic);
    let new_leader = await_new_leader(&cluster, &topic.name, &[2, 3], 1).await;
    let after = produce(&cluster, 2, &topic, &["after-a", "c1"], Leader).await;
    assert!(
        (alone, duplicate, isolated_log, new_leader, after)
            == (
                3,
                5,
                at(&[(0, "c0"), (1, "c1"), (2, "c2"), (3, "un-a"), (4, "un-b")]),
                2,
                3
            )
    );

    let healed = heal_broker_1(&cluster, &topic).await;
    assert!(healed == at(&[(0, "c0"), (1, "c1"), (2, "c2"), (3, "after-a")]));

    // Written again, the lost keys are kept.
    assert!(produce(&cluster, 2, &topic, &["un-a", "un-b"], Leader).await == 5);
    // Broker 1 truncated `un-a un-b` and then replicated them: led again, it
    // drops them, and what it replicated, as duplicates.
    assert!(
        lead_again(&cluster, &topic, &["un-b", "after-a", "c2", "late"], Leader).await
            == LeadAgain {
                base_offset: 7,
                dropped: 3.0,
                log: at(&[
                    (0, "c0"),
                    (1, "c1"),
                    (2, "c2"),
                    (3, "after-a"),
                    (5, "un-a"),
                    (6, "un-b"),
                    (10, "late"),
                ]),
            }
    );
    cluster.finish().await;
}
