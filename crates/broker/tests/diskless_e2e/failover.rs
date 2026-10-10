//! An `acks=all` diskless append survives the loss of the broker that acked it.
//!
//! This is the subsystem's whole value proposition, and it is the one claim
//! that a single-broker or single-process test cannot make. The case asserts
//! three separate things, in this order, because each one is a different way
//! the claim could be false:
//!
//! 1. **A second voter's durable checkpoint advanced.** Bytes can sit in a
//!    file the voter would discard on recovery. The checkpoint is what makes
//!    a prefix survive a restart, so it has to have moved to cover the acked
//!    range and no further.
//! 2. **That voter holds the bytes.** Read out of the broker's own
//!    `__diskless_wal_quorum` tree, byte for byte against what the leader
//!    serves. A local fsync on the leader would satisfy the produce path and
//!    fail here.
//! 3. **The promoted broker serves the same offsets byte for byte.** Not "the
//!    same number of records" and not "the same values": the same batches --
//!    and served from the WAL replica, because both survivors' ordinary
//!    partition logs are emptied first. See the comment on that truncation for
//!    why it is needed and what it does not fully close.

use std::time::Duration;

use assert2::assert;

use crate::{
    CLIENT_PRINCIPAL, PASSWORD, RECORDS, TOPIC,
    cluster::{start_diskless_cluster, wait_for},
    support,
    topic::{await_wal_quorum, create_diskless_topic},
    voter_dir::{DurableRange, durable_bytes, durable_range, voter_dir},
    wire::{assert_matches_produced, fetch_log, produce_all, value_at},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diskless_acks_all_append_survives_the_acking_broker() {
    // Park the object flusher for the length of the case. A flush would trim
    // the committed prefix out of both the leader's log and the voters', and
    // this case is about what the *WAL quorum* holds, not about what the
    // object store holds. `cold_read` covers that half.
    let mut cluster = start_diskless_cluster(|config| {
        config.diskless_wal_flush_interval = krabka_units::hours(1);
    })
    .await;
    cluster.await_ready().await;

    let admin = support::sasl_client(
        &cluster.bootstrap_for_node(cluster.node_ids()[0]),
        CLIENT_PRINCIPAL,
        PASSWORD,
    )
    .await;
    let topic_id = create_diskless_topic(&admin).await;
    let leader = await_wal_quorum(&cluster).await;
    let topic_uuid = uuid::Uuid::from_bytes(topic_id.0);

    let values: Vec<bytes::Bytes> = (0..RECORDS).map(value_at).collect();
    let producer = support::sasl_client(
        &cluster.bootstrap_for_node(leader),
        CLIENT_PRINCIPAL,
        PASSWORD,
    )
    .await;
    produce_all(&producer, topic_id, &values).await;

    let committed = i64::try_from(RECORDS).expect("small count");
    let followers: Vec<_> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| *node != leader)
        .collect();

    // (1) Every other voter fsynced the acked prefix and said so on disk.
    for node in &followers {
        let dir = voter_dir(cluster.log_dir_for_node(*node), topic_uuid, 0, *node);
        wait_for(
            &format!(
                "broker {}'s WAL checkpoint to reach offset {committed}",
                node.0
            ),
            Duration::from_mins(1),
            || async { durable_range(&dir).is_some_and(|range| range.end >= committed) },
        )
        .await;
        let range = durable_range(&dir).expect("checkpoint written");
        assert!(
            range
                == DurableRange {
                    start: 0,
                    end: committed
                },
            "broker {} checkpointed {range:?}",
            node.0
        );
    }

    // The leader's own view of the committed log, over the wire.
    let from_leader = fetch_log(
        &cluster.bootstrap_for_node(leader),
        topic_id,
        0,
        RECORDS,
        Duration::from_mins(1),
    )
    .await;
    assert_matches_produced(&from_leader, 0, RECORDS);

    // (2) Each other voter's WAL directory holds exactly those bytes. The
    // voters are quiescent: every record is acked, no more are coming, and the
    // flusher is parked, so nothing is writing into these directories now.
    for node in &followers {
        let dir = voter_dir(cluster.log_dir_for_node(*node), topic_uuid, 0, *node);
        let bytes = durable_bytes(
            &dir,
            DurableRange {
                start: 0,
                end: committed,
            },
        );
        assert!(
            bytes == from_leader.bytes,
            "broker {}'s WAL replica does not hold the leader's bytes",
            node.0
        );
    }

    // Empty both survivors' *canonical* partition logs before killing the
    // leader.
    //
    // Without this the case proves less than it looks like it does. The topic
    // is rf=3, and `desired_follower_set` does not exclude diskless topics, so
    // every non-leader replica also runs an ordinary follower replicator and
    // already holds these batches in its normal partition log. The promoted
    // broker could then serve the post-crash fetch out of that copy even if
    // WAL hydration adopted nothing, and the WAL directory assertions above
    // would not notice. Truncating both candidates away leaves the WAL replica
    // as the only place the prefix can come from.
    //
    // The ordinary replicator could in principle refill a truncated log in the
    // moment before its leader dies, which would put the case back where it
    // was -- it can never turn a real failure into a pass. In practice it does
    // not: both logs are still empty when `crash` returns.
    for node in &followers {
        cluster
            .handle_for_node(*node)
            .expect("a survivor is up")
            .test_truncate_local_log(TOPIC, 0, 0)
            .await
            .expect("truncate the survivor's canonical partition log");
        assert!(
            cluster
                .handle_for_node(*node)
                .expect("a survivor is up")
                .local_log_end_offset(TOPIC, 0)
                == Some(0),
            "broker {}'s canonical log still holds the prefix",
            node.0
        );
    }

    // Kill the broker that acknowledged every one of those writes. Drop the
    // clients first so no open connection holds its listener shutdown.
    drop(producer);
    drop(admin);
    cluster.crash(leader).await;

    // The surviving two of three are still a majority, so the controller can
    // commit a new partition-leader record.
    let survivor = *followers.first().expect("a surviving voter");
    let promoted = cluster.promoted_leader(survivor, leader).await;
    assert!(promoted != leader);

    // (3) The promoted broker serves the same offsets, byte for byte.
    let from_promoted = fetch_log(
        &cluster.bootstrap_for_node(promoted),
        topic_id,
        0,
        RECORDS,
        Duration::from_mins(2),
    )
    .await;
    assert_matches_produced(&from_promoted, 0, RECORDS);
    assert!(
        from_promoted.bytes == from_leader.bytes,
        "broker {} serves different bytes than the broker that acked them",
        promoted.0
    );

    cluster.shutdown().await;
}
