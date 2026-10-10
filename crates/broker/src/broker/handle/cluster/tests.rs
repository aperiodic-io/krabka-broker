use std::net::SocketAddr;

use assert2::{assert, check};
use krabka_raft::NodeId;
use tokio::net::TcpListener;

use super::*;
use crate::{broker::test_support::submit_metadata_topic_partition, config::BrokerConfig};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct StaticVoterSetup<'a> {
    #[default(NodeId(1))]
    node_id: NodeId,
    #[default(SocketAddr::from(([127, 0, 0, 1], 0)))]
    listen_addr: SocketAddr,
    #[default(SocketAddr::from(([127, 0, 0, 1], 0)))]
    controller_addr: SocketAddr,
    // An empty list retains the automatic quorum used by ordinary single-broker fixtures.
    voters: &'a [(NodeId, SocketAddr)],
}

fn static_voter_test_config(
    log_dir: &std::path::Path,
    setup: StaticVoterSetup<'_>,
) -> BrokerConfig {
    let StaticVoterSetup {
        node_id,
        listen_addr,
        controller_addr,
        voters,
    } = setup;
    let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
    config.broker_id = i32::try_from(node_id.0).expect("node id fits broker id");
    config.node_id = node_id;
    config.listen_addr = listen_addr;
    config.advertised_listener = listen_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.directory_id = uuid::Uuid::from_u128(u128::from(node_id.0));
    config.controller_quorum_voters = voters
        .iter()
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_handle_reports_non_default_node_and_voter_state() {
    let dir7 = tempfile::tempdir().unwrap();
    let dir8 = tempfile::tempdir().unwrap();
    let data_listener7 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_listener8 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_listener7 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_listener8 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen7 = data_listener7.local_addr().unwrap();
    let listen8 = data_listener8.local_addr().unwrap();
    let controller7 = controller_listener7.local_addr().unwrap();
    let controller8 = controller_listener8.local_addr().unwrap();
    let voters = [(NodeId(7), controller7), (NodeId(8), controller8)];

    let config7 = static_voter_test_config(
        dir7.path(),
        StaticVoterSetup {
            node_id: NodeId(7),
            listen_addr: listen7,
            controller_addr: controller7,
            voters: &voters,
        },
    );
    let config8 = static_voter_test_config(
        dir8.path(),
        StaticVoterSetup {
            node_id: NodeId(8),
            listen_addr: listen8,
            controller_addr: controller8,
            voters: &voters,
        },
    );
    let start = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(10),
        async {
            tokio::try_join!(
                Broker::start_with_listeners(
                    config7,
                    Some(controller_listener7),
                    Some(data_listener7),
                ),
                Broker::start_with_listeners(
                    config8,
                    Some(controller_listener8),
                    Some(data_listener8),
                ),
            )
        },
    ));
    let (handle7, handle8) = start
        .await
        .expect("two-voter brokers started before timeout")
        .expect("two-voter broker start");

    let leader = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(leader) = handle7.controller_leader_id()
                && leader != krabka_raft::NodeId(0)
            {
                return leader;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("two-voter cluster leader");
    assert!(leader == krabka_raft::NodeId(7) || leader == krabka_raft::NodeId(8));
    handle7.wait_for_image(|img| img.voters().len() == 2).await;
    handle8.wait_for_image(|img| img.voters().len() == 2).await;

    check!(handle7.node_id() == 7);
    check!(handle8.node_id() == 8);
    check!(handle7.controller_leader_id() == Some(leader));
    check!(
        handle7
            .quorum_voters_for_test()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            == [krabka_raft::NodeId(7), krabka_raft::NodeId(8)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
    );
    check!(handle7.voter_count_for_test() == 2);
    check!(
        handle7.voter_ids_for_test()
            == [krabka_raft::NodeId(7), krabka_raft::NodeId(8)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
    );

    // The multi-thread test runtime aborts remaining tasks on exit if raft
    // shutdown takes longer than the helper assertions above.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(handle7.shutdown(), handle8.shutdown());
    })
    .await;
}

#[tokio::test]
async fn wait_helpers_remain_pending_until_their_conditions_are_met() {
    type PendingWait<'a> = (
        &'a str,
        std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
    );
    type LeaderChangedCase<'a> = (&'a str, u128, u64, &'a [u64], i32, u64);

    let (handle, _dir) = crate::test_support::start_broker_with(|_| {}).await;
    let timeout = std::time::Duration::from_millis(75);
    let topic_id = uuid::Uuid::from_u128(0xFEED);

    // Every wait helper must still be pending (time out) while its
    // condition is unmet. The futures are lazy async fns, so building the
    // table up front does no work; each is awaited sequentially below.
    let pending_waits: [PendingWait<'_>; 9] = [
        (
            "wait_for_share_state_summary",
            Box::pin(async {
                let () = handle
                    .wait_for_share_state_summary("missing-mutant-group", topic_id, 0)
                    .await;
            }),
        ),
        (
            "wait_until_share_spso",
            Box::pin(async {
                handle
                    .wait_until_share_spso("missing-mutant-group", topic_id, 0, 1)
                    .await;
            }),
        ),
        (
            "wait_until_share_delivery_complete",
            Box::pin(async {
                handle
                    .wait_until_share_delivery_complete("missing-mutant-group", topic_id, 0, 1)
                    .await;
            }),
        ),
        (
            "wait_until_group_member_count",
            Box::pin(async {
                handle
                    .wait_until_group_member_count("missing-mutant-group", 1)
                    .await;
            }),
        ),
        (
            "wait_until_streams_group_member_count",
            Box::pin(async {
                handle
                    .wait_until_streams_group_member_count("missing-mutant-streams", 1)
                    .await;
            }),
        ),
        (
            "wait_until_brokers_registered",
            Box::pin(async {
                handle.wait_until_brokers_registered(2).await;
            }),
        ),
        (
            "wait_until_partition_present",
            Box::pin(async {
                handle
                    .wait_until_partition_present("missing-mutant-topic", 0)
                    .await;
            }),
        ),
        (
            "wait_until_partition_leader_changed",
            Box::pin(async {
                handle
                    .wait_until_partition_leader_changed(
                        "missing-mutant-topic",
                        0,
                        krabka_raft::NodeId(1),
                    )
                    .await;
            }),
        ),
        (
            "wait_until_isr_len",
            Box::pin(async {
                handle
                    .wait_until_isr_len("missing-mutant-topic", 0, 1)
                    .await;
            }),
        ),
    ];
    for (name, wait) in pending_waits {
        assert!(
            tokio::time::timeout(timeout, wait).await.is_err(),
            "{name} resolved while its condition was unmet"
        );
    }

    // wait_until_partition_leader_changed must stay pending for each of
    // these submitted partitions:
    // (topic, topic_id, leader, replicas/isr, leader_epoch, excluded leader)
    let leader_changed_cases: [LeaderChangedCase<'_>; 4] = [
        // leader 0 means "no leader" — never counts as a change.
        ("leader-zero-mutant-topic", 0xF001, 0, &[1], 3, 1),
        // the current leader is exactly the excluded node.
        ("leader-excluded-mutant-topic", 0xF002, 2, &[1, 2], 3, 2),
        // leader epoch 0 is not a completed election.
        ("leader-epoch-zero-mutant-topic", 0xF003, 2, &[1, 2], 0, 1),
        // negative leader epoch likewise.
        (
            "leader-epoch-negative-mutant-topic",
            0xF004,
            2,
            &[1, 2],
            -1,
            1,
        ),
    ];
    for (topic, topic_id, leader, replicas, leader_epoch, excluded) in leader_changed_cases {
        submit_metadata_topic_partition(
            &handle,
            crate::broker::test_support::MetadataPartitionSetup {
                topic,
                topic_id: uuid::Uuid::from_u128(topic_id),
                leader: krabka_ids::NodeId(leader),
                replicas: (replicas).iter().copied().map(krabka_ids::NodeId).collect(),
                isr: (replicas).iter().copied().map(krabka_ids::NodeId).collect(),
                leader_epoch: krabka_ids::LeaderEpoch(leader_epoch),
                ..Default::default()
            },
        )
        .await;
        assert!(
            tokio::time::timeout(
                timeout,
                handle.wait_until_partition_leader_changed(topic, 0, krabka_raft::NodeId(excluded)),
            )
            .await
            .is_err(),
            "{topic}: wait_until_partition_leader_changed resolved"
        );
    }
    // Leader 0 is also reported as "no leader" by the direct helper.
    assert!(
        handle
            .partition_leader_for_test("leader-zero-mutant-topic", 0)
            .is_none()
    );

    submit_metadata_topic_partition(
        &handle,
        crate::broker::test_support::MetadataPartitionSetup {
            topic: "isr-len-mutant-topic",
            topic_id: uuid::Uuid::from_u128(0xF005),
            replicas: [1, 2].iter().copied().map(krabka_ids::NodeId).collect(),
            isr: [1, 2].iter().copied().map(krabka_ids::NodeId).collect(),
            ..Default::default()
        },
    )
    .await;
    assert!(
        tokio::time::timeout(
            timeout,
            handle.wait_until_isr_len("isr-len-mutant-topic", 0, 1)
        )
        .await
        .is_err()
    );

    handle.shutdown().await;
}

/// A wait whose predicate never holds times out and names the line that
/// started it, so a failing suite says which of its waits stuck.
#[tokio::test(start_paused = true)]
async fn an_image_wait_that_never_holds_names_its_caller() {
    let (_tx, rx) =
        tokio::sync::watch::channel(Arc::new(krabka_metadata::MetadataImage::default()));
    let line = line!() + 1;
    let waiting = image_awaiter(rx, |_| false);

    let timeout = waiting
        .await
        .expect_err("a predicate that never holds times out");

    assert!(timeout.caller.file() == file!());
    assert!(timeout.caller.line() == line);
    assert!(
        timeout.message(17)
            == format!(
                "wait_for_image called at {}:{line}:{} timed out after 30s; the image is at \
                 metadata offset 17",
                file!(),
                timeout.caller.column()
            )
    );
}

/// A wait ends as soon as a published image satisfies the predicate, and a
/// closed channel ends it too: the sender goes away only at broker shutdown.
#[tokio::test(start_paused = true)]
async fn an_image_wait_ends_on_a_matching_image_or_a_closed_channel() {
    let (tx, rx) = tokio::sync::watch::channel(Arc::new(krabka_metadata::MetadataImage::default()));
    let awaited = Arc::new(krabka_metadata::MetadataImage::default());
    let awaited_addr = Arc::as_ptr(&awaited).addr();
    let waiting = tokio::spawn(image_awaiter(rx, move |image| {
        std::ptr::from_ref(image).addr() == awaited_addr
    }));
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    tx.send(awaited).expect("the wait holds the receiver");
    assert!(waiting.await.expect("the wait task") == Ok(()));

    let (tx, rx) = tokio::sync::watch::channel(Arc::new(krabka_metadata::MetadataImage::default()));
    let waiting = tokio::spawn(image_awaiter(rx, |_| false));
    drop(tx);
    assert!(waiting.await.expect("the wait task") == Ok(()));
}
