//! The cold downgrade itself: a drained streams group flips to classic on a
//! classic `JoinGroup` and keeps its committed offset, both in the live broker
//! and across a restart that replays the group from the log.

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig};

use crate::{
    CONVERGE_TRIES, ERR_NONE,
    downgrade_classic_join::classic_join_sync,
    downgrade_harness::{
        boot, commit_offset_as_member, connect, create_topic, finalize_streams_version,
        rejoin_config, topic_id_for,
    },
    downgrade_streams_join::{streams_join_and_converge, streams_leave, topology},
    support::{
        client::start_client,
        offsets::{offset_fetch_group, offset_fetch_request, offset_fetch_topic},
    },
};

/// A drained streams group with a committed offset converts to classic on a
/// classic `JoinGroup`. The committed offset survives the flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_streams_group_downgrades_and_preserves_offsets() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in", 1).await;
    let topic_id = topic_id_for(&streams_client, "in").await;

    // ── Phase 1: form a streams group, commit offset 42, then leave. ──
    let (member_id, resp) =
        streams_join_and_converge(&streams_client, "g", topology("in"), 1, CONVERGE_TRIES).await;
    broker
        .wait_until_group_type("g", krabka_broker::coordinator::unified::GroupType::Streams)
        .await;
    let group_type = broker.group_type_for_test("g");
    let empty_waiter_timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(75),
        broker.wait_until_streams_group_empty("g"),
    )
    .await
    .is_err();
    check!(
        resp.error_code == ERR_NONE,
        "streams member must converge without error (precondition for the downgrade): {resp:?}"
    );
    check!(
        group_type == Some(krabka_broker::coordinator::unified::GroupType::Streams),
        "streams member must converge on a Streams-typed group (precondition for the \
         downgrade): {resp:?}"
    );
    check!(
        empty_waiter_timed_out,
        "the streams-group-empty waiter must not complete while a member is live: {resp:?}"
    );

    // Commit offset 42 as the live streams member.
    commit_offset_as_member(
        &streams_client,
        "g",
        (&member_id, resp.member_epoch),
        "in",
        topic_id,
        0,
        42,
    )
    .await;

    // Leave so the streams group is drained.
    streams_leave(&streams_client, "g", &member_id).await;
    // Wait for the leave to propagate through the streams actor before the
    // classic JoinGroup triggers the streams→classic conversion.
    broker.wait_until_streams_group_empty("g").await;

    // ── Phase 2: classic JoinGroup for the same id → downgrade to classic. ──
    let (_cm, _gen) = classic_join_sync(&classic_client, "g").await;
    broker
        .wait_until_group_type("g", krabka_broker::coordinator::unified::GroupType::Classic)
        .await;
    assert!(
        broker.group_type_for_test("g")
            == Some(krabka_broker::coordinator::unified::GroupType::Classic),
        "group_type must be Classic after downgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 3: committed offset survives the flip. ──
    let fr = classic_client
        .send(offset_fetch_request(offset_fetch_group(
            "g",
            Some(vec![offset_fetch_topic("in", topic_id, vec![0])]),
        )))
        .await
        .expect("OffsetFetch");
    let part = &fr.groups[0].topics[0].partitions[0];
    assert!(part.error_code == ERR_NONE, "OffsetFetch error: {part:?}");
    assert!(
        part.committed_offset == 42,
        "committed offset must survive classic↔streams downgrade, got {}",
        part.committed_offset
    );
}

/// A downgrade from streams to classic survives a broker restart. After
/// replay the group is Classic and its committed offset is intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgrade_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();
    let topic_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        broker.wait_until_group_coordinator_ready().await;
        let bootstrap = broker.listen_addr().to_string();
        let sc = connect(&bootstrap).await;
        let cc = connect(&bootstrap).await;
        finalize_streams_version(&sc).await;
        create_topic(&sc, "in4", 1).await;
        topic_id = topic_id_for(&sc, "in4").await;

        let (mid, resp) =
            streams_join_and_converge(&sc, "g4", topology("in4"), 1, CONVERGE_TRIES).await;
        assert!(resp.error_code == ERR_NONE, "streams converge: {resp:?}");

        // Commit offset 42 as the live streams member.
        commit_offset_as_member(&sc, "g4", (&mid, resp.member_epoch), "in4", topic_id, 0, 42).await;

        // Leave to drain.
        streams_leave(&sc, "g4", &mid).await;
        // Wait for the streams leave to propagate before the downgrade JoinGroup.
        broker.wait_until_streams_group_empty("g4").await;

        // Downgrade: classic JoinGroup on drained streams group.
        let _ = classic_join_sync(&cc, "g4").await;
        broker
            .wait_until_group_type(
                "g4",
                krabka_broker::coordinator::unified::GroupType::Classic,
            )
            .await;
        assert!(
            broker.group_type_for_test("g4")
                == Some(krabka_broker::coordinator::unified::GroupType::Classic)
        );
        broker.shutdown().await;
    }
    {
        let (broker, _bootstrap, client) = start_client(rejoin_config(log_dir), Some("c1")).await;
        let cc = std::sync::Arc::new(client);
        // Replay must reconstruct g4 as a classic actor from the committed
        // offset. Offset-only groups are Kafka-typeless, so they do not carry a
        // Classic type lock in `group_type_for_test`.
        assert!(
            broker.classic_group_inspect_for_test("g4").await.is_some(),
            "offset-only replay must seed a classic actor for g4"
        );
        assert!(
            broker.group_type_for_test("g4")
                != Some(krabka_broker::coordinator::unified::GroupType::Streams),
            "group must not replay as Streams after downgrade"
        );
        let fr = cc
            .send(offset_fetch_request(offset_fetch_group(
                "g4",
                Some(vec![offset_fetch_topic("in4", topic_id, vec![0])]),
            )))
            .await
            .expect("OffsetFetch");
        assert!(
            fr.groups[0].topics[0].partitions[0].committed_offset == 42,
            "committed offset must survive downgrade + restart"
        );
    }
}
