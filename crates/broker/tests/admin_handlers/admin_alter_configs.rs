//! `AlterConfigs` (`api_key` 33): the round-trip that pushes a topic override
//! into the partition's log, the rejection of an unknown config key, and the
//! `min.insync.replicas` pre-flight that gates an `acks=-1` produce but leaves
//! `acks=1` alone.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::RecordBatch};

use crate::{
    RESOURCE_TYPE_TOPIC,
    admin_harness::create_topic_helper,
    support::{
        configs::{legacy_config, legacy_request, legacy_resource},
        records::{batch_from_records, value_record},
    },
};

/// `AlterConfigs` round-trip: a request that sets `retention.ms` on a known
/// topic returns `error_code == 0`. The supervisor then pushes the new config
/// into the partition's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_round_trip() {
    let (cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    create_topic_helper(&client, "t-alter", 1).await;

    let req = legacy_request(
        vec![legacy_resource(
            RESOURCE_TYPE_TOPIC,
            "t-alter".into(),
            vec![legacy_config("retention.ms".into(), Some("60000".into()))],
        )],
        false,
    );
    let resp = client.send(req).await.expect("alter_configs");
    assert!(
        resp.responses[0].error_code == 0,
        "alter_configs response: {:?}",
        resp.responses[0].error_message
    );

    // Wait for the supervisor reconcile loop to push the new config into the
    // partition's log. The supervisor runs on every metadata-image update
    // (typically within a few hundred ms). The partition is queryable
    // immediately after `create_topic_helper` returns, carrying the broker's
    // default retention; we poll until the supervisor swaps in the override
    // (or until the deadline).
    //
    // intentional poll (not an awaiter): the override lands in the local log
    // config *after* the image commits, so no image/metric signal reflects it
    // — same convergence gate the recompression / tiered-storage tests use.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let want = Duration::from_mins(1);
    let last = loop {
        let cur = broker
            .partition_retention_ms_for_test("t-alter", 0)
            .and_then(|inner| inner);
        if cur == Some(want) {
            break cur;
        }
        if std::time::Instant::now() > deadline {
            break cur;
        }
        tokio::task::yield_now().await;
    };
    assert!(
        last == Some(want),
        "retention_ms did not converge within 10 s after AlterConfigs"
    );
}

/// `AlterConfigs` rejects an unknown key with `error_code == 40` (`INVALID_CONFIG`)
/// and includes the offending key name in the error message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_rejects_unknown_key() {
    let (_cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;

    create_topic_helper(&client, "t-bad-cfg", 1).await;

    let req = legacy_request(
        vec![legacy_resource(
            RESOURCE_TYPE_TOPIC,
            "t-bad-cfg".into(),
            vec![legacy_config(
                "not.a.topic.config".into(),
                Some("1000".into()),
            )],
        )],
        false,
    );
    let resp = client.send(req).await.expect("alter_configs");
    // 40 = INVALID_CONFIG
    assert!(
        resp.responses[0].error_code == 40,
        "expected INVALID_CONFIG(40), got {}",
        resp.responses[0].error_code
    );
    assert!(
        resp.responses[0]
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("not.a.topic.config"),
        "expected error_message to mention `not.a.topic.config`, got {:?}",
        resp.responses[0].error_message
    );
}

/// `min.insync.replicas` pre-flight against a replication-factor-1 topic:
/// the operator sets `min.insync.replicas=2` with `AlterConfigs`. Kafka's
/// `Partition.effectiveMinIsr` clamps the threshold to the replica count,
/// so an `acks=-1` produce with ISR={1} is accepted, as is an `acks=1`
/// produce. The refusal when the ISR falls below a satisfiable threshold is
/// covered by `leadership.rs`'s table test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_insync_replicas_is_clamped_to_the_replica_count_for_acks_all() {
    let (cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    create_topic_helper(&client, "t-min-isr", 1).await;

    // Wait for partition 0 to materialize; otherwise the produce path returns
    // UNKNOWN_TOPIC_OR_PARTITION before the min.insync.replicas pre-flight runs.
    broker.wait_until_partition_present("t-min-isr", 0).await;

    // Produce v13+ drops `name` from the wire and demands `topic_id`.
    // Fetch it via Metadata so the produce calls below resolve.
    let md = client
        .send(crate::support::discovery::named_topic_metadata("t-min-isr"))
        .await
        .expect("Metadata for topic_id");
    let topic_id: WireUuid = md
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("t-min-isr"))
        .expect("topic in Metadata response")
        .topic_id;

    // Set min.insync.replicas=2 on the topic. The topic has one replica, so
    // the effective threshold is min(2, 1) = 1.
    let alter = legacy_request(
        vec![legacy_resource(
            RESOURCE_TYPE_TOPIC,
            "t-min-isr".into(),
            vec![legacy_config(
                "min.insync.replicas".into(),
                Some("2".into()),
            )],
        )],
        false,
    );
    let alter_resp = client.send(alter).await.expect("alter_configs");
    assert!(
        alter_resp.responses[0].error_code == 0,
        "AlterConfigs must accept min.insync.replicas=2: {:?}",
        alter_resp.responses[0].error_message
    );

    // Build a one-record batch for the produce calls below.
    let batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 0,
        ..batch_from_records(vec![value_record(0, Some(Bytes::from_static(b"x")))])
    };

    // acks=-1 ("all"): ISR={1} meets the clamped threshold of 1.
    let all = client
        .send(crate::support::produce::batch_request(
            batch.clone(),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("t-min-isr").into(),
                topic_id,
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        ))
        .await
        .expect("Produce (acks=-1)");
    assert!(
        all.responses[0].partition_responses[0].error_code == 0,
        "acks=-1 with isr.len()=1 and min(min.insync.replicas=2, replicas=1) = 1 must succeed; \
         got code = {}",
        all.responses[0].partition_responses[0].error_code
    );

    // acks=1: leader-only; min.insync.replicas never gates it.
    let ok = client
        .send(crate::support::produce::batch_request(
            batch,
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("t-min-isr").into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .expect("Produce (acks=1)");
    assert!(
        ok.responses[0].partition_responses[0].error_code == 0,
        "acks=1 must succeed regardless of min.insync.replicas; got code = {}",
        ok.responses[0].partition_responses[0].error_code
    );
}
