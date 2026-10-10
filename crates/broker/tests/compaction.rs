// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
// `clippy::pedantic` lints — annotate-snippets upstream bug.

//! Log compaction end-to-end broker integration test.
//!
//! The test produces 30 records across 3 keys, k1, k2, and k3, into a compacted
//! topic. It waits for a compaction pass, force-rolls the active segment, and
//! waits for another pass. It then fetches the records and asserts that exactly
//! 3 distinct keys survive with only their latest values, v10-kN. Old values
//! v0..v9 must be gone from the sealed segments.
//!
//! Gated to non-Windows to match the multi-broker test convention established by the existing integration suites.

mod support;

use krabka_broker::metrics::PartitionLabel;

use crate::{
    compaction_cluster::start_broker_with_fast_cleaner,
    compaction_records::{assert_latest_records_survive, fetch_all},
    compaction_rpc::{create_topic_with_configs, get_topic_id, produce_record},
};

mod kafka_wire;
// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `compaction/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "compaction/compaction_cluster.rs"]
mod compaction_cluster;
#[path = "compaction/compaction_records.rs"]
mod compaction_records;
#[path = "compaction/compaction_rpc.rs"]
mod compaction_rpc;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-compaction-test";

/// End-to-end compaction test:
///
/// 1. Boot a single broker with cleaner interval = 1s.
/// 2. Create topic `compacted` with `cleanup.policy=compact` and `internal.segment.bytes=256`.
/// 3. Produce 30 records, 10 for each of the 3 keys, values v0-k1..v9-k3.
/// 4. Wait for a compaction pass so the cleaner compacts the sealed segments.
/// 5. Force-roll the active segment. Produce v10-k1, v10-k2, and v10-k3.
/// 6. Wait for another compaction pass so the cleaner compacts the newly-sealed
///    segments.
/// 7. Fetch all records from offset 0.
/// 8. Assert that exactly 3 distinct keys survive.
/// 9. Assert that no stale value from v0-* to v9-* remains.
/// 10. Assert that each key has its latest value, v10-kN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_dedupes_via_native_client() {
    let (handle, _dir, addr) = start_broker_with_fast_cleaner().await;

    // Create the compacted topic.
    create_topic_with_configs(
        addr,
        crate::support::topics::CreateTopicSetup {
            topic: "compacted",
            configs: &[
                ("cleanup.policy", "compact"),
                ("internal.segment.bytes", "256"),
            ],
            ..Default::default()
        },
    )
    .await;

    // Wait for the partition to appear in the broker's registry.
    handle.wait_until_partition_present("compacted", 0).await;

    // Wait for the topic-config overrides (cleanup.policy=compact +
    // internal.segment.bytes=256) to propagate from the metadata image through the
    // ReplicatorSupervisor reconcile loop into the partition's LogConfig.
    // Without this wait, produces can start before the supervisor reconciles,
    // so they land in a default-config Log (1GiB segments, Delete policy) →
    // no segment rolls, no compaction, test sees every record.
    // The LogConfig materializes downstream of the image, so poll the
    // partition's live LogConfig rather than the metadata image itself.
    handle
        .wait_for_metrics(
            "cleanup.policy/internal.segment.bytes propagate to partition LogConfig",
            |_m| {
                handle
                    .partition_log_config_for_test("compacted", 0)
                    .is_some_and(|cfg| {
                        cfg.cleanup_policy == krabka_log::CleanupPolicy::Compact
                            && cfg.segment_size == krabka_units::bytes(256)
                    })
            },
        )
        .await;

    // Get the topic_id (needed for Fetch).
    let topic_id = get_topic_id(addr, "compacted").await;

    // Produce 30 records: 10 each under k1, k2, k3.
    // Values are "v{round}-{key}" so we can identify stale vs. latest.
    for round in 0..10u32 {
        for key in ["k1", "k2", "k3"] {
            let value = format!("v{round}-{key}");
            produce_record(
                addr,
                crate::compaction_rpc::ProduceRecordSetup {
                    topic_id,
                    key: key.as_bytes(),
                    value: value.as_bytes(),
                    ..Default::default()
                },
            )
            .await;
        }
    }

    // The 256-byte segment limit causes many segment rolls during the produce
    // loop, so the cleaner finds sealed segments ready for compaction. Wait for
    // a compaction pass to run on this partition instead of sleeping. Capture
    // the current pass count right before the wait so the +1 pass is guaranteed
    // to run after the sealed segments exist.
    let compactions_before = handle
        .metrics()
        .log_compactions_total
        .get_or_create(&PartitionLabel {
            topic: "compacted".into(),
            partition: 0,
        })
        .get();
    handle
        .wait_for_metrics("compaction pass ran on sealed segments", |m| {
            m.log_compactions_total
                .get_or_create(&PartitionLabel {
                    topic: "compacted".into(),
                    partition: 0,
                })
                .get()
                > compactions_before
        })
        .await;

    // Force-roll the active segment by writing one more record per key.
    // After this the previously-active segment becomes sealed and eligible
    // for the next compaction pass.
    for key in ["k1", "k2", "k3"] {
        let value = format!("v10-{key}");
        produce_record(
            addr,
            crate::compaction_rpc::ProduceRecordSetup {
                topic_id,
                key: key.as_bytes(),
                value: value.as_bytes(),
                ..Default::default()
            },
        )
        .await;
    }

    // Push the active segment into a sealed state so the FINAL v10-* records
    // can also be deduped. Without this the active still holds (at least) the
    // very last v10-k3 record, the compactor (which never touches the active)
    // can't see it, and the previous compaction's "latest" entry for k3 — the
    // v9-k3 record in the now-sealed segment — survives.
    //
    // We can't directly call `Log::roll_active_segment` from a test, so we
    // produce a small burst of records using a sentinel "pad" key (which the
    // assertions below ignore) until enough bytes accumulate to roll the
    // segment past `internal.segment.bytes=256`. ~8 small records is more than enough.
    for round in 0..8 {
        let value = format!("padding-{round}");
        produce_record(
            addr,
            crate::compaction_rpc::ProduceRecordSetup {
                topic_id,
                key: b"__pad__",
                value: value.as_bytes(),
                ..Default::default()
            },
        )
        .await;
    }

    // Wait for another compaction pass so the newly-sealed segments (holding
    // the final v10-* records and the now-sealed prior "latest" entries) get
    // compacted. Capture the pass count after the force-roll + padding burst
    // seal the active segment so the awaited +1 pass runs against them.
    let compactions_before_reroll = handle
        .metrics()
        .log_compactions_total
        .get_or_create(&PartitionLabel {
            topic: "compacted".into(),
            partition: 0,
        })
        .get();
    handle
        .wait_for_metrics("compaction pass ran on newly-sealed segments", |m| {
            m.log_compactions_total
                .get_or_create(&PartitionLabel {
                    topic: "compacted".into(),
                    partition: 0,
                })
                .get()
                > compactions_before_reroll
        })
        .await;

    // Fetch all records from offset 0.
    let records = fetch_all(addr, "compacted", topic_id).await;

    assert_latest_records_survive(&records);

    handle.shutdown().await;
}
