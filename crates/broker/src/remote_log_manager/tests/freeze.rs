//! KFC-9 topic write freeze, as the tiered-storage sweep sees it.
//!
//! Each case runs the same one-partition tick three times -- unfrozen, under a
//! literal freeze, and under a prefix freeze that covers the topic -- so the
//! unfrozen control proves the sweep would otherwise have done the work the
//! freeze holds back.

use assert2::check;
use fixtures::{local_backends, sweep_counts};
use krabka_metadata::PatternType;

use super::*;
use crate::{remote_log_manager::test_support as fixtures, test_support::FreezeSetup};

/// The `orders` topic, plus the one live freeze entry `freeze` names.
/// `None` is the unfrozen control every freeze case runs against.
fn image_with_orders_freeze(freeze: Option<(&str, PatternType)>) -> MetadataImage {
    let mut image = image_with_orders_topic();
    if let Some((scope, pattern_type)) = freeze {
        image.apply(&MetadataRecord::V1TopicFreeze(
            crate::test_support::topic_freeze_record(FreezeSetup {
                scope,
                pattern_type,
                ..Default::default()
            }),
        ));
    }
    image
}

/// A tiered topic whose retention settings evict nothing on their own, so
/// what a tick does is decided by the freeze alone.
fn tiered_no_eviction() -> LogConfig {
    LogConfig {
        segment_size: bytes(256),
        remote_storage_enable: true,
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    }
}

/// A tiered topic whose local budget is zero, so every copied segment is
/// past the local-retention window the moment the copy finishes.
fn tiered_local_eviction() -> LogConfig {
    LogConfig {
        local_retention_size: Some(NO_BYTES),
        ..tiered_no_eviction()
    }
}

/// A tiered topic whose *remote* budget is zero while its local budget is
/// generous, so a tick evicts from the archive and never from disk.
fn tiered_remote_eviction() -> LogConfig {
    LogConfig {
        retention_size: Some(NO_BYTES),
        local_retention_size: Some(bytes(1_048_576)),
        ..tiered_no_eviction()
    }
}

/// What one full [`tick_all`] left behind for the single `orders`
/// partition it swept.
struct TickOutcome {
    /// Sealed segments on local disk before the tick.
    sealed_before: usize,
    /// Segments the remote tier holds at `CopySegmentFinished` after it.
    remote_finished: usize,
    /// Sealed segments still on local disk after it.
    local_sealed_after: usize,
}

/// Drive exactly one sweep over one locally-led, tiered `orders`
/// partition against `image`, and report what it did.
async fn tick_once(image: MetadataImage, config: LogConfig) -> TickOutcome {
    let (log_dir, remote_dir) = fixtures::temporary_dirs();
    let partitions = PartitionRegistry::new();
    let partition = rolled_tiered_partition_with_config(log_dir.path(), config);
    let sealed_before = partition
        .log
        .lock()
        .expect("partition log mutex poisoned")
        .tierable_segments()
        .len();
    partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));

    let controller = fixed_source(image);
    let (rsm, rlmm) = local_backends(remote_dir.path());

    fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;

    let (remote_finished, local_sealed_after) = sweep_counts(&partition, &rlmm);
    TickOutcome {
        sealed_before,
        remote_finished,
        local_sealed_after,
    }
}

/// The freeze cases every retention test runs, each beside the unfrozen
/// control that proves the tick would otherwise have done the work.
const FREEZE_CASES: [(&str, Option<(&str, PatternType)>); 3] = [
    ("an unfrozen control", None),
    (
        "a literal freeze on the topic",
        Some(("orders", PatternType::Literal)),
    ),
    (
        "a prefix freeze covering the topic",
        Some(("ord", PatternType::Prefixed)),
    ),
];

#[tokio::test]
async fn tick_all_still_copies_to_remote_storage_for_a_frozen_partition() {
    // The invariant this file most needs held: a copy adds a replica and
    // removes nothing, so a freeze never refuses it. Tiering a frozen
    // topic is exactly what a disaster-recovery migration wants.
    for (label, freeze) in FREEZE_CASES {
        let outcome = tick_once(image_with_orders_freeze(freeze), tiered_no_eviction()).await;

        check!(
            outcome.sealed_before >= 2,
            "{label}: the fixture needs multiple sealed segments"
        );
        check!(
            outcome.remote_finished == outcome.sealed_before,
            "{label}: every sealed segment reaches the remote tier"
        );
    }
}

#[tokio::test]
async fn tick_all_evicts_no_local_segment_for_a_frozen_partition() {
    for (label, freeze) in FREEZE_CASES {
        let frozen = freeze.is_some();
        let outcome = tick_once(image_with_orders_freeze(freeze), tiered_local_eviction()).await;

        check!(
            outcome.sealed_before >= 2,
            "{label}: the fixture needs multiple sealed segments"
        );
        check!(
            outcome.remote_finished == outcome.sealed_before,
            "{label}: the copy runs whatever the freeze says"
        );
        // The control drops every copied segment. The zero budget covers the
        // active segment too, so the control also rolls it for the next
        // copy, as Kafka's `deletableSegments` does, and that one segment
        // stays.
        let want_local = if frozen { outcome.sealed_before } else { 1 };
        check!(
            outcome.local_sealed_after == want_local,
            "{label}: local sealed segments after the sweep"
        );
    }
}

#[tokio::test]
async fn tick_all_evicts_no_remote_segment_for_a_frozen_partition() {
    for (label, freeze) in FREEZE_CASES {
        let frozen = freeze.is_some();
        let outcome = tick_once(image_with_orders_freeze(freeze), tiered_remote_eviction()).await;

        check!(
            outcome.sealed_before >= 2,
            "{label}: the fixture needs multiple sealed segments"
        );
        // `DeleteSegmentFinished` drops the entry from the RLMM, so the
        // control ends the sweep with nothing archived and a frozen topic
        // ends it with everything it copied.
        let want_remote = if frozen { outcome.sealed_before } else { 0 };
        check!(
            outcome.remote_finished == want_remote,
            "{label}: archived segments after the sweep"
        );
        check!(
            outcome.local_sealed_after == outcome.sealed_before,
            "{label}: the generous local budget evicts nothing on disk"
        );
    }
}
