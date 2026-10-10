//! A transaction marker a follower learns through replication must update the
//! produce-path tracker exactly as a marker this broker appends as leader
//! does.
//!
//! `WriterMessage::Replicate` is the only path a control batch reaches on a
//! follower: `replicator/response.rs` decodes every control batch (raw or
//! `V2`) and sends it through `Partition::replicate_batch`, never through
//! `ReplicateVerbatim`. Without mirroring on that arm, the follower's tracker
//! keeps an empty or pre-marker entry after a transaction-version-2 marker
//! until a promotion copies the log's producer state: an old-epoch retry the
//! marker fenced could be accepted, and an empty tracker accepts any first
//! sequence, not only 0.

use assert2::assert;
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;

use super::Decision;
use crate::txn::marker::{MarkerType, build_marker_batch};

const TOPIC: &str = "orders";
const PARTITION: PartitionIndex = PartitionIndex(0);
const PRODUCER_ID: i64 = 42;

#[tokio::test]
async fn a_replicated_marker_bumps_the_tracked_epoch() {
    let (_directory, state, partition) = super::restart_agreement::producer_partition();

    // A follower replicates the leader's data batch at epoch 3, then its
    // transaction-version-2 commit marker at the bumped epoch 4. Neither
    // append goes through `Partition::produce_batch`: this is exactly the
    // `WriterMessage::Replicate` path a follower's Fetch loop drives.
    partition
        .replicate_batch(super::restart_agreement::data_batch(
            super::restart_agreement::DataBatchSetup::default(),
        ))
        .await
        .expect("replicate data batch");

    let marker = build_marker_batch(
        ProducerId(PRODUCER_ID),
        4,
        partition.log_end_offset(),
        MarkerType::Commit,
        0,
    );
    partition
        .replicate_batch(marker)
        .await
        .expect("replicate marker");

    // A follower's Fetch loop never calls `producer_state.check`, but this is
    // the exact question the produce path asks after a promotion: the
    // marker's epoch must be the tracked one, and the transaction-version-2
    // rule that #622 fixed (a new epoch appends only at sequence 0) must
    // already hold for it, because the mirror ran when the marker replicated,
    // not only at the next restart.
    assert!(
        state.check(TOPIC, PARTITION, PRODUCER_ID, 4, 0, 0).await == Decision::Append,
        "the bumped epoch at sequence 0 must append"
    );
    assert!(
        state.check(TOPIC, PARTITION, PRODUCER_ID, 4, 3, 0).await == Decision::OutOfOrder,
        "the bumped epoch at a continuing sequence must be out of order"
    );
    assert!(
        state.check(TOPIC, PARTITION, PRODUCER_ID, 3, 0, 2).await == Decision::Fenced,
        "the pre-marker epoch must be fenced, not treated as unknown"
    );
}
