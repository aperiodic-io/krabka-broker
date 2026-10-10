//! The freeze registry lives in the metadata log, so a restart cannot thaw a
//! cluster silently.
//!
//! A registry that lived anywhere else would pass every other case in this
//! suite and fail this one in the worst possible way: the topic would start
//! accepting writes during the incident the freeze was declared for.

use assert2::check;
use krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL;

use crate::{
    control_plane::wait_for_registry_len,
    support,
    wire::{CONTROL, accepted, refused},
};

/// A freeze survives a controller restart.
///
/// The registry lives in the metadata log rather than in memory precisely so
/// that a restart cannot thaw a cluster silently. This case restarts the only
/// controller in the cluster and asserts the refusal is still there. A registry
/// that lived anywhere else would pass every other case in this suite and fail
/// this one, in the worst possible way: the topic would start accepting writes
/// during the incident that the freeze was declared for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_freeze_survives_a_controller_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        let (_frozen, _control) =
            crate::wire::frozen_topic_with_record(&broker, &client, "orders", "cutover").await;
        broker.shutdown().await;
    }

    let (broker, client) = support::start_with_dir(dir.path()).await;
    crate::wire::wait_controlled_partitions(&broker, "orders").await;

    let entries = wait_for_registry_len(&client, 1).await;
    check!(entries[0].scope == "orders");
    check!(entries[0].pattern_type == PATTERN_TYPE_LITERAL);

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    crate::wire::check_produce!(&broker, &client;
        "orders", frozen => refused("literal", "orders", "cutover", 1), CONTROL, control => accepted(1));

    broker.shutdown().await;
}
