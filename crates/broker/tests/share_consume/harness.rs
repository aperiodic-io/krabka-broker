//! Cluster, topic, and membership plumbing that every KIP-932 consume test in
//! this binary shares. It starts an in-process broker whose share-coordinator
//! state topic has a single partition, creates a data topic and resolves its
//! id, produces records into it, joins a share group, and waits until the
//! group lifecycle has durably initialized the share state a consume needs.

// These single-broker tests only need one state partition. Keeping the test
// geometry small also prevents the parallel test runner from exhausting its
// process-wide file-descriptor limit while eleven brokers run concurrently.
/// Wait until the group-coordinator lifecycle hook has durably initialized the
/// share state for `(group, topic, partition)`. The persister summary then
/// becomes present. Until that happens the share coordinator is not yet
/// write-ready, and a consume's SPSO advance would not persist.
///
/// The lifecycle hook fires on each heartbeat, so this helper drives
/// steady-state heartbeats inside the wait loop rather than sleeping. It mirrors
/// the `lifecycle_initializes_share_state` pattern in `share_groups.rs`.
pub use crate::support::share::wait_for_share_init;
use crate::support::share::{
    ShareRecordCount, ShareRecordSeedSetup, consumption_fixture as share_consumption_fixture,
};
pub use crate::support::share::{
    bootstrap_share_state, broker_config, broker_test_permit, join, produce_n, produce_values,
    topic_id, wire,
};

crate::share_consumption_fixture!(
    /// Initialize the common g1/t fixture before its first consume.
    initialize_consumption,
    join,
    |broker, client, member, epoch, tid| wait_for_share_init(broker, client, &member, epoch, tid)
);

pub use crate::support::share::ConsumptionFixture;

pub async fn consumption_fixture(setup: ShareRecordSeedSetup) -> ConsumptionFixture {
    share_consumption_fixture(setup, initialize_consumption).await
}

/// A fresh g1/t session whose initial batch contains exactly one record.
pub async fn single_record_consumption_fixture() -> ConsumptionFixture {
    consumption_fixture(ShareRecordSeedSetup {
        count: ShareRecordCount(1),
    })
    .await
}
