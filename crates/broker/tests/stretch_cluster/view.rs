//! The partition as an operator sees it, and the two waits written over it.
//!
//! Both waits check on every observation they make that the witness is not the
//! leader, which is why they belong next to the view rather than inside the
//! tests: a witness that led for a moment and then handed leadership on must
//! fail the test that was watching.

use {
    crate::{TOPIC, WITNESS, within},
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    },
};

/// The partition as an operator sees it, as one value.
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionView {
    pub leader: u64,
    pub replicas: Vec<u64>,
    pub isr: BTreeSet<u64>,
    pub adding_replicas: Vec<u64>,
    pub removing_replicas: Vec<u64>,
}

pub fn partition_view(handle: &BrokerHandle) -> Option<PartitionView> {
    handle.partition_record_for_test(TOPIC, 0).map(|record| {
        // Placement starts at a random site, as Kafka's does, so the order
        // of the replicas after the first one, the preferred leader, is
        // not fixed.
        let mut replicas: Vec<u64> = record.replicas.iter().map(|n| n.0).collect();
        if let Some((_, followers)) = replicas.split_first_mut() {
            followers.sort_unstable();
        }
        PartitionView {
            leader: record.leader.0,
            replicas,
            isr: record.isr.iter().map(|n| n.0).collect(),
            adding_replicas: record.adding_replicas.iter().map(|n| n.0).collect(),
            removing_replicas: record.removing_replicas.iter().map(|n| n.0).collect(),
        }
    })
}

/// Poll `handle`'s image until the partition has `leader` and exactly `isr`,
/// failing the moment the witness is seen as the leader.
///
/// The witness check is inside the loop on purpose. A witness that led for a
/// moment and then handed leadership on would satisfy an after-the-fact check
/// while having served as a leader it must never be.
pub async fn wait_for_leader_and_isr(handle: &BrokerHandle, what: &str, leader: u64, isr: &[u64]) {
    let want: BTreeSet<u64> = isr.iter().copied().collect();
    let poll = async {
        loop {
            if let Some(view) = partition_view(handle) {
                assert!(
                    view.leader != WITNESS,
                    "the witness led the partition while waiting for {what}: {view:?}"
                );
                if view.leader == leader && view.isr == want {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    within(what, poll).await;
}

/// Watch the partition for `window`, failing if the witness ever leads it.
pub async fn witness_never_leads(handle: &BrokerHandle, window: Duration) {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        if let Some(view) = partition_view(handle) {
            assert!(
                view.leader != WITNESS,
                "the witness must never lead the partition: {view:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
