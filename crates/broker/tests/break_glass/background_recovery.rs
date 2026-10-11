//! The background unclean-recovery path, under each of the three settings
//! `break_glass.background_unclean_recovery` takes.
//!
//! This path runs from leader election and from a broker heartbeat, with no
//! request, no connection and no principal, so a two-person rule cannot sit on
//! it. The table is what keeps the documented three-way choice honest: `off`
//! is today's behaviour, `audit-only` recovers and counts the bypass, and
//! `require` fails closed and leaves the partition visibly offline.

use std::time::Duration;

use assert2::check;
use krabka_broker::{BrokerHandle, NodeId, config::BackgroundUncleanRecovery};
use krabka_metadata::{
    BreakGlassAction as GatedAction, MetadataRecord, PartitionRecord, TopicConfigRecord,
};

use crate::{
    cluster::{index_of, plain_client, start_gated_cluster},
    support::{self, shutdown_cluster},
    topics::create_topic,
    transitions::bypassed,
};

/// One row of the background-recovery table.
struct BackgroundCase {
    /// What the row is about. First, so a failure names it.
    label: &'static str,
    /// The `break_glass.background_unclean_recovery` setting under test.
    mode: BackgroundUncleanRecovery,
    /// Whether the recovery runs and elects a survivor at all.
    recovers: bool,
    /// Whether the broker counts the election as a bypass of a rule that
    /// nobody on this path could have satisfied.
    counts_bypass: bool,
}

/// The three answers the design gives to a recovery with no caller.
const BACKGROUND: [BackgroundCase; 3] = [
    BackgroundCase {
        label: "off keeps today's behaviour",
        mode: BackgroundUncleanRecovery::Off,
        recovers: true,
        counts_bypass: false,
    },
    BackgroundCase {
        label: "audit-only recovers and accounts for it",
        mode: BackgroundUncleanRecovery::AuditOnly,
        recovers: true,
        counts_bypass: true,
    },
    BackgroundCase {
        label: "require leaves the partition offline",
        mode: BackgroundUncleanRecovery::Require,
        recovers: false,
        counts_bypass: false,
    },
];

/// The topic the background cases take offline.
const RECOVERED: &str = "recovered";

/// Take partition 0 of [`RECOVERED`] offline behind the dead broker `victim`.
///
/// Every live ISR member is gone, so the controller's failover sweep hands the
/// partition to the offset-aware recovery manager with no proposal and nobody
/// to ask for one. That is the path this table is about, and it is the one path
/// a two-person rule cannot sit on.
async fn take_offline(controller: &BrokerHandle, victim: NodeId) {
    let current = controller
        .partition_record_for_test(RECOVERED, 0)
        .expect("a partition record");
    controller
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            leader: victim,
            isr: vec![victim],
            leader_epoch: current.leader_epoch.next(),
            partition_epoch: current.partition_epoch + 1,
            ..current.clone()
        }))
        .await
        .expect("take the partition offline");
}

/// Assert what `case` says the broker does with a recovery nobody approved.
async fn check_background_outcome(
    controller: &BrokerHandle,
    case: &BackgroundCase,
    victim: NodeId,
) {
    let label = case.label;
    if case.recovers {
        controller
            .wait_for_image(|image| {
                image
                    .partition(RECOVERED, 0)
                    .is_some_and(|record| record.leader != victim)
            })
            .await;
    } else {
        // A negative outcome, so there is no event to await. The partition has
        // to still be leaderless after the recovery manager has had every
        // chance to elect, which a bounded wait is the only way to state.
        tokio::time::sleep(Duration::from_secs(5)).await;
        check!(
            controller
                .partition_record_for_test(RECOVERED, 0)
                .map(|record| record.leader)
                == Some(victim),
            "case {label}: the partition stays leaderless and visibly offline"
        );
    }

    if case.counts_bypass {
        controller
            .wait_for_metrics("a counted unclean-recovery bypass", |_| {
                bypassed(controller, GatedAction::UncleanRecovery) >= 1
            })
            .await;
    } else {
        check!(
            bypassed(controller, GatedAction::UncleanRecovery) == 0,
            "case {label}: nothing was recorded as bypassed"
        );
    }
}

/// Drive one background-recovery row on its own three-node cluster.
async fn background_recovery_case(case: &BackgroundCase) {
    let mut cluster = start_gated_cluster(3, case.mode).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let leader = cluster[0].0.wait_until_controller_leader().await;
    let victim_index = (0..cluster.len())
        .find(|index| cluster[*index].1.node_id != leader)
        .expect("a non-controller node");
    let victim = cluster[victim_index].1.node_id;

    let client = plain_client(
        &cluster[index_of(&cluster, leader)]
            .1
            .listen_addr
            .to_string(),
    )
    .await;
    create_topic(&client, RECOVERED, 3).await;
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(RECOVERED, 0).await;
    }
    // Only a topic that opted into an offset-aware strategy reaches the
    // recovery manager. Without one the failover path elects directly and never
    // consults the background rule at all.
    cluster[index_of(&cluster, leader)]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: RECOVERED.to_owned(),
            overrides: [(
                "unclean.recovery.strategy".to_owned(),
                "Aggressive".to_owned(),
            )]
            .into_iter()
            .collect(),
        }))
        .await
        .expect("set unclean.recovery.strategy");

    let (dead, _config, _dir) = cluster.remove(victim_index);
    dead.shutdown().await;
    let controller = &cluster[index_of(&cluster, leader)].0;
    // The ISR shrink is the controller's own signal that liveness has marked
    // the broker dead, which is what the failover sweep reads.
    controller
        .wait_for_image(|image| {
            image
                .partition(RECOVERED, 0)
                .is_some_and(|record| !record.isr.contains(&victim))
        })
        .await;

    take_offline(controller, victim).await;
    check_background_outcome(controller, case, victim).await;

    shutdown_cluster(cluster).await;
}

/// The background unclean-recovery path under each of the three settings it
/// takes.
///
/// This path runs from leader election and from a broker heartbeat, with no
/// request, no connection, and no principal, so a two-person rule cannot exist
/// on it. The design says that plainly rather than leaving a silent gap, and
/// this case is what keeps the statement true: `off` is today's behaviour,
/// `audit-only` recovers and counts the bypass so an operator can prove after
/// the fact that a data-losing election happened with nobody's approval, and
/// `require` fails closed and leaves the partition visibly offline. A setting
/// that quietly collapsed into another would turn the documented three-way
/// choice into a lie, and `break_glass_bypassed` is the series an operator is
/// told to alert on.
///
/// The rows run one cluster at a time rather than three at once: each needs
/// three brokers and a broker death, and the sequence keeps the peak cost of
/// the case to one cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_unclean_recovery_follows_its_configured_rule() {
    for case in &BACKGROUND {
        background_recovery_case(case).await;
    }
}
