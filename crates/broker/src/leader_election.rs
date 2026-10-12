//! Controller-side leader-election scan. The liveness ticker calls it
//! when a broker transitions `alive → dead`. The scan reads every partition
//! where the dead broker is leader, picks the first alive ISR replica as new
//! leader, bumps `leader_epoch`, and emits the new `PartitionRecord`s
//! through openraft.
//!
//! KIP-841: when ISR becomes empty and the topic's
//! `unclean.leader.election.enable` is `true`, the scan falls through to
//! an out-of-ISR pick, the first alive replica, with a singleton ISR. This
//! accepts possible data loss in exchange for availability. The default
//! `false` keeps Kafka's safe-by-default behavior. The partition stays
//! unavailable until a former ISR member returns.
//!
//! KIP-966: a partition with nothing to elect has no leader. Its record keeps
//! the last leader, and the ELR it publishes says so: a one-member last-known
//! ELR naming that leader, which `Metadata` and `DescribeTopicPartitions`
//! project as `leader = -1`. When a broker unfences,
//! [`compute_unfence_changes`] elects again for such a partition. The last
//! leader takes it back from the ELR, or, if an unclean restart kept it out
//! of the ELR, as an unclean leader in `RECOVERING` -- with no
//! `unclean.leader.election.enable` and no `unclean.recovery.strategy` in
//! front of it, as in Kafka's `canElectLastKnownLeader`.

mod driver;
mod operator;
mod policy;
mod scan;

// `unclean_recovery::selection` reads the same witness set the failover scans
// do, so its tests build one out of these fixtures too.
#[cfg(test)]
pub(crate) mod test_support;

// `leader_failover_model` checks the real per-partition policy, so these
// two are re-exported for it alone.
#[cfg(test)]
pub(crate) use self::policy::{FailoverDecision, Ladder, failover_one};
pub(crate) use self::{
    driver::{LivenessTickState, run_liveness_tick},
    operator::{ElectError, ElectionType, select_new_leader_for_partition},
    policy::FailoverPlan,
    scan::{
        compute_failover_changes, compute_offline_dir_failover_changes,
        compute_unclean_restart_changes, compute_unfence_changes,
    },
};

#[cfg(test)]
#[path = "leader_failover_model.rs"]
mod leader_failover_model;
