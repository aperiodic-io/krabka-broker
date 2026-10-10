//! The simulation harness: building an engine over a fresh tempdir log, the
//! metadata values the acceptances submit, and the polling helpers that wait
//! for the quorum to converge.
//!
//! Every acceptance in this suite starts by standing up engines and ends by
//! waiting for agreement, so both halves of that shape are kept here rather
//! than repeated per test module.

pub(crate) use std::{collections::HashMap, sync::Arc, time::Duration};

pub(crate) use krabka_raft::{
    ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax,
    kraft::{
        KraftConfig, KraftController, KraftLog, NodeId, QuorumState,
        snapshot_fetch::MetadataSnapshotFetchMax,
    },
};
use krabka_units::prelude::{Time, millis};

use crate::sim_net::SimNet;

pub(crate) fn three_voter_network() -> (SimNet, [NodeId; 3]) {
    (SimNet::new(), [NodeId(1), NodeId(2), NodeId(3)])
}

pub(crate) async fn shutdown_nodes(net: &SimNet, ids: &[NodeId]) {
    for &id in ids {
        if let Some(controller) = net.get(id) {
            controller.shutdown().await;
        }
    }
}

/// Per-node election timeouts, staggered so one node reliably wins the first
/// round rather than splitting the vote.
pub(crate) const STAGGERED_TIMEOUTS: [Time; 3] = [millis(150), millis(300), millis(450)];

krabka_macros::topic_record_fixture!(single_partition_topic);

krabka_macros::empty_endpoint_voters!(voter_set);

pub(crate) fn topic_record(name: &str, id: u128) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(single_partition_topic(
        name,
        uuid::Uuid::from_u128(id),
    ))
}

/// Builds a single engine over a fresh tempdir log, and does not register it.
/// Start and register every node while retaining its backing directory.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct SimClusterSetup<'a> {
    #[default(&[NodeId(1), NodeId(2), NodeId(3)])]
    pub ids: &'a [NodeId],
    pub cluster_id: uuid::Uuid,
    #[default(&STAGGERED_TIMEOUTS)]
    pub timeouts: &'a [Time],
}

pub(crate) fn start_engines(net: &SimNet, setup: SimClusterSetup<'_>) -> Vec<tempfile::TempDir> {
    let SimClusterSetup {
        ids,
        cluster_id,
        timeouts,
    } = setup;
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(
            net,
            crate::harness::SimEngineSetup {
                me: id,
                ids,
                cluster_id,
                election_timeout: timeouts[i],
                ..Default::default()
            },
        );
        net.register(id, ctrl);
        dirs.push(dir);
    }
    dirs
}

/// The metadata log configuration of a simulated engine: Kafka's segment
/// defaults, no KIP-835 no-op records, so the simulated logs hold only the
/// records the scenario writes, and no size allowance, so every cleaning keeps
/// only the newest snapshot and moves the log start up to it.
pub(crate) fn metadata_log() -> krabka_raft::MetadataLogConfig {
    krabka_raft::MetadataLogConfig {
        max_retention_size: Some(krabka_units::prelude::bytes(0)),
        max_idle_interval: millis(0),
        ..krabka_raft::MetadataLogConfig::default()
    }
}

/// Engine setup with snapshots disabled by default. Snapshot catch-up scenarios
/// set a small record interval so the leader snapshots and prunes after a burst.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(crate) struct SnapshotRecordCount(pub u64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct SimEngineSetup<'a> {
    #[default(NodeId(1))]
    pub me: NodeId,
    #[default(&[NodeId(1), NodeId(2), NodeId(3)])]
    pub ids: &'a [NodeId],
    pub cluster_id: uuid::Uuid,
    #[default(STAGGERED_TIMEOUTS[0])]
    pub election_timeout: Time,
    pub snapshot_interval_records: SnapshotRecordCount,
}

pub(crate) fn build_engine(
    net: &SimNet,
    setup: SimEngineSetup<'_>,
) -> (KraftController, tempfile::TempDir) {
    let SimEngineSetup {
        me,
        ids,
        cluster_id,
        election_timeout,
        snapshot_interval_records,
    } = setup;
    let dir = tempfile::tempdir().expect("tempdir");
    let log =
        KraftLog::open(dir.path(), &krabka_raft::MetadataLogConfig::default()).expect("open log");
    let ctrl = KraftController::spawn(
        KraftConfig {
            me,
            cluster_id,
            directory_id: uuid::Uuid::nil(),
            initial_state: QuorumState::bootstrap(cluster_id, voter_set(ids)),
            election_timeout,
            heartbeat_interval: None,
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            peers: Arc::new(net.as_peer(me)),
            snapshot_interval_records: snapshot_interval_records.0,
            max_bytes_between_snapshots: krabka_units::prelude::bytes(0),
            max_snapshot_interval: millis(0),
            metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
            metadata_log: metadata_log(),
            activation: krabka_raft::kraft::Activation::default(),
        },
        log,
        dir.path().to_path_buf(),
    );
    (ctrl, dir)
}

/// Polls `f` until it returns `Some`, bounded by `timeout`. The helper yields
/// between polls, so the engine loops make progress. It returns the value, or
/// panics on a timeout.
pub(crate) async fn await_until<T, F>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Option<T>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met before timeout"
        );
        tokio::task::yield_now().await;
    }
}

/// The set of `(node, leader_id, leader_epoch)` that each live engine currently
/// believes, read through the non-mutating `quorum_state` handle op.
async fn leaders(net: &SimNet, ids: &[NodeId]) -> Vec<(NodeId, Option<NodeId>, u32)> {
    let mut out = Vec::new();
    for &id in ids {
        if let Some(ctrl) = net.get(id)
            && let Ok(qs) = ctrl.quorum_state().await
        {
            out.push((id, qs.leader_id, qs.leader_epoch));
        }
    }
    out
}

/// Waits until exactly one node believes it is the leader and every live node
/// agrees on that leader id and epoch. Returns `(leader_id, epoch)`.
pub(crate) async fn await_single_leader(
    net: &SimNet,
    ids: &[NodeId],
    timeout: Duration,
) -> (NodeId, u32) {
    let net = net.clone();
    let ids = ids.to_vec();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let snap = leaders(&net, &ids).await;
        // All live nodes must report the same Some(leader) and same epoch, and
        // that leader must be one of the live nodes.
        if !snap.is_empty() {
            let first_leader = snap[0].1;
            let first_epoch = snap[0].2;
            if let Some(leader) = first_leader {
                let agree = snap
                    .iter()
                    .all(|(_, l, e)| *l == Some(leader) && *e == first_epoch);
                let live = ids.contains(&leader);
                if agree && live {
                    return (leader, first_epoch);
                }
            }
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "no single leader before timeout; states={snap:?}"
        );
        tokio::task::yield_now().await;
    }
}
