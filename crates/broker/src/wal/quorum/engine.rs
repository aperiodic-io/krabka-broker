//! In-process WAL quorum engine.
//!
//! This file is the module root. It holds the replica set, the shard engine
//! state, and the distributed durability path. Each child holds one concern:
//! `batches` decodes a raw WAL byte range, `replica_io` runs the blocking log
//! operations off the async worker threads, `recovery` picks and enforces the
//! durable prefix a shard opens on, and `distributed` keeps the diskless
//! voter-set quorum.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_kraft_core::{LogView as _, NodeId};
use krabka_log::{Log, VerbatimBatch};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::sync::Notify;

mod batches;
mod distributed;
#[cfg(test)]
mod model;
#[cfg(test)]
mod recovery;
mod replica_io;

#[cfg(test)]
use self::recovery::{bootstrap_durable_prefix, recover_durable_prefix};
use self::replica_io::trim_log;
pub(super) use self::{
    batches::{
        read_batches_exact, read_log_batches_covering, read_log_batches_exact, split_batches,
    },
    replica_io::sync_replica,
};
use crate::{
    error::BrokerError,
    metrics::BrokerMetrics,
    wal::quorum::{log_view::ShardLog, registry::ShardId},
};

/// A single durable member of a WAL quorum.
#[derive(Debug)]
pub(crate) struct WalReplica {
    #[cfg(test)]
    pub(super) id: NodeId,
    log: ShardLog,
    alive: AtomicBool,
}

impl WalReplica {
    #[must_use]
    fn new(log: Arc<Mutex<Log>>) -> Self {
        Self {
            #[cfg(test)]
            id: NodeId(0),
            log: ShardLog::new(log),
            alive: AtomicBool::new(true),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test(id: NodeId, log: Arc<Mutex<Log>>) -> Self {
        Self {
            id,
            ..Self::new(log)
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
}

/// Drives the durable quorum frontier of a WAL shard.
#[derive(Debug)]
pub(crate) struct WalShardEngine {
    replicas: Vec<WalReplica>,
    expected_voters: usize,
    durable_watermark: AtomicI64,
    local_durable: AtomicI64,
    distributed_required: AtomicBool,
    distributed: Mutex<Option<DistributedQuorum>>,
    durable_advanced: Notify,
    observability: OnceLock<(ShardId, BrokerMetrics)>,
}

#[derive(Debug)]
struct DistributedQuorum {
    me: NodeId,
    voters: Vec<NodeId>,
    durable_offsets: HashMap<NodeId, Offset>,
}

pub(super) fn strict_majority(voter_count: usize) -> usize {
    voter_count.div_euclid(2).saturating_add(1)
}

/// One response from the leader-side WAL fetch path.
#[derive(Debug)]
pub(crate) struct WalFetchData {
    pub(crate) high_watermark: Offset,
    pub(crate) log_end_offset: Offset,
    pub(crate) log_start_offset: Offset,
    pub(crate) records: Bytes,
    pub(crate) offset_out_of_range: bool,
    pub(crate) diverging_epoch: Option<(LeaderEpoch, Offset)>,
}

#[derive(Debug, Clone, Copy)]
#[cfg(test)]
pub(crate) enum OpenMode {
    BootstrapFrom(NodeId),
    Recover,
}

impl WalShardEngine {
    #[cfg(test)]
    pub(crate) fn new(replicas: Vec<WalReplica>, mode: OpenMode) -> Result<Self, BrokerError> {
        if replicas.is_empty() {
            return Err(BrokerError::Replication(
                "wal quorum must contain at least one replica".into(),
            ));
        }
        let expected_voters = replicas.len();
        let durable_watermark = match mode {
            OpenMode::BootstrapFrom(source) => bootstrap_durable_prefix(&replicas, source)?,
            OpenMode::Recover => {
                recover_durable_prefix(&replicas, strict_majority(expected_voters))?
            }
        };
        Ok(Self::with_frontiers(
            replicas,
            expected_voters,
            durable_watermark,
            durable_watermark,
        ))
    }

    pub(crate) fn new_distributed(
        source: Arc<Mutex<Log>>,
        expected_voters: usize,
    ) -> Result<Self, BrokerError> {
        if expected_voters == 0 {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be positive".into(),
            ));
        }
        if expected_voters.is_multiple_of(2) {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be odd".into(),
            ));
        }
        let (log_start, local_durable) = {
            let mut log = source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let log_start = log.log_start_offset();
            log.sync()?;
            (log_start, log.log_end_offset())
        };
        Ok(Self::with_frontiers(
            vec![WalReplica::new(source)],
            expected_voters,
            log_start,
            local_durable,
        ))
    }

    fn with_frontiers(
        replicas: Vec<WalReplica>,
        expected_voters: usize,
        durable_watermark: Offset,
        local_durable: Offset,
    ) -> Self {
        Self {
            replicas,
            expected_voters,
            durable_watermark: AtomicI64::new(durable_watermark.0),
            local_durable: AtomicI64::new(local_durable.0),
            distributed_required: AtomicBool::new(false),
            distributed: Mutex::new(None),
            durable_advanced: Notify::new(),
            observability: OnceLock::new(),
        }
    }

    #[cfg(test)]
    fn for_model(replicas: Vec<WalReplica>, durable_watermark: Offset) -> Self {
        let expected_voters = replicas.len();
        Self::with_frontiers(
            replicas,
            expected_voters,
            durable_watermark,
            durable_watermark,
        )
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_logs(logs: std::collections::BTreeMap<NodeId, Arc<Mutex<Log>>>) -> Self {
        let replicas = logs
            .into_iter()
            .map(|(id, log)| WalReplica::for_test(id, log))
            .collect();
        Self::new(replicas, OpenMode::Recover).expect("test WAL quorum recovers")
    }

    #[must_use]
    pub(crate) fn durable_watermark(&self) -> Offset {
        Offset(self.durable_watermark.load(Ordering::Acquire))
    }

    pub(crate) fn attach_observability(&self, shard: ShardId, metrics: BrokerMetrics) {
        let _ = self.observability.set((shard, metrics));
        self.record_observability();
    }

    fn record_observability(&self) {
        let Some(source) = self.replicas.first() else {
            return;
        };
        let (log_start, leader_end) = {
            let log = source.log.lock();
            (log.log_start_offset(), log.log_end_offset())
        };
        self.record_observability_at(log_start, leader_end);
    }

    fn record_observability_at(&self, log_start: Offset, leader_end: Offset) {
        let Some((shard, metrics)) = self.observability.get() else {
            return;
        };
        let distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(quorum) = distributed.as_ref() {
            metrics.record_diskless_wal_watermark(
                shard.topic_id,
                shard.partition,
                self.durable_watermark().0,
            );
            metrics.initialize_diskless_wal_flusher_metrics(
                shard.topic_id,
                shard.partition,
                leader_end.0.saturating_sub(log_start.0),
                log_start.0,
            );
            for voter in &quorum.voters {
                let durable = quorum
                    .durable_offsets
                    .get(voter)
                    .copied()
                    .unwrap_or(log_start);
                metrics.record_diskless_wal_voter_lag(
                    shard.topic_id,
                    shard.partition,
                    *voter,
                    leader_end.0.saturating_sub(durable.0),
                );
            }
        }
    }

    fn remove_voter_observability(&self, voters: &[NodeId]) {
        if let Some((shard, metrics)) = self.observability.get() {
            metrics.remove_diskless_wal_voters(shard.topic_id, shard.partition, voters);
        }
    }

    pub(crate) fn clear_observability(&self) {
        let voters = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or_else(Vec::new, |quorum| quorum.voters.clone());
        if let Some((shard, metrics)) = self.observability.get() {
            metrics.remove_diskless_wal_shard(shard.topic_id, shard.partition, &voters);
        }
    }

    fn record_quorum_loss(&self, target: Offset, error: &BrokerError) {
        let Some((shard, metrics)) = self.observability.get() else {
            return;
        };
        metrics.diskless_wal_quorum_loss_events_total.inc();
        tracing::warn!(
            topic_id = %shard.topic_id,
            partition = shard.partition.0,
            target = target.0,
            durable_watermark = self.durable_watermark().0,
            %error,
            "diskless WAL leader failed to reach quorum"
        );
    }

    pub(crate) async fn wait_for_durable_advance(&self, after: Offset) -> Offset {
        loop {
            let advanced = self.durable_advanced.notified();
            let current = self.durable_watermark();
            if current.cmp(&after).is_gt() {
                return current;
            }
            advanced.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_replica_alive(&self, id: NodeId, alive: bool) {
        if let Some(replica) = self.replicas.iter().find(|replica| replica.id == id) {
            replica.alive.store(alive, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn replica_end_offsets(&self) -> Vec<Offset> {
        self.replicas
            .iter()
            .map(|replica| Offset(replica.log.end_offset()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn replica_start_offsets(&self) -> Vec<Offset> {
        self.replicas
            .iter()
            .map(|replica| replica.log.lock().log_start_offset())
            .collect()
    }

    pub(crate) async fn replicate_and_sync(
        &self,
        source: &Arc<Mutex<Log>>,
        target: Offset,
    ) -> Result<Offset, BrokerError> {
        let committed = self.durable_watermark();
        if target <= committed {
            return Ok(committed);
        }
        let source = ShardLog::new(source.clone());
        let source_end = Offset(source.end_offset());
        if target > source_end {
            return Err(BrokerError::Replication(format!(
                "wal source ends at {}, before requested durable offset {}",
                source_end.0, target.0
            )));
        }

        if self.distributed_required.load(Ordering::Acquire) {
            // The local fsync comes first, placement or not: the leader votes
            // with the offset it has fsynced, and `configure_distributed`
            // records that offset when a placement arrives after this call
            // has already returned.
            if let Err(error) = sync_replica(source.clone(), Vec::new()).await {
                self.record_quorum_loss(target, &error);
                return Err(error);
            }
            self.local_durable.fetch_max(target.0, Ordering::AcqRel);
            let configured = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
            if !configured {
                let error = BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                );
                self.record_quorum_loss(target, &error);
                return Err(error);
            }
            let Some(me) = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|quorum| quorum.me)
            else {
                let error =
                    BrokerError::Replication("diskless WAL broker placement disappeared".into());
                self.record_quorum_loss(target, &error);
                return Err(error);
            };
            let log_start = source.lock().log_start_offset();
            self.record_durable_offset(me, target, log_start, source_end);
            loop {
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                let advanced = self.durable_advanced.notified();
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                if self
                    .distributed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none()
                {
                    let error = BrokerError::Replication(
                        "diskless WAL broker placement disappeared".into(),
                    );
                    self.record_quorum_loss(target, &error);
                    return Err(error);
                }
                advanced.await;
            }
        }

        let mut synced = 0usize;
        let source_start = source.lock().log_start_offset();
        for replica in &self.replicas {
            if !replica.alive.load(Ordering::Acquire) {
                continue;
            }
            // Verify the replica's whole retained log against the source, not
            // only the records past the committed offset: a replica that
            // led an epoch nobody acknowledged can hold a different record
            // below that offset, and counting it would acknowledge a prefix
            // that fewer than a majority hold.
            let sync_start = replica_start_offset(replica).max(source_start);
            let Ok(batches) = read_batches_exact(&source, sync_start, target) else {
                continue;
            };
            if sync_replica(replica.log.clone(), batches).await.is_ok() {
                synced += 1;
            }
        }
        let required = strict_majority(self.expected_voters);
        if synced < required {
            let error = BrokerError::Replication(format!(
                "wal quorum has {synced} synced replicas, needs {required}"
            ));
            self.record_quorum_loss(target, &error);
            return Err(error);
        }
        self.durable_watermark.store(target.0, Ordering::Release);
        self.record_observability();
        Ok(target)
    }

    pub(crate) fn serve_fetch(
        &self,
        fetch_offset: Offset,
        last_fetched_epoch: i32,
        max_size: ByteSize,
    ) -> Result<WalFetchData, BrokerError> {
        let replica = self
            .replicas
            .iter()
            .find(|replica| replica.alive.load(Ordering::Acquire))
            .ok_or_else(|| {
                BrokerError::Replication("wal quorum has no live fetch replica".into())
            })?;
        let log = replica.log.lock();
        let log_start_offset = log.log_start_offset();
        let log_end_offset = log.log_end_offset();
        let offset_out_of_range = fetch_offset < log_start_offset || fetch_offset > log_end_offset;
        let diverging_epoch = u32::try_from(last_fetched_epoch)
            .ok()
            .and_then(|epoch| fetch_divergence(&log, epoch, fetch_offset));
        let records = if diverging_epoch.is_some()
            || offset_out_of_range
            || fetch_offset == log_end_offset
            || max_size == ByteSize::ZERO
        {
            Bytes::new()
        } else {
            // A WAL follower must receive the leader's uncommitted tail and
            // fsync it before that follower can acknowledge the range. Limiting
            // this read to the current high watermark creates a deadlock: no
            // follower can fetch the bytes needed to advance the watermark.
            log.read_raw(fetch_offset, log_end_offset, max_size)?.bytes
        };
        Ok(WalFetchData {
            high_watermark: self.durable_watermark(),
            log_end_offset,
            log_start_offset,
            records,
            offset_out_of_range,
            diverging_epoch,
        })
    }

    pub(crate) async fn trim_to_offset(
        &self,
        source: &Arc<Mutex<Log>>,
        new_start: Offset,
    ) -> Result<Offset, BrokerError> {
        let source_replica = self
            .replicas
            .first()
            .ok_or_else(|| BrokerError::Replication("wal quorum has no source replica".into()))?;
        if !source_replica.log.shares_log(source) {
            return Err(BrokerError::Replication(
                "wal quorum source is not its first replica".into(),
            ));
        }
        if self.distributed_required.load(Ordering::Acquire) {
            if self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
            {
                return Err(BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                ));
            }
            return trim_log(ShardLog::new(source.clone()), new_start).await;
        }
        // Trim replica copies before the partition source. If one copy fails,
        // the source remains available and a later flusher tick can retry.
        for replica in &self.replicas[1..] {
            trim_log(replica.log.clone(), new_start).await?;
        }
        trim_log(ShardLog::new(source.clone()), new_start).await
    }
}

/// The diverging epoch a WAL leader answers a follower Fetch with, or `None`
/// when the fetch is consistent with `log`.
///
/// This is KIP-595's `KafkaMetadataLog.validateOffsetAndEpoch`: the fetch is
/// consistent only when the leader holds `last_fetched_epoch` itself and that
/// epoch reaches `fetch_offset`. The lookup is
/// `KafkaMetadataLog.endOffsetForEpoch`, shared with the `KRaft` log through
/// [`krabka_raft::kraft::log::end_offset_for_epoch_in`]: an epoch the leader
/// cannot place, because it is newer than every epoch in the leader's log,
/// answers the leader's log end with its latest epoch. That answer is always
/// usable; the follower truncates to the end of its own copy of that epoch
/// (`KafkaMetadataLog.truncateToEndOffset`) and fetches again. The raw
/// `LeaderEpochFileCache.endOffsetFor` answer, `(-1, -1)`, would instead reach
/// the follower as "no divergence" and stall it at its own log end.
fn fetch_divergence(
    log: &Log,
    last_fetched_epoch: u32,
    fetch_offset: Offset,
) -> Option<(LeaderEpoch, Offset)> {
    let end = krabka_raft::kraft::log::end_offset_for_epoch_in(
        log.epoch_checkpoint(),
        log.log_end_offset(),
        last_fetched_epoch,
    );
    if end.epoch == last_fetched_epoch && end.offset >= fetch_offset.0 {
        return None;
    }
    // Checkpoint epochs are `i32`s, so `end.epoch`, which is one of them or
    // the requested epoch that came from an `i32`, always fits.
    let epoch = i32::try_from(end.epoch).unwrap_or(i32::MAX);
    Some((LeaderEpoch(epoch), Offset(end.offset)))
}

#[cfg(test)]
fn replica_end_offset(replica: &WalReplica) -> Offset {
    Offset(replica.log.end_offset())
}

fn replica_start_offset(replica: &WalReplica) -> Offset {
    replica.log.lock().log_start_offset()
}

#[derive(Debug, Clone)]
pub(super) struct BatchBytes {
    pub(super) base_offset: Offset,
    pub(super) last_offset: Offset,
    pub(super) verbatim: VerbatimBatch,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_log::LogConfig;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;
    use crate::wal::quorum::test_support::batch;

    fn configured_engine(path: &Path) -> WalShardEngine {
        let source = crate::wal::quorum::test_support::open_log(path);
        crate::wal::quorum::test_support::distributed_engine(
            &source,
            3,
            &[NodeId(1), NodeId(2), NodeId(3)],
        )
    }

    fn log_with_records(path: &Path, records: usize) -> Arc<Mutex<Log>> {
        let mut log = Log::open(path, LogConfig::default()).unwrap();
        for _ in 0..records {
            log.append(&mut batch(1)).unwrap();
        }
        log.sync().unwrap();
        Arc::new(Mutex::new(log))
    }

    fn log_with_values(path: &Path, values: &[&'static [u8]]) -> Arc<Mutex<Log>> {
        let mut log = Log::open(path, LogConfig::default()).unwrap();
        for value in values {
            log.append(&mut RecordBatch {
                records: vec![Record {
                    value: Some(Bytes::from_static(value)),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            })
            .unwrap();
        }
        log.sync().unwrap();
        Arc::new(Mutex::new(log))
    }

    #[test]
    fn distributed_watermark_honors_membership_and_log_start_floor() {
        for (name, from, offset, expected, remembered) in [
            ("non-voter", NodeId(9), 10, 0, None),
            ("below log start", NodeId(2), 4, 5, None),
            ("at log start", NodeId(2), 5, 5, None),
            // One voter past the log start is not a majority: the leader
            // votes with its own durable offset, still the log start.
            ("past log start", NodeId(2), 6, 5, Some(6)),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let engine = configured_engine(dir.path());

            let advanced =
                engine.record_durable_offset(from, Offset(offset), Offset(5), Offset(10));
            let stored = engine
                .distributed
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .durable_offsets
                .get(&from)
                .copied();

            assert!(advanced == (expected > 0), "case {name}");
            assert!(
                engine.durable_watermark() == Offset(expected),
                "case {name}"
            );
            assert!(stored == remembered.map(Offset), "case {name}");
        }
    }

    #[test]
    fn distributed_reconfiguration_keeps_only_current_voter_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let engine = configured_engine(dir.path());
        engine.record_durable_offset(NodeId(2), Offset(8), Offset(0), Offset(10));
        engine.record_durable_offset(NodeId(3), Offset(7), Offset(0), Offset(10));

        engine.configure_distributed(NodeId(1), &[NodeId(1), NodeId(3), NodeId(4)]);

        let distributed = engine.distributed.lock().unwrap();
        let quorum = distributed.as_ref().unwrap();
        assert!(quorum.voters == [NodeId(1), NodeId(3), NodeId(4)]);
        assert!(!quorum.durable_offsets.contains_key(&NodeId(2)));
        assert!(quorum.durable_offsets.get(&NodeId(3)) == Some(&Offset(7)));
        assert!(!quorum.durable_offsets.contains_key(&NodeId(4)));
    }

    #[test]
    fn adopted_prefix_must_be_inside_its_log_bounds() {
        for (name, durable, expected) in [
            ("below start", 4, 0),
            ("at start", 5, 5),
            ("at end", 10, 10),
            ("past end", 11, 0),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let engine = configured_engine(dir.path());

            engine.adopt_local_durable_prefix(Offset(durable), Offset(5), Offset(10));

            assert!(
                engine.local_durable.load(Ordering::Acquire) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn recovery_uses_the_majority_prefix_and_repairs_every_replica() {
        for (ends, expected) in [([5, 3, 1], 3), ([5, 5, 1], 5)] {
            let dir = tempfile::tempdir().unwrap();
            let replicas = ends
                .into_iter()
                .enumerate()
                .map(|(id, end)| {
                    WalReplica::for_test(
                        NodeId(u64::try_from(id).unwrap()),
                        log_with_records(&dir.path().join(format!("replica-{id}")), end),
                    )
                })
                .collect();

            let engine = WalShardEngine::new(replicas, OpenMode::Recover).unwrap();

            assert!(engine.durable_watermark() == Offset(expected));
            assert!(engine.replica_end_offsets() == vec![Offset(expected); 3]);
        }
    }

    #[test]
    fn recovery_uses_byte_agreement_not_equal_length_order() {
        let dir = tempfile::tempdir().unwrap();
        let majority_a =
            log_with_values(&dir.path().join("majority-a"), &[b"common", b"committed"]);
        let majority_b =
            log_with_values(&dir.path().join("majority-b"), &[b"common", b"committed"]);
        // Put the equal-length minority last. Length-only max selection used
        // to choose this replica and overwrite the committed majority.
        let minority = log_with_values(&dir.path().join("minority"), &[b"common", b"stale"]);
        let committed = majority_a
            .lock()
            .unwrap()
            .read_raw(Offset(0), Offset(2), ByteSize::from_bytes(u64::MAX))
            .unwrap()
            .bytes;
        let replicas = vec![
            WalReplica::for_test(NodeId(1), majority_a),
            WalReplica::for_test(NodeId(2), majority_b),
            WalReplica::for_test(NodeId(3), minority),
        ];

        let engine = WalShardEngine::new(replicas, OpenMode::Recover).unwrap();

        assert!(engine.durable_watermark() == Offset(2));
        for replica in &engine.replicas {
            let recovered = replica
                .log
                .lock()
                .read_raw(Offset(0), Offset(2), ByteSize::from_bytes(u64::MAX))
                .unwrap()
                .bytes;
            assert!(recovered == committed);
        }
    }

    /// Equal lengths are not agreement. Recovery keeps the longest prefix a
    /// byte-identical majority holds, drops the rest as unacknowledged, and
    /// opens rather than refusing.
    #[test]
    fn recovery_keeps_the_longest_byte_identical_majority_prefix() {
        let three_ways: [&[&'static [u8]]; 3] = [&[b"a"], &[b"b"], &[b"c"]];
        let split_tail: [&[&'static [u8]]; 3] = [&[b"a", b"x"], &[b"a", b"y"], &[b"b"]];
        for (name, values, expected) in [
            ("three-way disagreement", three_ways, 0),
            ("majority agrees only on the first record", split_tail, 1),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let replicas = values
                .into_iter()
                .enumerate()
                .map(|(index, values)| {
                    WalReplica::for_test(
                        NodeId(u64::try_from(index).unwrap()),
                        log_with_values(&dir.path().join(format!("replica-{index}")), values),
                    )
                })
                .collect();

            let engine = WalShardEngine::new(replicas, OpenMode::Recover).unwrap();

            assert!(
                engine.durable_watermark() == Offset(expected),
                "case {name}"
            );
            assert!(
                engine.replica_end_offsets() == vec![Offset(expected); 3],
                "case {name}"
            );
        }
    }

    /// A live replica that disagrees with the leader is not a synced copy,
    /// whether it disagrees past the committed offset or below it.
    #[tokio::test]
    async fn acknowledgement_rejects_a_divergent_replica() {
        type Values = &'static [&'static [u8]];
        let cases: [(&str, Values, Values, Values, i64); 2] = [
            (
                "equal-length divergence past the committed offset",
                &[b"leader"],
                &[b"stale"],
                &[],
                0,
            ),
            (
                "divergence below the committed offset",
                &[b"committed", b"next"],
                &[b"stale"],
                &[b"committed"],
                1,
            ),
        ];
        for (name, leader_values, divergent_values, unavailable_values, committed) in cases {
            let dir = tempfile::tempdir().unwrap();
            let leader = log_with_values(&dir.path().join("leader"), leader_values);
            let divergent = log_with_values(&dir.path().join("divergent"), divergent_values);
            let unavailable = log_with_values(&dir.path().join("unavailable"), unavailable_values);
            let target = Offset(leader.lock().unwrap().log_end_offset().0);
            let replicas = vec![
                WalReplica::for_test(NodeId(1), Arc::clone(&leader)),
                WalReplica::for_test(NodeId(2), divergent),
                WalReplica::for_test(NodeId(3), unavailable),
            ];
            let engine = WalShardEngine::for_model(replicas, Offset(committed));
            engine.set_replica_alive(NodeId(3), false);

            let error = engine
                .replicate_and_sync(&leader, target)
                .await
                .unwrap_err();

            assert!(
                error.to_string().contains("has 1 synced replicas"),
                "case {name}"
            );
            assert!(
                engine.durable_watermark() == Offset(committed),
                "case {name}"
            );
        }
    }

    #[test]
    fn batch_parsing_rejects_invalid_offsets_and_inexact_ranges() {
        let negative_delta = RecordBatch {
            last_offset_delta: -1,
            ..RecordBatch::default()
        };
        let mut overflowing_offset = batch(2);
        overflowing_offset.base_offset = i64::MAX;
        let mut negative_wire = BytesMut::new();
        negative_delta.encode(&mut negative_wire).unwrap();
        let mut overflowing_wire = BytesMut::new();
        overflowing_offset.encode(&mut overflowing_wire).unwrap();

        for (name, wire) in [
            ("truncated batch", Bytes::from_static(&[0])),
            ("negative offset delta", negative_wire.freeze()),
            ("offset overflow", overflowing_wire.freeze()),
        ] {
            assert!(split_batches(&wire).is_err(), "case {name}");
        }

        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut batch(3)).unwrap();
        for (name, start, target) in [
            ("starts inside a batch", 1, 3),
            ("ends inside a batch", 0, 2),
        ] {
            assert!(
                read_log_batches_exact(&log, Offset(start), Offset(target)).is_err(),
                "case {name}"
            );
        }
    }

    #[test]
    fn exact_batch_range_rejects_internal_gaps_overlaps_and_reordering() {
        fn exact(ranges: &[(i64, i32)], start: i64, target: i64) -> bool {
            let mut wire = BytesMut::new();
            for &(base_offset, records) in ranges {
                let mut record_batch = batch(records);
                record_batch.base_offset = base_offset;
                record_batch.encode(&mut wire).expect("encode test batch");
            }
            let decoded = split_batches(&wire.freeze()).expect("decode test batches");
            batches::exact_batches(decoded, Offset(start), Offset(target)).is_ok()
        }

        assert!(exact(&[], 4, 4));
        assert!(exact(&[(4, 2), (6, 3)], 4, 9));
        assert!(!exact(&[(4, 2), (7, 2)], 4, 9));
        assert!(!exact(&[(4, 2), (5, 4)], 4, 9));
        assert!(!exact(&[(6, 3), (4, 2)], 4, 9));
        assert!(!exact(&[(i64::MAX, 1)], i64::MAX, i64::MAX));
        assert!(exact(&[(i64::MAX - 1, 1)], i64::MAX - 1, i64::MAX));
    }
}
