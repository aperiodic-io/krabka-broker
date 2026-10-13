//! Per-partition gating and read planning: the authorization, witness,
//! leader-epoch and log-directory checks a requested partition passes
//! before anything reads it, and the [`PendingRead`] each requested tuple
//! resolves to.

use {
    super::request::{EffectivePartition, EffectiveTopic, FetchAuthorization},
    crate::{
        broker::Broker,
        codes,
        config_keys::resolve_broker_witness,
        partition::{Partition, ReplicationTarget},
        replica_selector::ReplicaView,
        topic_resolve::resolve,
    },
    krabka_log::{LeaderEpoch, Offset},
    krabka_protocol::{
        owned::fetch_response::{EpochEndOffset, LeaderIdAndEpoch, PartitionData},
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

/// Resolved read for a single requested (topic, partition) tuple.
///
/// The handler keeps it so that it can read again after a long-poll wake.
pub(crate) struct PendingRead {
    pub(crate) topic_name: String,
    pub(crate) topic_id: WireUuid,
    pub(crate) partition_index: i32,
    /// Epoch the follower supplied with the original Fetch request.
    pub(crate) current_leader_epoch: i32,
    /// Epoch of the follower's last fetched record.
    pub(crate) last_fetched_epoch: i32,
    pub(crate) fetch_offset: i64,
    pub(crate) max_bytes: i32,
    /// `true` when `isolation_level == 1` on a consumer fetch, and not on a
    /// follower fetch. It causes batch-level LSO filtering and fills
    /// `aborted_transactions` in the response.
    pub(crate) read_committed: bool,
    /// `true` when `replica_id >= 0`, that is, when the request comes from a
    /// follower replicator and not from a consumer. Follower fetches see all
    /// records up to LEO and report the leader's HW and LSO, as consumer
    /// fetches do. The handler clamps consumer fetches at HW.
    pub(crate) is_follower_fetch: bool,
    /// `true` when only the partition leader may serve this fetch. Kafka's
    /// `FetchParams.fetchOnlyLeader` is true for a follower fetch, and for a
    /// consumer fetch that carries no client metadata, which is every
    /// consumer fetch below v11. A long-poll wake checks leadership again.
    pub(crate) fetch_only_leader: bool,
    /// What the leader records of the follower once the first read of this
    /// row succeeds. `None` on a consumer fetch, and on a row that is never
    /// read. See [`record_follower_position`].
    pub(crate) follower_position: Option<FollowerPosition>,
    /// `None` for an unknown topic or partition, or for an out-of-range
    /// offset. The final response is already complete, and the handler does
    /// not read it again on a wake.
    pub(crate) partition: Option<Arc<Partition>>,
    /// Per-partition output. `do_read` mutates it in place.
    pub(crate) out: PartitionData,
    /// Accumulator for the microseconds spent in this partition's `do_read`
    /// calls. It covers the first pass and every long-poll re-read. The
    /// handler measures an `Instant` elapsed delta around each `do_read`. The
    /// heavy byte read runs in `spawn_blocking`, so this charges the read work
    /// and allocates no `tokio_metrics::TaskMonitor` per partition per fetch.
    /// The response-emit loop drains it into its
    /// `record_partition_cpu_micros` call.
    pub(crate) cpu_micros: u64,
}

impl PendingRead {
    pub(super) fn planned(
        topic_name: &str,
        topic_id: WireUuid,
        partition: &EffectivePartition,
        mode: FetchMode,
        resolved: Option<Arc<Partition>>,
        out: PartitionData,
    ) -> Self {
        Self {
            topic_name: topic_name.to_owned(),
            topic_id,
            partition_index: partition.partition,
            current_leader_epoch: partition.current_leader_epoch,
            last_fetched_epoch: partition.last_fetched_epoch,
            fetch_offset: partition.fetch_offset,
            max_bytes: partition.partition_max_bytes,
            read_committed: mode.read_committed,
            is_follower_fetch: mode.is_follower_fetch,
            fetch_only_leader: mode.is_follower_fetch,
            follower_position: None,
            partition: resolved,
            out,
            cpu_micros: 0,
        }
    }
}

/// The follower state that a follower fetch reports for one partition, kept
/// until the first read of that partition runs.
///
/// Kafka's `Partition.fetchRecords` reads the follower's records first and
/// only then calls `updateFollowerFetchState`. So a follower fetch answered by
/// that read carries the high watermark from before its own fetch offset
/// counted toward it, and the follower learns the watermark its fetch moved
/// from its next response. Only a fetch that parks reads again after the
/// update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FollowerPosition {
    pub(crate) follower_id: i32,
    /// The broker epoch the follower's Fetch carried. KIP-841 compares it
    /// with the broker's registered epoch before the follower may rejoin the
    /// ISR.
    pub(crate) broker_epoch: i64,
    /// The follower's own log start offset, which a `DeleteRecords` waits for.
    pub(crate) log_start_offset: i64,
    /// The replication target that the leader checks of this fetch passed
    /// under. The leader records the position only while this target is
    /// still installed.
    pub(crate) target: ReplicationTarget,
}

/// Kafka's `Partition.updateFollowerFetchState`: record that `position`'s
/// follower holds every record below `fetch_offset`, and recompute the high
/// watermark. `leader_log_end` is the leader's log end offset when the read
/// started, as Kafka passes `LogReadInfo.logEndOffset`. Returns whether the
/// high watermark advanced.
///
/// The caller runs this after the follower's read succeeded. Kafka records
/// nothing for a read that failed or found a diverging epoch, because
/// `fetchRecords` throws or returns before the update.
///
/// A leadership change between the leader checks and this update installs a
/// new replication target and clears the follower state. Kafka's
/// `maybeIncrementLeaderHW` runs under `leaderIsrUpdateLock` only while this
/// broker still leads. This method also records nothing after the target
/// changed.
pub(crate) async fn record_follower_position(
    partition: &Partition,
    position: FollowerPosition,
    fetch_offset: i64,
    leader_log_end: Offset,
) -> bool {
    let transition = partition.lock_produce_transition().await;
    if *transition != position.target {
        return false;
    }
    let follower = krabka_metadata::NodeId(u64::try_from(position.follower_id).unwrap_or(0));
    let advanced = {
        let mut state = partition.replica_state.lock().await;
        let previous = state.hw;
        // Kafka's `Replica.updateFetchStateOrThrow` records the follower's log
        // start offset with its fetch offset. A `DeleteRecords` waits for it.
        state.record_follower_log_start(follower, Offset(position.log_start_offset));
        // KIP-841: the ISR-expansion eligibility check compares this with the
        // broker's registered epoch before `update_follower_leo` reads it.
        state.record_follower_broker_epoch(follower, position.broker_epoch);
        state.update_follower_leo(
            follower,
            Offset(fetch_offset),
            leader_log_end,
            std::time::Instant::now(),
        ) > previous
    };
    drop(transition);
    if advanced {
        partition.hw_advance_notify.notify_waiters();
    }
    advanced
}

/// Chooses a preferred read replica, or `-1` for "read from the leader".
///
/// Kafka's `ReplicaManager.findPreferredReadReplica` only offers a candidate
/// whose reported range can serve `fetch_offset`: `logEndOffset >=
/// fetchOffset` and `logStartOffset <= fetchOffset`. Without that check a
/// consumer can be redirected to an in-ISR replica that has not replicated
/// far enough yet, or has already trimmed the offset away, and the follower
/// answers it `OFFSET_OUT_OF_RANGE` in the redirect's place.
async fn preferred_read_replica(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    partition: &Partition,
    topic: &str,
    partition_index: i32,
    rack_id: &str,
    fetch_offset: Offset,
) -> i32 {
    if rack_id.is_empty() {
        return -1;
    }
    let Some(record) = image.partition(topic, partition_index) else {
        return -1;
    };
    let isr: std::collections::HashSet<krabka_metadata::NodeId> =
        record.isr.iter().copied().collect();
    // The leader's own view is its log end and zero time since caught up, as
    // Kafka builds it. Read before the replica state lock, so the log's
    // mutex is never taken under it.
    let leader_log_end = partition.log_end_offset();
    let now = std::time::Instant::now();
    let replicas: Vec<ReplicaView> = {
        let state = partition.replica_state.lock().await;
        record
            .replicas
            .iter()
            .filter(|&&node_id| {
                node_id == record.leader || state.follower_can_serve(node_id, fetch_offset)
            })
            .map(|&node_id| {
                let (log_end_offset, last_caught_up) = if node_id == record.leader {
                    (leader_log_end, Some(now))
                } else {
                    state.follower_progress(node_id)
                };
                ReplicaView {
                    node_id: i32::try_from(node_id.0).unwrap_or(-1),
                    rack: image.broker(node_id).and_then(|broker| broker.rack.clone()),
                    in_isr: isr.contains(&node_id),
                    is_witness: resolve_broker_witness(image, node_id),
                    log_end_offset: log_end_offset.0,
                    time_since_caught_up: last_caught_up.map_or(std::time::Duration::MAX, |at| {
                        now.saturating_duration_since(at)
                    }),
                }
            })
            .collect()
    };
    broker.config.replica_selector.select(
        Some(rack_id),
        i32::try_from(record.leader.0).unwrap_or(-1),
        &replicas,
    )
}

/// The live bounds of a partition's offsets.
struct OffsetSnapshot {
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
}

/// The response Kafka's `ReplicaManager.readFromLog` gives a fetch it never
/// reads: empty records at the offset snapshot's live bounds.
///
/// `ReplicaManager.fetchMessages` skips the log read entirely once it has
/// named a preferred read replica (`hasPreferredReadReplica` in
/// `maybeReadFromLocalLog` and again at `tryComplete`), and answers with this
/// snapshot rather than parking in the fetch purgatory -- a consumer told to
/// go elsewhere should not also wait out `fetch.max.wait.ms` on this broker
/// first.
async fn preferred_replica_snapshot(partition: &Partition) -> OffsetSnapshot {
    let hw = partition.high_watermark().await;
    let mut log = partition.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    let last_stable_offset = log.last_stable_offset(hw);
    OffsetSnapshot {
        high_watermark: hw.0,
        last_stable_offset: last_stable_offset.0,
        log_start_offset: log_start.0,
    }
}

/// The node that must lead a partition before a fetch may read it, or `None`
/// when any local replica may serve the fetch.
///
/// Kafka's `Partition.localLogWithEpochOrThrow` requires the leader when
/// `FetchParams.fetchOnlyLeader` is true. A diskless partition has no single
/// leader replica, so it keeps the path it has.
pub(super) fn required_leader(
    fetch_only_leader: bool,
    node_id: krabka_metadata::NodeId,
    partition: &Partition,
) -> Option<krabka_metadata::NodeId> {
    (fetch_only_leader && !partition.diskless).then_some(node_id)
}

/// The partition row that Kafka's `LogReadResult(Errors)` gives a read that
/// `ReplicaManager.readFromLog` refuses: every offset is -1, the records are
/// empty, and there are no aborted transactions.
pub(super) fn refused_read(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(krabka_protocol::records::RecordsPayload::Raw(
            bytes::Bytes::new(),
        )),
        ..Default::default()
    }
}

/// The leader the image names for a partition, as a KIP-951 `CurrentLeader`.
fn image_leader(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
) -> LeaderIdAndEpoch {
    image
        .partition(topic, partition_index)
        .map_or_else(LeaderIdAndEpoch::default, |record| LeaderIdAndEpoch {
            leader_id: i32::try_from(record.leader.0).unwrap_or(-1),
            leader_epoch: record.leader_epoch.0,
            ..Default::default()
        })
}

/// A partition named by topic and index.
#[derive(Clone, Copy)]
pub(super) struct TopicPartitionRef<'a> {
    pub(super) topic: &'a str,
    pub(super) partition_index: i32,
}

/// The refused row when `required_leader` names this node and this node does
/// not lead the partition, or `None` when the read may go on.
///
/// The partition's installed local role must name this node, and the metadata
/// image must not name another leader. The image can name a new leader before
/// the supervisor installs the new role, and the installed role can lag the
/// other way while a promotion prepares the log. Kafka checks the one
/// partition state that its metadata publisher updates; this broker has two,
/// so it checks both.
pub(super) fn leader_refusal(
    image: &krabka_metadata::MetadataImage,
    TopicPartitionRef {
        topic,
        partition_index,
    }: TopicPartitionRef<'_>,
    partition: &Partition,
    required_leader: Option<krabka_metadata::NodeId>,
) -> Option<PartitionData> {
    let node_id = required_leader?;
    let installed = partition
        .current_leader
        .load(std::sync::atomic::Ordering::Acquire)
        == node_id.0;
    let committed_elsewhere = image
        .partition(topic, partition_index)
        .is_some_and(|record| record.leader != node_id);
    (!installed || committed_elsewhere).then(|| PartitionData {
        current_leader: image_leader(image, topic, partition_index),
        ..refused_read(partition_index, codes::NOT_LEADER_OR_FOLLOWER)
    })
}

/// What a fetch may read from one local partition.
#[derive(Clone, Copy)]
pub(super) struct ReadRole<'a> {
    pub(super) partition: &'a Partition,
    /// The node that must lead the partition. See [`required_leader`].
    pub(super) required_leader: Option<krabka_metadata::NodeId>,
    /// `false` for a follower fetch whose replica id is not a follower in the
    /// partition's assignment. See [`is_assigned_follower`].
    pub(super) assigned_follower: bool,
    /// Whether this partition's log directory is currently marked offline.
    /// The below-log-start check in [`apply_epoch_checks`] defers to the
    /// caller's own offline gate instead of racing to answer
    /// `OFFSET_OUT_OF_RANGE` first: an offline directory can make
    /// `Log::log_start_offset` unreadable or stale, and a client or follower
    /// told to reset its offset in response to what is really a storage
    /// failure would discard state it should have kept.
    pub(super) log_dir_offline: bool,
}

/// Kafka's order in `Partition.fetchRecords`: the leader-epoch fence, the
/// leader check, the follower replica check, and then the diverging epoch of
/// the read. Returns `true` when `output` is final.
pub(super) async fn apply_epoch_checks(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    request: &EffectivePartition,
    role: ReadRole<'_>,
    output: &mut PartitionData,
) -> bool {
    let ReadRole {
        partition,
        required_leader,
        assigned_follower,
        log_dir_offline,
    } = role;
    if let Some((error_code, current_epoch)) =
        partition.fetch_leader_epoch_fence(request.current_leader_epoch)
    {
        // Kafka's `FetchResponse.partitionResponse` gives this row the -1
        // sentinels of a refused read, same as `leader_refusal` below it: the
        // fence trips before the log is ever touched. `CurrentLeader` is
        // filled for `FENCED_LEADER_EPOCH` (the client is ahead) and for
        // `NOT_LEADER_OR_FOLLOWER`, but not for `UNKNOWN_LEADER_EPOCH` (the
        // client is behind and has nothing new to learn from it).
        *output = PartitionData {
            current_leader: if error_code == codes::FENCED_LEADER_EPOCH {
                LeaderIdAndEpoch {
                    leader_id: image
                        .partition(topic, partition_index)
                        .map_or(-1, |record| i32::try_from(record.leader.0).unwrap_or(-1)),
                    leader_epoch: current_epoch,
                    ..Default::default()
                }
            } else {
                LeaderIdAndEpoch::default()
            },
            ..refused_read(partition_index, error_code)
        };
        return true;
    }
    if let Some(refused) = leader_refusal(
        image,
        TopicPartitionRef {
            topic,
            partition_index,
        },
        partition,
        required_leader,
    ) {
        *output = refused;
        return true;
    }
    if !assigned_follower {
        // Kafka's `Partition.followerReplicaOrThrow`: a fetch that carries a
        // leader epoch gets UNKNOWN_LEADER_EPOCH, and one without gets
        // NOT_LEADER_OR_FOLLOWER. Either way it moves no follower state.
        *output = if request.current_leader_epoch >= 0 {
            refused_read(partition_index, codes::UNKNOWN_LEADER_EPOCH)
        } else {
            PartitionData {
                current_leader: image_leader(image, topic, partition_index),
                ..refused_read(partition_index, codes::NOT_LEADER_OR_FOLLOWER)
            }
        };
        return true;
    }
    if request.last_fetched_epoch < 0 {
        return false;
    }
    // The live bounds a diverging-epoch row reports: the partition's real
    // high watermark, `min(LSO, HW)` and log start, as every Fetch row does.
    let high_watermark = partition.high_watermark().await;
    let (log_start_offset, established_log_start, found_epoch, end_offset, last_stable_offset) = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        let (found_epoch, end_offset) = log.epoch_checkpoint().epoch_and_offset_for(
            LeaderEpoch(request.last_fetched_epoch),
            log.log_end_offset(),
        );
        let last_stable_offset = log.last_stable_offset(high_watermark).min(high_watermark);
        (
            log.log_start_offset(),
            log.established_log_start(),
            found_epoch,
            end_offset,
            last_stable_offset,
        )
    };
    // Kafka's `Partition.readRecords`: an epoch this log cannot place --
    // `LeaderEpochFileCache.endOffsetFor` answered `(UNDEFINED_EPOCH,
    // UNDEFINED_EPOCH_OFFSET)` for an empty history or a `last_fetched_epoch`
    // above every recorded epoch -- is `OFFSET_OUT_OF_RANGE`, ahead of the
    // log-start check. `ReplicaManager.readFromLog` answers that exception
    // with `LogReadResult(Errors)`, whose offsets are all -1: exactly
    // `refused_read`. The offline-directory deferral below applies here too.
    if found_epoch == LeaderEpoch::UNKNOWN || end_offset == Offset(-1) {
        if log_dir_offline {
            return false;
        }
        *output = refused_read(partition_index, codes::OFFSET_OUT_OF_RANGE);
        return true;
    }
    // Kafka's `Partition.readRecords`: a fetch offset below the log start is
    // `OFFSET_OUT_OF_RANGE` whatever the epochs say, checked before any
    // divergence row is built. Only an *established*
    // floor counts: `Log::open` also infers a floor from whatever segments are
    // left on disk, and on a tiered partition reopened with its local segments
    // evicted that inferred floor sits above everything the remote tier still
    // holds. Refusing on the strength of it would hide readable records
    // instead of letting the request fall through to the remote-tier read
    // still ahead of it -- so this only fires once something has actually
    // moved the global floor. An offline log directory takes precedence: it
    // can make the floor unreadable or stale, and resetting a client or
    // follower in response to a storage failure would discard state it
    // should have kept, so this defers to the caller's own offline gate
    // instead of racing it. `Partition.readRecords` throws
    // `OffsetOutOfRangeException` here, and the non-tiered branch of
    // `ReplicaManager.handleOffsetOutOfRangeError` answers it with
    // `LogReadResult(Errors)`, whose offsets are all -1: `refused_read`. A
    // follower learns the leader's bounds from `ListOffsets` instead
    // (`AbstractFetcherThread.fetchOffsetAndTruncate`).
    if let Some(established) = established_log_start
        && request.fetch_offset < established.0
    {
        if log_dir_offline {
            return false;
        }
        *output = refused_read(partition_index, codes::OFFSET_OUT_OF_RANGE);
        return true;
    }
    // Kafka's divergence test: the leader's epoch for the follower's range is
    // older than `last_fetched_epoch`, or that epoch ends before the fetch
    // offset. The row carries the lookup's pair verbatim.
    if found_epoch.0 >= request.last_fetched_epoch && end_offset.0 >= request.fetch_offset {
        return false;
    }
    // Kafka's `Partition.readRecords` fills a diverging-epoch row with the
    // partition's live bounds (`initialHighWatermark`, `initialLogStartOffset`
    // and `initialLastStableOffset`), the same values this check already read
    // under the log mutex above, not the zero defaults the row started from.
    output.error_code = codes::NONE;
    output.high_watermark = high_watermark.0;
    output.last_stable_offset = last_stable_offset.0;
    output.log_start_offset = log_start_offset.0;
    output.diverging_epoch = EpochEndOffset {
        epoch: found_epoch.0,
        end_offset: end_offset.0,
        ..Default::default()
    };
    true
}

/// The partition row that Kafka's `FetchResponse.partitionResponse` builds.
///
/// `KafkaApis.handleFetchRequest` uses it for a row that it refuses before the
/// read: `UNKNOWN_TOPIC_ID`, `TOPIC_AUTHORIZATION_FAILED` and
/// `UNKNOWN_TOPIC_OR_PARTITION`. Every offset is -1. `partitionResponse` never
/// sets the aborted transactions, so they keep the generated default: the
/// schema gives `AbortedTransactions` no `"default": "null"`, which makes it
/// an empty list, not a null one. A row refused by the read itself
/// ([`refused_read`]) is the one that carries null.
pub(super) fn refused_partition(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        ..Default::default()
    }
}

/// How a fetch reads: with the read-committed isolation level, and as a
/// follower fetch.
#[derive(Clone, Copy)]
pub(super) struct FetchMode {
    pub(super) read_committed: bool,
    pub(super) is_follower_fetch: bool,
}

pub(super) struct PendingPlanContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a krabka_metadata::MetadataImage,
    pub(super) authorization: &'a FetchAuthorization,
    pub(super) rack_id: &'a str,
    /// The negotiated `Fetch` version. It decides whether a topic row names
    /// its topic by name or by id.
    pub(super) version: i16,
    pub(super) mode: FetchMode,
    pub(super) follower_id: i32,
    /// The broker epoch the follower's Fetch carried in
    /// `ReplicaState.ReplicaEpoch` (-1 when absent), Kafka's
    /// `FetchParams.replicaEpoch`.
    pub(super) follower_broker_epoch: i64,
}

/// Where a planned row goes in the response.
///
/// Kafka's `KafkaApis.handleFetchRequest` collects the rows it refuses before
/// the read in its `erroneous` buffer and puts them after the rows
/// `ReplicaManager` read. A consumer fetch fills that buffer in two passes:
/// the rows whose topic id does not resolve first, then the rows refused for
/// authorization or for a partition the metadata does not hold. A follower
/// fetch fills it in one pass. Within a slot the rows keep request order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ResponseSlot {
    /// A row the read decided, refusals of the read itself included.
    Read,
    /// A row refused before the read, in the first pass.
    RefusedFirst,
    /// A row a consumer fetch refused before the read, in the second pass.
    RefusedSecond,
}

impl ResponseSlot {
    /// The slot of a row refused before the read with `error_code`.
    fn refused(is_follower_fetch: bool, error_code: i16) -> Self {
        if is_follower_fetch || error_code == codes::UNKNOWN_TOPIC_ID {
            Self::RefusedFirst
        } else {
            Self::RefusedSecond
        }
    }
}

/// A planned row and the slot of the response it goes in.
pub(super) struct PlannedRead {
    pub(super) slot: ResponseSlot,
    pub(super) read: PendingRead,
}

pub(super) async fn plan_partition_read(
    context: &PendingPlanContext<'_>,
    topic_name: &str,
    topic_id: WireUuid,
    topic_error: Option<i16>,
    request: &EffectivePartition,
) -> PlannedRead {
    let refused = |error_code| PlannedRead {
        slot: ResponseSlot::refused(context.mode.is_follower_fetch, error_code),
        read: PendingRead::planned(
            topic_name,
            topic_id,
            request,
            context.mode,
            None,
            refused_partition(request.partition, error_code),
        ),
    };
    // Kafka's `KafkaApis.handleFetchRequest` refuses every row of a follower
    // fetch without `ClusterAction` before it resolves any topic. So that
    // refusal comes first, and the fetch never reaches
    // `record_follower_position`.
    if context.authorization.refuses_every_row() {
        return refused(codes::TOPIC_AUTHORIZATION_FAILED);
    }
    // A topic that does not resolve is answered before the consumer `Read`
    // gate, on the consumer path and on the follower path.
    if let Some(error_code) = topic_error {
        return refused(error_code);
    }
    if context.authorization.refuses_topic(topic_name) {
        return refused(codes::TOPIC_AUTHORIZATION_FAILED);
    }
    // `finish_read` fills `aborted_transactions` for a `read_committed`
    // consumer. Every other read leaves it null, as Kafka's `LocalLog.read`
    // returns no aborted list outside `TXN_COMMITTED` isolation.
    let mut output = PartitionData {
        partition_index: request.partition,
        aborted_transactions: None,
        ..Default::default()
    };
    // A witness replicates the partition and counts toward
    // `min.insync.replicas`, but it serves no client traffic. A consumer that
    // reaches one gets NOT_LEADER_OR_FOLLOWER, the partition-level code that
    // makes a Kafka client refresh its metadata and read somewhere else. A
    // FOLLOWER fetch passes: replication is the reason the witness holds the
    // data at all. The check sits below the two topic gates, so an
    // authorization failure still wins, and it sits above the partition
    // lookup, so a witness answers a consumer the same way whether or not it
    // hosts the partition.
    if !context.mode.is_follower_fetch && context.broker.config.is_witness() {
        // This row never touches the log, so it carries the -1 sentinels of
        // a refused read, like `leader_refusal` below it.
        output = refused_read(request.partition, codes::NOT_LEADER_OR_FOLLOWER);
        // KIP-951: name the leader the consumer should go to instead. Kafka
        // fills `CurrentLeader` on every NOT_LEADER_OR_FOLLOWER row of a v16+
        // Fetch response, and the response's `NodeEndpoints` then carries that
        // node's address, so the consumer re-targets without a full Metadata
        // round-trip. A witness never leads, so the image's leader is always
        // some other node.
        if let Some(record) = context.image.partition(topic_name, request.partition) {
            output.current_leader = LeaderIdAndEpoch {
                leader_id: i32::try_from(record.leader.0).unwrap_or(-1),
                leader_epoch: record.leader_epoch.0,
                ..Default::default()
            };
        }
        return PlannedRead {
            slot: ResponseSlot::Read,
            read: PendingRead::planned(topic_name, topic_id, request, context.mode, None, output),
        };
    }
    let partition = context
        .broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(request.partition));
    // Kafka's `FetchParams.fetchOnlyLeader`: a follower fetch, or a consumer
    // fetch below v11, which has no client metadata.
    let fetch_only_leader =
        context.mode.is_follower_fetch || context.version < FIRST_CLIENT_METADATA_VERSION;
    let node_id = context.broker.config.node_id;
    // A follower fetch holds the partition's replication-target read guard
    // through the leader checks, as Kafka's `Partition.fetchRecords` holds
    // `leaderIsrUpdateLock`. A leadership change takes the write guard, so it
    // cannot land in between. The read loop records the follower position
    // after the read, and only while the target that the checks saw is still
    // installed.
    let transition = match partition.as_ref() {
        Some(partition) if context.mode.is_follower_fetch => {
            Some(partition.lock_produce_transition().await)
        }
        _ => None,
    };
    let follower_position = transition.as_deref().map(|target| FollowerPosition {
        follower_id: context.follower_id,
        broker_epoch: context.follower_broker_epoch,
        log_start_offset: request.log_start_offset,
        target: *target,
    });
    if let Some(partition) = partition.as_ref()
        && apply_epoch_checks(
            context.image,
            topic_name,
            request.partition,
            request,
            ReadRole {
                partition,
                required_leader: required_leader(fetch_only_leader, node_id, partition),
                assigned_follower: !context.mode.is_follower_fetch
                    || partition.diskless
                    || is_assigned_follower(
                        context.image,
                        topic_name,
                        request.partition,
                        context.follower_id,
                        node_id,
                    ),
                log_dir_offline: context
                    .broker
                    .log_dir_status
                    .is_offline(&partition.log_dir.load()),
            },
            &mut output,
        )
        .await
    {
        return PlannedRead {
            slot: ResponseSlot::Read,
            read: PendingRead::planned(topic_name, topic_id, request, context.mode, None, output),
        };
    }
    if let Some(partition) = partition.as_ref()
        && context
            .broker
            .log_dir_status
            .is_offline(&partition.log_dir.load())
    {
        // `KAFKA_STORAGE_ERROR` (56) postdates Fetch v5; a fetcher that old
        // has no code for it, so Kafka's `KafkaApis.handleFetchRequest`
        // down-converts it to `NOT_LEADER_OR_FOLLOWER` for those versions.
        // Either way this row never touches the log, so it carries the -1
        // sentinels of a refused read.
        let error_code = if context.version <= LAST_PRE_STORAGE_ERROR_FETCH_VERSION {
            codes::NOT_LEADER_OR_FOLLOWER
        } else {
            codes::KAFKA_STORAGE_ERROR
        };
        let output = refused_read(request.partition, error_code);
        return PlannedRead {
            slot: ResponseSlot::Read,
            read: PendingRead::planned(topic_name, topic_id, request, context.mode, None, output),
        };
    }
    if partition.is_none() || topic_name.is_empty() {
        // Kafka refuses a partition its metadata does not hold before the
        // read, with `UNKNOWN_TOPIC_OR_PARTITION`.
        let in_metadata = !topic_name.is_empty()
            && context
                .image
                .partition(topic_name, request.partition)
                .is_some();
        if !in_metadata {
            return refused(codes::UNKNOWN_TOPIC_OR_PARTITION);
        }
        // One the metadata holds but this broker does not host, after a
        // reassignment removed the local replica for example, is the read's
        // own refusal: `ReplicaManager.getPartitionOrError` answers
        // `HostedPartition.None` with `NOT_LEADER_OR_FOLLOWER` when
        // `metadataCache.contains` the partition, to make the client refresh
        // its metadata. The row keeps its place among the rows read, and
        // carries the KIP-951 leader hint the way every other
        // `NOT_LEADER_OR_FOLLOWER` row of this fetch does.
        let output = PartitionData {
            current_leader: image_leader(context.image, topic_name, request.partition),
            ..refused_read(request.partition, codes::NOT_LEADER_OR_FOLLOWER)
        };
        return PlannedRead {
            slot: ResponseSlot::Read,
            read: PendingRead::planned(topic_name, topic_id, request, context.mode, None, output),
        };
    }
    // Kafka's `ReplicaManager.findPreferredReadReplica` names a read replica
    // only on the leader.
    let leads = partition.as_ref().is_some_and(|partition| {
        partition
            .current_leader
            .load(std::sync::atomic::Ordering::Acquire)
            == node_id.0
    });
    if !context.mode.is_follower_fetch && leads {
        let leader_partition = partition
            .as_ref()
            .expect("`leads` implies a local partition");
        output.preferred_read_replica = preferred_read_replica(
            context.broker,
            context.image,
            leader_partition,
            topic_name,
            request.partition,
            context.rack_id,
            Offset(request.fetch_offset),
        )
        .await;
        if output.preferred_read_replica >= 0 {
            // Kafka never reads the log for a fetch it is redirecting: it
            // answers at once from the offset snapshot instead of also
            // parking this broker's copy of the request in the fetch
            // purgatory.
            let snapshot = preferred_replica_snapshot(leader_partition).await;
            output.error_code = codes::NONE;
            output.high_watermark = snapshot.high_watermark;
            output.last_stable_offset = snapshot.last_stable_offset;
            output.log_start_offset = snapshot.log_start_offset;
            output.records = Some(krabka_protocol::records::RecordsPayload::Raw(
                bytes::Bytes::new(),
            ));
            return PlannedRead {
                slot: ResponseSlot::Read,
                read: PendingRead {
                    fetch_only_leader,
                    ..PendingRead::planned(
                        topic_name,
                        topic_id,
                        request,
                        context.mode,
                        // No `partition`: the read loop skips both the read and
                        // the long-poll arm for an entry with none.
                        None,
                        output,
                    )
                },
            };
        }
    }
    PlannedRead {
        slot: ResponseSlot::Read,
        read: PendingRead {
            fetch_only_leader,
            follower_position,
            ..PendingRead::planned(
                topic_name,
                topic_id,
                request,
                context.mode,
                partition,
                output,
            )
        },
    }
}

/// The first `Fetch` version that carries client metadata, from which Kafka
/// lets a consumer read from a follower replica (KIP-392).
const FIRST_CLIENT_METADATA_VERSION: i16 = 11;

/// The last `Fetch` version a fetcher could speak before `KAFKA_STORAGE_ERROR`
/// (56) existed. Kafka's `KafkaApis.handleFetchRequest` down-converts that
/// code to `NOT_LEADER_OR_FOLLOWER` at this version and below, because an
/// older fetcher has no case for the code it does not know.
const LAST_PRE_STORAGE_ERROR_FETCH_VERSION: i16 = 5;

/// Whether `follower_id` is a replica of the partition other than this node,
/// as Kafka's `Partition.getReplica` finds it in the remote replicas of the
/// assignment.
fn is_assigned_follower(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    follower_id: i32,
    node_id: krabka_metadata::NodeId,
) -> bool {
    let Ok(follower) = u64::try_from(follower_id).map(krabka_metadata::NodeId) else {
        return false;
    };
    follower != node_id
        && image
            .partition(topic, partition_index)
            .is_some_and(|record| record.replicas.contains(&follower))
}

/// Plan every requested row, and order the plan the way Kafka orders the
/// response: the rows read first, then the rows refused before the read (see
/// [`ResponseSlot`]), each in request order. A refused row reads nothing, so
/// moving it changes no read and no byte budget.
pub(super) async fn build_pending_reads(
    context: &PendingPlanContext<'_>,
    topics: &[EffectiveTopic],
) -> Vec<PendingRead> {
    let mut pending: Vec<PlannedRead> = Vec::new();
    for topic in topics {
        // Versions 12 and earlier name the topic. A name that does not resolve
        // goes on to the `Read` gate and then to the partition gate, which
        // answer TOPIC_AUTHORIZATION_FAILED or UNKNOWN_TOPIC_OR_PARTITION.
        // Version 13 and later name the topic by id only. Kafka's
        // `KafkaApis.handleFetchRequest` resolves the id through
        // `metadataCache.topicIdsToNames()` and answers UNKNOWN_TOPIC_ID on
        // every partition row when that gives no name. The zero id gives no
        // name, and the resolver sends it down the name path with an empty
        // name.
        let (name, id, error) = match resolve(context.image, &topic.topic, topic.topic_id) {
            Ok(record) => (
                record.name.clone(),
                WireUuid(record.topic_id.into_bytes()),
                None,
            ),
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION)
                if context.version < super::FIRST_TOPIC_ID_VERSION =>
            {
                (topic.topic.clone(), topic.topic_id, None)
            }
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => (
                topic.topic.clone(),
                topic.topic_id,
                Some(codes::UNKNOWN_TOPIC_ID),
            ),
            Err(error_code) => (topic.topic.clone(), topic.topic_id, Some(error_code)),
        };
        for partition in &topic.partitions {
            pending.push(plan_partition_read(context, &name, id, error, partition).await);
        }
    }
    pending.sort_by_key(|planned| planned.slot);
    pending.into_iter().map(|planned| planned.read).collect()
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            broker::{Broker, BrokerHandle, spawn_partition},
            codes::{NONE, NOT_LEADER_OR_FOLLOWER, OFFSET_OUT_OF_RANGE},
            config::{BrokerConfig, NodeRole},
            config_keys::{BROKER_WITNESS, WITNESS_TRUE},
            handlers::{
                fetch::test_support::append_epoch_batches,
                produce::test_support::image_with_topic,
                test_support::{StartedBroker, local_partition, start_broker_with},
            },
            partition::Partition,
            producer_state::ProducerState,
            replica_selector::ReplicaSelectorKind,
            test_support::{
                TemporaryPartition, broker_registration, spawn_standalone_partition,
                temporary_partition,
            },
        },
        assert2::assert,
        krabka_ids::PartitionIndex,
        krabka_log::{Log, LogConfig},
        std::sync::Arc,
    };

    /// A three-site image: node 1 leads `orders` from `dc-a`, node 2 replicates
    /// it from `dc-b`, and both are in the ISR. Every node in `witness_ids`
    /// carries `broker.witness=true`, the way a real witness registers.
    fn stretch_image(witness_ids: &[u64]) -> krabka_metadata::MetadataImage {
        use krabka_metadata::{BrokerConfigRecord, BrokerRegistrationRecord, MetadataRecord};

        let mut image = image_with_topic("orders", &[1, 2]);
        for (node_id, rack) in [(1u64, "dc-a"), (2u64, "dc-b")] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
                    rack: Some(rack.into()),
                    ..broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        for &node_id in witness_ids {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_audit::NodeId(node_id),
                config_name: BROKER_WITNESS.into(),
                config_value: Some(WITNESS_TRUE.into()),
            }));
        }
        image
    }

    struct RackAwareFixture {
        // Drop partition/log owners before removing their directory.
        partition: Arc<Partition>,
        broker: Arc<Broker>,
        handle: BrokerHandle,
        _dir: tempfile::TempDir,
    }

    async fn rack_aware_fixture() -> RackAwareFixture {
        let StartedBroker {
            broker: handle,
            dir: directory,
        } = start_broker_with(|config| {
            config.replica_selector = ReplicaSelectorKind::RackAware;
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let partition = local_partition(&broker, directory.path(), "orders");
        RackAwareFixture {
            partition,
            broker,
            handle,
            _dir: directory,
        }
    }

    /// The witness gate refuses a client fetch with the witness row. It lets a
    /// follower fetch through, and the leader check then refuses it with the
    /// read row, because a witness never leads the partition.
    #[tokio::test]
    async fn witness_refuses_a_client_fetch_and_passes_a_follower_fetch_on() {
        const TOPIC: &str = "witness-fetch";

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.roles.push(NodeRole::Witness);
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        broker.partitions.insert(
            TOPIC.into(),
            PartitionIndex(0),
            spawn_partition(
                TOPIC.to_string(),
                PartitionIndex(0),
                dir.path().to_path_buf(),
                Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
                broker.log_dir_status.clone(),
                broker.producer_state.clone(),
                false,
            ),
        );
        let image = broker.controller.current_image();
        let consumer = super::FetchAuthorization::Consumer {
            denied_topics: std::collections::HashSet::new(),
        };
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };

        for (name, is_follower_fetch, follower_id, want) in [
            (
                "client fetch",
                false,
                -1,
                super::refused_read(0, NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "follower fetch",
                true,
                2,
                super::refused_read(0, NOT_LEADER_OR_FOLLOWER),
            ),
        ] {
            let context = super::PendingPlanContext {
                broker: &broker,
                image: &image,
                authorization: if is_follower_fetch {
                    &super::FetchAuthorization::Follower
                } else {
                    &consumer
                },
                rack_id: "",
                version: super::super::FIRST_TOPIC_ID_VERSION,
                mode: super::FetchMode {
                    read_committed: false,
                    is_follower_fetch,
                },
                follower_id,
                follower_broker_epoch: -1,
            };
            let super::PlannedRead { read, .. } =
                super::plan_partition_read(&context, TOPIC, super::WireUuid::ZERO, None, &request)
                    .await;
            assert!(read.out == want, "{name}: got {:?}", read.out);
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn rack_aware_preferred_read_replica_never_names_a_witness() {
        // The consumer sits in `dc-b`, the witness site. Node 2 is the only
        // same-rack in-ISR replica, so it is exactly the redirect a rack-aware
        // selector wants to make.
        let rack = rack_aware_fixture().await;

        for (name, witness_ids, want) in [
            ("node 2 is a plain broker in dc-b", &[][..], 2),
            ("node 2 is the witness in dc-b", &[2u64][..], -1),
        ] {
            let image = stretch_image(witness_ids);
            let got = super::preferred_read_replica(
                &rack.broker,
                &image,
                &rack.partition,
                "orders",
                0,
                "dc-b",
                super::Offset(0),
            )
            .await;
            assert!(got == want, "{name}: got {got}, want {want}");
        }
        rack.handle.shutdown().await;
    }

    /// Table over the follower's reported log end and log start offset
    /// against the fetch offset, to whether `preferred_read_replica` still
    /// names it (#873). A candidate that could not serve the offset, either
    /// because it has not replicated far enough or because it has already
    /// trimmed the offset away, must never be offered.
    #[tokio::test]
    async fn preferred_read_replica_excludes_a_follower_outside_its_reported_range() {
        let rack = rack_aware_fixture().await;
        let image = stretch_image(&[]);

        for (name, follower_leo, follower_log_start, fetch_offset, want) in [
            ("caught up, fetch at the tail: eligible", 40, 0, 40, 2),
            ("not replicated that far yet: excluded", 39, 0, 40, -1),
            ("already trimmed the offset away: excluded", 40, 41, 40, -1),
            ("in range on both bounds: eligible", 40, 10, 20, 2),
        ] {
            {
                let mut state = rack.partition.replica_state.lock().await;
                state.install_isr(
                    &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                    &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                    krabka_raft::NodeId(1),
                    std::time::Instant::now(),
                );
                state.update_follower_leo(
                    krabka_raft::NodeId(2),
                    super::Offset(follower_leo),
                    super::Offset(follower_leo.max(fetch_offset)),
                    std::time::Instant::now(),
                );
                state.record_follower_log_start(
                    krabka_raft::NodeId(2),
                    super::Offset(follower_log_start),
                );
            }
            let got = super::preferred_read_replica(
                &rack.broker,
                &image,
                &rack.partition,
                "orders",
                0,
                "dc-b",
                super::Offset(fetch_offset),
            )
            .await;
            assert!(got == want, "{name}: got {got}, want {want}");
        }
        rack.handle.shutdown().await;
    }

    /// A named preferred read replica skips the log read and the long poll
    /// entirely: the response is the offset snapshot taken at plan time, with
    /// empty records, and there is nothing left for the read loop to do with
    /// this partition (#873).
    #[tokio::test]
    async fn a_named_preferred_read_replica_skips_the_read() {
        let rack = rack_aware_fixture().await;
        rack.broker.partitions.insert(
            "orders".into(),
            PartitionIndex(0),
            std::sync::Arc::clone(&rack.partition),
        );
        rack.partition
            .install_replication_target(None, rack.broker.config.node_id.0, 0)
            .await;
        let image = stretch_image(&[]);
        let consumer = super::FetchAuthorization::Consumer {
            denied_topics: std::collections::HashSet::new(),
        };
        let context = super::PendingPlanContext {
            broker: &rack.broker,
            image: &image,
            authorization: &consumer,
            rack_id: "dc-b",
            version: super::super::FIRST_TOPIC_ID_VERSION,
            mode: super::FetchMode {
                read_committed: false,
                is_follower_fetch: false,
            },
            follower_id: -1,
            follower_broker_epoch: -1,
        };
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };

        let super::PlannedRead { read, .. } =
            super::plan_partition_read(&context, "orders", super::WireUuid::ZERO, None, &request)
                .await;

        assert!(read.out.preferred_read_replica == 2);
        assert!(read.out.error_code == crate::codes::NONE);
        assert!(
            matches!(read.out.records, Some(ref r) if r.payload_len() == 0),
            "empty records, not absent ones: {:?}",
            read.out.records
        );
        assert!(
            read.partition.is_none(),
            "no `Partition` left to read: the loop must not read or park on it"
        );
        rack.handle.shutdown().await;
    }

    /// The leader check needs this node in the installed local role, and no
    /// other leader in the committed image. `stretch_image` names node 1 as the
    /// leader of `orders`-0.
    #[tokio::test]
    async fn a_read_needs_the_installed_role_and_the_image_to_name_this_node() {
        let image = stretch_image(&[]);
        let TemporaryPartition {
            dir: _dir,
            partition,
        } = temporary_partition().call();
        let refused = |leader_id| super::PartitionData {
            current_leader: super::LeaderIdAndEpoch {
                leader_id,
                leader_epoch: 0,
                ..Default::default()
            },
            ..super::refused_read(0, NOT_LEADER_OR_FOLLOWER)
        };
        let cases = [
            ("installed and committed", 1, 1, None),
            (
                "installed, committed to another node",
                2,
                2,
                Some(refused(1)),
            ),
            ("committed, not installed yet", 1, 2, Some(refused(1))),
        ];
        for (name, node, installed, want) in cases {
            partition
                .install_replication_target(None, installed, 0)
                .await;
            let got = super::leader_refusal(
                &image,
                super::TopicPartitionRef {
                    topic: "orders",
                    partition_index: 0,
                },
                &partition,
                Some(krabka_metadata::NodeId(node)),
            );
            assert!(got == want, "{name}");
        }
        assert!(
            super::leader_refusal(
                &image,
                super::TopicPartitionRef {
                    topic: "orders",
                    partition_index: 0
                },
                &partition,
                None
            ) == None
        );
    }

    fn epoch_checks_partition(dir: &std::path::Path) -> Arc<Partition> {
        spawn_standalone_partition()
            .log_dir(dir)
            .log(Log::open(dir, LogConfig::default()).expect("open partition log"))
            .topic("diverge")
            .call()
    }

    fn epoch_checks_partition_with_history(epochs: &[i32]) -> (tempfile::TempDir, Arc<Partition>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let partition = epoch_checks_partition(dir.path());
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            append_epoch_batches(&mut log, epochs);
        }
        (dir, partition)
    }

    fn read_role(partition: &Partition) -> super::ReadRole<'_> {
        super::ReadRole {
            partition,
            required_leader: None,
            assigned_follower: true,
            log_dir_offline: false,
        }
    }

    fn effective_partition(
        last_fetched_epoch: i32,
        fetch_offset: i64,
    ) -> super::EffectivePartition {
        super::EffectivePartition {
            partition: 0,
            // -1 skips the KIP-101 fence so the KIP-320 check below it runs.
            current_leader_epoch: -1,
            last_fetched_epoch,
            fetch_offset,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        }
    }

    /// Kafka's `Partition.readRecords` order. An epoch lookup that cannot
    /// place `last_fetched_epoch` on this log -- an empty epoch history, or a
    /// `last_fetched_epoch` above every recorded epoch -- answers
    /// `OFFSET_OUT_OF_RANGE` with Kafka's all -1 offsets, never a
    /// `diverging_epoch`. A fetch offset below an *established* log start
    /// answers `OFFSET_OUT_OF_RANGE` with the same all -1 offsets, checked
    /// before the epoch lookup's divergence row is built. An unestablished
    /// (segment-inferred) floor never refuses here, since it may sit above
    /// data the remote tier still holds. A gap epoch diverges to the floor
    /// epoch and the start of the next recorded epoch.
    #[tokio::test]
    async fn below_established_log_start_answers_offset_out_of_range() {
        // No records at all: the epoch cache is empty, and nothing has ever
        // moved the log start, so it is unestablished.
        let empty_dir = tempfile::tempdir().expect("tempdir");
        let empty = epoch_checks_partition(empty_dir.path());

        // Two epochs of two records each: checkpoint `0 -> 0`, `1 -> 2`, LEO 4.
        // Also never trimmed, so its log start is unestablished too.
        let history_dir = tempfile::tempdir().expect("tempdir");
        let with_history = epoch_checks_partition(history_dir.path());
        {
            let mut log = with_history.log.lock().expect("log mutex poisoned");
            append_epoch_batches(&mut log, &[0, 0, 1, 1]);
        }
        // Every record is committed: a diverging row reports this live HW.
        with_history.replica_state.lock().await.hw = super::Offset(4);

        // A gap: epoch 0 then epoch 3, two records each. Checkpoint `0 -> 0`,
        // `3 -> 2`, LEO 4. A follower that last fetched epoch 2 is placed on
        // the floor epoch 0, which ends where epoch 3 begins.
        let gap_dir = tempfile::tempdir().expect("tempdir");
        let with_gap = epoch_checks_partition(gap_dir.path());
        {
            let mut log = with_gap.log.lock().expect("log mutex poisoned");
            append_epoch_batches(&mut log, &[0, 0, 3, 3]);
        }
        // Every record is committed: a diverging row reports this live HW.
        with_gap.replica_state.lock().await.hw = super::Offset(4);

        // Three epochs of two records each: checkpoint `0 -> 0`, `1 -> 2`,
        // `2 -> 4`, LEO 6. The log start then moves to 5 and becomes
        // established, above the epoch-0 boundary (2) that a
        // `last_fetched_epoch = 0` fetch would otherwise diverge to.
        let trimmed_dir = tempfile::tempdir().expect("tempdir");
        let trimmed = epoch_checks_partition(trimmed_dir.path());
        {
            let mut log = trimmed.log.lock().expect("log mutex poisoned");
            append_epoch_batches(&mut log, &[0, 0, 1, 1, 2, 2]);
            log.set_log_start_offset(super::Offset(5))
                .expect("move log start");
        }

        // Both histories have HW/LSO 4 and start 0; epoch 0 ends at offset 2.
        let expected_history_divergence = || super::PartitionData {
            partition_index: 0,
            error_code: NONE,
            high_watermark: 4,
            last_stable_offset: 4,
            log_start_offset: 0,
            diverging_epoch: super::EpochEndOffset {
                epoch: 0,
                end_offset: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        for (name, partition, last_fetched_epoch, fetch_offset, want_final, want_out) in [
            (
                "empty epoch history: Kafka cannot place the epoch",
                Arc::clone(&empty),
                0,
                0,
                true,
                super::refused_read(0, OFFSET_OUT_OF_RANGE),
            ),
            (
                "last_fetched_epoch above every recorded epoch: Kafka cannot \
                 place the epoch",
                Arc::clone(&with_history),
                5,
                4,
                true,
                super::refused_read(0, OFFSET_OUT_OF_RANGE),
            ),
            (
                "gap epoch diverges to the floor epoch and the next start",
                Arc::clone(&with_gap),
                2,
                4,
                true,
                expected_history_divergence(),
            ),
            (
                "last_fetched_epoch is the latest and the fetch offset is \
                 inside the log: no divergence, the read goes ahead",
                Arc::clone(&with_history),
                1,
                3,
                false,
                super::PartitionData {
                    partition_index: 0,
                    ..Default::default()
                },
            ),
            (
                "fetch offset below an established log start, even though the \
                 epoch history alone would diverge",
                Arc::clone(&trimmed),
                0,
                4,
                true,
                super::refused_read(0, OFFSET_OUT_OF_RANGE),
            ),
            (
                "a true divergence still answers a diverging_epoch with epoch >= 0",
                Arc::clone(&with_history),
                0,
                4,
                true,
                expected_history_divergence(),
            ),
        ] {
            check_epoch_placement(
                name,
                &partition,
                last_fetched_epoch,
                fetch_offset,
                want_final,
                want_out,
            )
            .await;
        }
    }

    /// Kafka's `Partition.makeLeader` records the new leader epoch at the log
    /// end before anything is written in it, so a leader promoted a moment
    /// ago still places every follower. Before promotion the same log has
    /// only epochs 0 and 1 (LEO 4), and a follower that last fetched epoch 2
    /// or 3 cannot be placed at all.
    #[tokio::test]
    async fn a_freshly_promoted_leader_places_followers_before_its_first_write() {
        // A diverging row carries the partition's live bounds; nothing is
        // committed here, so the high watermark, LSO and log start are all 0.
        let diverge_to = |epoch, end_offset| super::PartitionData {
            partition_index: 0,
            error_code: NONE,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            diverging_epoch: super::EpochEndOffset {
                epoch,
                end_offset,
                ..Default::default()
            },
            ..Default::default()
        };
        let proceed = super::PartitionData {
            partition_index: 0,
            ..Default::default()
        };
        let out_of_range = super::refused_read(0, OFFSET_OUT_OF_RANGE);
        // (name, promoted to epoch 3, last_fetched_epoch, fetch_offset,
        //  final, output)
        let cases = [
            (
                "not yet promoted: the new epoch cannot be placed",
                false,
                3,
                4,
                true,
                out_of_range.clone(),
            ),
            (
                "promoted: a follower already on the new epoch reads on",
                true,
                3,
                4,
                false,
                proceed.clone(),
            ),
            (
                "promoted: a follower on the previous epoch reads on",
                true,
                1,
                4,
                false,
                proceed,
            ),
            (
                "not yet promoted: an epoch this leader never saw cannot be \
                 placed",
                false,
                2,
                6,
                true,
                out_of_range,
            ),
            (
                "promoted: an epoch this leader never saw ends where the new \
                 epoch starts",
                true,
                2,
                6,
                true,
                diverge_to(1, 4),
            ),
        ];
        for (name, promoted, last_fetched_epoch, fetch_offset, want_final, want_out) in cases {
            let (_dir, partition) = epoch_checks_partition_with_history(&[0, 0, 1, 1]);
            if promoted {
                partition
                    .install_local_leadership(&ProducerState::new(), None, 1, 3)
                    .await
                    .expect("promote");
            }
            check_epoch_placement(
                name,
                &partition,
                last_fetched_epoch,
                fetch_offset,
                want_final,
                want_out,
            )
            .await;
        }
    }

    /// An offline log directory takes precedence over the below-established-
    /// log-start refusal: the caller's own offline gate answers
    /// `KAFKA_STORAGE_ERROR` instead, since a storage failure should never
    /// look like a client or follower reset.
    #[tokio::test]
    async fn below_established_log_start_defers_to_an_offline_log_directory() {
        let (_dir, partition) = epoch_checks_partition_with_history(&[0, 0]);
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.set_log_start_offset(super::Offset(1))
                .expect("move log start");
        }

        let EpochCheck {
            is_final: final_, ..
        } = epoch_check(&partition, 0, 0, true).await;
        assert!(!final_, "defers to the caller's own offline check");
    }

    /// Kafka's `maybeIncrementLeaderHW` runs only while this broker still
    /// leads. The leader records a follower position only while the
    /// replication target that its leader checks passed under is installed.
    /// A leadership change installs another target and clears the follower
    /// state. A position from before that change belongs to the old
    /// leadership, and the leader drops it.
    #[tokio::test]
    async fn a_follower_position_is_recorded_only_under_its_own_target() {
        use krabka_raft::NodeId;

        #[derive(Debug, PartialEq)]
        struct Recorded {
            advanced: bool,
            high_watermark: super::Offset,
            follower_log_end: super::Offset,
        }

        let cases = [
            ("the target is still installed", false, Recorded {
                advanced: true,
                high_watermark: super::Offset(2),
                follower_log_end: super::Offset(2),
            }),
            (
                "a new leader epoch was installed after the checks",
                true,
                Recorded {
                    advanced: false,
                    high_watermark: super::Offset(0),
                    follower_log_end: super::Offset(0),
                },
            ),
        ];

        for (name, epoch_moved, want) in cases {
            let (_dir, partition) = epoch_checks_partition_with_history(&[0, 0]);
            partition.install_replication_target(None, 1, 0).await;
            partition
                .install_isr(&[NodeId(1), NodeId(2)], &[NodeId(1), NodeId(2)], NodeId(1))
                .await;
            let position = super::FollowerPosition {
                follower_id: 2,
                broker_epoch: -1,
                log_start_offset: 0,
                target: *partition.replication_target.read().await,
            };
            if epoch_moved {
                partition.install_replication_target(None, 1, 1).await;
            }

            let advanced =
                super::record_follower_position(&partition, position, 2, super::Offset(2)).await;

            let recorded = Recorded {
                advanced,
                high_watermark: partition.high_watermark().await,
                follower_log_end: partition
                    .replica_state
                    .lock()
                    .await
                    .follower_progress(NodeId(2))
                    .0,
            };
            assert!(recorded == want, "{name}");
        }
    }
    /// Whether the epoch gate decided the row, and the row it left.
    struct EpochCheck {
        is_final: bool,
        output: super::PartitionData,
    }

    /// Exercise the epoch gate with the same wire row and caller-selected offline status.
    async fn epoch_check(
        partition: &Partition,
        last_fetched_epoch: i32,
        fetch_offset: i64,
        log_dir_offline: bool,
    ) -> EpochCheck {
        let request = effective_partition(last_fetched_epoch, fetch_offset);
        let mut output = super::PartitionData {
            partition_index: 0,
            ..Default::default()
        };
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let final_ = super::apply_epoch_checks(
            &image,
            "diverge",
            0,
            &request,
            super::ReadRole {
                log_dir_offline,
                ..read_role(partition)
            },
            &mut output,
        )
        .await;
        EpochCheck {
            is_final: final_,
            output,
        }
    }

    async fn check_epoch_placement(
        name: &str,
        partition: &Partition,
        last_fetched_epoch: i32,
        fetch_offset: i64,
        want_final: bool,
        want_out: super::PartitionData,
    ) {
        let EpochCheck {
            is_final: final_,
            output,
        } = epoch_check(partition, last_fetched_epoch, fetch_offset, false).await;
        assert!(final_ == want_final, "{name}: final");
        assert!(output == want_out, "{name}: got {output:?}");
    }
}
