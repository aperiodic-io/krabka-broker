//! Fixtures shared by the controller's unit tests: engine and controller
//! builders over a temporary data directory, a recording [`PeerSender`], and
//! the election and submission helpers the behaviour tests drive them with.

use std::time::Duration as StdDuration;

use krabka_units::prelude::{TimeExt as _, secs};

use super::*;
use crate::kraft::transport::NullPeerSender;

/// Deadline every test-side channel receive is bounded by.
pub const TEST_RECV_TIMEOUT: Time = secs(1);

/// Default election timeout for engines built by [`build`].
pub const TEST_ELECTION_TIMEOUT: Time = secs(1);

/// The metadata log configuration of an engine under test: Kafka's segment
/// defaults, no KIP-835 no-op records, so a test sees only the offsets its own
/// writes land at, and no size allowance, so every cleaning keeps only the
/// newest snapshot and moves the log start up to it.
pub fn test_metadata_log() -> MetadataLogConfig {
    MetadataLogConfig {
        max_retention_size: Some(krabka_units::prelude::bytes(0)),
        max_idle_interval: krabka_units::prelude::millis(0),
        ..MetadataLogConfig::default()
    }
}

pub fn voter_set(ids: &[NodeId]) -> krabka_metadata::voters::VoterSet {
    krabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
        krabka_metadata::voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![krabka_metadata::voters::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9_093,
            }],
            kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
        }
    }))
}

pub fn build(me: NodeId, ids: &[NodeId]) -> (KraftController, tempfile::TempDir) {
    build_with_timeout(me, ids, TEST_ELECTION_TIMEOUT)
}

pub fn build_with_timeout(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
) -> (KraftController, tempfile::TempDir) {
    build_full(me, ids, election_timeout, 0)
}

pub fn build_with_snapshot_interval(
    me: NodeId,
    ids: &[NodeId],
    snapshot_interval_records: u64,
) -> (KraftController, tempfile::TempDir) {
    build_full(me, ids, TEST_ELECTION_TIMEOUT, snapshot_interval_records)
}

/// Like [`build_with_snapshot_interval`], but with a caller-chosen
/// `max_bytes_between_snapshots` instead of a record-count interval.
pub fn build_with_max_bytes_between_snapshots(
    me: NodeId,
    ids: &[NodeId],
    max_bytes_between_snapshots: krabka_units::prelude::ByteSize,
) -> (KraftController, tempfile::TempDir) {
    spawn_test_controller(me, ids, |config| {
        config.max_bytes_between_snapshots = max_bytes_between_snapshots;
    })
}

pub fn build_full(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
    snapshot_interval_records: u64,
) -> (KraftController, tempfile::TempDir) {
    build_full_with_policy(
        me,
        ids,
        election_timeout,
        snapshot_interval_records,
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_full_with_policy(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
    snapshot_interval_records: u64,
    heartbeat_interval: Option<Time>,
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
) -> (KraftController, tempfile::TempDir) {
    spawn_test_controller(me, ids, |config| {
        config.election_timeout = election_timeout;
        config.snapshot_interval_records = snapshot_interval_records;
        config.heartbeat_interval = heartbeat_interval;
        config.controller_fetch_miss_limit = controller_fetch_miss_limit;
        config.metadata_raft_command_queue_capacity = metadata_raft_command_queue_capacity;
        config.metadata_raft_fetch_max = metadata_raft_fetch_max;
    })
}

pub fn test_kraft_config(me: NodeId, cluster_id: uuid::Uuid, state: QuorumState) -> KraftConfig {
    KraftConfig {
        me,
        cluster_id,
        directory_id: uuid::Uuid::nil(),
        initial_state: state,
        election_timeout: TEST_ELECTION_TIMEOUT,
        heartbeat_interval: None,
        controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
        metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
        metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
        peers: Arc::new(NullPeerSender),
        snapshot_interval_records: 0,
        max_bytes_between_snapshots: krabka_units::prelude::bytes(0),
        max_snapshot_interval: krabka_units::prelude::millis(0),
        metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
        metadata_log: test_metadata_log(),
        activation: crate::kraft::Activation::default(),
    }
}

pub fn open_test_controller(
    data_dir: std::path::PathBuf,
    cluster_id: uuid::Uuid,
    voters: VoterSet,
) -> Result<KraftController, RaftError> {
    KraftController::open(
        data_dir,
        NodeId(1),
        cluster_id,
        uuid::Uuid::nil(),
        voters,
        TEST_ELECTION_TIMEOUT,
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
        Arc::new(NullPeerSender),
        0,
        krabka_units::prelude::bytes(0),
        krabka_units::prelude::millis(0),
        MetadataSnapshotFetchMax::default(),
        test_metadata_log(),
        crate::kraft::Activation::default(),
    )
}

fn spawn_test_controller(
    me: NodeId,
    ids: &[NodeId],
    customize: impl FnOnce(&mut KraftConfig),
) -> (KraftController, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("open log");
    let state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids));
    let mut config = test_kraft_config(me, uuid::Uuid::nil(), state);
    customize(&mut config);
    let ctrl = KraftController::spawn(config, log, dir.path().to_path_buf());
    (ctrl, dir)
}

pub fn build_engine_only(me: NodeId, ids: &[NodeId]) -> (Engine, tempfile::TempDir) {
    build_engine_only_with_policy(
        me,
        ids,
        ControllerFetchMissLimit::default(),
        MetadataRaftFetchMax::default(),
    )
}

pub fn build_engine_only_with_policy(
    me: NodeId,
    ids: &[NodeId],
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
) -> (Engine, tempfile::TempDir) {
    build_engine_only_with_metadata_log(
        me,
        ids,
        controller_fetch_miss_limit,
        metadata_raft_fetch_max,
        test_metadata_log(),
    )
}

/// Like [`build_engine_only_with_policy`], with the metadata log rolled,
/// cleaned and kept alive as `metadata_log` says.
pub fn build_engine_only_with_metadata_log(
    me: NodeId,
    ids: &[NodeId],
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
    metadata_log: MetadataLogConfig,
) -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path(), &metadata_log).expect("open log");
    let core = QuorumStateMachine::new(
        me,
        QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids)),
        TEST_ELECTION_TIMEOUT,
    );
    let image = MetadataImage::new(uuid::Uuid::nil());
    let (image_tx, _image_rx) = watch::channel(Arc::new(image.clone()));
    let (leader_tx, _leader_rx) = watch::channel(core.quorum_state().leader_id);
    let log_hwm_at_open = log.hwm().0;
    let initial_snapshot = super::queries::initial_quorum_snapshot(&core, &log, log_hwm_at_open, 0);
    let (quorum_tx, _quorum_rx) = watch::channel(initial_snapshot);
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let held_epoch = core.quorum_state().leader_epoch;
    let was_leader = core.role().is_leader();
    let controls = KraftControlState::new(core.quorum_state().voters.clone(), 0);
    let clock_base = Instant::now();
    (
        Engine {
            me,
            core,
            log,
            image,
            peers: Arc::new(NullPeerSender),
            image_tx,
            leader_tx,
            quorum_tx,
            cmd_tx,
            fault_tx: watch::channel(None).0,
            data_dir: dir.path().to_path_buf(),
            clock_base,
            election_timeout: TEST_ELECTION_TIMEOUT,
            heartbeat_interval: None,
            controller_fetch_miss_limit,
            metadata_raft_fetch_max,
            election_at: None,
            fetch_at: None,
            check_quorum_at: None,
            fetch_misses: 0,
            discovery_attempts: 0,
            commit_waiters: Vec::new(),
            was_leader,
            held_epoch,
            registration_writes: std::collections::BTreeMap::new(),
            snapshot_interval_records: 0,
            max_bytes_between_snapshots: krabka_units::prelude::bytes(0),
            max_snapshot_interval: krabka_units::prelude::millis(0),
            metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
            metadata_log,
            noop_at: None,
            clean_at: clock_base + METADATA_LOG_CLEAN_INTERVAL,
            last_snapshot_end_offset: Offset(0),
            last_snapshot_timestamp_ms: 0,
            last_applied_record_timestamp_ms: 0,
            pending_record_processing_time_us_total: 0,
            pending_record_processing_time_samples: 0,
            pending_record_processing_time_us_max: 0,
            last_snapshot_at_ms: 0,
            bytes_since_snapshot: 0,
            downgrade_snapshot_pending: None,
            downgrade_snapshot_failures_remaining: 0,
            snapshot_fetch: None,
            installed_snapshot_epoch: None,
            controls,
            directory_id: uuid::Uuid::nil(),
            observers: BTreeMap::new(),
            fetch_purgatory: Vec::new(),
            wall_clock_base: std::time::SystemTime::now(),
            leader_reported_hwm: log_hwm_at_open,
            pending_reconfig: None,
            activation: crate::kraft::Activation::default(),
            activation_fault: None,
            replay_fault: None,
        },
        dir,
    )
}

#[derive(Debug)]
pub struct CapturedPeerSend {
    pub peer: NodeId,
    pub api_key: i16,
    pub body: bytes::Bytes,
}

struct RecordingPeerSender {
    sends: mpsc::UnboundedSender<CapturedPeerSend>,
    response: bytes::Bytes,
}

#[async_trait::async_trait]
impl PeerSender for RecordingPeerSender {
    async fn send(
        &self,
        peer: NodeId,
        api_key: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        self.sends
            .send(CapturedPeerSend {
                peer,
                api_key,
                body,
            })
            .expect("record peer send");
        Ok(self.response.clone())
    }
}

pub fn record_peer_sends(
    engine: &mut Engine,
    response: bytes::Bytes,
) -> mpsc::UnboundedReceiver<CapturedPeerSend> {
    let (sends, rx) = mpsc::unbounded_channel();
    engine.peers = Arc::new(RecordingPeerSender { sends, response });
    rx
}

pub async fn recv_peer_send(
    rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
) -> CapturedPeerSend {
    tokio::time::timeout(TEST_RECV_TIMEOUT.to_std(), rx.recv())
        .await
        .expect("peer send timed out")
        .expect("peer send channel closed")
}

pub async fn recv_peer_send_with_api(
    rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
    api_key: i16,
) -> CapturedPeerSend {
    let deadline = tokio::time::Instant::now() + TEST_RECV_TIMEOUT.to_std();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let send = tokio::time::timeout(remaining, rx.recv())
            .await
            .expect("peer send with api timed out")
            .expect("peer send channel closed");
        if send.api_key == api_key {
            return send;
        }
    }
}

pub fn one_offset_batch(base_offset: i64, epoch: i32, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: epoch,
        last_offset_delta: 0,
        records: vec![Record {
            value: Some(bytes::Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn elect_single_voter_engine(engine: &mut Engine) {
    engine.on_event(Event::ElectionTimeout);
    assert2::assert!(engine.core.role().is_leader());
}

/// A realistic single-partition create batch: a `V1Topic` plus its one
/// `V1Partition`. KIP-631 framing derives the topic's partition count from
/// the partition records (the `TopicRecord` wire shape carries no count), so
/// a bare `V1Topic` would round-trip back to zero partitions and fail
/// validation on apply.
pub fn topic_record(name: &str) -> Vec<krabka_metadata::MetadataRecord> {
    topic_record_named(name, 1)
}

pub fn topic_record_named(name: &str, id: u128) -> Vec<krabka_metadata::MetadataRecord> {
    vec![
        krabka_metadata::MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
            name,
            uuid::Uuid::from_u128(id),
        )),
        krabka_metadata::MetadataRecord::V1Partition(
            crate::test_support::single_replica_partition(name, 0, NodeId(1)),
        ),
    ]
}

/// Drive a voter to leadership in a multi-voter cluster under `NullPeerSender`
/// by injecting the vote responses it would have received: `ElectionTimeout`
/// starts a pre-vote round (epoch unchanged), a granted pre-vote from `helper`
/// promotes to `Candidate` (epoch +1) and broadcasts a real vote, and a
/// granted real vote from `helper` reaches majority and promotes to `Leader`.
pub async fn elect_leader_with_helper(ctrl: &KraftController, me: NodeId, helper: NodeId) {
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    // Pre-vote round runs at the current (pre-bump) epoch 0.
    ctrl.inject_event(Event::ReceiveVoteResponse {
        from: helper,
        epoch: 0,
        vote_granted: true,
    })
    .await
    .unwrap();
    // Candidate round runs at the bumped epoch 1.
    ctrl.inject_event(Event::ReceiveVoteResponse {
        from: helper,
        epoch: 1,
        vote_granted: true,
    })
    .await
    .unwrap();
    await_leader(ctrl, Some(me)).await;
}

pub async fn await_leader(ctrl: &KraftController, want: Option<NodeId>) {
    let result = tokio::time::timeout(StdDuration::from_secs(2), async {
        let mut rx = ctrl.watch_leader();
        loop {
            if *rx.borrow() == want {
                return;
            }
            rx.changed().await.expect("leader watch closed");
        }
    })
    .await;
    assert2::assert!(result.is_ok());
}

pub async fn submit_change_with_timeout(
    ctrl: &KraftController,
    records: Vec<krabka_metadata::MetadataRecord>,
    context: &str,
) -> Result<(), RaftError> {
    tokio::time::timeout(StdDuration::from_secs(2), ctrl.submit_change(records))
        .await
        .unwrap_or_else(|_| panic!("{context} submit_change timed out"))
        .map(|_| ())
}

/// Commit the current tail by delivering a follower fetch at its end offset.
pub async fn commit_pending(ctrl: &KraftController, follower: NodeId) {
    let quorum = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: follower,
        fetch_epoch: quorum.leader_epoch,
        fetch_offset: quorum.log_end_offset,
    })
    .await
    .unwrap();
}

pub fn single_voter_leader_engine() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    (engine, dir)
}

pub async fn single_voter_leader() -> (KraftController, tempfile::TempDir) {
    let (ctrl, dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;
    (ctrl, dir)
}

pub async fn three_voter_leader() -> (KraftController, tempfile::TempDir) {
    let (ctrl, dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    (ctrl, dir)
}

pub fn submit_on_engine(
    engine: &mut Engine,
    records: &[krabka_metadata::MetadataRecord],
) -> oneshot::Receiver<Result<SubmitChangeResult, RaftError>> {
    let (reply, receiver) = oneshot::channel();
    engine.on_submit_change(records, reply);
    receiver
}

pub fn spawn_submit(
    ctrl: &KraftController,
    records: Vec<krabka_metadata::MetadataRecord>,
) -> tokio::task::JoinHandle<Result<SubmitChangeResult, RaftError>> {
    let ctrl = ctrl.clone();
    tokio::spawn(async move { ctrl.submit_change(records).await })
}
