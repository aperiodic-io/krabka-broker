//! Shared unit-test fixtures for the coordinator modules: coordinator
//! builders, a metadata provider and a metadata source with fixed contents, a
//! share persister, and the record values the replay tests feed in.
//!
//! Several sibling modules assert against the same fixtures, so they live in
//! one module rather than being rebuilt per test file.

pub(crate) use super::{
    actor::MetadataProvider, config::NextGenConfig, group_coordinator::GroupCoordinator,
    offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
    share::config::ShareGroupConfig, streams::config::StreamsGroupConfig,
};
use {
    super::{persistence_next_gen, reconciler, share, streams},
    crate::{
        codes::{COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR},
        error::BrokerError,
        metadata_source::MetadataSource,
        network::client::InterBrokerClient,
        partition_registry::PartitionRegistry,
        share_coordinator::{
            config::ShareCoordinatorConfig, coordinator::ShareCoordinator,
            persister_client::SharePersister,
        },
        test_support::{FakeMetadataSource, KafkaErrorCode, UnixMillis, string_pairs},
    },
    bytes::Bytes,
    std::{collections::BTreeMap, sync::Arc},
};

pub(crate) type AppendedRecords = Vec<(Option<Bytes>, Option<Bytes>)>;

pub(crate) fn assert_group_epoch_exhaustion<State>(
    mut state: State,
    exhaust: impl FnOnce(&mut State),
    bump_epoch: impl FnOnce(&mut State) -> bool,
    epoch: impl FnOnce(&State) -> i32,
) {
    exhaust(&mut state);
    ::assert2::assert!(!bump_epoch(&mut state));
    ::assert2::assert!(epoch(&state) == i32::MAX);
}

pub(crate) async fn appended_offsets_records(log: &InMemoryOffsetsLog) -> AppendedRecords {
    log.batches()
        .await
        .into_iter()
        .flat_map(|batch| batch.records)
        .map(|record| (record.key, record.value))
        .collect()
}

#[derive(
    derive_more::From,
    derive_more::Into,
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub(crate) struct MemberEpoch(pub i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct MemberEpochs {
    #[default(MemberEpoch(6))]
    pub current: MemberEpoch,
    #[default(MemberEpoch(5))]
    pub previous: MemberEpoch,
}

/// An active-only stable streams seed, with no standby, warmup or revocations.
pub(crate) fn stable_streams_assignment(
    epochs: (i32, i32),
    active: BTreeMap<String, Vec<i32>>,
) -> streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
    streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
        member_epoch: epochs.0,
        previous_member_epoch: epochs.1,
        active,
        ..Default::default()
    }
}

pub(crate) fn make_coord() -> Arc<GroupCoordinator> { make_coord_with_log().0 }

pub(crate) fn make_coord_with_log() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
    let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
    let offsets_log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
        metadata,
        offsets_log.clone(),
        StreamsGroupConfig::default(),
    ));
    (coord, offsets_log)
}

#[derive(Debug)]
pub(super) struct ImageMetadatalessProvider;
impl MetadataProvider for ImageMetadatalessProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput { reconciler::ReconcileInput::default() }
}

/// A metadata provider whose snapshot a test replaces, as the controller
/// replaces the metadata image.
#[derive(Debug, Default)]
pub(crate) struct SwitchableMetadata(std::sync::Mutex<reconciler::ReconcileInput>);

impl SwitchableMetadata {
    pub(crate) fn new(input: reconciler::ReconcileInput) -> Arc<Self> {
        Arc::new(Self(std::sync::Mutex::new(input)))
    }

    pub(crate) fn set(&self, input: reconciler::ReconcileInput) {
        *self.0.lock().expect("metadata lock") = input;
    }
}

impl MetadataProvider for SwitchableMetadata {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        self.0.lock().expect("metadata lock").clone()
    }
}

/// A snapshot of `topics`, each a name, the byte its topic id repeats and a
/// partition count.
pub(crate) fn snapshot_of(topics: &[(&str, u8, i32)]) -> reconciler::ReconcileInput {
    reconciler::ReconcileInput {
        topic_id_by_name: topics
            .iter()
            .map(|(name, id, _)| ((*name).to_string(), proto_uuid(*id)))
            .collect(),
        partitions_per_topic: topics
            .iter()
            .map(|(_, id, partitions)| (proto_uuid(*id), *partitions))
            .collect(),
        ..reconciler::ReconcileInput::default()
    }
}

/// A coordinator whose group actors read `metadata`.
pub(crate) fn make_coord_with_metadata(
    metadata: Arc<dyn MetadataProvider>,
) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
        metadata,
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ))
}

/// A metadata source over `image`, with node 1 reported as the controller
/// leader.
pub(super) fn fixed_source(image: krabka_metadata::MetadataImage) -> Arc<dyn MetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(krabka_raft::NodeId(1)))
            .build(),
    )
}

pub(super) fn make_share_persister(source: Arc<dyn MetadataSource>) -> Arc<SharePersister> {
    let share_coordinator = Arc::new(ShareCoordinator::new(
        krabka_metadata::NodeId(1),
        Arc::new(PartitionRegistry::new()),
        ShareCoordinatorConfig::default(),
    ));
    Arc::new(SharePersister::new(
        krabka_metadata::NodeId(1),
        share_coordinator,
        source,
        Arc::default(),
        Arc::new(InterBrokerClient::new(None, None)),
        krabka_security::ListenerProtocol::Plaintext,
        "PLAINTEXT".into(),
    ))
}

pub(super) fn proto_uuid(byte: u8) -> krabka_protocol::primitives::uuid::Uuid {
    krabka_protocol::primitives::uuid::Uuid([byte; 16])
}

pub(super) fn real_uuid(byte: u8) -> uuid::Uuid { uuid::Uuid::from_bytes([byte; 16]) }

pub(super) fn next_member(client_id: &str) -> persistence_next_gen::MemberMetadataValue {
    persistence_next_gen::MemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-a".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["topic-a".into()],
        subscribed_topic_regex: Some("topic-.*".into()),
        server_assignor: Some("range".into()),
        rebalance_timeout_ms: 45_000,
        classic: None,
    }
}

pub(super) fn next_current(epoch: i32) -> persistence_next_gen::CurrentMemberAssignmentValue {
    persistence_next_gen::CurrentMemberAssignmentValue {
        member_epoch: epoch,
        previous_member_epoch: epoch - 1,
        state: persistence_next_gen::MemberAssignmentState::Stable,
        assigned_partitions: vec![persistence_next_gen::CurrentTopicPartitions {
            topic_id: proto_uuid(1),
            partitions: vec![0, 1],
            assignment_epochs: None,
        }],
        partitions_pending_revocation: vec![],
    }
}

pub(super) fn share_member(client_id: &str) -> share::persistence::ShareGroupMemberMetadataValue {
    share::persistence::ShareGroupMemberMetadataValue {
        rack_id: Some("rack-b".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["share-topic".into()],
    }
}

pub(super) fn streams_member(
    client_id: &str,
) -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-c".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        process_id: "process".into(),
        user_endpoint: Some(streams::persistence::StreamsEndpoint {
            host: "localhost".into(),
            port: 8080,
        }),
        client_tags: vec![("app".into(), "streams".into())],
        rebalance_timeout_ms: 30_000,
        topology_epoch: 4,
    }
}

/// A metadata image with the requested per-group config records.
pub(crate) fn group_config_image(
    overrides: &[(&str, &[(&str, &str)])],
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for (group_id, entries) in overrides {
        image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: (*group_id).to_owned(),
                configs: string_pairs(entries),
            },
        ));
    }
    image
}

/// The fields every heartbeat capacity/timing contract observes.
pub(crate) trait HeartbeatStatus {
    fn status(self) -> (i16, i32);
}

macro_rules! heartbeat_status {
    ($($response:ty),+ $(,)?) => {$(
        impl HeartbeatStatus for $response {
            fn status(self) -> (i16, i32) {
                (self.error_code, self.member_epoch)
            }
        }
    )+};
}
heartbeat_status!(
    krabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
);

/// Exercise one-member capacity through each protocol's heartbeat RPC.
pub(crate) async fn assert_single_member_limit<H, Q, R: HeartbeatStatus>(
    handle: &Arc<H>,
    request: impl Fn(&str, i32) -> Q,
    heartbeat: impl AsyncFn(&H, Q) -> R,
) {
    let status = async |id: &'static str, epoch| {
        let request = request(id, epoch);
        heartbeat(&Arc::clone(handle), request).await.status()
    };

    let (code, epoch) = status("m1", 0).await;
    assert2::check!(code == crate::codes::NONE);
    let (code, _) = status("m2", 0).await;
    assert2::check!(code == crate::codes::GROUP_MAX_SIZE_REACHED);
    let (code, existing_epoch) = status("m1", epoch).await;
    assert2::check!(code == crate::codes::NONE);
    assert2::check!(existing_epoch == epoch);
}

/// Build a one-member coordinator fixture and exercise its heartbeat limit.
pub(crate) async fn assert_single_member_limit_with_log<H, Q, R: HeartbeatStatus>(
    make_handle: impl FnOnce(Arc<InMemoryOffsetsLog>) -> Arc<H>,
    request: impl Fn(&str, i32) -> Q,
    heartbeat: impl AsyncFn(&H, Q) -> R,
) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let handle = make_handle(log);
    assert_single_member_limit(&handle, request, heartbeat).await;
}

/// Run the shared session-timeout contract after protocol-specific setup.
pub(crate) async fn check_group_session_timeout<H, R: HeartbeatStatus>(
    create: impl Fn(&str) -> Arc<H>,
    heartbeat: impl AsyncFn(&H, &str, i32) -> R,
) {
    use assert2::{assert, check};
    let mut epochs = Vec::new();
    for group_id in ["brief", "plain"] {
        let handle = create(group_id);
        let (code, epoch) = heartbeat(&handle, "m1", 0).await.status();
        check!(code == crate::codes::NONE, "{group_id}");
        epochs.push((group_id, handle, epoch));
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut answers = Vec::new();
    for (group_id, handle, epoch) in &epochs {
        answers.push((*group_id, heartbeat(handle, "m1", *epoch).await.status().0));
    }
    assert!(
        answers
            == [
                ("brief", crate::codes::UNKNOWN_MEMBER_ID),
                ("plain", crate::codes::NONE)
            ]
    );
}

/// Join two members in each configured group, then heartbeat after the paced
/// interval. Each protocol checks the first paced response with its own oracle.
pub(crate) async fn check_group_assignment_timing<H, R: HeartbeatStatus>(
    create: impl AsyncFn(&str) -> Arc<H>,
    heartbeat: impl AsyncFn(&H, &str, i32) -> R,
    check_first: impl Fn((i16, i32)),
) {
    use assert2::{assert, check};
    let mut joined = Vec::new();
    let mut handles = Vec::new();
    for group_id in ["slow", "fast", "paced"] {
        let handle = create(group_id).await;
        check!(
            heartbeat(&handle, "m1", 0).await.status().1 == 2,
            "{group_id}"
        );
        joined.push((group_id, heartbeat(&handle, "m2", 0).await.status().1));
        handles.push((group_id, handle));
    }
    assert!(joined == [("slow", 2), ("fast", 3), ("paced", 2)]);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let (_, paced) = &handles[2];
    check_first(heartbeat(paced, "m1", 2).await.status());
    check!(heartbeat(paced, "m2", 2).await.status().1 == 3);
    // The slow group still waits for the broker's minute.
    let (_, slow) = &handles[0];
    check!(heartbeat(slow, "m2", 2).await.status().1 == 2);
}

/// Split a freshly encoded key into its leading version and body, mirroring
/// the broker's `__consumer_offsets` dispatch.
pub(crate) fn peek_version(mut buf: &[u8]) -> (i16, &[u8]) {
    let version = bytes::Buf::get_i16(&mut buf);
    (version, buf)
}

/// Literal wire fields, decoded independently of the record codecs. Each field
/// must contain complete hexadecimal byte pairs; malformed literals fail the test.
pub(crate) fn wire_bytes(fields: &[&str]) -> Vec<u8> {
    fields
        .iter()
        .flat_map(|field| hex::decode(field).expect("valid literal wire bytes"))
        .collect()
}

pub(crate) struct HeartbeatWriteFailure {
    label: &'static str,
    failure: Option<BrokerError>,
    expected: KafkaErrorCode,
}

pub(crate) struct FailedHeartbeatFixture {
    pub coordinator: std::sync::Arc<super::GroupCoordinator>,
    pub log: std::sync::Arc<super::offsets_log::fake::InMemoryOffsetsLog>,
    pub expected: KafkaErrorCode,
    pub label: &'static str,
}

impl FailedHeartbeatFixture {
    pub async fn check_no_batches(&self) {
        assert2::check!(self.log.batches().await.is_empty(), "{}", self.label);
    }
}

impl HeartbeatWriteFailure {
    /// Arm the fixture's next durable append without changing its coordinator configuration.
    pub fn coordinator(
        self,
        factory: impl FnOnce() -> (
            std::sync::Arc<super::GroupCoordinator>,
            std::sync::Arc<super::offsets_log::fake::InMemoryOffsetsLog>,
        ),
    ) -> FailedHeartbeatFixture {
        let (coordinator, log) = factory();
        log.fail_next_append(self.failure);
        FailedHeartbeatFixture {
            coordinator,
            log,
            expected: self.expected,
            label: self.label,
        }
    }
}

/// The three independent failure expectations for a heartbeat's durable write.
pub(crate) fn heartbeat_write_failures() -> [HeartbeatWriteFailure; 3] {
    let uncommitted = |code| Some(BrokerError::CoordinatorWriteUncommitted { partition: 0, code });
    [
        HeartbeatWriteFailure {
            label: "the partition writer is gone",
            failure: None,
            expected: KafkaErrorCode(COORDINATOR_LOAD_IN_PROGRESS),
        },
        HeartbeatWriteFailure {
            label: "the leadership moved before the write committed",
            failure: uncommitted(NOT_COORDINATOR),
            expected: KafkaErrorCode(NOT_COORDINATOR),
        },
        HeartbeatWriteFailure {
            label: "the write did not commit in time",
            failure: uncommitted(COORDINATOR_NOT_AVAILABLE),
            expected: KafkaErrorCode(COORDINATOR_NOT_AVAILABLE),
        },
    ]
}

/// A successful leave appends exactly one batch containing a tombstone.
pub(crate) async fn assert_next_tombstone_batch(log: &InMemoryOffsetsLog, pre_leave: usize) {
    let batches = log.batches().await;
    assert2::assert!(batches.len() == pre_leave + 1);
    let leave_batch = &batches[batches.len() - 1];
    assert2::assert!(
        leave_batch.records.iter().any(|r| r.value.is_none()),
        "leave batch must contain at least one tombstone"
    );
}

pub(crate) type AssignmentIntervalRow = (&'static str, Option<i64>, (i32, bool));

/// Replayed assignment timestamps outside, inside and beyond the assignment interval.
pub(crate) const ASSIGNMENT_INTERVAL_CASES: [AssignmentIntervalRow; 3] = [
    ("no stored time", None, (3, true)),
    ("an assignment a second ago", Some(1_000), (2, false)),
    ("an assignment two minutes ago", Some(120_000), (3, true)),
];

/// Fresh independent result rows for the assignment-interval replay scenarios.
pub(crate) fn assignment_interval_fixture()
-> ([AssignmentIntervalRow; 3], AssignmentIntervalResults) {
    (
        ASSIGNMENT_INTERVAL_CASES,
        AssignmentIntervalResults::default(),
    )
}

/// Both next-generation protocols fence two expired members without changing the target epoch.
pub(crate) fn check_two_members_expired(actual: (usize, i32, i32)) {
    assert2::check!(actual == (0, 4, 2));
}

type AssignmentAnswer = (&'static str, i32, bool);

/// Compare each assignment epoch and its timestamp window with an independent expected row.
#[derive(Default)]
pub(crate) struct AssignmentIntervalResults {
    observed: Vec<AssignmentAnswer>,
    expected: Vec<AssignmentAnswer>,
}

impl AssignmentIntervalResults {
    pub(crate) fn record(
        &mut self,
        case: &'static str,
        epoch: i32,
        window: (i64, i64),
        written: i64,
        expected: (i32, bool),
    ) {
        self.observed
            .push((case, epoch, (window.0..=window.1).contains(&written)));
        self.expected.push((case, expected.0, expected.1));
    }

    pub(crate) fn check(&self) {
        assert2::check!(self.observed == self.expected);
    }
}

/// The recovered seed, absence, or failure reported by a replay case.
pub(crate) type ReplayOutcome<S> = Result<Option<S>, String>;

/// The explicit mutations a case applies to its expected seed.
pub(crate) type SeedUpdate<'a, S> = &'a dyn Fn(&mut S);

/// Expected successful replay, built only from the fixture's explicitly supplied changes.
pub(crate) fn expected_seed<S>(
    seed: impl Fn() -> S,
) -> impl Fn(SeedUpdate<'_, S>) -> ReplayOutcome<S> {
    move |update| Ok(Some(updated_seed(seed(), update)))
}

#[derive(Clone, Copy, Default)]
pub(crate) enum AssignmentDeletionProtocol {
    #[default]
    Share,
    Streams,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct AssignmentDeletionSetup<'a> {
    #[default("m")]
    pub member: &'a str,
    pub protocol: AssignmentDeletionProtocol,
}

/// Kafka removes current and target assignments before member metadata.
#[bon::builder]
pub(crate) fn assignment_tombstones<R>(
    mut tombstone: impl FnMut(&str, &str) -> R,
    #[builder(default = "m")] member: &'_ str,
    #[builder(default)] protocol: AssignmentDeletionProtocol,
) -> Vec<R> {
    let setup = AssignmentDeletionSetup { member, protocol };
    let mut records: Vec<R> = [
        ("current", setup.member),
        ("target", setup.member),
        ("target-epoch", ""),
        ("member", setup.member),
    ]
    .into_iter()
    .map(|(kind, id)| tombstone(kind, id))
    .collect();
    let suffix = match setup.protocol {
        AssignmentDeletionProtocol::Share => ["state", "group"],
        AssignmentDeletionProtocol::Streams => ["group", "topology"],
    };
    records.extend(suffix.iter().map(|kind| tombstone(kind, "")));
    records
}

/// The replay oracle and ordered Kafka deletion records for one protocol.
#[bon::builder]
pub(crate) fn replay_fixtures<S, R>(
    seed: impl Fn() -> S,
    tombstone: impl Fn(&str, &str) -> R,
    #[builder(default = "m")] member: &'_ str,
    #[builder(default)] protocol: AssignmentDeletionProtocol,
) -> (
    impl Fn(SeedUpdate<'_, S>) -> ReplayOutcome<S>,
    impl Fn() -> Vec<R>,
) {
    let setup = AssignmentDeletionSetup { member, protocol };
    (expected_seed(seed), move || {
        assignment_tombstones()
            .tombstone(&tombstone)
            .member(setup.member)
            .protocol(setup.protocol)
            .call()
    })
}

/// A member deletion that leaves its target assignment behind.
pub(crate) fn undeleted_target_records<R>(
    member: R,
    target: R,
    mut tombstone: impl FnMut(&str, &str) -> R,
) -> Vec<R> {
    vec![
        member,
        target,
        tombstone("current", "m"),
        tombstone("member", "m"),
    ]
}

/// Bind protocol-specific keys to a test record without sharing the replay implementation.
pub(crate) fn replay_tombstone_constructor<K, R>(
    key: fn(&str, &str) -> K,
    record: fn(K) -> R,
) -> impl Fn(&str, &str) -> R + Copy {
    move |kind, id| record(key(kind, id))
}

/// Independent rejection case when a member tombstone leaves its target assignment behind.
pub(crate) fn member_deletion_with_target_case<R, S>(
    (member, target): (R, R),
    tombstone: impl FnMut(&str, &str) -> R,
    error: &str,
) -> ReplayCase<'static, R, S> {
    (
        "a member tombstone while the member has a target",
        undeleted_target_records(member, target, tombstone),
        Err(error.into()),
    )
}

/// Independent Kafka expectation for deleting a group while its member remains.
pub(crate) fn group_deletion_with_member_case<R, S>(
    member: R,
    mut tombstone: impl FnMut(&str, &str) -> R,
) -> ReplayCase<'static, R, S> {
    (
        "a group tombstone with a member left",
        vec![
            member,
            tombstone("target-epoch", ""),
            tombstone("group", ""),
        ],
        Err(
            "Received a tombstone record to delete group g but the group still has 1 members."
                .into(),
        ),
    )
}

/// Build the expected seed after applying a case's changes.
pub(crate) fn updated_seed<S>(mut seed: S, update: &dyn Fn(&mut S)) -> S {
    update(&mut seed);
    seed
}

/// Replay a complete case, observing the seed only after successful replay.
pub(crate) fn replay_outcome<R, S>(
    records: &[R],
    replay: impl FnMut(&R) -> Result<(), BrokerError>,
    seed: impl FnOnce() -> Option<S>,
) -> Result<Option<S>, String> {
    records
        .iter()
        .try_for_each(replay)
        .map(|()| seed())
        .map_err(|error| match error {
            BrokerError::Startup(message) => message,
            other => other.to_string(),
        })
}

/// Independently supplied records and expected replay results for one protocol.
pub(crate) type ReplayCase<'a, R, S> = (&'a str, Vec<R>, ReplayOutcome<S>);

/// The independent absence expectations shared by every modern protocol's deletion replay.
pub(crate) fn deletion_replay_cases<R: Clone, S>(
    mut full_group: Vec<R>,
    deletion: Vec<R>,
) -> [ReplayCase<'static, R, S>; 2] {
    full_group.extend(deletion.iter().cloned());
    [
        (
            "Kafka's deletion order removes the whole group",
            full_group,
            Ok(None),
        ),
        (
            "tombstones of a group the log does not hold are ignored",
            deletion,
            Ok(None),
        ),
    ]
}

/// Each protocol checks both its replay result and the cache populated by replay.
pub(crate) fn check_replay_cases<R, S: PartialEq + std::fmt::Debug>(
    rows: &[ReplayCase<'_, R, S>],
    replay: impl Fn(&GroupCoordinator, &R) -> Result<(), BrokerError>,
    seed: impl Fn(&GroupCoordinator) -> Option<S>,
    cached: impl Fn(&GroupCoordinator) -> Option<S>,
) {
    for (case, log, expected) in rows {
        let coordinator = make_coord();
        let outcome = replay_outcome(
            log,
            |record| replay(&coordinator, record),
            || seed(&coordinator),
        );
        assert2::check!(&outcome == expected, "{case}");
        if let Ok(seed) = outcome {
            assert2::check!(cached(&coordinator) == seed, "{case}");
        }
    }
}

/// Streams member without instance/rack/endpoint/tags, for persisted seeds.
pub(crate) fn plain_streams_member(
    topology_epoch: i32,
) -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: "c1".into(),
        client_host: "/127.0.0.1".into(),
        process_id: "p1".into(),
        user_endpoint: None,
        client_tags: vec![],
        rebalance_timeout_ms: 60_000,
        topology_epoch,
    }
}

/// First consumer heartbeat, with an empty owned assignment and the standard rebalance timeout.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ConsumerJoinSetup<'a> {
    #[default("g")]
    pub group_id: &'a str,
    #[default("m1")]
    pub member_id: &'a str,
    #[default(&["t"])]
    pub topics: &'a [&'a str],
}

#[bon::builder]
pub(crate) fn consumer_join_request(
    #[builder(default = "g")] group_id: &'_ str,
    #[builder(default = "m1")] member_id: &'_ str,
    #[builder(default = & ["t"])] topics: &'_ [&'_ str],
) -> krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest {
    let setup = ConsumerJoinSetup {
        group_id,
        member_id,
        topics,
    };
    krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest {
        group_id: setup.group_id.into(),
        member_id: setup.member_id.into(),
        member_epoch: 0,
        subscribed_topic_names: Some(setup.topics.iter().map(|topic| (*topic).into()).collect()),
        rebalance_timeout_ms: 60_000,
        topic_partitions: Some(vec![]),
        ..Default::default()
    }
}

/// A session seen fifty milliseconds ago, preserving each expiry fixture's clock read.
pub(crate) fn expired_session_last_seen() -> std::time::Instant {
    std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(50))
        .expect("50ms is always within Instant range")
}

/// Independent tombstone rows for a fenced member, parameterized by record family.
pub(crate) fn member_tombstones<T>(member_id: &str) -> Vec<(String, Option<T>)> {
    vec![(member_id.into(), None)]
}

#[derive(Clone, Copy)]
pub(crate) struct TargetAssignmentEpoch(pub i32);

/// Independent Kafka bytes for the default and tagged assignment timestamps.
pub(crate) struct AssignmentMetadataGoldenCase {
    pub description: &'static str,
    pub epoch: TargetAssignmentEpoch,
    pub timestamp: UnixMillis,
    pub bytes: &'static [u8],
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct AssignmentMetadataGoldenSetup {
    #[default("the time is unknown")]
    pub unknown_time_description: &'static str,
}

#[bon::builder]
pub(crate) fn assignment_metadata_golden_cases(
    #[builder(default = "the time is unknown")] unknown_time_description: &'static str,
) -> [AssignmentMetadataGoldenCase; 2] {
    let setup = AssignmentMetadataGoldenSetup {
        unknown_time_description,
    };
    [
        AssignmentMetadataGoldenCase {
            description: setup.unknown_time_description,
            epoch: TargetAssignmentEpoch(12),
            timestamp: UnixMillis(0),
            bytes: b"\x00\x00\x00\x00\x00\x0c\x00",
        },
        AssignmentMetadataGoldenCase {
            description: "an assignment that finished at 2026-10-07T00:00:00Z",
            epoch: TargetAssignmentEpoch(12),
            timestamp: UnixMillis(1_791_331_200_000),
            bytes: b"\x00\x00\x00\x00\x00\x0c\x01\x00\x08\x00\x00\x01\xa1\x13\xa8\xec\x00",
        },
    ]
}
