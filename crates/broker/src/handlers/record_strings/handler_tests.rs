//! Each group protocol's join or heartbeat handler takes a group id, member id,
//! instance id or client id of 32767 bytes, the longest a coordinator record can
//! carry, and refuses an id of 32768 bytes before the request reaches any
//! group. `InitializeShareGroupState` does the same with its group id.
//!
//! The refusal is what Kafka's request reader does with such a string: a
//! protocol error, which closes the connection. It is not an error response,
//! and it must not be a panic in the group actor that would write the record.
//! The client id is a header field with an `INT16` length, so 32767 bytes is
//! the longest one that can arrive.
//!
//! The member id that a classic group generates from an id of 32731 bytes or
//! more is over the bound, and Kafka cannot write it. The join answers with an
//! error and no member id.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::{self, ConsumerGroupHeartbeatRequest},
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    heartbeat_request::{self, HeartbeatRequest},
    initialize_share_group_state_request::{
        self, InitializeShareGroupStateRequest, InitializeStateData, PartitionData,
    },
    initialize_share_group_state_response::InitializeShareGroupStateResponse,
    join_group_request::{self, JoinGroupRequest, JoinGroupRequestProtocol},
    join_group_response::JoinGroupResponse,
    share_group_heartbeat_request::{self, ShareGroupHeartbeatRequest},
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    streams_group_heartbeat_request::{self, StreamsGroupHeartbeatRequest},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    sync_group_request::{self, SyncGroupRequest, SyncGroupRequestAssignment},
    sync_group_response::SyncGroupResponse,
};
use krabka_security::Principal;

use crate::{
    broker::{Broker, BrokerHandle},
    codes,
    error::BrokerError,
    test_support::{decode_response, encode_request, peer},
};

/// The longest string a coordinator record carries.
const LIMIT: usize = 32_767;

/// Which of a request's ids is the long one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Long {
    Group,
    Member,
    Instance,
    /// The client id of the request header.
    Client,
}

/// Every id at each side of the bound: `(the long id, its length)`. The client id
/// is a header field, which its `INT16` length bounds at 32767 bytes, so only the
/// longest one can arrive.
fn cases() -> impl Iterator<Item = (Long, usize)> {
    [Long::Group, Long::Member, Long::Instance]
        .into_iter()
        .flat_map(|long| {
            [LIMIT, LIMIT + 1]
                .into_iter()
                .map(move |length| (long, length))
        })
        .chain([(Long::Client, LIMIT)])
}

/// The client id of the header of a request of `long` at `length`.
fn header_client_id(long: Long, length: usize) -> String {
    if long == Long::Client {
        "a".repeat(length)
    } else {
        "record-strings-test".to_owned()
    }
}

/// The group id, member id and instance id of case `case`, with the one that
/// is `long` set to `length` bytes and the others short.
fn ids(case: usize, long: Long, length: usize, member: &str) -> (String, String, String) {
    let pick = |which: Long, short: String| {
        if which == long {
            "a".repeat(length)
        } else {
            short
        }
    };
    (
        pick(Long::Group, format!("record-strings-{case}")),
        pick(Long::Member, member.to_owned()),
        pick(Long::Instance, format!("instance-{case}")),
    )
}

/// A handler takes an id of 32767 bytes, and refuses one of 32768 bytes with a
/// protocol error.
fn check(label: &str, result: &Result<Bytes, BrokerError>, length: usize) {
    if length > LIMIT {
        assert!(
            matches!(result, Err(BrokerError::Protocol(_))),
            "{label}: {result:?}"
        );
    } else {
        assert!(result.is_ok(), "{label}: {result:?}");
    }
}

/// A running broker with the group, share and streams protocols on.
struct Env {
    handle: BrokerHandle,
    broker: Arc<Broker>,
    _dir: tempfile::TempDir,
    principal: Principal,
    peer: SocketAddr,
}

impl Env {
    async fn start() -> Self {
        let (handle, dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.streams_group.enable = true;
            cfg.classic_group_initial_rebalance_delay = krabka_units::secs(0);
        })
        .await;
        handle.wait_until_group_coordinator_ready().await;
        let broker = handle.broker_arc_for_test();
        for feature in [
            krabka_metadata::group_version::GROUP_VERSION_FEATURE,
            krabka_metadata::metadata_version::SHARE_VERSION_FEATURE,
            crate::features::STREAMS_VERSION,
        ] {
            finalize(&broker, feature).await;
        }
        create_source_topic(&broker, "in").await;
        Self {
            handle,
            broker,
            _dir: dir,
            principal: crate::test_support::principal("ANONYMOUS"),
            peer: peer(),
        }
    }

    fn ctx<'a>(&'a self, client_id: &'a str) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(&self.principal, &self.peer, client_id)
    }

    async fn stop(self) {
        self.handle.shutdown().await;
    }
}

/// Keep encoding and context construction at the call site, before any timeout starts.
macro_rules! dispatch_record_request {
    ($env:ident, $api:expr, $version:expr, $request:expr, $client:expr) => {
        crate::test_support::try_dispatch_context(
            &$env.broker,
            $api,
            $version,
            &encode_request($request, $version),
            &$env.ctx($client),
        )
    };
}

/// Bind the independently labelled long-id case in the original declaration order.
macro_rules! long_identity {
    (($label:ident, $client:ident, $bindings:pat), $case:expr, $long:ident, $length:ident, $member:expr) => {
        let $label = format!("{:?} of {} bytes", $long, $length);
        let $client = header_client_id($long, $length);
        let $bindings = ids($case, $long, $length, $member);
    };
}

/// Create a one-partition topic, so that a streams topology can read it.
async fn create_source_topic(broker: &Broker, name: &str) {
    use krabka_metadata::TopicRecord;

    let node_id = krabka_audit::NodeId(broker.config.node_id.0);
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(crate::handlers::test_support::single_replica_partition(
                name,
                krabka_ids::PartitionIndex(0),
                node_id,
            )),
        ])
        .await
        .expect("create the source topic");
}

/// Finalize `feature` at level 1 and wait until the image shows it.
async fn finalize(broker: &Broker, feature: &'static str) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: feature.into(),
            level: 1,
        })])
        .await
        .expect("finalize the feature");
    tokio::time::timeout(Duration::from_secs(5), async {
        while broker.controller.current_image().finalized_feature(feature) != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the image shows the feature");
}

/// Each protocol keeps its request and independent outcome checks in the same case scope.
macro_rules! long_identity_cases {
    ($api:ident, ($env:ident, $version:ident), ($case:ident, $long:ident, $length:ident),
     ($label:ident, $client_id:ident, ($group_id:ident, $member_id:ident, $instance_id:ident)), $member:expr, { $($body:tt)* }) => {
        const $version: i16 = $api::MAX_VERSION;
        let $env = Env::start().await;
        for ($case, ($long, $length)) in cases().enumerate() {
            long_identity!(($label, $client_id, ($group_id, $member_id, $instance_id)), $case, $long, $length, $member);
            $($body)*
        }
        $env.stop().await;
    };
}

#[tokio::test]
async fn consumer_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    long_identity_cases!(
        consumer_group_heartbeat_request,
        (env, VERSION),
        (case, long, length),
        (label, client_id, (group_id, member_id, instance_id)),
        "member-short",
        {
            let request = ConsumerGroupHeartbeatRequest {
                group_id: group_id.clone(),
                member_id,
                instance_id: Some(instance_id),
                member_epoch: 0,
                rebalance_timeout_ms: 30_000,
                topic_partitions: Some(vec![]),
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            };

            let result = dispatch_record_request!(
                env,
                consumer_group_heartbeat_request::API_KEY,
                VERSION,
                &request,
                &client_id
            )
            .await;

            check(&label, &result, length);
            let response = result
                .ok()
                .map(|bytes| decode_response::<ConsumerGroupHeartbeatResponse>(&bytes, VERSION));
            assert!(
                env.broker.group_coordinator.find(&group_id).is_some() == (length <= LIMIT),
                "{label}: the group exists only when the request was taken: {response:?}"
            );
            assert!(
                response.is_none_or(|response| response.error_code == codes::NONE),
                "{label}: the join succeeds"
            );
        }
    );
}

#[tokio::test]
async fn share_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = share_group_heartbeat_request::MAX_VERSION;
    let env = Env::start().await;
    // A share group has no instance id.
    for (case, (long, length)) in cases()
        .filter(|(long, _)| *long != Long::Instance)
        .enumerate()
    {
        long_identity!(
            (label, client_id, (group_id, member_id, _)),
            case,
            long,
            length,
            "member-short"
        );
        let request = ShareGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id,
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };

        let result = dispatch_record_request!(
            env,
            share_group_heartbeat_request::API_KEY,
            VERSION,
            &request,
            &client_id
        )
        .await;

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<ShareGroupHeartbeatResponse>(&bytes, VERSION));
        assert!(
            env.broker.group_coordinator.find_share(&group_id).is_some()
                == (length <= LIMIT && long != Long::Member),
            "{label}: the group exists only when a join was taken: {response:?}"
        );
        // Kafka's `isMemberIdValid` refuses a share member id over 36
        // characters, so a long one is answered `INVALID_REQUEST` and creates
        // no group.
        let want = if long == Long::Member {
            codes::INVALID_REQUEST
        } else {
            codes::NONE
        };
        assert!(
            response.is_none_or(|response| response.error_code == want),
            "{label}: the answer is {want}"
        );
    }
    env.stop().await;
}

#[tokio::test]
async fn streams_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    long_identity_cases!(
        streams_group_heartbeat_request,
        (env, VERSION),
        (case, long, length),
        (label, client_id, (group_id, member_id, instance_id)),
        "member-short",
        {
            let request = StreamsGroupHeartbeatRequest {
                instance_id: (long == Long::Instance).then_some(instance_id),
                ..crate::handlers::group_heartbeat_test_support::streams_request(
                    &group_id, &member_id,
                )
            };

            let result = dispatch_record_request!(
                env,
                streams_group_heartbeat_request::API_KEY,
                VERSION,
                &request,
                &client_id
            )
            .await;

            check(&label, &result, length);
            let response = result
                .ok()
                .map(|bytes| decode_response::<StreamsGroupHeartbeatResponse>(&bytes, VERSION));
            // Kafka 4.3.1 refuses a streams member with an instance id (static
            // membership) as `INVALID_REQUEST`, so that request creates no group.
            assert!(
                env.broker
                    .group_coordinator
                    .find_streams(&group_id)
                    .is_some()
                    == (length <= LIMIT && long != Long::Instance),
                "{label}: the group exists only when a join was taken: {response:?}"
            );
        }
    );
}

#[tokio::test]
async fn classic_join_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = join_group_request::MAX_VERSION;
    let env = Env::start().await;
    for (case, (long, length)) in cases().enumerate() {
        let label = format!("{long:?} of {length} bytes");
        let client_id = header_client_id(long, length);
        // A static member with no member id yet, which is the join that
        // records the group and its member in one round.
        let (group_id, member_id, instance_id) = ids(case, long, length, "");
        let request = JoinGroupRequest {
            group_id: group_id.clone(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            member_id,
            group_instance_id: Some(instance_id),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = tokio::time::timeout(
            Duration::from_secs(20),
            dispatch_record_request!(
                env,
                join_group_request::API_KEY,
                VERSION,
                &request,
                &client_id
            ),
        )
        .await
        .expect("the join answers");

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<JoinGroupResponse>(&bytes, VERSION));
        // A join by an unknown member id creates no group, whatever its length.
        assert!(
            env.broker.group_coordinator.find(&group_id).is_some()
                == (length <= LIMIT && long != Long::Member),
            "{label}: the group exists only when a join was taken: {response:?}"
        );
        // The member id of a static member with no id is its instance id, a
        // hyphen and a UUID, which is too long for a record when the instance
        // id has 32767 bytes.
        let want = match long {
            Long::Member => codes::UNKNOWN_MEMBER_ID,
            Long::Instance if length == LIMIT => codes::UNKNOWN_SERVER_ERROR,
            Long::Group | Long::Instance | Long::Client => codes::NONE,
        };
        assert!(
            response.is_none_or(|response| response.error_code == want),
            "{label}: the answer is {want}"
        );
    }
    env.stop().await;
}

/// The bytes that `ClassicGroup.generateMemberId` adds to the instance id or
/// the client id: a hyphen and a UUID of 36 characters.
const GENERATED_SUFFIX: usize = 37;

/// A `JoinGroup` of `request` at `version` from `client_id`, answered.
async fn join(
    env: &Env,
    request: &JoinGroupRequest,
    version: i16,
    client_id: &str,
) -> JoinGroupResponse {
    let bytes = tokio::time::timeout(
        Duration::from_secs(20),
        dispatch_record_request!(
            env,
            join_group_request::API_KEY,
            version,
            request,
            client_id
        ),
    )
    .await
    .expect("the join answers")
    .expect("the join is not a protocol error");
    decode_response::<JoinGroupResponse>(&bytes, version)
}

/// The member id that a classic group generates is the instance id, or the
/// client id of a member with no instance id, a hyphen and a UUID
/// (`ClassicGroup.generateMemberId`). Both prefixes take 32767 bytes, so the id
/// can have up to 32804 bytes. A `JoinGroup` before version 6 writes its ids
/// with an `INT16` length, and so does the group metadata record that the
/// leader's `SyncGroup` appends. Kafka's generated writers refuse an id over
/// 32767 bytes, so there the join fails with an internal error. Here the join
/// answers `UNKNOWN_SERVER_ERROR` with no member id before it changes the
/// group, and an id of exactly 32767 bytes joins, syncs and is recorded.
#[tokio::test]
async fn a_generated_member_id_over_32767_bytes_fails_the_join() {
    const SYNC_VERSION: i16 = sync_group_request::MAX_VERSION;
    /// The length of the prefix that makes an id of exactly 32767 bytes.
    const LONGEST_PREFIX: usize = LIMIT - GENERATED_SUFFIX;

    struct Row {
        name: &'static str,
        /// `true` for a static member, whose prefix is its instance id, and
        /// `false` for a dynamic member, whose prefix is its client id.
        static_member: bool,
        prefix: usize,
        version: i16,
        joins: bool,
    }
    let rows = [
        Row {
            name: "static member, the longest instance id that fits, v5",
            static_member: true,
            prefix: LONGEST_PREFIX,
            version: 5,
            joins: true,
        },
        Row {
            name: "static member, one byte more, v5",
            static_member: true,
            prefix: LONGEST_PREFIX + 1,
            version: 5,
            joins: false,
        },
        Row {
            name: "static member, the longest instance id that fits, v9",
            static_member: true,
            prefix: LONGEST_PREFIX,
            version: 9,
            joins: true,
        },
        Row {
            name: "static member, one byte more, v9",
            static_member: true,
            prefix: LONGEST_PREFIX + 1,
            version: 9,
            joins: false,
        },
        Row {
            name: "dynamic member, the longest client id that fits, v5",
            static_member: false,
            prefix: LONGEST_PREFIX,
            version: 5,
            joins: true,
        },
        Row {
            name: "dynamic member, one byte more, v5",
            static_member: false,
            prefix: LONGEST_PREFIX + 1,
            version: 5,
            joins: false,
        },
        Row {
            name: "dynamic member, the longest client id, v5",
            static_member: false,
            prefix: LIMIT,
            version: 5,
            joins: false,
        },
        Row {
            name: "dynamic member, the longest client id that fits, v3",
            static_member: false,
            prefix: LONGEST_PREFIX,
            version: 3,
            joins: true,
        },
        Row {
            name: "dynamic member, one byte more, v3",
            static_member: false,
            prefix: LONGEST_PREFIX + 1,
            version: 3,
            joins: false,
        },
    ];

    let env = Env::start().await;
    for (case, row) in rows.into_iter().enumerate() {
        let group_id = format!("generated-member-id-{case}");
        let instance_id = row.static_member.then(|| "i".repeat(row.prefix));
        let client_id = if row.static_member {
            "record-strings-test".to_owned()
        } else {
            "c".repeat(row.prefix)
        };
        let mut request = JoinGroupRequest {
            group_id: group_id.clone(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            group_instance_id: instance_id.clone(),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let mut response = join(&env, &request, row.version, &client_id).await;
        if response.error_code == codes::MEMBER_ID_REQUIRED {
            // Version 4 and later hand a dynamic member its id before it joins.
            assert!(
                response.member_id.len() == LIMIT,
                "{}: the id answered is {} bytes",
                row.name,
                response.member_id.len()
            );
            request.member_id.clone_from(&response.member_id);
            response = join(&env, &request, row.version, &client_id).await;
        }

        if !row.joins {
            assert!(
                (response.error_code, response.member_id.as_str())
                    == (codes::UNKNOWN_SERVER_ERROR, ""),
                "{}: {response:?}",
                row.name
            );
            continue;
        }
        assert!(
            response.error_code == codes::NONE
                && response.member_id.len() == LIMIT
                && response.leader == response.member_id,
            "{}: the join answers with an id of {} bytes: {:?}",
            row.name,
            response.member_id.len(),
            response.error_code
        );

        // The leader's `SyncGroup` appends the group metadata record, which
        // carries the id, the instance id and the client id.
        let sync = SyncGroupRequest {
            group_id,
            generation_id: response.generation_id,
            member_id: response.member_id.clone(),
            group_instance_id: instance_id,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: response.member_id.clone(),
                assignment: Bytes::from_static(b"assignment"),
                ..Default::default()
            }],
            ..Default::default()
        };
        let synced = dispatch_record_request!(
            env,
            sync_group_request::API_KEY,
            SYNC_VERSION,
            &sync,
            &client_id
        )
        .await
        .expect("the sync is not a protocol error");
        let synced = decode_response::<SyncGroupResponse>(&synced, SYNC_VERSION);
        assert!(
            (synced.error_code, synced.assignment.as_ref())
                == (codes::NONE, b"assignment".as_slice()),
            "{}: {synced:?}",
            row.name
        );
    }
    env.stop().await;
}

/// `InitializeShareGroupState` is the one request that creates share-state
/// keys, so its group id is the string of a `__share_group_state` record key.
/// Kafka's reader closes the connection on a group id of 32768 bytes, and a
/// group id of 32767 bytes is taken.
#[tokio::test]
async fn initialize_share_group_state_takes_a_32767_byte_group_id_and_refuses_32768() {
    const VERSION: i16 = initialize_share_group_state_request::MAX_VERSION;
    let env = Env::start().await;
    env.handle.wait_until_share_coordinator_ready().await;
    let topic_id = env
        .broker
        .controller
        .current_image()
        .topic("in")
        .expect("the source topic")
        .topic_id;
    for length in [LIMIT, LIMIT + 1] {
        let label = format!("group id of {length} bytes");
        let request = InitializeShareGroupStateRequest {
            group_id: "a".repeat(length),
            topics: vec![InitializeStateData {
                topic_id: krabka_protocol::primitives::uuid::Uuid(topic_id.into_bytes()),
                partitions: vec![PartitionData {
                    partition: 0,
                    state_epoch: 1,
                    start_offset: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = dispatch_record_request!(
            env,
            initialize_share_group_state_request::API_KEY,
            VERSION,
            &request,
            "record-strings-test"
        )
        .await;

        check(&label, &result, length);
        if let Ok(bytes) = result {
            let response = decode_response::<InitializeShareGroupStateResponse>(&bytes, VERSION);
            let error_codes: Vec<i16> = response
                .results
                .iter()
                .flat_map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .map(|partition| partition.error_code)
                })
                .collect();
            assert!(error_codes == vec![codes::NONE], "{label}: {response:?}");
        }
    }
    env.stop().await;
}

#[tokio::test]
async fn classic_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    long_identity_cases!(
        heartbeat_request,
        (env, VERSION),
        (case, long, length),
        (label, client_id, (group_id, member_id, instance_id)),
        "member-short",
        {
            let request = HeartbeatRequest {
                group_id,
                generation_id: 1,
                member_id,
                group_instance_id: Some(instance_id),
                ..Default::default()
            };

            // Through the dispatch adapter, which decodes the request.
            let result = dispatch_record_request!(
                env,
                heartbeat_request::API_KEY,
                VERSION,
                &request,
                &client_id
            )
            .await;

            check(&label, &result, length);
        }
    );
}
