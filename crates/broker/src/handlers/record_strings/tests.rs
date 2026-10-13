use {
    super::*,
    assert2::assert,
    bytes::BytesMut,
    krabka_protocol::{
        Encode,
        owned::{
            alter_share_group_offsets_request::{
                self, AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestTopic,
            },
            consumer_group_heartbeat_request::{self, ConsumerGroupHeartbeatRequest},
            delete_groups_request::{self, DeleteGroupsRequest},
            delete_share_group_offsets_request::{
                self, DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
            },
            delete_share_group_state_request::{self, DeleteShareGroupStateRequest},
            heartbeat_request::{self, HeartbeatRequest},
            init_producer_id_request::{self, InitProducerIdRequest},
            initialize_share_group_state_request::{self, InitializeShareGroupStateRequest},
            join_group_request::{self, JoinGroupRequest, JoinGroupRequestProtocol},
            leave_group_request::{self, LeaveGroupRequest, MemberIdentity},
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            read_share_group_state_request::{self, ReadShareGroupStateRequest},
            read_share_group_state_summary_request::{self, ReadShareGroupStateSummaryRequest},
            share_group_heartbeat_request::{self, ShareGroupHeartbeatRequest},
            streams_group_heartbeat_request::{self, StreamsGroupHeartbeatRequest},
            sync_group_request::{self, SyncGroupRequest, SyncGroupRequestAssignment},
            txn_offset_commit_request::{
                TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition,
                TxnOffsetCommitRequestTopic,
            },
            write_share_group_state_request::{self, WriteShareGroupStateRequest},
        },
    },
};

/// The longest string Kafka's reader accepts.
const LIMIT: usize = 32_767;

/// A request field set from a string, with the name the case reports.
struct Field<R> {
    name: &'static str,
    set: fn(&mut R, String),
}

/// Encode `request` at `version` and decode it as a handler does.
fn decoded<R>(request: &R, version: i16) -> Result<R, BrokerError>
where
    R: Encode + for<'de> Decode<'de> + RecordStrings,
{
    let mut buf = BytesMut::with_capacity(request.encoded_len(version));
    request.encode(&mut buf, version).expect("encode request");
    let mut cur: &[u8] = &buf;
    decode_group_request(&mut cur, version)
}

/// Every one of `fields`, set to a string of 32767 bytes, decodes, and set to
/// one of 32768 bytes is refused as a protocol error, as Kafka's reader
/// refuses it.
fn check_request<R>(version: i16, base: &R, fields: &[Field<R>])
where
    R: Clone + Encode + for<'de> Decode<'de> + RecordStrings,
{
    for Field { name, set } in fields {
        for (length, accepted) in [(LIMIT, true), (LIMIT + 1, false)] {
            let mut request = base.clone();
            set(&mut request, "a".repeat(length));

            let outcome = decoded(&request, version);

            assert!(
                outcome.is_ok() == accepted,
                "{name} of {length} bytes: {:?}",
                outcome.as_ref().err()
            );
            assert!(
                accepted || matches!(outcome, Err(BrokerError::Protocol(_))),
                "{name} of {length} bytes is a protocol error"
            );
        }
    }
}

/// The independent field table names every record string exercised by a case.
macro_rules! string_mutations {
    ($($label:literal => $kind:ident[$($path:tt)+]),* $(,)?) => {
        &[$(Field { name: $label, set: |request, value| string_mutations!(@$kind request, value; $($path)+) },)*]
    };
    (@required $request:ident, $value:ident; $($path:tt)+) => { $request.$($path)+ = $value };
    (@optional $request:ident, $value:ident; $($path:tt)+) => { $request.$($path)+ = Some($value) };
}

#[test]
fn a_request_with_a_record_string_over_32767_bytes_is_refused() {
    check_request(
        join_group_request::MAX_VERSION,
        &JoinGroupRequest {
            protocols: vec![JoinGroupRequestProtocol::default()],
            ..Default::default()
        },
        string_mutations!("group id" => required[group_id], "member id" => required[member_id], "instance id" => optional[group_instance_id], "protocol type" => required[protocol_type], "protocol name" => required[protocols[0].name]),
    );
    check_request(
        sync_group_request::MAX_VERSION,
        &SyncGroupRequest {
            assignments: vec![SyncGroupRequestAssignment::default()],
            ..Default::default()
        },
        string_mutations!("group id" => required[group_id], "member id" => required[member_id], "instance id" => optional[group_instance_id], "protocol type" => optional[protocol_type], "protocol name" => optional[protocol_name], "assigned member id" => required[assignments[0].member_id]),
    );
    check_request(
        heartbeat_request::MAX_VERSION,
        &HeartbeatRequest::default(),
        string_mutations!("group id" => required[group_id], "member id" => required[member_id], "instance id" => optional[group_instance_id]),
    );
    check_request(
        leave_group_request::MAX_VERSION,
        &LeaveGroupRequest {
            members: vec![MemberIdentity::default()],
            ..Default::default()
        },
        string_mutations!("group id" => required[group_id], "member id" => required[members[0].member_id], "instance id" => optional[members[0].group_instance_id]),
    );
    macro_rules! offset_commit_mutations {
        ($($extra:expr),* $(,)?) => {
            &[$($extra,)*
                Field { name: "group id", set: |r, s| r.group_id = s },
                Field { name: "member id", set: |r, s| r.member_id = s },
                Field { name: "instance id", set: |r, s| r.group_instance_id = Some(s) },
                Field { name: "topic name", set: |r, s| r.topics[0].name = s },
                Field { name: "metadata", set: |r, s| r.topics[0].partitions[0].committed_metadata = Some(s) },
            ]
        };
    }
    // The last version of each that names its topics: the next one names them by
    // id.
    check_request(
        9,
        &OffsetCommitRequest {
            topics: vec![OffsetCommitRequestTopic {
                partitions: vec![OffsetCommitRequestPartition::default()],
                ..Default::default()
            }],
            ..Default::default()
        },
        offset_commit_mutations!(),
    );
    check_request(
        5,
        &TxnOffsetCommitRequest {
            topics: vec![TxnOffsetCommitRequestTopic {
                partitions: vec![TxnOffsetCommitRequestPartition::default()],
                ..Default::default()
            }],
            ..Default::default()
        },
        offset_commit_mutations!(Field {
            name: "transactional id",
            set: |r, s| r.transactional_id = s
        }),
    );
}

#[test]
fn a_group_heartbeat_with_an_id_over_32767_bytes_is_refused() {
    check_request(
        consumer_group_heartbeat_request::MAX_VERSION,
        &ConsumerGroupHeartbeatRequest::default(),
        string_mutations!("group id" => required[group_id], "member id" => required[member_id], "instance id" => optional[instance_id], "topic regex" => optional[subscribed_topic_regex]),
    );
    check_request(
        share_group_heartbeat_request::MAX_VERSION,
        &ShareGroupHeartbeatRequest::default(),
        string_mutations!("group id" => required[group_id], "member id" => required[member_id]),
    );
    check_request(
        streams_group_heartbeat_request::MAX_VERSION,
        &StreamsGroupHeartbeatRequest::default(),
        string_mutations!("group id" => required[group_id], "member id" => required[member_id], "instance id" => optional[instance_id]),
    );
}

/// The share-state requests carry the group id of a `__share_group_state`
/// record key, and `InitProducerId` carries the transactional id of a
/// `__transaction_state` record key.
#[test]
fn a_state_request_with_an_id_over_32767_bytes_is_refused() {
    check_request(
        initialize_share_group_state_request::MAX_VERSION,
        &InitializeShareGroupStateRequest::default(),
        string_mutations!("group id" => required[group_id]),
    );
    check_request(
        read_share_group_state_request::MAX_VERSION,
        &ReadShareGroupStateRequest::default(),
        string_mutations!("group id" => required[group_id]),
    );
    check_request(
        read_share_group_state_summary_request::MAX_VERSION,
        &ReadShareGroupStateSummaryRequest::default(),
        string_mutations!("group id" => required[group_id]),
    );
    check_request(
        write_share_group_state_request::MAX_VERSION,
        &WriteShareGroupStateRequest::default(),
        string_mutations!("group id" => required[group_id]),
    );
    check_request(
        delete_share_group_state_request::MAX_VERSION,
        &DeleteShareGroupStateRequest::default(),
        string_mutations!("group id" => required[group_id]),
    );
    check_request(
        init_producer_id_request::MAX_VERSION,
        &InitProducerIdRequest::default(),
        string_mutations!("transactional id" => optional[transactional_id]),
    );
}

#[test]
fn a_group_admin_request_with_a_name_over_32767_bytes_is_refused() {
    check_request(
        delete_groups_request::MAX_VERSION,
        &DeleteGroupsRequest {
            groups_names: vec![String::new()],
            ..Default::default()
        },
        string_mutations!("group name" => required[groups_names[0]]),
    );
    check_request(
        alter_share_group_offsets_request::MAX_VERSION,
        &AlterShareGroupOffsetsRequest {
            topics: vec![AlterShareGroupOffsetsRequestTopic::default()],
            ..Default::default()
        },
        string_mutations!("group id" => required[group_id], "topic name" => required[topics[0].topic_name]),
    );
    check_request(
        delete_share_group_offsets_request::MAX_VERSION,
        &DeleteShareGroupOffsetsRequest {
            topics: vec![DeleteShareGroupOffsetsRequestTopic::default()],
            ..Default::default()
        },
        string_mutations!("group id" => required[group_id], "topic name" => required[topics[0].topic_name]),
    );
}
