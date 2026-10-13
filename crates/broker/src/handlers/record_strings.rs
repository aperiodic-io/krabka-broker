//! The bound on the strings a coordinator request hands to a record.
//!
//! Kafka's generated request readers refuse a string field longer than
//! `0x7fff` bytes: `MessageDataGenerator.generateVariableLengthReader` throws
//! `string field <name> had invalid length <n>`, `RequestContext.parseRequest`
//! wraps it in an `InvalidRequestException`, and
//! `SocketServer.Processor.processCompletedReceives` closes the connection
//! without a response. Only a flexible version can carry such a string, since
//! an older one has an `INT16` length.
//!
//! krabka's decoder accepts a longer compact string, and the coordinators
//! write every record key, and the classic group value, with an `INT16` string
//! length ([`put_string`](crate::coordinator::unified::persistence::put_string)).
//! [`decode_group_request`] refuses such a string at the handler boundary as
//! Kafka's reader does, before the request reaches any coordinator.
//!
//! That matches the wire behaviour of Kafka, and it is not the only guard. Every
//! record encoder refuses a string that does not fit an `INT16` length, and the
//! transition that asked for the write answers with the error, as Kafka
//! answers a record that its writer cannot serialize. A string that this list
//! misses therefore cannot panic a coordinator. The same holds for a string
//! that the broker derives: `handle_join` refuses a generated classic member id
//! over the bound before it changes the group.
//!
//! The strings that a record can carry are listed once, per request type, in
//! the [`RecordStrings`] impls below. A request that is not listed writes no
//! coordinator record with a string of its own, or has no flexible version
//! (`OffsetDelete`), so its strings cannot pass the `INT16` bound.
//!
//! The listing is not a wire-parity guard for every request. `ShareFetch`,
//! `ShareAcknowledge`, `DescribeShareGroupOffsets` and `FindCoordinator` are
//! flexible-only, and they decode a group id of more than 32767 bytes where
//! Kafka's reader would close the connection. No group with such an id can
//! exist, so none of them writes a record for it: the share requests find no
//! initialized share state, and the share-state key encoder refuses the id if a
//! request reaches it. `FindCoordinator` only names a coordinator. The barrier
//! requests have their own bound, in `barrier::handlers`, because their
//! strings go to `__barrier_state` and not to a group coordinator.

use {
    crate::{coordinator::unified::persistence::MAX_STRING_BYTES, error::BrokerError},
    krabka_protocol::{
        Decode,
        owned::{
            alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            delete_groups_request::DeleteGroupsRequest,
            delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
            delete_share_group_state_request::DeleteShareGroupStateRequest,
            heartbeat_request::HeartbeatRequest, init_producer_id_request::InitProducerIdRequest,
            initialize_share_group_state_request::InitializeShareGroupStateRequest,
            join_group_request::JoinGroupRequest, leave_group_request::LeaveGroupRequest,
            offset_commit_request::OffsetCommitRequest,
            read_share_group_state_request::ReadShareGroupStateRequest,
            read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
            streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
            sync_group_request::SyncGroupRequest,
            txn_offset_commit_request::TxnOffsetCommitRequest,
            write_share_group_state_request::WriteShareGroupStateRequest,
        },
    },
};

/// A group request whose strings a coordinator record can carry.
pub(crate) trait RecordStrings {
    /// The length in bytes of the longest string of the request that a
    /// coordinator record can carry: a group, member or instance id, a
    /// regular expression, a protocol type or name, a topic name, or offset
    /// metadata.
    fn longest_record_string(&self) -> usize;
}

/// Decode a group request, refusing one that carries a string Kafka's reader
/// would refuse.
///
/// The refusal is a protocol error, which closes the connection as Kafka does.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the request does not decode, or when
/// one of its record strings is longer than 32767 bytes.
pub(crate) fn decode_group_request<R>(buf: &mut &[u8], version: i16) -> Result<R, BrokerError>
where
    R: for<'de> Decode<'de> + RecordStrings,
{
    let request = R::decode(buf, version)?;
    if request.longest_record_string() > MAX_STRING_BYTES {
        return Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("string field had invalid length"),
        ));
    }
    Ok(request)
}

/// The length of the longest of `strings`.
fn longest<'a>(strings: impl IntoIterator<Item = &'a str>) -> usize {
    strings.into_iter().map(str::len).max().unwrap_or(0)
}

/// List the record strings of a request once, retaining required, optional
/// and nested field traversal without repeating the bound-check implementation.
macro_rules! record_string_fields {
    ($request:ty, $receiver:ident; [$($required:ident),*]; [$($optional:ident),*] $(; $extra:expr)?) => {
        impl RecordStrings for $request {
            fn longest_record_string(&$receiver) -> usize {
                longest(
                    [$($receiver.$required.as_str(),)*].into_iter()
                    $(.chain($receiver.$optional.as_deref()))*
                    $(.chain($extra))?
                )
            }
        }
    };
}

/// Both offset-commit APIs store topic names and each partition's metadata.
macro_rules! offset_record_strings {
    ($request:ident) => {
        $request.topics.iter().flat_map(|topic| {
            std::iter::once(topic.name.as_str()).chain(
                topic
                    .partitions
                    .iter()
                    .filter_map(|partition| partition.committed_metadata.as_deref()),
            )
        })
    };
}

record_string_fields!(JoinGroupRequest, self;
    [group_id, member_id, protocol_type]; [group_instance_id, reason];
    self.protocols.iter().map(|protocol| protocol.name.as_str())
);
record_string_fields!(SyncGroupRequest, self;
    [group_id, member_id]; [group_instance_id, protocol_type, protocol_name];
    self.assignments.iter().map(|assignment| assignment.member_id.as_str())
);
record_string_fields!(HeartbeatRequest, self; [group_id, member_id]; [group_instance_id]);
record_string_fields!(LeaveGroupRequest, self; [group_id, member_id]; [];
    self.members.iter().flat_map(|member| {
        std::iter::once(member.member_id.as_str())
            .chain(member.group_instance_id.as_deref())
            .chain(member.reason.as_deref())
    })
);
record_string_fields!(OffsetCommitRequest, self;
    [group_id, member_id]; [group_instance_id]; offset_record_strings!(self)
);
record_string_fields!(TxnOffsetCommitRequest, self;
    [transactional_id, group_id, member_id]; [group_instance_id]; offset_record_strings!(self)
);
record_string_fields!(ConsumerGroupHeartbeatRequest, self;
    [group_id, member_id]; [instance_id, rack_id, subscribed_topic_regex, server_assignor];
    self.subscribed_topic_names.iter().flatten().map(String::as_str)
);
record_string_fields!(ShareGroupHeartbeatRequest, self;
    [group_id, member_id]; [rack_id];
    self.subscribed_topic_names.iter().flatten().map(String::as_str)
);
record_string_fields!(StreamsGroupHeartbeatRequest, self;
    [group_id, member_id]; [instance_id, rack_id, process_id]
);
record_string_fields!(DeleteGroupsRequest, self; []; [];
    self.groups_names.iter().map(String::as_str)
);
record_string_fields!(AlterShareGroupOffsetsRequest, self; [group_id]; [];
    self.topics.iter().map(|topic| topic.topic_name.as_str())
);
record_string_fields!(DeleteShareGroupOffsetsRequest, self; [group_id]; [];
    self.topics.iter().map(|topic| topic.topic_name.as_str())
);
record_string_fields!(InitProducerIdRequest, self; []; [transactional_id]);

/// The share-state requests carry no string but the group id, which names the
/// `__share_group_state` record key.
macro_rules! share_state_group_id {
    ($($request:ty),+ $(,)?) => {
        $(impl RecordStrings for $request {
            fn longest_record_string(&self) -> usize {
                self.group_id.len()
            }
        })+
    };
}

share_state_group_id!(
    InitializeShareGroupStateRequest,
    ReadShareGroupStateRequest,
    ReadShareGroupStateSummaryRequest,
    WriteShareGroupStateRequest,
    DeleteShareGroupStateRequest,
);

#[cfg(test)]
mod handler_tests;
#[cfg(test)]
mod tests;
