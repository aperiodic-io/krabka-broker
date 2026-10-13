//! Mutation tables compile against Kafka's real request schemas.

use {
    assert2::assert,
    krabka_protocol::owned::{
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
    },
};

struct Field<R> {
    name: &'static str,
    set: fn(&mut R, String),
}

#[test]
fn required_optional_and_indexed_fields_receive_the_owned_string() {
    let mut request = JoinGroupRequest {
        protocols: vec![JoinGroupRequestProtocol::default()],
        ..JoinGroupRequest::default()
    };
    let mutations: &[Field<JoinGroupRequest>] = krabka_macros::string_mutations! {
        "group" => required[group_id],
        "instance" => optional[group_instance_id],
        "protocol" => required[protocols[0].name],
    };
    for field in mutations {
        (field.set)(&mut request, field.name.to_owned());
    }
    assert!(request.group_id == "group");
    assert!(request.group_instance_id.as_deref() == Some("instance"));
    assert!(request.protocols[0].name == "protocol");
}

#[test]
fn offset_policy_appends_independent_fields_after_the_transactional_id() {
    let fields: &[Field<TxnOffsetCommitRequest>] = krabka_macros::string_mutations! {
        offsets;
        "transactional id" => required[transactional_id]
    };
    assert!(
        fields.iter().map(|field| field.name).collect::<Vec<_>>()
            == [
                "transactional id",
                "group id",
                "member id",
                "instance id",
                "topic name",
                "metadata",
            ]
    );
    let mut request = TxnOffsetCommitRequest {
        topics: vec![TxnOffsetCommitRequestTopic {
            partitions: vec![TxnOffsetCommitRequestPartition::default()],
            ..TxnOffsetCommitRequestTopic::default()
        }],
        ..TxnOffsetCommitRequest::default()
    };
    for field in fields {
        (field.set)(&mut request, field.name.to_owned());
    }
    assert!(request.transactional_id == "transactional id" && request.group_id == "group id");
    assert!(
        request.member_id == "member id"
            && request.group_instance_id.as_deref() == Some("instance id")
    );
    assert!(request.topics[0].name == "topic name");
    assert!(
        request.topics[0].partitions[0]
            .committed_metadata
            .as_deref()
            == Some("metadata")
    );
}
