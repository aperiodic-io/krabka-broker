//! Shared group-handler fixtures and subscription authorization cases.

#[cfg(test)]
pub(crate) use crate::handlers::test_support::{
    LiveBrokerFixture, StartedBroker as HandlerStartedBroker,
};
use {
    crate::{test_support as broker_fixture_support, test_support::principal as make_principal},
    assert2::assert,
    krabka_metadata::{
        AclOperation, FeatureLevelRecord, MetadataImage, MetadataRecord, ResourceType,
    },
    std::collections::HashSet,
};

pub(super) fn acl_authorizer() -> SimpleAclAuthorizer { SimpleAclAuthorizer::new(HashSet::new()) }

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct GroupAclSetup<'a> {
    #[default("consumer-client")]
    pub client_id: &'a str,
    #[default("ANONYMOUS")]
    pub principal: &'a str,
    #[default("g")]
    pub group: &'a str,
}

/// An anonymous group-read probe is denied against an empty authorizer and image.
#[bon::builder]
pub(super) fn check_group_read_denied(
    #[builder(default = "consumer-client")] client_id: &'_ str,
    #[builder(default = "ANONYMOUS")] principal: &'_ str,
    #[builder(default = "g")] group: &'_ str,
) {
    let setup = GroupAclSetup {
        client_id,
        principal,
        group,
    };
    let authorizer = acl_authorizer();
    let image = MetadataImage::new(uuid::Uuid::nil());
    let principal = make_principal(setup.principal);
    let peer = peer();
    let ctx = request_context(&principal, &peer, setup.client_id);
    assert!(super::group_read_denied(
        &authorizer,
        &image,
        &ctx,
        setup.group
    ));
}

/// An operation on a named resource, allowed to `User:alice` from any host.
pub(super) fn acl(
    resource_type: ResourceType,
    name: &str,
    operation: AclOperation,
) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(
        allow_acl()
            .resource_type(resource_type)
            .resource_name(name)
            .operation(operation)
            .call(),
    )
}

/// `Describe` on `Topic(name)`, allowed to `User:alice` from any host.
pub(super) fn describe_acl(name: &str) -> MetadataRecord {
    acl(ResourceType::Topic, name, AclOperation::Describe)
}

/// `Read` on `Group(name)`, allowed to `User:alice` from any host.
pub(super) fn group_read_acl(name: &str) -> MetadataRecord {
    acl(ResourceType::Group, name, AclOperation::Read)
}

/// The finalized metadata feature level, including the disabled zero sentinel.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct GroupFeatureLevel(pub i16);

pub(super) async fn grant_group_read(broker: &Broker, group: &str) {
    broker
        .controller
        .submit_change(vec![group_read_acl(group)])
        .await
        .expect("grant group Read");
}

fn group_version(level: GroupFeatureLevel) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
        level: level.0,
    })
}

pub(super) fn image_with_group_version(level: GroupFeatureLevel) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&group_version(level));
    image
}

/// Override the broker's release-default finalized consumer-group feature.
pub(super) async fn set_group_version(broker: &Broker, level: GroupFeatureLevel) {
    broker
        .controller
        .submit_change(vec![group_version(level)])
        .await
        .expect("set group.version");
}

/// Start the coordinator fixture with an explicit finalized group version.
pub(super) async fn versioned_group_broker(
    authorizer: std::sync::Arc<dyn Authorizer>,
    level: GroupFeatureLevel,
) -> LiveBrokerFixture {
    let HandlerStartedBroker {
        broker: handle,
        dir,
    } = start_group_broker(authorizer).await;
    let broker = handle.broker_arc_for_test();
    set_group_version(&broker, level).await;
    LiveBrokerFixture {
        handle,
        directory: dir,
        broker,
    }
}

/// A bare `V1Topic` record of `partitions` partitions.
fn topic_record(name: &str, topic_id: uuid::Uuid, partitions: PartitionCount) -> MetadataRecord {
    MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id,
        partitions: partitions.0,
        replication_factor: 1,
    })
}

use {
    crate::{
        authorizer::{Authorizer, SimpleAclAuthorizer},
        broker::Broker,
        handlers::test_support::single_replica_partition,
        test_support::{
            PartitionCount, allow_acl, peer, request_context, sasl_principal, start_group_broker,
            streams_join_request,
        },
    },
    krabka_raft::NodeId,
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct GroupTopicSetup<'a> {
    #[default("orders")]
    pub name: &'a str,
    pub topic_id: uuid::Uuid,
    #[default(PartitionCount(2))]
    pub partitions: PartitionCount,
    #[default(NodeId(1))]
    pub node: NodeId,
}

/// A `V1Topic` record plus one `V1Partition` per index, assigned to
/// `node`. The KIP-631 wire framing does not carry `TopicRecord.partitions`
/// -- a decoded `V1Topic` round-trips back at `partitions == 0`, and the
/// real count comes from the `V1Partition` records that follow it -- so a
/// topic meant to be assignable needs both, unlike [`topic_record`] alone
/// (used only where a test never reaches the assignor).
#[bon::builder]
pub(super) fn topic_with_partitions(
    #[builder(default = "orders")] name: &'_ str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default = PartitionCount (2))] partitions: PartitionCount,
    #[builder(into, default = NodeId (1))] node: NodeId,
) -> Vec<MetadataRecord> {
    let setup = GroupTopicSetup {
        name,
        topic_id,
        partitions,
        node,
    };
    let GroupTopicSetup {
        name,
        topic_id,
        partitions,
        node,
    } = setup;
    let mut records = vec![topic_record(name, topic_id, partitions)];
    records.extend((0..partitions.0).map(|partition| {
        MetadataRecord::V1Partition(single_replica_partition(
            name,
            krabka_ids::PartitionIndex(partition),
            node,
        ))
    }));
    records
}

/// The SASL/PLAIN principal `User:alice` that the ACLs above name.
pub(super) fn alice() -> krabka_security::Principal { sasl_principal("alice") }

/// The authenticated user and host used together by subscription ACL probes.
pub(super) struct RequestIdentity {
    pub(super) principal: krabka_security::Principal,
    pub(super) peer: std::net::SocketAddr,
}
pub(super) fn alice_peer() -> RequestIdentity {
    RequestIdentity {
        principal: alice(),
        peer: peer(),
    }
}

/// Kafka's explicit-name Describe check, shared by consumer and share heartbeats.
pub(super) fn subscribed_names_describe_denied_table() {
    let authorizer = acl_authorizer();
    krabka_macros::request_test_identity! {
        bind(principal, peer, ctx);
        identity [alice()];
        address [broker_fixture_support::peer()];
        client ["c"];
    };
    for (label, granted, names, expected_denied) in [
        ("no subscription", &[][..], None, false),
        ("empty subscription", &[][..], Some(&[][..]), false),
        (
            "single name, fully authorized",
            &["orders"][..],
            Some(&["orders"][..]),
            false,
        ),
        (
            "single name, not authorized",
            &[][..],
            Some(&["orders"][..]),
            true,
        ),
        (
            "two names, one denied",
            &["orders"][..],
            Some(&["orders", "shipments"][..]),
            true,
        ),
        (
            "two names, both authorized",
            &["orders", "shipments"][..],
            Some(&["orders", "shipments"][..]),
            false,
        ),
    ] {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for name in granted {
            image.apply(&describe_acl(name));
        }
        let names = names.map(|names| {
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        });
        assert!(
            crate::handlers::acl_gates::subscribed_names_describe_denied(
                &authorizer,
                &image,
                &ctx,
                names.as_deref(),
            ) == expected_denied,
            "{label}"
        );
    }
}
pub(super) fn streams_request(
    group_id: &str,
    member_id: &str,
) -> krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
    streams_request_with_topology()
        .group_id(group_id)
        .member_id(member_id)
        .call()
}

use krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct StreamsTopologySetup<'a> {
    #[default("g")]
    pub group_id: &'a str,
    #[default("m1")]
    pub member_id: &'a str,
    #[default("in")]
    pub source_topic: &'a str,
    pub state_changelog_topics: Vec<TopicInfo>,
}

#[bon::builder]
pub(super) fn streams_request_with_topology(
    #[builder(default = "g")] group_id: &'_ str,
    #[builder(default = "m1")] member_id: &'_ str,
    #[builder(default = "in")] source_topic: &'_ str,
    #[builder(default)] state_changelog_topics: Vec<TopicInfo>,
) -> krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
    use krabka_protocol::owned::streams_group_heartbeat_request::{Subtopology, Topology};

    let setup = StreamsTopologySetup {
        group_id,
        member_id,
        source_topic,
        state_changelog_topics,
    };
    let StreamsTopologySetup {
        group_id,
        member_id,
        source_topic,
        state_changelog_topics,
    } = setup;
    streams_join_request()
        .topology(Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec![source_topic.into()],
                state_changelog_topics,
                ..Default::default()
            }],
            ..Default::default()
        })
        .group_id(group_id)
        .member_id(member_id)
        .call()
}
