//! Pure topic request fixtures, usable by both clients and raw socket drivers.

use krabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};

pub fn topic_configs<'a>(
    configs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<CreatableTopicConfig> {
    configs
        .into_iter()
        .map(|(name, value)| CreatableTopicConfig {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        })
        .collect()
}

krabka_macros::create_topic_fixture!(configured_topic_request);
krabka_macros::consumer_fetch_fixture!(consumer_fetch_request);

/// The complete named topic row, with one partition and one replica by default.
#[bon::builder]
pub fn creatable_topic(
    #[builder(default = "orders" . into ())] name: String,
    #[builder(into, default = TopicPartitionCount (1))] partitions: TopicPartitionCount,
    #[builder(into, default = TopicReplicationFactor (1))] replicas: TopicReplicationFactor,
    #[builder(default)] configs: Vec<CreatableTopicConfig>,
) -> CreatableTopic {
    let setup = ConfiguredTopicSetup {
        name,
        partitions,
        replicas,
        configs,
    };
    let ConfiguredTopicSetup {
        name,
        partitions,
        replicas,
        configs,
    } = setup;
    CreatableTopic {
        name,
        num_partitions: partitions.0,
        replication_factor: replicas.0,
        configs,
        ..Default::default()
    }
}

#[derive(derive_more::From, derive_more::Into)]
/// Signed protocol deadlines retain deliberately invalid fixture values.
#[derive(Clone, Copy)]
pub struct CreateTopicsTimeoutMillis(pub i32);

impl Default for CreateTopicsTimeoutMillis {
    fn default() -> Self { Self(5_000) }
}

#[derive(Clone, Copy, Default)]
pub struct CreateTopicRequestSetup {
    pub timeout: CreateTopicsTimeoutMillis,
}

pub fn create_topic_request(topic: CreatableTopic) -> CreateTopicsRequest {
    create_topic_request_with_setup().topic(topic).call()
}

#[bon::builder]
pub fn create_named_topic_request(
    #[builder(default = "orders")] name: &'_ str,
) -> CreateTopicsRequest {
    create_topic_request(creatable_topic().name(name.to_owned()).call())
}

#[bon::builder]
pub fn create_topic_request_with_setup(
    topic: CreatableTopic,
    #[builder(into, default)] timeout: CreateTopicsTimeoutMillis,
) -> CreateTopicsRequest {
    let setup = CreateTopicRequestSetup { timeout };
    CreateTopicsRequest {
        topics: vec![topic],
        timeout_ms: setup.timeout.0,
        ..Default::default()
    }
}

/// A metadata lookup row, including name-free topic-ID lookups.
pub fn metadata_topic(
    name: Option<String>,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) -> krabka_protocol::owned::metadata_request::MetadataRequestTopic {
    krabka_protocol::owned::metadata_request::MetadataRequestTopic {
        name,
        topic_id,
        ..Default::default()
    }
}

/// A configured topic row with caller-owned values and the unchanged protocol defaults.
#[derive(krabka_macros::FieldDefaults)]
pub struct ConfiguredTopicSetup {
    #[default("orders".into())]
    pub name: String,
    #[default(TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
    #[default(TopicReplicationFactor(1))]
    pub replicas: TopicReplicationFactor,
    pub configs: Vec<CreatableTopicConfig>,
}

#[bon::builder]
pub fn configured_topic_setup(
    #[builder(default = "orders")] name: &'_ str,
) -> ConfiguredTopicSetup {
    ConfiguredTopicSetup {
        name: name.into(),
        ..Default::default()
    }
}

pub use creatable_topic as creatable_topic_with_configs;

/// A single-partition diskless topic created through the ordinary admin handler.
pub fn diskless_topic_request(name: impl Into<String>, replication: i16) -> CreateTopicsRequest {
    create_topic_request_with_setup()
        .topic(
            creatable_topic_with_configs()
                .name(name.into())
                .replicas(replication)
                .configs(topic_configs([("krabka.diskless", "true")]))
                .call(),
        )
        .timeout(10_000)
        .call()
}

/// KIP-516 distinguishes a missing nonzero UUID from the all-zero sentinel.
pub fn unresolved_topic_ids(
    id: u128,
) -> [(&'static str, krabka_protocol::primitives::uuid::Uuid); 2] {
    [
        (
            "non-zero id",
            krabka_protocol::primitives::uuid::Uuid(uuid::Uuid::from_u128(id).into_bytes()),
        ),
        ("zero id", krabka_protocol::primitives::uuid::Uuid::ZERO),
    ]
}

use krabka_metadata::NodeId as MetadataNodeId;

#[derive(krabka_macros::FieldDefaults)]
pub struct AssignedTopicSetup {
    #[default("orders".into())]
    pub name: String,
    #[default(vec![MetadataNodeId(1)])]
    pub replicas: Vec<MetadataNodeId>,
}

impl AssignedTopicSetup {
    /// An explicitly ordered assignment, without converting identities back to wire integers.
    pub fn on_replicas(name: impl Into<String>, replicas: Vec<MetadataNodeId>) -> Self {
        Self {
            name: name.into(),
            replicas,
        }
    }

    /// Three replicas in preferred-leader order for coordinated failover fixtures.
    pub fn leader_first(name: impl Into<String>, leader: MetadataNodeId) -> Self {
        let mut replicas = vec![leader];
        replicas.extend((1..=3).map(MetadataNodeId).filter(|node| *node != leader));
        Self {
            name: name.into(),
            replicas,
        }
    }

    /// The replication fixtures' ordered three-node assignment.
    pub fn three_replicas(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            replicas: vec![MetadataNodeId(1), MetadataNodeId(2), MetadataNodeId(3)],
        }
    }
}

/// Build one explicitly assigned partition with ordered node identities.
///
/// # Panics
/// Panics if a node identity does not fit Kafka's broker ID.
#[bon::builder]
pub fn assigned_topic(
    #[builder(default = "orders" . into ())] name: String,
    #[builder(default = vec ! [MetadataNodeId (1)])] replicas: Vec<MetadataNodeId>,
) -> krabka_protocol::owned::create_topics_request::CreatableTopic {
    let setup = AssignedTopicSetup { name, replicas };
    let replicas: Vec<i32> = setup
        .replicas
        .into_iter()
        .map(|node| i32::try_from(node.0).expect("replica node fits Kafka broker ID"))
        .collect();
    super::topic_on(&setup.name, &[&replicas])
}

/// Create an explicitly assigned partition and wait for every replica to install it.
#[bon::builder]
pub async fn create_assigned_partition<'a>(
    client: &krabka_client_core::Client,
    #[builder(default = "orders" . into ())] name: String,
    #[builder(default = vec ! [MetadataNodeId (1)])] replicas: Vec<MetadataNodeId>,
    brokers: impl IntoIterator<Item = &'a krabka_broker::BrokerHandle>,
) -> krabka_protocol::primitives::uuid::Uuid {
    let setup = AssignedTopicSetup { name, replicas };
    let topic = setup.name.clone();
    let response = client
        .send(create_topic_request(
            assigned_topic()
                .name(setup.name)
                .replicas(setup.replicas)
                .call(),
        ))
        .await
        .unwrap();
    assert2::assert!(response.topics[0].error_code == 0);
    let topic_id = response.topics[0].topic_id;
    for broker in brokers {
        broker.wait_until_partition_present(&topic, 0).await;
    }
    topic_id
}
