use std::path::PathBuf;

use assert2::{assert, check};
use krabka_metadata::{BrokerRegistrationRecord, MetadataError};

use super::*;
use crate::{config::NodeRole, coordinator::AUDIT_TOPIC, test_support::FakeMetadataSource};

// The isolated controller of the Kafka system tests that surfaced the defect.
const CONTROLLER: NodeId = NodeId(3001);
const CONTROLLER_ONLY: &[NodeRole] = &[NodeRole::Controller];
const COMBINED: &[NodeRole] = &[NodeRole::Controller, NodeRole::Broker];
const BROKER_ONLY: &[NodeRole] = &[NodeRole::Broker];

krabka_macros::single_replica_partition_fixture!(partition_record);

fn node(node_id: NodeId, roles: &[NodeRole]) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(PathBuf::new());
    config.node_id = node_id;
    config.roles = roles.to_vec();
    config
}

fn registration(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: true,
        incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
        ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
    })
}

fn registrations(node_ids: &[u64]) -> Vec<MetadataRecord> {
    node_ids.iter().copied().map(registration).collect()
}

fn audit_batch(topic_id: uuid::Uuid, replicas: &[u64]) -> Vec<MetadataRecord> {
    let mut batch = vec![MetadataRecord::V1Topic(TopicRecord {
        name: AUDIT_TOPIC.into(),
        topic_id,
        partitions: i32::try_from(replicas.len()).unwrap(),
        replication_factor: 1,
    })];
    for (partition, &replica) in (0_i32..).zip(replicas) {
        batch.push(MetadataRecord::V1Partition(partition_record(
            AUDIT_TOPIC,
            krabka_ids::PartitionIndex(partition),
            NodeId(replica),
        )));
    }
    batch
}

// The id the code under test drew for the topic. It is random, so the
// expected batch borrows it, and every other field is compared as written.
fn submitted_topic_id(submitted: &[Vec<MetadataRecord>]) -> uuid::Uuid {
    match submitted.first().and_then(|batch| batch.first()) {
        Some(MetadataRecord::V1Topic(topic)) => topic.topic_id,
        _ => uuid::Uuid::nil(),
    }
}

struct Placement {
    case: &'static str,
    roles: &'static [NodeRole],
    node_id: NodeId,
    leader: NodeId,
    registered: &'static [u64],
    // The replica of each audit partition, in partition order, or `None` when
    // the node submits nothing.
    replicas: Option<&'static [u64]>,
}

// The image never carries the topic in these cases, so each submitting node
// waits out `audit_partition_wait_timeout`. The paused clock skips the wait.
#[tokio::test(start_paused = true)]
async fn the_audit_partitions_land_on_registered_brokers_only() {
    let cases = [
        Placement {
            case: "a controller-only leader with no registered broker",
            roles: CONTROLLER_ONLY,
            node_id: CONTROLLER,
            leader: CONTROLLER,
            registered: &[],
            replicas: None,
        },
        Placement {
            case: "a controller-only leader with registered brokers",
            roles: CONTROLLER_ONLY,
            node_id: CONTROLLER,
            leader: CONTROLLER,
            registered: &[2, 1],
            replicas: Some(&[1, 2]),
        },
        Placement {
            case: "a controller-only follower",
            roles: CONTROLLER_ONLY,
            node_id: NodeId(3002),
            leader: CONTROLLER,
            registered: &[1],
            replicas: None,
        },
        Placement {
            case: "a combined leader before its registration reaches the image",
            roles: COMBINED,
            node_id: NodeId(1),
            leader: NodeId(1),
            registered: &[],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a combined leader after its registration",
            roles: COMBINED,
            node_id: NodeId(1),
            leader: NodeId(1),
            registered: &[1],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a combined follower",
            roles: COMBINED,
            node_id: NodeId(2),
            leader: NodeId(1),
            registered: &[1, 2],
            replicas: None,
        },
        Placement {
            case: "a broker-only node under an isolated controller",
            roles: BROKER_ONLY,
            node_id: NodeId(1),
            leader: CONTROLLER,
            registered: &[1],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a broker-only node that registered after two others",
            roles: BROKER_ONLY,
            node_id: NodeId(3),
            leader: CONTROLLER,
            registered: &[3, 1, 2],
            replicas: Some(&[1, 2, 3]),
        },
    ];
    for case in cases {
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(&registrations(case.registered))
                .leader(Some(case.leader))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();

        let result = bootstrap_audit_topic(&node(case.node_id, case.roles), &source).await;

        let submitted = fake.submitted();
        let expected: Vec<Vec<MetadataRecord>> = case
            .replicas
            .map(|replicas| audit_batch(submitted_topic_id(&submitted), replicas))
            .into_iter()
            .collect();
        check!(result.is_ok(), "{}", case.case);
        check!(submitted == expected, "{}", case.case);
    }
}

#[tokio::test(start_paused = true)]
async fn a_node_submits_nothing_when_the_audit_topic_exists_or_audit_is_off() {
    let existing = [
        registrations(&[1]),
        audit_batch(uuid::Uuid::from_u128(7), &[1]),
    ]
    .concat();
    let cases = [
        ("the topic exists", existing.as_slice(), true),
        ("audit is off", &[] as &[MetadataRecord], false),
    ];
    for (case, records, audit_enabled) in cases {
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(records)
                .leader(Some(NodeId(1)))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();
        let mut config = node(NodeId(1), COMBINED);
        config.audit_enabled = audit_enabled;

        let result = bootstrap_audit_topic(&config, &source).await;

        check!(result.is_ok(), "{case}");
        check!(
            fake.submitted() == Vec::<Vec<MetadataRecord>>::new(),
            "{case}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn only_an_existing_topic_excuses_a_refused_audit_batch() {
    type Case = (fn() -> RaftError, Result<(), String>);
    let cases: [Case; 2] = [
        (
            || RaftError::Metadata(MetadataError::TopicExists(String::new())),
            Ok(()),
        ),
        (
            || RaftError::ChangeRejected("refused".into()),
            Err("startup failed: change rejected: refused".into()),
        ),
    ];
    for (refusal, expected) in cases {
        let source: Arc<dyn MetadataSource> = Arc::new(
            FakeMetadataSource::builder()
                .records(&registrations(&[1]))
                .leader(Some(CONTROLLER))
                .on_submit(move |_| Err(refusal()))
                .build(),
        );

        let result = bootstrap_audit_topic(&node(NodeId(1), BROKER_ONLY), &source).await;

        check!(result.map_err(|error| error.to_string()) == expected);
    }
}

// A broker-only node sees its own forwarded batch only on its next metadata
// fetch. The audit pipeline reads the image right after this bootstrap, so the
// bootstrap holds startup until the topic arrives.
#[tokio::test(start_paused = true)]
async fn a_broker_only_node_waits_for_the_audit_topic_to_reach_its_image() {
    let fake = Arc::new(
        FakeMetadataSource::builder()
            .records(&registrations(&[1]))
            .leader(Some(CONTROLLER))
            .build(),
    );
    let source: Arc<dyn MetadataSource> = fake.clone();
    let config = node(NodeId(1), BROKER_ONLY);
    let timeout = config.audit_partition_wait_timeout.to_std();
    let started = tokio::time::Instant::now();
    let bootstrap = tokio::spawn(async move { bootstrap_audit_topic(&config, &source).await });
    while fake.submitted().is_empty() {
        tokio::task::yield_now().await;
    }

    tokio::time::sleep(timeout / 2).await;
    check!(!bootstrap.is_finished());
    fake.set_records(&[registrations(&[1]), fake.submitted_records()].concat());
    let result = bootstrap.await.expect("the bootstrap task does not panic");

    assert!(result.is_ok());
    assert!(started.elapsed() < timeout);
}

#[tokio::test(start_paused = true)]
async fn a_broker_only_node_starts_when_the_audit_topic_never_reaches_its_image() {
    let source: Arc<dyn MetadataSource> = Arc::new(
        FakeMetadataSource::builder()
            .records(&registrations(&[1]))
            .leader(Some(CONTROLLER))
            .build(),
    );
    let config = node(NodeId(1), BROKER_ONLY);
    let started = tokio::time::Instant::now();

    let result = bootstrap_audit_topic(&config, &source).await;

    assert!(result.is_ok());
    assert!(started.elapsed() >= config.audit_partition_wait_timeout.to_std());
}
