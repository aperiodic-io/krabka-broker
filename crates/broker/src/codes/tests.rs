//! Guard tests for the wire-level error codes in [`super`], plus the
//! [`super::from_broker_error`] mapping.
//!
//! The guards are total rather than sampled: every constant the module
//! declares is checked against the Apache Kafka table extracted from the
//! pinned container image, and every constant is checked against every other
//! for a collision. A new constant that invents a number therefore fails here
//! rather than reaching a JVM client.
//!
//! The table itself is checked in, so none of this needs a container. What
//! keeps a checked-in oracle honest is
//! [`the_error_table_records_the_image_the_build_pins`], which holds its
//! recorded provenance against the image the build still pins.

mod kafka_error_table;

use {
    super::*,
    crate::error::BrokerError,
    assert2::assert,
    kafka_error_table::{KAFKA_ERROR_TABLE, KAFKA_ERROR_TABLE_IMAGE_DIGEST},
};

/// The digest `//MODULE.bazel` pins for `apache_kafka_4_3_1`.
///
/// The build supplies it -- `crates/broker/build.rs` under Cargo,
/// `//bazel/images:pinned_digests` under Bazel -- so the comparison below is
/// between two strings this compilation already holds. Reading `MODULE.bazel`
/// here instead would be a test asserting on the text of a source file, and
/// would not survive Bazel's sandbox, which hands a test only its declared
/// inputs.
const PINNED_KAFKA_ORACLE_IMAGE_DIGEST: &str = env!("KRABKA_PINNED_IMAGE_APACHE_KAFKA_4_3_1");

#[test]
fn the_error_table_records_the_image_the_build_pins() {
    assert!(
        KAFKA_ERROR_TABLE_IMAGE_DIGEST == PINNED_KAFKA_ORACLE_IMAGE_DIGEST,
        "the Kafka error table was extracted from an image the build no longer \
         pins; re-derive crates/broker/src/codes/tests/kafka_error_table.rs \
         from {PINNED_KAFKA_ORACLE_IMAGE_DIGEST} and record that digest there",
    );
}

#[test]
fn every_kafka_range_constant_appears_in_the_kafka_table() {
    for wire in KAFKA_RANGE_CODES {
        let NamedCode { name, code } = wire;
        let kafka = KAFKA_ERROR_TABLE
            .iter()
            .find(|kafka| kafka.name == wire.name);
        assert!(kafka == Some(wire), "{name} = {code}");
    }
}

/// The codes Kafka trunk adds after the 4.3.1 table ends, as
/// `org.apache.kafka.common.protocol.Errors` assigns them at trunk
/// `abf522e1ca5d`: 134 is `GROUP_DELETION_FAILED`, which krabka does not
/// return, 135 is KIP-1331's and 136 is KIP-1312's.
const KAFKA_TRUNK_ERROR_TABLE: &[NamedCode] = &[
    NamedCode {
        name: "GROUP_DELETION_FAILED",
        code: 134,
    },
    NamedCode {
        name: "STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED",
        code: 135,
    },
    NamedCode {
        name: "CONTROLLER_ID_NOT_REGISTERED",
        code: 136,
    },
];

#[test]
fn every_trunk_constant_appears_in_the_trunk_table_and_not_in_the_release() {
    for wire in KAFKA_TRUNK_CODES {
        let NamedCode { name, code } = wire;
        let trunk = KAFKA_TRUNK_ERROR_TABLE
            .iter()
            .find(|trunk| trunk.name == wire.name);
        assert!(trunk == Some(wire), "{name} = {code}");
        assert!(
            KAFKA_ERROR_TABLE
                .iter()
                .all(|released| released.code != *code),
            "{name} is in the release table and belongs with the Kafka-range codes"
        );
    }
}

#[test]
fn no_two_error_code_constants_collide() {
    let all: Vec<NamedCode> = KAFKA_RANGE_CODES
        .iter()
        .chain(KAFKA_TRUNK_CODES)
        .chain(KRABKA_PRIVATE_CODES)
        .copied()
        .collect();
    for (
        index,
        NamedCode {
            name: left_name,
            code: left,
        },
    ) in all.iter().enumerate()
    {
        for NamedCode {
            name: right_name,
            code: right,
        } in &all[index + 1..]
        {
            assert!(
                left != right,
                "{left_name} and {right_name} both use {left}"
            );
        }
    }
}

#[test]
fn krabka_private_codes_sit_above_the_whole_kafka_table() {
    let highest_kafka = KAFKA_ERROR_TABLE
        .iter()
        .map(|kafka| kafka.code)
        .max()
        .expect("the extracted Kafka table is not empty");
    let highest_trunk = KAFKA_TRUNK_ERROR_TABLE
        .iter()
        .map(|trunk| trunk.code)
        .max()
        .expect("the trunk table is not empty");
    for NamedCode { name, code } in KRABKA_PRIVATE_CODES {
        assert!(*code >= 1000, "{name}");
        assert!(*code > highest_kafka, "{name}");
        assert!(*code > highest_trunk, "{name}");
    }
}

#[yare::parameterized(
    unsupported_api = {
        BrokerError::UnsupportedApi { api_key: 0, version: 99 },
        UNSUPPORTED_VERSION,
    },
    partition_writer_died = {
        BrokerError::PartitionWriterDied { topic: "t".into(), partition: 0 },
        NOT_LEADER_OR_FOLLOWER,
    },
    invalid_group_state = {
        BrokerError::GroupInvalidState {
            group_id: "g".into(),
            state: "PreparingRebalance".into(),
        },
        REBALANCE_IN_PROGRESS,
    },
    unknown_member = {
        BrokerError::UnknownMember { group_id: "g".into(), member_id: "m".into() },
        UNKNOWN_MEMBER_ID,
    },
    generation_mismatch = {
        BrokerError::GenerationMismatch { group_id: "g".into(), current: 5, requested: 4 },
        ILLEGAL_GENERATION,
    },
    producer_epoch_fenced = {
        BrokerError::ProducerEpochFenced { producer_id: 1000, current: 2, requested: 1 },
        INVALID_PRODUCER_EPOCH,
    },
    fenced_leader_epoch = {
        BrokerError::FencedLeaderEpoch { have: 0, current: 1 },
        FENCED_LEADER_EPOCH,
    },
    unknown_leader_epoch = { BrokerError::UnknownLeaderEpoch(2), UNKNOWN_LEADER_EPOCH },
    coordinator_write_not_coordinator = {
        BrokerError::CoordinatorWriteUncommitted { partition: 0, code: NOT_COORDINATOR },
        NOT_COORDINATOR,
    },
    coordinator_write_unavailable = {
        BrokerError::CoordinatorWriteUncommitted {
            partition: 0,
            code: COORDINATOR_NOT_AVAILABLE,
        },
        COORDINATOR_NOT_AVAILABLE,
    },
    transaction_state_write_uncommitted = {
        BrokerError::TransactionStateWriteUncommitted {
            partition: 0,
            code: COORDINATOR_LOAD_IN_PROGRESS,
        },
        COORDINATOR_LOAD_IN_PROGRESS,
    },
    log_io_error = {
        BrokerError::Log(krabka_log::LogError::Io(std::io::Error::other("EIO"))),
        KAFKA_STORAGE_ERROR,
    },
    catch_all = { BrokerError::Txn("test".into()), UNKNOWN_SERVER_ERROR },
)]
fn from_broker_error_maps_variants_to_wire_codes(err: BrokerError, want: i16) {
    assert2::assert!(from_broker_error(&err) == want, "{err:?}");
}

#[test]
fn policy_violation_is_not_a_code_that_poisons_the_metadata_cache() {
    // KFC-9 chose 44 and rejected both of these, because each one also
    // changes the JVM client's metadata cache. 29 marks the topic
    // unauthorized, and 17 marks the name permanently invalid.
    assert!(POLICY_VIOLATION != TOPIC_AUTHORIZATION_FAILED);
    assert!(POLICY_VIOLATION != INVALID_TOPIC_EXCEPTION);
}

#[test]
fn replica_not_available_is_not_the_controller_moved_code() {
    // A JVM AdminClient turns 11 into StaleControllerEpochException, which
    // makes `kafka-reassign-partitions` report a controller failure for a
    // replica this broker simply does not host.
    let stale_controller_epoch = KAFKA_ERROR_TABLE
        .iter()
        .find(|kafka| kafka.name == "STALE_CONTROLLER_EPOCH")
        .map(|kafka| kafka.code);
    assert!(stale_controller_epoch == Some(11));
    assert!(REPLICA_NOT_AVAILABLE == 9);
}
