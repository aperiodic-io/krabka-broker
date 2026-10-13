//! The Apache Kafka error table, extracted from the pinned container image.
//!
//! Every pair below comes from `org.apache.kafka.common.protocol.Errors` in
//! `kafka-clients` inside `mirror.gcr.io/apache/kafka:4.3.1`, the newest image
//! that `//bazel/images` pins. The extraction iterated `Errors.values()` and
//! printed `code()` and `name()`, so the table is the JVM's own view of the
//! wire numbers rather than a transcription of prose.
//!
//! `every_kafka_range_constant_appears_in_the_kafka_table` checks each
//! constant in [`crate::codes`] against this table, which is what stops a
//! later addition inventing a number.
//!
//! Re-deriving it needs the image the digest below names, and nothing else:
//!
//! ```text
//! image=mirror.gcr.io/apache/kafka@<KAFKA_ERROR_TABLE_IMAGE_DIGEST>
//! id=$(docker create "${image}" true)
//! docker cp "${id}:/opt/kafka/libs/kafka-clients-4.3.1.jar" .
//! docker cp "${id}:/opt/kafka/libs/slf4j-api-1.7.36.jar" .
//! # Dump.java: for (Errors e : Errors.values()) print e.name() and e.code().
//! java -cp 'kafka-clients-4.3.1.jar:slf4j-api-1.7.36.jar' Dump.java
//! ```
//!
//! The image ships a JRE rather than a JDK, so the dump runs on the host's
//! `java` against the jars copied out of it.

use crate::codes::NamedCode;

/// The image digest [`KAFKA_ERROR_TABLE`] was extracted from.
///
/// `the_error_table_records_the_image_the_build_pins` compares this with the
/// digest `//MODULE.bazel` pins for `apache_kafka_4_3_1`, which the build hands
/// the compilation rather than the test reading it from the tree. The table is
/// hermetic on purpose -- nothing here needs a container -- and this is what
/// keeps it honest: bumping the pin fails that guard until the table above is
/// re-derived from the new image.
///
/// The jar the recorded extraction read is `kafka-clients-4.3.1.jar`, sha256
/// `dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e`.
pub(super) const KAFKA_ERROR_TABLE_IMAGE_DIGEST: &str =
    "sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26";

/// Name and wire code of every variant of the Apache Kafka `Errors` enum.
pub(super) const KAFKA_ERROR_TABLE: &[NamedCode] = &[
    NamedCode {
        name: "UNKNOWN_SERVER_ERROR",
        code: -1,
    },
    NamedCode {
        name: "NONE",
        code: 0,
    },
    NamedCode {
        name: "OFFSET_OUT_OF_RANGE",
        code: 1,
    },
    NamedCode {
        name: "CORRUPT_MESSAGE",
        code: 2,
    },
    NamedCode {
        name: "UNKNOWN_TOPIC_OR_PARTITION",
        code: 3,
    },
    NamedCode {
        name: "INVALID_FETCH_SIZE",
        code: 4,
    },
    NamedCode {
        name: "LEADER_NOT_AVAILABLE",
        code: 5,
    },
    NamedCode {
        name: "NOT_LEADER_OR_FOLLOWER",
        code: 6,
    },
    NamedCode {
        name: "REQUEST_TIMED_OUT",
        code: 7,
    },
    NamedCode {
        name: "BROKER_NOT_AVAILABLE",
        code: 8,
    },
    NamedCode {
        name: "REPLICA_NOT_AVAILABLE",
        code: 9,
    },
    NamedCode {
        name: "MESSAGE_TOO_LARGE",
        code: 10,
    },
    NamedCode {
        name: "STALE_CONTROLLER_EPOCH",
        code: 11,
    },
    NamedCode {
        name: "OFFSET_METADATA_TOO_LARGE",
        code: 12,
    },
    NamedCode {
        name: "NETWORK_EXCEPTION",
        code: 13,
    },
    NamedCode {
        name: "COORDINATOR_LOAD_IN_PROGRESS",
        code: 14,
    },
    NamedCode {
        name: "COORDINATOR_NOT_AVAILABLE",
        code: 15,
    },
    NamedCode {
        name: "NOT_COORDINATOR",
        code: 16,
    },
    NamedCode {
        name: "INVALID_TOPIC_EXCEPTION",
        code: 17,
    },
    NamedCode {
        name: "RECORD_LIST_TOO_LARGE",
        code: 18,
    },
    NamedCode {
        name: "NOT_ENOUGH_REPLICAS",
        code: 19,
    },
    NamedCode {
        name: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        code: 20,
    },
    NamedCode {
        name: "INVALID_REQUIRED_ACKS",
        code: 21,
    },
    NamedCode {
        name: "ILLEGAL_GENERATION",
        code: 22,
    },
    NamedCode {
        name: "INCONSISTENT_GROUP_PROTOCOL",
        code: 23,
    },
    NamedCode {
        name: "INVALID_GROUP_ID",
        code: 24,
    },
    NamedCode {
        name: "UNKNOWN_MEMBER_ID",
        code: 25,
    },
    NamedCode {
        name: "INVALID_SESSION_TIMEOUT",
        code: 26,
    },
    NamedCode {
        name: "REBALANCE_IN_PROGRESS",
        code: 27,
    },
    NamedCode {
        name: "INVALID_COMMIT_OFFSET_SIZE",
        code: 28,
    },
    NamedCode {
        name: "TOPIC_AUTHORIZATION_FAILED",
        code: 29,
    },
    NamedCode {
        name: "GROUP_AUTHORIZATION_FAILED",
        code: 30,
    },
    NamedCode {
        name: "CLUSTER_AUTHORIZATION_FAILED",
        code: 31,
    },
    NamedCode {
        name: "INVALID_TIMESTAMP",
        code: 32,
    },
    NamedCode {
        name: "UNSUPPORTED_SASL_MECHANISM",
        code: 33,
    },
    NamedCode {
        name: "ILLEGAL_SASL_STATE",
        code: 34,
    },
    NamedCode {
        name: "UNSUPPORTED_VERSION",
        code: 35,
    },
    NamedCode {
        name: "TOPIC_ALREADY_EXISTS",
        code: 36,
    },
    NamedCode {
        name: "INVALID_PARTITIONS",
        code: 37,
    },
    NamedCode {
        name: "INVALID_REPLICATION_FACTOR",
        code: 38,
    },
    NamedCode {
        name: "INVALID_REPLICA_ASSIGNMENT",
        code: 39,
    },
    NamedCode {
        name: "INVALID_CONFIG",
        code: 40,
    },
    NamedCode {
        name: "NOT_CONTROLLER",
        code: 41,
    },
    NamedCode {
        name: "INVALID_REQUEST",
        code: 42,
    },
    NamedCode {
        name: "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        code: 43,
    },
    NamedCode {
        name: "POLICY_VIOLATION",
        code: 44,
    },
    NamedCode {
        name: "OUT_OF_ORDER_SEQUENCE_NUMBER",
        code: 45,
    },
    NamedCode {
        name: "DUPLICATE_SEQUENCE_NUMBER",
        code: 46,
    },
    NamedCode {
        name: "INVALID_PRODUCER_EPOCH",
        code: 47,
    },
    NamedCode {
        name: "INVALID_TXN_STATE",
        code: 48,
    },
    NamedCode {
        name: "INVALID_PRODUCER_ID_MAPPING",
        code: 49,
    },
    NamedCode {
        name: "INVALID_TRANSACTION_TIMEOUT",
        code: 50,
    },
    NamedCode {
        name: "CONCURRENT_TRANSACTIONS",
        code: 51,
    },
    NamedCode {
        name: "TRANSACTION_COORDINATOR_FENCED",
        code: 52,
    },
    NamedCode {
        name: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        code: 53,
    },
    NamedCode {
        name: "SECURITY_DISABLED",
        code: 54,
    },
    NamedCode {
        name: "OPERATION_NOT_ATTEMPTED",
        code: 55,
    },
    NamedCode {
        name: "KAFKA_STORAGE_ERROR",
        code: 56,
    },
    NamedCode {
        name: "LOG_DIR_NOT_FOUND",
        code: 57,
    },
    NamedCode {
        name: "SASL_AUTHENTICATION_FAILED",
        code: 58,
    },
    NamedCode {
        name: "UNKNOWN_PRODUCER_ID",
        code: 59,
    },
    NamedCode {
        name: "REASSIGNMENT_IN_PROGRESS",
        code: 60,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_AUTH_DISABLED",
        code: 61,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_NOT_FOUND",
        code: 62,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_OWNER_MISMATCH",
        code: 63,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        code: 64,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        code: 65,
    },
    NamedCode {
        name: "DELEGATION_TOKEN_EXPIRED",
        code: 66,
    },
    NamedCode {
        name: "INVALID_PRINCIPAL_TYPE",
        code: 67,
    },
    NamedCode {
        name: "NON_EMPTY_GROUP",
        code: 68,
    },
    NamedCode {
        name: "GROUP_ID_NOT_FOUND",
        code: 69,
    },
    NamedCode {
        name: "FETCH_SESSION_ID_NOT_FOUND",
        code: 70,
    },
    NamedCode {
        name: "INVALID_FETCH_SESSION_EPOCH",
        code: 71,
    },
    NamedCode {
        name: "LISTENER_NOT_FOUND",
        code: 72,
    },
    NamedCode {
        name: "TOPIC_DELETION_DISABLED",
        code: 73,
    },
    NamedCode {
        name: "FENCED_LEADER_EPOCH",
        code: 74,
    },
    NamedCode {
        name: "UNKNOWN_LEADER_EPOCH",
        code: 75,
    },
    NamedCode {
        name: "UNSUPPORTED_COMPRESSION_TYPE",
        code: 76,
    },
    NamedCode {
        name: "STALE_BROKER_EPOCH",
        code: 77,
    },
    NamedCode {
        name: "OFFSET_NOT_AVAILABLE",
        code: 78,
    },
    NamedCode {
        name: "MEMBER_ID_REQUIRED",
        code: 79,
    },
    NamedCode {
        name: "PREFERRED_LEADER_NOT_AVAILABLE",
        code: 80,
    },
    NamedCode {
        name: "GROUP_MAX_SIZE_REACHED",
        code: 81,
    },
    NamedCode {
        name: "FENCED_INSTANCE_ID",
        code: 82,
    },
    NamedCode {
        name: "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        code: 83,
    },
    NamedCode {
        name: "ELECTION_NOT_NEEDED",
        code: 84,
    },
    NamedCode {
        name: "NO_REASSIGNMENT_IN_PROGRESS",
        code: 85,
    },
    NamedCode {
        name: "GROUP_SUBSCRIBED_TO_TOPIC",
        code: 86,
    },
    NamedCode {
        name: "INVALID_RECORD",
        code: 87,
    },
    NamedCode {
        name: "UNSTABLE_OFFSET_COMMIT",
        code: 88,
    },
    NamedCode {
        name: "THROTTLING_QUOTA_EXCEEDED",
        code: 89,
    },
    NamedCode {
        name: "PRODUCER_FENCED",
        code: 90,
    },
    NamedCode {
        name: "RESOURCE_NOT_FOUND",
        code: 91,
    },
    NamedCode {
        name: "DUPLICATE_RESOURCE",
        code: 92,
    },
    NamedCode {
        name: "UNACCEPTABLE_CREDENTIAL",
        code: 93,
    },
    NamedCode {
        name: "INCONSISTENT_VOTER_SET",
        code: 94,
    },
    NamedCode {
        name: "INVALID_UPDATE_VERSION",
        code: 95,
    },
    NamedCode {
        name: "FEATURE_UPDATE_FAILED",
        code: 96,
    },
    NamedCode {
        name: "PRINCIPAL_DESERIALIZATION_FAILURE",
        code: 97,
    },
    NamedCode {
        name: "SNAPSHOT_NOT_FOUND",
        code: 98,
    },
    NamedCode {
        name: "POSITION_OUT_OF_RANGE",
        code: 99,
    },
    NamedCode {
        name: "UNKNOWN_TOPIC_ID",
        code: 100,
    },
    NamedCode {
        name: "DUPLICATE_BROKER_REGISTRATION",
        code: 101,
    },
    NamedCode {
        name: "BROKER_ID_NOT_REGISTERED",
        code: 102,
    },
    NamedCode {
        name: "INCONSISTENT_TOPIC_ID",
        code: 103,
    },
    NamedCode {
        name: "INCONSISTENT_CLUSTER_ID",
        code: 104,
    },
    NamedCode {
        name: "TRANSACTIONAL_ID_NOT_FOUND",
        code: 105,
    },
    NamedCode {
        name: "FETCH_SESSION_TOPIC_ID_ERROR",
        code: 106,
    },
    NamedCode {
        name: "INELIGIBLE_REPLICA",
        code: 107,
    },
    NamedCode {
        name: "NEW_LEADER_ELECTED",
        code: 108,
    },
    NamedCode {
        name: "OFFSET_MOVED_TO_TIERED_STORAGE",
        code: 109,
    },
    NamedCode {
        name: "FENCED_MEMBER_EPOCH",
        code: 110,
    },
    NamedCode {
        name: "UNRELEASED_INSTANCE_ID",
        code: 111,
    },
    NamedCode {
        name: "UNSUPPORTED_ASSIGNOR",
        code: 112,
    },
    NamedCode {
        name: "STALE_MEMBER_EPOCH",
        code: 113,
    },
    NamedCode {
        name: "MISMATCHED_ENDPOINT_TYPE",
        code: 114,
    },
    NamedCode {
        name: "UNSUPPORTED_ENDPOINT_TYPE",
        code: 115,
    },
    NamedCode {
        name: "UNKNOWN_CONTROLLER_ID",
        code: 116,
    },
    NamedCode {
        name: "UNKNOWN_SUBSCRIPTION_ID",
        code: 117,
    },
    NamedCode {
        name: "TELEMETRY_TOO_LARGE",
        code: 118,
    },
    NamedCode {
        name: "INVALID_REGISTRATION",
        code: 119,
    },
    NamedCode {
        name: "TRANSACTION_ABORTABLE",
        code: 120,
    },
    NamedCode {
        name: "INVALID_RECORD_STATE",
        code: 121,
    },
    NamedCode {
        name: "SHARE_SESSION_NOT_FOUND",
        code: 122,
    },
    NamedCode {
        name: "INVALID_SHARE_SESSION_EPOCH",
        code: 123,
    },
    NamedCode {
        name: "FENCED_STATE_EPOCH",
        code: 124,
    },
    NamedCode {
        name: "INVALID_VOTER_KEY",
        code: 125,
    },
    NamedCode {
        name: "DUPLICATE_VOTER",
        code: 126,
    },
    NamedCode {
        name: "VOTER_NOT_FOUND",
        code: 127,
    },
    NamedCode {
        name: "INVALID_REGULAR_EXPRESSION",
        code: 128,
    },
    NamedCode {
        name: "REBOOTSTRAP_REQUIRED",
        code: 129,
    },
    NamedCode {
        name: "STREAMS_INVALID_TOPOLOGY",
        code: 130,
    },
    NamedCode {
        name: "STREAMS_INVALID_TOPOLOGY_EPOCH",
        code: 131,
    },
    NamedCode {
        name: "STREAMS_TOPOLOGY_FENCED",
        code: 132,
    },
    NamedCode {
        name: "SHARE_SESSION_LIMIT_REACHED",
        code: 133,
    },
];
