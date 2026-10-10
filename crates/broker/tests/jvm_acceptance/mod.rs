//! Shared harness for the `jvm_acceptance_*` suites.
//!
//! These tests drive the official Apache Kafka command-line tools against a
//! `krabka-broker` running on the host, with the tools inside cp-kafka
//! containers. They are split across several `tests/jvm_acceptance_*.rs` files
//! so Bazel runs them as separate targets concurrently; as one binary the set
//! took roughly nine minutes, serialised by a single shared port allocation.
//! Each binary is its own process, so [`ports`] hands each one a private set of
//! listeners and the groups cannot collide.
//!
//! Networking: the broker listens on an allocated host port. The CLI containers
//! use Docker's default bridge plus `--add-host=host.docker.internal:host-gateway`,
//! which maps that name onto the bridge gateway the host bound. The broker
//! advertises the same name, because `AdminClient` reconnects after `Metadata`
//! and that connect has to resolve from inside the container.
//!
//! These tests deliberately do NOT use `--network host`. On hosted GitHub
//! Actions ubuntu-24.04 runners that mode silently fails to share the host's
//! loopback: the container can run `nc -zv 127.0.0.1 9092`, but a Java NIO
//! `SocketChannel.connect()` to the same address times out.
//!
//! The harness itself is split by concern: [`ports`] allocates the listeners,
//! [`docker`] and [`minio`] run the containers, [`broker`], [`sasl`], [`tls`],
//! [`two_broker_cluster`], [`three_broker_cluster`], [`delegation_tokens`] and
//! [`tiered`] boot the brokers a suite drives, [`tiered_workload`] produces and
//! consumes through the JVM tools, and [`wait`] waits for the result to reach
//! the metadata image.

// Each suite links the whole harness and uses only the helpers it needs, the
// same arrangement as `tests/support/mod.rs`.
#![allow(dead_code)]

pub(crate) mod break_glass;
mod broker;
mod delegation_tokens;
mod docker;
mod minio;
mod ports;
mod sasl;
mod three_broker_cluster;
mod tiered;
mod tiered_workload;
mod tls;
mod two_broker_cluster;
mod wait;

// The suites reach every helper through `use jvm_acceptance::*;`, so the whole
// surface is re-exported here. A suite uses only part of it, which is why this
// one statement carries the `unused_imports` allow: the same reason the module
// carries `allow(dead_code)`.
#[allow(unused_imports)]
pub(crate) use self::{
    broker::{
        host_broker_config, start_console_broker, start_console_group, start_host_broker,
        start_host_broker_in, start_host_broker_jbod, start_host_broker_with,
        start_legacy_console_broker, start_legacy_host_broker,
    },
    delegation_tokens::{
        extract_jvm_kv, start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens,
    },
    docker::{
        ClientPropsFile, KAFKA_IMAGE, KAFKA_IMAGE_ELR, KAFKA_IMAGE_LEGACY, KAFKA_IMAGE_TIERED,
        KAFKA_IMAGE_TXN, STREAMS_APP_JAVA, TRANSACTIONAL_PRODUCER_JAVA, TempFileMount,
        add_console_acl, alter_console_topic_config, assert_console_produced,
        assert_console_values, authenticated_console_round_trip, console_round_trip_with_props,
        consume_committed_at, consume_console, consume_console_group, consume_console_partition,
        create_console_topic, create_console_topic_at, create_plain_console_topic_at,
        denied_console_consumer, describe_console_entity, describe_console_group,
        describe_console_topic_configs, docker_run_kafka_tool,
        docker_run_kafka_tool_allowing_failure, docker_run_kafka_tool_allowing_failure_with_image,
        docker_run_kafka_tool_with_image, docker_run_kafka_tool_with_image_and_mount,
        docker_run_kafka_tool_with_image_and_mounts, docker_run_kafka_tool_with_mount,
        nc_check_connectivity, numbered_payload, plain_client_properties, produce_console,
        produce_console_checked, provision_console_scram, provision_plain_scram,
        provision_ssl_scram_sha512, provision_ssl_topic, tool_output, verify_console_reassignment,
        write_client_props, write_plain_props, write_ssl_sasl_props, write_temp_file,
    },
    minio::{
        MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_CLIENT_IMAGE, MINIO_IMAGE, MINIO_SECRET_KEY,
        MinioContainer, minio_list_objects, minio_make_bucket, minio_make_locked_bucket,
        wait_for_minio_ready,
    },
    ports::{
        Ports, broker0_advertised, broker0_listen, broker1_advertised, broker1_listen,
        broker2_advertised, broker2_listen, controller_addr_0, controller_addr_1,
        controller_addr_2, host_port, minio_port, ports, rlmm_broker0_advertised,
    },
    sasl::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, oauthbearer_jaas, plain_jaas, scram_jaas,
        start_dual_mech_broker, start_dual_mech_broker_with_reauth, start_oauthbearer_broker,
        start_plain_acl_topic, start_plain_acl_topic_with_ops, start_sasl_plaintext_broker,
        start_sasl_plaintext_broker_with_super_user,
    },
    three_broker_cluster::{
        SaslCluster, SaslClusterSetup, start_registered_sasl_cluster,
        start_three_broker_sasl_plaintext_jvm_cluster,
        start_three_broker_sasl_plaintext_jvm_cluster_with_users,
    },
    tiered::start_host_broker_with_minio_tier,
    tiered_workload::{
        TIERED_SEGMENT_SIZE, consume_record_values, consume_records, create_tiered_topic,
        produce_records, wait_for_minio_segments, wait_for_settled_minio_segments,
    },
    tls::{prepare_jks_truststore, start_sasl_ssl_broker, start_ssl_broker},
    two_broker_cluster::{
        start_two_sasl_brokers, start_two_sasl_ssl_brokers_with_controller_protocol,
    },
    wait::{
        wait_jvm_isr_contains, wait_jvm_log_config, wait_jvm_partition_any_leader,
        wait_jvm_partition_leader, wait_three_brokers_registered,
    },
};
