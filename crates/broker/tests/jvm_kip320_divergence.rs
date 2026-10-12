//! KIP-320 JVM mixed-cluster acceptance scenarios.
//!
//! KIP-320 is in-band log-truncation detection. These scenarios are
//! Docker-gated (`#[ignore]`) and Linux-bound. See the project benchmark/JVM
//! memory. The hosted-Mac Docker bridge does not reliably share the host
//! loopback, so these run on the Linux harness/CI, not on a dev Mac.
//!
//! Run on Linux/CI:
//! ```text
//! cargo test -p krabka-broker --test jvm_kip320_divergence -- --ignored --nocapture
//! ```
//!
//! Four scenarios, each independently `#[ignore]`d:
//!
//! 1. [`kip320_wire_conformance_offset_for_leader_epoch`][]: wire-conformance.
//!    The test starts a single Krabka broker and produces across two leader
//!    epochs. A small Java helper drives the official
//!    `org.apache.kafka.clients.consumer.KafkaConsumer` against Krabka. The
//!    test compiles that helper in-container with the cp-kafka JDK's `javac`.
//!    The consumer's offset/position-validation pass issues a real
//!    `OffsetForLeaderEpoch` (`api_key` 23) for KIP-320, and it consumes
//!    at Fetch v12+, so the JVM `Fetcher` decodes Krabka's tagged
//!    `diverging_epoch` / `current_leader` fields. The byte-exactness signal
//!    is a clean drain across both epochs with no deserialization or
//!    truncation fault, plus the observed end-offset that frames the
//!    old-epoch boundary. The Rust side independently cross-checks the same
//!    `OffsetForLeaderEpoch` answer over the wire with the Task-2 client
//!    helper.
//!
//! 2. [`kip320_jvm_follower_truncates_from_krabka_leader`][]: induced divergence.
//!    The test runs a mixed JVM+Krabka cluster: one
//!    `mirror.gcr.io/apache/kafka:4.0.0` broker and a Krabka broker that share
//!    a Krabka-led `KRaft` metadata quorum, per the JVM mixed-quorum work
//!    in `jvm_static_quorum_spike.rs`. The test forces a real divergent
//!    suffix. It produces a committed prefix, takes the partition offline with
//!    a forged `PartitionRecord` that names a dead phantom leader and so also
//!    parks the replication fetchers, diverges the two replicas' logs so the
//!    survivor that becomes leader has a *shorter* log at a *new* epoch, then
//!    rejoins the old leader as a follower. The test asserts that the JVM
//!    follower truncates its divergent suffix to converge on the Krabka
//!    leader. Its on-disk log, dumped with `kafka-dump-log`, contains the
//!    leader's rewritten suffix at the leader's exact LEO. The test also asserts that a
//!    `kafka-console-consumer` recovers and continues without a fatal
//!    deserialization/`LogTruncationException`.
//!
//! 3. [`kip320_krabka_follower_truncates_from_jvm_leader`][]: the reverse
//!    direction. The test parks replication behind a phantom leader, appends a
//!    Krabka-only suffix, then promotes the JVM replica. The Krabka follower
//!    must truncate that suffix and resume at the JVM leader's exact LEO.
//!
//! 4. [`metadata_version_downgrade_refuses_lossy_levels`][]: a
//!    `metadata.version` downgrade across levels that changed metadata. Both
//!    safe and unsafe downgrades are refused with Kafka's messages, without
//!    changing the finalized version or projecting away metadata.
//!
//! ## Topology & networking
//!
//! The topology is the same as the rest of the JVM harness. Krabka brokers
//! bind `0.0.0.0:<port>` on the host and advertise
//! `host.docker.internal:<port>`. The cp-kafka / apache-kafka tool containers
//! get `--add-host=host.docker.internal:host-gateway`. Controller (`KRaft`
//! metadata-quorum) traffic uses host loopback between the Krabka voters and
//! the JVM voter's published port. These tests deliberately do NOT use
//! `--network host`. It silently fails to share the host loopback on hosted
//! ubuntu runners. See the `jvm_acceptance.rs` module docs.

mod support;

mod test_support {
    pub(crate) mod truncation {
        pub(crate) use crate::{
            docker::{produce_lines_via_jvm, set_container_paused},
            dump_log::{dump_log_in_container, max_offset_in_dump},
            mixed_cluster::{
                DivergenceDirection, PartitionEpochDelta, SingleLeaderSetup, prepare_divergence,
                single_leader_record,
            },
            topic_admin::{LEADER_WAIT, wait_for_described_leader},
        };
    }
}

#[path = "jvm_kip320_divergence/docker.rs"]
mod docker;
#[path = "jvm_kip320_divergence/dump_log.rs"]
mod dump_log;
#[path = "jvm_kip320_divergence/jvm_follower_truncation.rs"]
mod jvm_follower_truncation;
#[path = "jvm_kip320_divergence/krabka_follower_truncation.rs"]
mod krabka_follower_truncation;
#[path = "jvm_kip320_divergence/metadata_version_downgrade.rs"]
mod metadata_version_downgrade;
#[path = "jvm_kip320_divergence/mixed_cluster.rs"]
mod mixed_cluster;
#[path = "jvm_kip320_divergence/topic_admin.rs"]
mod topic_admin;
#[path = "jvm_kip320_divergence/wire_conformance.rs"]
mod wire_conformance;
