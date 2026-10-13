use {super::*, assert2::assert};

/// Kafka's `TransactionCoordinator.handleVerifyPartitionsInTransaction`, in
/// its order of checks.
#[test]
fn a_verify_only_answer_follows_kafka() {
    let producer = ProducerId(7);
    let cases = [
        (
            "another producer id",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: ProducerId(8),
                    producer_epoch: 3,
                },
                state: TxnState::Ongoing,
            },
            true,
            codes::INVALID_PRODUCER_ID_MAPPING,
        ),
        (
            "another epoch",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 4,
                },
                state: TxnState::Ongoing,
            },
            true,
            codes::PRODUCER_FENCED,
        ),
        (
            "prepare commit",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 3,
                },
                state: TxnState::PrepareCommit,
            },
            true,
            codes::CONCURRENT_TRANSACTIONS,
        ),
        (
            "prepare abort",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 3,
                },
                state: TxnState::PrepareAbort,
            },
            false,
            codes::CONCURRENT_TRANSACTIONS,
        ),
        (
            "partition in the transaction",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 3,
                },
                state: TxnState::Ongoing,
            },
            true,
            codes::NONE,
        ),
        (
            "partition not in the transaction",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 3,
                },
                state: TxnState::Ongoing,
            },
            false,
            codes::TRANSACTION_ABORTABLE,
        ),
        (
            "complete commit",
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: producer,
                    producer_epoch: 3,
                },
                state: TxnState::CompleteCommit,
            },
            false,
            codes::TRANSACTION_ABORTABLE,
        ),
    ];
    for (name, entry, contains, want) in cases {
        assert!(
            verification_code(entry, contains, ProducerIdentity {
                producer_id: producer,
                producer_epoch: 3
            }) == want,
            "{name}"
        );
    }
}

/// `transaction.partition.verification.enable` is a dynamic broker config
/// with a static layer: the broker's own dynamic value beats the cluster-wide
/// one, which beats the static value of `server.properties`, which beats
/// Kafka's default `true`. A dynamic value that is not a boolean does not
/// apply.
#[test]
fn the_verification_knob_reads_the_broker_then_the_cluster_then_the_static_value() {
    use {
        crate::config::BrokerConfig,
        krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            NodeId,
        },
    };

    const NODE: NodeId = NodeId(1);
    /// A dynamic config value set for one broker, or for the cluster.
    struct DynamicValue<'a> {
        node_id: NodeId,
        value: &'a str,
    }
    let image = |records: &[DynamicValue<'_>]| {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for DynamicValue { node_id, value } in records {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: *node_id,
                config_name: PARTITION_VERIFICATION_ENABLE.to_owned(),
                config_value: Some((*value).to_owned()),
            }));
        }
        image
    };
    let cluster = |value| DynamicValue {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        value,
    };
    let broker = |node_id, value| DynamicValue { node_id, value };
    // (label, dynamic records, the static value, the answer)
    let cases = [
        ("no config", image(&[]), true, true),
        ("static off", image(&[]), false, false),
        ("cluster off", image(&[cluster("false")]), true, false),
        (
            "cluster off, case and blanks",
            image(&[cluster(" False ")]),
            true,
            false,
        ),
        (
            "cluster on over static off",
            image(&[cluster("true")]),
            false,
            true,
        ),
        (
            "cluster off over static on",
            image(&[cluster("false")]),
            true,
            false,
        ),
        (
            "broker on over cluster off",
            image(&[cluster("false"), broker(NODE, "true")]),
            true,
            true,
        ),
        (
            "broker off over cluster on",
            image(&[cluster("true"), broker(NODE, "false")]),
            true,
            false,
        ),
        (
            "broker on over static off",
            image(&[broker(NODE, "true")]),
            false,
            true,
        ),
        (
            "broker off over static on",
            image(&[broker(NODE, "false")]),
            true,
            false,
        ),
        (
            "another broker's value",
            image(&[broker(NodeId(2), "false")]),
            true,
            true,
        ),
        (
            "another broker's value, static off",
            image(&[broker(NodeId(2), "true")]),
            false,
            false,
        ),
        (
            "not a boolean falls to static on",
            image(&[broker(NODE, "no")]),
            true,
            true,
        ),
        (
            "not a boolean falls to static off",
            image(&[broker(NODE, "no")]),
            false,
            false,
        ),
    ];
    for (name, image, static_value, want) in cases {
        let config = BrokerConfig {
            node_id: NODE,
            transaction_partition_verification_enable: static_value,
            ..BrokerConfig::default()
        };
        assert!(
            partition_verification_enabled(&image, &config) == want,
            "{name}"
        );
    }
}

/// Only a verify-only operation skips the coordinator, and only with the knob
/// off: an operation that adds the partition still has to reach the
/// coordinator.
#[test]
fn only_a_verify_only_call_skips_the_coordinator_and_only_with_the_knob_off() {
    // (the operation adds the partition, the knob is on, the call is skipped)
    let cases = [
        (false, true, false),
        (false, false, true),
        (true, true, false),
        (true, false, false),
    ];
    for (adds_partition, enabled, skipped) in cases {
        assert!(
            skips_coordinator_verification(adds_partition, enabled) == skipped,
            "adds_partition={adds_partition} enabled={enabled}"
        );
    }
}
