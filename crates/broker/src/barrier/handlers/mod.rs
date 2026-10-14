//! The wire handlers of the barrier control plane.
//!
//! Five RPCs expose [`BarrierCoordinator`][crate::barrier::coordinator::BarrierCoordinator]
//! to a caller. Four of them are a control plane that an operator drives, and
//! the fifth is inter-broker traffic that one coordinator sends to the leader
//! of a target partition.
//!
//! | Api key | Module | Purpose |
//! | --- | --- | --- |
//! | 1010 | [`alter_groups`] | create, update, and delete groups |
//! | 1011 | [`describe_groups`] | read the group definitions |
//! | 1012 | [`trigger`] | make one cut on demand |
//! | 1013 | [`list_cuts`] | read the cuts a group retains |
//! | 1014 | [`write_markers`] | append markers into locally-led partitions |
//!
//! [`transport`] is the other half of api key 1014. It sends the request that
//! [`write_markers`] receives.
//!
//! Every one of the five keys sits in the krabka-private range at 1000 and
//! above, and every one speaks version 0 only with flexible framing. The
//! broker registers them for dispatch and never advertises them, so
//! `kafka-broker-api-versions.sh` prints no row for them.
//!
//! # Error codes
//!
//! [`error_code`] maps one [`BarrierError`] onto the code that the response
//! carries. A control-plane response carries the code in a per-group row or in
//! a top-level field, and never as a transport failure, so a caller reads one
//! response shape whatever the outcome.

pub(crate) mod alter_groups;
pub(crate) mod describe_groups;
pub(crate) mod list_cuts;
#[cfg(test)]
mod string_bounds_tests;
pub(crate) mod transport;
pub(crate) mod trigger;
pub(crate) mod write_markers;

// The `Describe` gate that `describe_groups` and `list_cuts` apply, and the
// `ClusterAction` gate that `write_markers` applies. Both live in
// `crate::handlers` beside their `Alter` twin, because the write-freeze and
// break-glass control planes read the cluster through the same gates. Kafka
// applies `ClusterAction` to `WriteTxnMarkers`, and `WriteBarrierMarkers` is
// the same kind of inter-broker traffic.
pub(crate) use crate::handlers::{cluster_action_denied, cluster_describe_image};
use {
    crate::{
        barrier::{error::BarrierError, persistence::CutValue},
        codes,
    },
    krabka_protocol::krabka::barrier::{
        BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition,
    },
    krabka_units::{Time, convert::TimeExt as _},
};

/// The `interval_ms` value that turns periodic injection off.
pub(crate) const INTERVAL_OFF: i64 = -1;

/// The `epoch` value of a response that carries no cut.
pub(crate) const NO_EPOCH: i64 = -1;

/// The Kafka error code that a [`BarrierError`] takes on the wire.
///
/// The mapping is total, so every response carries a code that a caller can
/// act on:
///
/// | Variant | Code |
/// | --- | --- |
/// | `InjectionInProgress` | `BARRIER_INJECTION_IN_PROGRESS` (1000) |
/// | `EpochExhausted` | `INVALID_REQUEST` (42) |
/// | `NotCoordinator`, `CoordinatorEpochChanged` | `NOT_COORDINATOR` (16) |
/// | `UnknownGroup` | `RESOURCE_NOT_FOUND` (91) |
/// | `GroupExists` | `TOPIC_ALREADY_EXISTS` (36) |
/// | `InvalidDefinition` | `INVALID_CONFIG` (40) |
/// | `StateNotLocal`, `Persist` | `COORDINATOR_NOT_AVAILABLE` (15) |
///
/// `InvalidDefinition` reaches this function only for a retention or an
/// interval that is out of range, because [`alter_groups`] rejects a malformed
/// topic list, and a group or topic name over 32767 bytes, with
/// `INVALID_REQUEST` before the coordinator sees the entry.
pub(crate) fn error_code(error: &BarrierError) -> i16 {
    match error {
        BarrierError::InjectionInProgress { .. } => codes::BARRIER_INJECTION_IN_PROGRESS,
        BarrierError::EpochExhausted { .. } => codes::INVALID_REQUEST,
        BarrierError::NotCoordinator { .. } | BarrierError::CoordinatorEpochChanged { .. } => {
            codes::NOT_COORDINATOR
        }
        BarrierError::UnknownGroup { .. } => codes::RESOURCE_NOT_FOUND,
        BarrierError::GroupExists { .. } => codes::TOPIC_ALREADY_EXISTS,
        BarrierError::InvalidDefinition(_) => codes::INVALID_CONFIG,
        BarrierError::StateNotLocal { .. } | BarrierError::Persist(_) => {
            codes::COORDINATOR_NOT_AVAILABLE
        }
    }
}

/// The text that a response carries beside a non-zero error code.
pub(crate) fn error_text(error: &BarrierError) -> String { error.to_string() }

/// The cut offsets of one published cut, grouped by topic.
pub(crate) fn cut_topics(cut: &CutValue) -> Vec<BarrierCutTopic> {
    cut.topics
        .iter()
        .map(|topic| BarrierCutTopic {
            topic: topic.topic.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|partition| BarrierCutPartition {
                    partition: partition.partition.get(),
                    offset: partition.offset.get(),
                    ..BarrierCutPartition::default()
                })
                .collect(),
            ..BarrierCutTopic::default()
        })
        .collect()
}

/// The partitions of one published cut that took no marker.
pub(crate) fn cut_missing(cut: &CutValue) -> Vec<BarrierMissingPartition> {
    cut.missing
        .iter()
        .map(|missing| BarrierMissingPartition {
            topic: missing.topic.clone(),
            partition: missing.partition.get(),
            ..BarrierMissingPartition::default()
        })
        .collect()
}

/// The `interval_ms` field of a group definition.
pub(crate) fn interval_to_wire(interval: Option<Time>) -> i64 {
    interval.map_or(INTERVAL_OFF, Time::millis_i64)
}

/// The injection interval that an `interval_ms` field names.
///
/// [`INTERVAL_OFF`] turns periodic injection off. Every other value becomes an
/// interval, and `validate_spec` rejects the ones at or below zero.
pub(crate) fn interval_from_wire(interval_ms: i64) -> Option<Time> {
    if interval_ms == INTERVAL_OFF {
        None
    } else {
        Some(Time::from_millis(interval_ms))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            barrier::persistence::{CutStatus, MissingPartition, PartitionOffset, TopicOffsets},
            error::BrokerError,
        },
        assert2::check,
        krabka_ids::PartitionIndex,
        krabka_log::Offset,
        krabka_units::millis,
    };

    fn cut() -> CutValue {
        CutValue {
            triggered_at: 1_724_500_000_000,
            completed_at: 1_724_500_000_020,
            status: CutStatus::Partial,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: vec![
                    PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(11),
                    },
                    PartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(12),
                    },
                ],
            }],
            missing: vec![MissingPartition {
                topic: "payments".to_owned(),
                partition: PartitionIndex(3),
            }],
        }
    }

    #[test]
    fn every_coordinator_error_takes_the_code_of_its_family() {
        struct Case {
            error: BarrierError,
            code: i16,
        }
        let cases: Vec<Case> = vec![
            Case {
                error: BarrierError::InjectionInProgress {
                    group: "g".to_owned(),
                },
                code: codes::BARRIER_INJECTION_IN_PROGRESS,
            },
            Case {
                error: BarrierError::EpochExhausted {
                    group: "g".to_owned(),
                },
                code: codes::INVALID_REQUEST,
            },
            Case {
                error: BarrierError::NotCoordinator {
                    group: "g".to_owned(),
                },
                code: codes::NOT_COORDINATOR,
            },
            Case {
                error: BarrierError::CoordinatorEpochChanged {
                    group: "g".to_owned(),
                    expected: 3,
                    current: 4,
                },
                code: codes::NOT_COORDINATOR,
            },
            Case {
                error: BarrierError::UnknownGroup {
                    group: "g".to_owned(),
                },
                code: codes::RESOURCE_NOT_FOUND,
            },
            Case {
                error: BarrierError::GroupExists {
                    group: "g".to_owned(),
                },
                code: codes::TOPIC_ALREADY_EXISTS,
            },
            Case {
                error: BarrierError::InvalidDefinition("retained_cuts is 0".to_owned()),
                code: codes::INVALID_CONFIG,
            },
            Case {
                error: BarrierError::StateNotLocal {
                    partition: PartitionIndex(2),
                },
                code: codes::COORDINATOR_NOT_AVAILABLE,
            },
            Case {
                error: BarrierError::Persist(BrokerError::Replication("append failed".into())),
                code: codes::COORDINATOR_NOT_AVAILABLE,
            },
        ];
        for Case {
            error,
            code: expected,
        } in cases
        {
            check!(error_code(&error) == expected, "{error}");
        }
    }

    #[test]
    fn an_error_text_repeats_the_display_text_of_the_error() {
        let error = BarrierError::UnknownGroup {
            group: "orders-cut".to_owned(),
        };
        check!(error_text(&error) == error.to_string());
    }

    #[test]
    fn a_cut_becomes_the_offsets_and_the_missing_partitions_of_the_wire() {
        let expected_topics = vec![BarrierCutTopic {
            topic: "orders".to_owned(),
            partitions: vec![
                BarrierCutPartition {
                    partition: 0,
                    offset: 11,
                    ..BarrierCutPartition::default()
                },
                BarrierCutPartition {
                    partition: 1,
                    offset: 12,
                    ..BarrierCutPartition::default()
                },
            ],
            ..BarrierCutTopic::default()
        }];
        let expected_missing = vec![BarrierMissingPartition {
            topic: "payments".to_owned(),
            partition: 3,
            ..BarrierMissingPartition::default()
        }];
        check!(cut_topics(&cut()) == expected_topics);
        check!(cut_missing(&cut()) == expected_missing);
    }

    #[test]
    fn an_interval_round_trips_through_the_wire_field() {
        struct Case {
            interval: Option<Time>,
            wire: i64,
        }
        let cases: &[Case] = &[
            Case {
                interval: None,
                wire: INTERVAL_OFF,
            },
            Case {
                interval: Some(millis(1)),
                wire: 1,
            },
            Case {
                interval: Some(millis(60_000)),
                wire: 60_000,
            },
        ];
        for Case { interval, wire } in cases {
            check!(interval_to_wire(*interval) == *wire, "{wire}");
            check!(interval_from_wire(*wire) == *interval, "{wire}");
        }
    }

    #[test]
    fn an_interval_of_zero_stays_an_interval_so_the_coordinator_rejects_it() {
        check!(interval_from_wire(0) == Some(Time::ZERO));
    }
}
