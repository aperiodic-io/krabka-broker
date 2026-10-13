//! `ListBarrierCuts`, api key 1013.
//!
//! The handler returns the cuts that one group retains, in ascending epoch
//! order. `from_epoch` drops every older cut, and `max_results` caps how many
//! come back. `-1` asks for every retained cut and is the request default, so
//! `0` honestly asks for none.
//!
//! This is the RPC read path. The coordinator also publishes every cut to the
//! `__barrier_state` topic, and an ordinary Kafka consumer reads it there, so a
//! client in another language does not need this api key.
//!
//! A group that another broker coordinates gets `NOT_COORDINATOR` (16), and a
//! group that no coordinator holds gets `RESOURCE_NOT_FOUND` (91).
//!
//! Authorization: `Describe` on `Cluster("kafka-cluster")`. On a deny the
//! response carries `CLUSTER_AUTHORIZATION_FAILED` (31).

use {
    crate::{
        barrier::{
            coordinator::RetainedCut,
            handlers::{cluster_describe_denied, cut_missing, cut_topics, error_code, error_text},
        },
        codes,
        handlers::{ErrorResponse as _, encode_response},
    },
    krabka_protocol::krabka::barrier::{
        BarrierCut, ListBarrierCutsRequest, ListBarrierCutsResponse,
    },
};

wire_handler!(
    handle,
    "handle_list_barrier_cuts",
    "ListBarrierCuts",
    "info",
    ListBarrierCutsRequest,
    |broker, version, req, ctx| {
        let image = broker.controller.current_image();
        if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return encode_response(
                &ListBarrierCutsResponse::error(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("list-barrier-cuts denied".to_owned()),
                ),
                version,
            );
        }

        let coordinator = &broker.barrier_coordinator;
        if !coordinator.is_coordinator_for(&req.group).await {
            return encode_response(
                &ListBarrierCutsResponse::error(
                    codes::NOT_COORDINATOR,
                    Some("this broker does not coordinate the barrier group".to_owned()),
                ),
                version,
            );
        }

        let resp = match coordinator.list_cuts(&req.group).await {
            Ok(cuts) => ListBarrierCutsResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                error_message: None,
                cuts: select(&cuts, req.from_epoch, req.max_results),
                ..ListBarrierCutsResponse::default()
            },
            Err(error) => {
                ListBarrierCutsResponse::error(error_code(&error), Some(error_text(&error)))
            }
        };
        encode_response(&resp, version)
    }
);

/// The cuts of the response, in ascending epoch order.
///
/// `from_epoch` drops every older cut and includes its own epoch.
/// `max_results` of `-1` asks for every cut, which is the request default. Any
/// other value is the cap, so `0` returns nothing rather than everything.
fn select(cuts: &[RetainedCut], from_epoch: i64, max_results: i32) -> Vec<BarrierCut> {
    let selected = cuts.iter().filter(|cut| cut.epoch >= from_epoch);
    let limited: Vec<&RetainedCut> = if max_results >= 0 {
        selected
            .take(usize::try_from(max_results).unwrap_or(usize::MAX))
            .collect()
    } else {
        selected.collect()
    };
    limited
        .into_iter()
        .map(|retained| BarrierCut {
            epoch: retained.epoch,
            triggered_at: retained.cut.triggered_at,
            completed_at: retained.cut.completed_at,
            status: retained.cut.status.code(),
            topics: cut_topics(&retained.cut),
            missing: cut_missing(&retained.cut),
            ..BarrierCut::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::{
            error::BarrierError,
            persistence::{CutStatus, CutValue, PartitionOffset, TopicOffsets},
        },
        assert2::check,
        krabka_ids::PartitionIndex,
        krabka_log::Offset,
        krabka_protocol::krabka::barrier::{
            BarrierCutPartition, BarrierCutTopic, CUT_STATUS_COMPLETE,
        },
    };

    fn retained(epoch: i64, offset: i64) -> RetainedCut {
        RetainedCut {
            epoch,
            cut: CutValue {
                triggered_at: 1_724_500_000_000 + epoch,
                completed_at: 1_724_500_000_010 + epoch,
                status: CutStatus::Complete,
                topics: vec![TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(offset),
                    }],
                }],
                missing: Vec::new(),
            },
        }
    }

    fn wire(epoch: i64, offset: i64) -> BarrierCut {
        BarrierCut {
            epoch,
            triggered_at: 1_724_500_000_000 + epoch,
            completed_at: 1_724_500_000_010 + epoch,
            status: CUT_STATUS_COMPLETE,
            topics: vec![BarrierCutTopic {
                topic: "orders".to_owned(),
                partitions: vec![BarrierCutPartition {
                    partition: 0,
                    offset,
                    ..BarrierCutPartition::default()
                }],
                ..BarrierCutTopic::default()
            }],
            missing: Vec::new(),
            ..BarrierCut::default()
        }
    }

    fn three_cuts() -> Vec<RetainedCut> { vec![retained(4, 40), retained(5, 50), retained(6, 60)] }

    #[test]
    fn a_selection_applies_the_epoch_floor_and_the_cap() {
        struct Case {
            from_epoch: i64,
            max_results: i32,
            expected: Vec<BarrierCut>,
        }
        let cases = &[
            // -1 is the request default, and asks for every retained cut.
            Case {
                from_epoch: 0,
                max_results: -1,
                expected: vec![wire(4, 40), wire(5, 50), wire(6, 60)],
            },
            Case {
                from_epoch: 5,
                max_results: -1,
                expected: vec![wire(5, 50), wire(6, 60)],
            },
            Case {
                from_epoch: 7,
                max_results: -1,
                expected: Vec::new(),
            },
            // 0 asks for none, rather than for everything.
            Case {
                from_epoch: 0,
                max_results: 0,
                expected: Vec::new(),
            },
            Case {
                from_epoch: 5,
                max_results: 0,
                expected: Vec::new(),
            },
            Case {
                from_epoch: 5,
                max_results: 1,
                expected: vec![wire(5, 50)],
            },
            Case {
                from_epoch: 0,
                max_results: 2,
                expected: vec![wire(4, 40), wire(5, 50)],
            },
            Case {
                from_epoch: 0,
                max_results: 99,
                expected: vec![wire(4, 40), wire(5, 50), wire(6, 60)],
            },
        ];
        for Case {
            from_epoch,
            max_results,
            expected,
        } in cases
        {
            check!(
                select(&three_cuts(), *from_epoch, *max_results) == *expected,
                "from_epoch {from_epoch} max_results {max_results}"
            );
        }
    }

    #[test]
    fn a_refused_read_returns_the_code_and_no_cut() {
        let error = BarrierError::UnknownGroup {
            group: "orders-cut".to_owned(),
        };
        let expected = ListBarrierCutsResponse {
            throttle_time_ms: 0,
            error_code: codes::RESOURCE_NOT_FOUND,
            error_message: Some(error.to_string()),
            cuts: Vec::new(),
            ..ListBarrierCutsResponse::default()
        };
        check!(
            ListBarrierCutsResponse::error(error_code(&error), Some(error_text(&error)))
                == expected
        );
    }
}
