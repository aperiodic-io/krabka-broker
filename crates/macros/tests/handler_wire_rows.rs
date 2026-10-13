//! Real wire rows exercise fixed projections and native acknowledgement expressions.

use {
    assert2::assert,
    krabka_protocol::owned::{
        offset_fetch_response as wire, share_acknowledge_request::AcknowledgementBatch,
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::AcknowledgementBatch as FetchBatch,
        share_fetch_response::ShareFetchResponse,
    },
    std::{cell::Cell, ops::ControlFlow},
};

mod codes {
    pub(crate) const NONE: i16 = 0;
    pub(crate) const GROUP_AUTHORIZATION_FAILED: i16 = 30;
}

struct Offset(i64);
struct Code(i8);
struct OffsetEntry {
    offset: Offset,
    leader_epoch: i32,
    metadata: String,
}
struct Batch {
    first_offset: Offset,
    last_offset: Offset,
    types: Vec<Code>,
}

mod rows {
    use super::{OffsetEntry, wire};

    krabka_macros::offset_fetch_rows! {
        wire::OffsetFetchResponsePartition { missing: missing_legacy, stable: stable_legacy }
        wire::OffsetFetchResponsePartitions { missing: missing_group, stable: stable_group }
    }
}

#[test]
fn both_offset_wire_shapes_preserve_missing_and_committed_values() {
    let entry = OffsetEntry {
        offset: Offset(41),
        leader_epoch: 7,
        metadata: "checkpoint".into(),
    };
    assert!(
        rows::missing_legacy(3, 29)
            == wire::OffsetFetchResponsePartition {
                partition_index: 3,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: Some(String::new()),
                error_code: 29,
                ..Default::default()
            }
    );
    assert!(
        rows::stable_group(5, &entry)
            == wire::OffsetFetchResponsePartitions {
                partition_index: 5,
                committed_offset: 41,
                committed_leader_epoch: 7,
                metadata: Some("checkpoint".into()),
                error_code: 0,
                ..Default::default()
            }
    );
    let stable_legacy = rows::stable_legacy(8, &entry);
    let missing_group = rows::missing_group(12, 16);
    assert!(
        stable_legacy.committed_offset == 41
            && stable_legacy.metadata.as_deref() == Some("checkpoint")
    );
    assert!(missing_group.committed_offset == -1 && missing_group.error_code == 16);
    assert!(entry.metadata == "checkpoint");
}

fn batches<const FIRST: i64, const LAST: i64>(calls: &Cell<u32>) -> Vec<Batch> {
    calls.set(calls.get() + 1);
    vec![Batch {
        first_offset: Offset(FIRST),
        last_offset: Offset(LAST),
        types: vec![Code(1), Code(3)],
    }]
}

#[test]
fn acknowledgement_inputs_evaluate_once_and_keep_each_foreign_wire_type() {
    let calls = Cell::new(0);
    let fetch: Vec<FetchBatch> = krabka_macros::acknowledgement_batches!(FetchBatch; [
        if calls.get() == 0 {
            batches::<5, 9>(&calls)
        } else {
            Vec::new()
        }
    ]);
    assert!(calls.get() == 1);
    assert!(
        fetch
            == vec![FetchBatch {
                first_offset: 5,
                last_offset: 9,
                acknowledge_types: vec![1, 3],
                ..Default::default()
            }]
    );
    let acknowledge: Vec<AcknowledgementBatch> =
        krabka_macros::acknowledgement_batches!(AcknowledgementBatch; [batches::<13, 18>(&calls)]);
    assert!(calls.get() == 2);
    assert!(
        acknowledge
            == vec![AcknowledgementBatch {
                first_offset: 13,
                last_offset: 18,
                acknowledge_types: vec![1, 3],
                ..Default::default()
            }]
    );
}

fn fallible_batches(calls: &Cell<u32>) -> Result<Vec<FetchBatch>, &'static str> {
    fn fail(calls: &Cell<u32>) -> Result<Vec<Batch>, &'static str> {
        calls.set(calls.get() + 1);
        Err("batch refusal")
    }
    Ok(krabka_macros::acknowledgement_batches!(FetchBatch; [fail(calls)?]))
}

#[test]
fn native_batch_errors_exit_the_enclosing_function() {
    let calls = Cell::new(0);
    assert!(fallible_batches(&calls) == Err("batch refusal"));
    assert!(calls.get() == 1);
}

#[test]
fn group_denials_keep_all_empty_wire_defaults_for_both_apis() {
    let fetch = krabka_macros::share_group_denial!(ShareFetchResponse);
    let acknowledge = krabka_macros::share_group_denial!(ShareAcknowledgeResponse);
    assert!(
        fetch
            == ShareFetchResponse {
                error_code: 30,
                ..Default::default()
            }
    );
    assert!(
        acknowledge
            == ShareAcknowledgeResponse {
                error_code: 30,
                ..Default::default()
            }
    );
}

enum RoutingDecision {
    Local,
    Answer,
    Refusal,
}

mod handlers {
    pub(crate) mod forward_to_controller {
        use super::super::{Cell, ControlFlow, RoutingDecision, assert};

        pub(crate) async fn controller_listener_dispatch(
            broker: &RoutingDecision,
            context: &Cell<u32>,
            request: u8,
            version: u8,
            operation: impl FnOnce(u8) -> u8,
            refusal: impl FnOnce() -> &'static str,
        ) -> ControlFlow<Result<u8, &'static str>, u8> {
            std::future::ready(()).await;
            context.set(context.get() + 1);
            assert!(version == 3);
            match broker {
                RoutingDecision::Local => ControlFlow::Continue(request),
                RoutingDecision::Answer => ControlFlow::Break(Ok(operation(request))),
                RoutingDecision::Refusal => ControlFlow::Break(Err(refusal())),
            }
        }
    }
}

fn local_answer(request: u8) -> u8 { request + 2 }
fn controller_refusal() -> &'static str { "controller refusal" }

async fn routed_request(decision: &RoutingDecision, calls: &Cell<u32>) -> Result<u8, &'static str> {
    let owned = 7;
    let version = 3;
    let request = krabka_macros::controller_request!(decision, calls, owned, version; local_answer, controller_refusal);
    calls.set(calls.get() + 1);
    Ok(request)
}

#[tokio::test]
async fn controller_routes_return_early_or_retain_the_owned_request() {
    let calls = Cell::new(0);
    assert!(routed_request(&RoutingDecision::Local, &calls).await == Ok(7));
    assert!(calls.get() == 2);
    calls.set(0);
    assert!(routed_request(&RoutingDecision::Answer, &calls).await == Ok(9));
    assert!(calls.get() == 1);
    calls.set(0);
    assert!(routed_request(&RoutingDecision::Refusal, &calls).await == Err("controller refusal"));
    assert!(calls.get() == 1);
}
