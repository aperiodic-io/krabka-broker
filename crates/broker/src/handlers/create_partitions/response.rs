//! Assembly of the `CreatePartitions` response, including the
//! KIP-599 throttle that the handler records after it has built the per-topic
//! result rows.

use krabka_protocol::owned::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use krabka_units::Time;

use crate::{handlers::RequestContext, metrics::QuotaType, quota::throttle_time_ms};

pub(super) fn create_partitions_response(
    results: Vec<CreatePartitionsTopicResult>,
    throttle_time_ms: i32,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results,
        throttle_time_ms,
        ..Default::default()
    }
}

pub(super) fn finish_response(
    context: &RequestContext<'_>,
    delay: Time,
    results: Vec<CreatePartitionsTopicResult>,
) -> CreatePartitionsResponse {
    // KIP-599: the controller-mutation delay goes to the dispatch loop, which
    // resolves it with the KIP-124 request quota in one metrics call and
    // reports the larger of the two, as Kafka's
    // `sendResponseMaybeThrottleWithControllerQuota` does. The response
    // carries the controller-mutation delay now; the dispatch loop raises it
    // when the request quota asks for more.
    context.defer_quota_charge((QuotaType::ControllerMutation, delay).into());
    create_partitions_response(results, throttle_time_ms(delay))
}
