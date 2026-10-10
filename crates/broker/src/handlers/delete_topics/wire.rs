//! The `DeleteTopics` response shapes: one row per requested topic and the
//! envelope that carries them.
//!
//! Field-for-field construction is the contract with the JVM `AdminClient`, so
//! it sits apart from the code that decides which error code a row gets.

use krabka_protocol::{
    owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `Errors.TOPIC_AUTHORIZATION_FAILED.message()`.
pub(super) const TOPIC_AUTHORIZATION_FAILED_MESSAGE: &str = "Topic authorization failed.";

/// Kafka's `Errors.UNKNOWN_TOPIC_OR_PARTITION.message()`.
pub(super) const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str =
    "This server does not host this topic-partition.";

/// Kafka's `Errors.UNKNOWN_TOPIC_ID.message()`.
pub(super) const UNKNOWN_TOPIC_ID_MESSAGE: &str = "This server does not host this topic ID.";

/// Kafka's `Errors.THROTTLING_QUOTA_EXCEEDED.message()`.
pub(super) const THROTTLING_QUOTA_EXCEEDED_MESSAGE: &str =
    "The throttling quota has been exceeded.";

/// The message Kafka's `new ApiError(error)` carries for the codes this
/// handler answers without a message of its own.
fn default_message(error_code: i16) -> Option<String> {
    let message = match error_code {
        crate::codes::TOPIC_AUTHORIZATION_FAILED => TOPIC_AUTHORIZATION_FAILED_MESSAGE,
        crate::codes::UNKNOWN_TOPIC_OR_PARTITION => UNKNOWN_TOPIC_OR_PARTITION_MESSAGE,
        crate::codes::UNKNOWN_TOPIC_ID => UNKNOWN_TOPIC_ID_MESSAGE,
        crate::codes::THROTTLING_QUOTA_EXCEEDED => THROTTLING_QUOTA_EXCEEDED_MESSAGE,
        _ => return None,
    };
    Some(message.to_string())
}

/// Builds one response row for a requested topic.
///
/// A row with an error carries the message Kafka's `new ApiError(error)` puts
/// on it, which v5 and later send. A success row carries none.
pub(super) fn delete_topic_result(
    name: Option<String>,
    topic_id: WireUuid,
    error_code: i16,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name,
        topic_id,
        error_code,
        error_message: default_message(error_code),
        ..Default::default()
    }
}

/// The rows of a request that fails as a whole, as Kafka's
/// `DeleteTopicsRequest.getErrorResponse` builds them: one per requested
/// topic, in request order, with the name and the id the client sent, the
/// code, and no message.
pub(super) fn request_error_results(
    request: &krabka_protocol::owned::delete_topics_request::DeleteTopicsRequest,
    error_code: i16,
) -> Vec<DeletableTopicResult> {
    let row = |name, topic_id| DeletableTopicResult {
        name,
        topic_id,
        error_code,
        ..Default::default()
    };
    request
        .topic_names
        .iter()
        .map(|name| row(Some(name.clone()), WireUuid::ZERO))
        .chain(
            request
                .topics
                .iter()
                .map(|topic| row(topic.name.clone(), topic.topic_id)),
        )
        .collect()
}

/// A refused row that also carries the text of the refusal.
///
/// `DeleteTopics` v5 and later carry `error_message`, and a break-glass refusal
/// is exactly the case an operator needs it for: the code says the policy
/// refused, and the message says which proposal nearly authorized the deletion.
pub(super) fn refused_topic_result(
    name: String,
    topic_id: WireUuid,
    error_code: i16,
    message: String,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name: Some(name),
        topic_id,
        error_code,
        error_message: Some(message),
        ..Default::default()
    }
}

/// An `INVALID_REQUEST` row from the request validation, with Kafka's message.
///
/// `name` and `topic_id` are the ones that Kafka's `ControllerApis.deleteTopics`
/// puts on the row, which differ per rule.
pub(super) fn invalid_topic_result(
    name: Option<String>,
    topic_id: WireUuid,
    message: &str,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name,
        topic_id,
        error_code: crate::codes::INVALID_REQUEST,
        error_message: Some(message.to_string()),
        ..Default::default()
    }
}

/// Shuffles the rows of a response in place, as Kafka's
/// `ControllerApis.deleteTopics` does with `Collections.shuffle(responses)`,
/// so that a client cannot use row positions to tell an absent topic from a
/// topic it may not see.
///
/// The permutation is a Fisher-Yates shuffle driven by a `SplitMix64` stream
/// from `seed`, so a test that passes a fixed seed sees a fixed order.
pub(super) fn shuffle_rows<T>(rows: &mut [T], seed: u64) {
    let mut state = seed;
    for upper in (1..rows.len()).rev() {
        let bound = u64::try_from(upper + 1).unwrap_or(u64::MAX);
        let pick = usize::try_from(split_mix_64(&mut state) % bound).unwrap_or(upper);
        rows.swap(upper, pick);
    }
}

/// A fresh seed for [`shuffle_rows`] from the process's hash randomness.
pub(super) fn random_seed() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(0_u8)
}

krabka_macros::splitmix64_step!(split_mix_64);

/// Builds the response envelope over the per-topic rows.
pub(super) fn delete_topics_response(
    responses: Vec<DeletableTopicResult>,
    throttle_time_ms: i32,
) -> DeleteTopicsResponse {
    DeleteTopicsResponse {
        responses,
        throttle_time_ms,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::codes;

    #[test]
    fn response_helpers_preserve_topic_identity_error_and_throttle_fields() {
        let id = WireUuid([9; 16]);
        let unknown_id = delete_topic_result(None, id, codes::UNKNOWN_TOPIC_ID);
        let expected_unknown = tagged_wire!(DeletableTopicResult {
            name: None,
            topic_id: id,
            error_code: codes::UNKNOWN_TOPIC_ID,
            error_message: Some(UNKNOWN_TOPIC_ID_MESSAGE.into()),
        });
        assert!(unknown_id == expected_unknown);

        let denied = delete_topic_result(
            Some("secret".into()),
            WireUuid::ZERO,
            codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected_denied = tagged_wire!(DeletableTopicResult {
            name: Some("secret".into()),
            topic_id: WireUuid::ZERO,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.into()),
        });
        assert!(denied == expected_denied);

        let resp = delete_topics_response(vec![denied], 123);
        let expected_resp = tagged_wire!(DeleteTopicsResponse {
            throttle_time_ms: 123,
            responses: vec![expected_denied],
        });
        assert!(resp == expected_resp);
    }

    /// Each code answers with Kafka's `new ApiError(error)` message, and a
    /// success row or a whole-request error row carries none.
    #[test]
    fn rows_carry_kafkas_default_messages() {
        let id = WireUuid([3; 16]);
        let row = |code, message: Option<&str>| {
            tagged_wire!(DeletableTopicResult {
                name: Some("t".into()),
                topic_id: id,
                error_code: code,
                error_message: message.map(str::to_string),
            })
        };
        let actual: Vec<DeletableTopicResult> = [
            codes::NONE,
            codes::TOPIC_AUTHORIZATION_FAILED,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            codes::UNKNOWN_TOPIC_ID,
            codes::THROTTLING_QUOTA_EXCEEDED,
            codes::NOT_CONTROLLER,
        ]
        .into_iter()
        .map(|code| delete_topic_result(Some("t".into()), id, code))
        .collect();
        let expected = vec![
            row(codes::NONE, None),
            row(
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some("Topic authorization failed."),
            ),
            row(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("This server does not host this topic-partition."),
            ),
            row(
                codes::UNKNOWN_TOPIC_ID,
                Some("This server does not host this topic ID."),
            ),
            row(
                codes::THROTTLING_QUOTA_EXCEEDED,
                Some("The throttling quota has been exceeded."),
            ),
            row(codes::NOT_CONTROLLER, None),
        ];
        assert!(actual == expected);
    }

    /// A shuffle keeps every row, a fixed seed gives a fixed order, and
    /// different seeds reach different orders.
    #[test]
    fn shuffle_rows_permutes_deterministically_per_seed() {
        let rows: Vec<u32> = (0..16).collect();
        let shuffled = |seed| {
            let mut out = rows.clone();
            shuffle_rows(&mut out, seed);
            out
        };
        let mut sorted = shuffled(7);
        sorted.sort_unstable();
        let orders: std::collections::HashSet<Vec<u32>> = (0..8).map(shuffled).collect();

        assert!(sorted == rows);
        assert!(shuffled(7) == shuffled(7));
        assert!(orders.len() > 1);
    }
}
