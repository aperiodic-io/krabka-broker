//! `DeleteTopics` (`api_key=20`). This handler routes through
//! `Controller::submit_change_if_image`, so the metadata quorum records every topic
//! deletion before the broker tears down the partition dirs and the in-memory
//! state.
//!
//! # KFC-9: deleting a topic needs two people
//!
//! A topic deletion destroys every record the topic holds, so it is one of the
//! transitions the break-glass two-person rule gates. The request gains no
//! field for it: an operator gets an approval out of band through
//! `krabka-guard`, targeted at the topic name, and then runs
//! `kafka-topics --delete`.
//!
//! # KFC-9: a frozen topic is never deleted
//!
//! A write freeze refuses every operation that removes data from the topic it
//! covers, so a frozen topic answers `POLICY_VIOLATION (44)` here whatever the
//! caller holds. The check runs ahead of the two-person rule, because an
//! approval to delete does not defeat a freeze and a refusal must not spend
//! one.
//!
//! A refused topic answers `POLICY_VIOLATION (44)` on its own row, which is the
//! code Apache Kafka already returns from `CreateTopicPolicy` and
//! `AlterConfigPolicy`, so `AdminClient` surfaces a `PolicyViolationException`
//! per topic. The consumed proposal rides the same `submit_change` call as the
//! delete record, so the approval and the deletion commit together. The gate is
//! active only when `[break_glass]` names an approver set.
//!
//! This file holds the request flow. Request resolution lives in `request`,
//! the `Describe` and `Delete` ACL checks in `authz`, the break-glass gate in `gate`, the
//! response shapes in `wire`, the post-commit local tear-down in `teardown`,
//! the remote-tier snapshot and cascade in `tiering`, and the audit record in
//! `audit`.

use std::time::Duration;

use krabka_protocol::owned::{
    delete_topics_request::{self, DeleteTopicsRequest},
    delete_topics_response::DeleteTopicsResponse,
};
use krabka_raft::RaftError;

use crate::{
    codes,
    handlers::{
        encode_response,
        forward_to_controller::{
            forward_if_not_controller, guarded_operation_on_source, is_active_controller,
        },
    },
};

mod audit;
mod authz;
mod gate;
mod operation;
mod request;
mod teardown;
mod tiering;
mod wire;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    authz::{Admission, admit_row, topic_access},
    operation::{delete_topic_once, generation_refusal, vanished_code},
    request::{ValidatedTopics, resolve_topic_names},
    wire::{
        delete_topic_result, delete_topics_response, random_seed, request_error_results,
        shuffle_rows,
    },
};

raw_dispatch! {
    /// Resolve topic identifiers and per-topic authorization on the active controller.
    DeleteTopicsRequest, (broker, version, req_bytes, ctx, req), {
    forward_if_not_controller!(
        broker,
        ctx,
        delete_topics_request::API_KEY,
        req_bytes,
        version,
        |code, _| {
            encode_response(
                &delete_topics_response(request_error_results(&req, code), 0),
                version,
            )
        }
    );
    if !is_active_controller(broker) {
        return encode_response(
            &delete_topics_response(request_error_results(&req, codes::NOT_CONTROLLER), 0),
            version,
        );
    }
    encode_response(&handle(broker, req, version, ctx).await?, version)
    }
}

context_handler! {
    DeleteTopicsRequest => DeleteTopicsResponse,
    (broker, req, version, ctx),
    {
        let controller = &broker.controller;

        // Kafka's `ControllerApis.deleteTopics` refuses the whole request when
        // `delete.topic.enable` is false, ahead of every other check: below v3
        // with INVALID_REQUEST, from v3 with TOPIC_DELETION_DISABLED.
        if !broker.config.delete_topic_enable {
            let error_code = if version < 3 {
                codes::INVALID_REQUEST
            } else {
                codes::TOPIC_DELETION_DISABLED
            };
            return Ok(delete_topics_response(
                request_error_results(&req, error_code),
                0,
            ));
        }

        let image = controller.current_image();
        // Kafka's `ControllerApis.deleteTopics` answers INVALID_REQUEST for a row
        // with no name and no id, a row with both, and a duplicate name or id.
        // Those rows take no further part: no quota charge, no authorization, and
        // no deletion.
        let mut validated = resolve_topic_names(&req, &image);

        // ── ACL preamble ────────────────────────────────────────
        // Cluster `Delete` is a shortcut: an Allow there makes every topic
        // describable and deletable without a further lookup. Only a Deny falls
        // back to `Describe` and `Delete` on each `Topic(name)` individually.
        let access = topic_access(
            broker,
            &image,
            ctx,
            validated
                .topics
                .iter()
                .filter_map(|(name, _, _)| name.as_deref()),
        );
        // A name whose topic id another row carries is INVALID_REQUEST too, but
        // Kafka decides that only for a topic the principal may delete.
        validated.reject_names_of_supplied_ids(&image, &access);
        let ValidatedTopics {
            topics: name_list,
            invalid: mut results,
            ..
        } = validated;

        // Existence and authorization, in Kafka's order per row shape. What
        // survives is the set of existing topics the principal may delete.
        let mut admitted = Vec::with_capacity(name_list.len());
        for row in name_list {
            let requested_by_id = row.1;
            match admit_row(row, &image, &access) {
                Admission::Answer(result) => results.push(result),
                Admission::Delete { name, topic_id } => admitted.push((name, topic_id, requested_by_id)),
            }
        }

        // KIP-599: Kafka's controller charges each topic it deletes with its
        // partition count, after the checks above (`deleteTopic`). A strict
        // version (v5+) refuses the topic that finds the bucket negative, and
        // every topic after it.
        let mut quota = ctx.controller_mutation_quota(broker, &image, version >= 5);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(u64::try_from(req.timeout_ms).unwrap_or(0));
        for (name, wire_topic_id, requested_by_id) in admitted {
            let image = controller.current_image();
            if let Some(row) = generation_refusal(&image, &name, wire_topic_id, requested_by_id) {
                results.push(row);
                continue;
            }
            // An internal retry does not charge the same admission twice.
            if quota.record(image.partitions_of(&name).count() as u64).is_err() {
                results.push(delete_topic_result(Some(name), wire_topic_id, codes::THROTTLING_QUOTA_EXCEEDED));
                continue;
            }
            let attempt = guarded_operation_on_source(broker.controller.as_ref(), deadline, |image|
                delete_topic_once(broker, ctx, &name, wire_topic_id, requested_by_id, image)).await;
            let row = match attempt {
                Ok(Ok(row)) => row,
                failure => {
                    let code = match failure {
                        Err(_) => codes::REQUEST_TIMED_OUT,
                        Ok(Err(RaftError::Metadata(krabka_metadata::MetadataError::UnknownTopic(_)))) => vanished_code(requested_by_id),
                        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => codes::NOT_CONTROLLER,
                        Ok(Err(error)) => {
                            tracing::error!(topic = %name, %error, "DeleteTopics guarded submission failed");
                            crate::handlers::submit_failure_code(&error, codes::UNKNOWN_SERVER_ERROR)
                        },
                        Ok(Ok(_)) => unreachable!("successful row handled above"),
                    };
                    delete_topic_result(Some(name), wire_topic_id, code)
                }
            };
            results.push(row);
        }
        shuffle_rows(&mut results, random_seed());

        // Each committed deletion audits from its owned completion task,
        // including a commit that finishes after this request times out.

        // KIP-599: the controller-mutation delay goes to the dispatch loop, which
        // resolves it with the KIP-124 request quota in one metrics call and
        // reports the larger of the two, as Kafka's
        // `sendResponseMaybeThrottleWithControllerQuota` does. The response
        // carries the controller-mutation delay now; the dispatch loop raises it
        // when the request quota asks for more.
        let delay = quota.delay();
        ctx.defer_quota_charge((crate::metrics::QuotaType::ControllerMutation, delay).into());
        let throttle_time_ms = crate::quota::throttle_time_ms(delay);

        let resp = delete_topics_response(results, throttle_time_ms);
        Ok(resp)
    }
}
