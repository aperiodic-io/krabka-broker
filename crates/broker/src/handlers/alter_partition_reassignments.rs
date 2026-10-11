//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! This file holds the wire handler: it authorizes the whole request, walks
//! every requested row, and submits the accepted records in one batch. The
//! pure planning that turns one alter row into a `PartitionRecord`, or into a
//! wire error code, lives in `plan`, the KFC-9 break-glass gate over a cancel
//! and the batch one request accumulates live in `cancel_approval`, and the
//! result rows and the whole-request refusal live in `response`.
//!
//! # KFC-9: a cancel needs two people, and every alter respects a freeze
//!
//! A cancel reverts a reassignment that is already under way, drops every
//! adding replica, and can move leadership off one. It is one of the
//! transitions the break-glass two-person rule gates. A start does not need an
//! approval, but a live topic freeze refuses starts and cancels alike. The
//! background completion path in [`crate::reassignment`] uses the same freeze
//! admission and classifies completion as work the cluster already accepted.
//!
//! KIP-455 defines the request that `kafka-reassign-partitions` sends and it
//! gains no field for this. An operator gets the approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and the broker looks it up in its own metadata image. A refused row
//! answers `POLICY_VIOLATION (44)` with the refusal text, and the gate is
//! active only when `[break_glass]` names an approver set.

use std::collections::{HashMap, HashSet};

use krabka_protocol::owned::{
    alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
    alter_partition_reassignments_response::{
        AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
        ReassignableTopicResponse,
    },
};
use krabka_verified::FreezeMutationKind;

mod cancel_approval;
mod plan;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::plan::process_one_partition;
use self::{
    cancel_approval::{ReassignBatch, ReassignEnv, alter_one},
    response::whole_request_error,
};
use crate::{
    codes::{CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE, NONE, POLICY_VIOLATION},
    freeze::resolve::resolve_freeze_mutation,
    handlers::{audit_resource, stamp_unset},
};

context_handler! {
    AlterPartitionReassignmentsRequest => AlterPartitionReassignmentsResponse,
    (broker, req, _version, ctx),
    {
        let image = broker.controller.current_image();
        // Whole-request Cluster Alter authorize.
        if crate::handlers::cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return Ok(whole_request_error(
                &req,
                CLUSTER_AUTHORIZATION_FAILED,
                "alter-reassignment denied",
            ));
        }

        let env = ReassignEnv {
            broker,
            image: &image,
            ctx,
            allow_rf_change: req.allow_replication_factor_change,
        };
        let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
        let mut batch = ReassignBatch::default();
        for topic in &req.topics {
            let mut rows = Vec::with_capacity(topic.partitions.len());
            let freeze = resolve_freeze_mutation(
                &image,
                &topic.name,
                true,
                FreezeMutationKind::ReassignmentAlter,
            );
            for p in &topic.partitions {
                rows.push(alter_one(&env, &mut batch, &topic.name, p, freeze));
            }
            by_topic.insert(topic.name.clone(), rows);
        }

        // KIP-966: starting or cancelling a reassignment rewrites the replica set
        // and the ISR with it, so the eligible-leader state the controller keeps
        // rides the same batch. Without it a cancel can leave a replica the
        // partition no longer has in the published ELR.
        crate::elr::ElrPublisher::new(&image).extend(&mut batch.records);

        let mut submit_failure = None;
        if !batch.records.is_empty() {
            match batch.require_audit(broker, ctx).await {
                Ok(()) => {
                    if let Err(error) = broker
                        .controller
                        .submit_change(std::mem::take(&mut batch.records))
                        .await
                    {
                        let message = format!("submit failed: {error}");
                        tracing::warn!(%error, "alter-reassignment submit failed");
                        stamp_unset(
                            by_topic.values_mut().flatten(),
                            crate::handlers::submit_failure_code(&error, COORDINATOR_NOT_AVAILABLE),
                            &message,
                        );
                        submit_failure = Some(message);
                    }
                }
                Err(error) => {
                    let message = format!("privileged action refused: {error}");
                    stamp_unset(by_topic.values_mut().flatten(), POLICY_VIOLATION, &message);
                    submit_failure = Some(message);
                }
            }
        }
        // KFC-9: audit the approvals this append spent, now that its outcome is
        // known.
        batch.audit_applied(broker, ctx, submit_failure.as_deref());

        // A cancel audits itself as a `PrivilegedAction` through the two-person
        // gate. An ordinary start spends no approval, so every partition the
        // request actually altered is audited here.
        crate::handlers::audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "AlterPartitionReassignments",
            audited_partitions(&by_topic, &batch.altered),
        );

        let responses: Vec<ReassignableTopicResponse> = by_topic
            .into_iter()
            .map(|(name, partitions)| tagged_wire!(ReassignableTopicResponse { name, partitions }))
            .collect();
        // Kafka's `ReplicationControlManager.alterPartitionReassignments` sets the
        // top-level `ErrorMessage` to null explicitly.
        Ok(unthrottled_wire!(AlterPartitionReassignmentsResponse {
            allow_replication_factor_change: req.allow_replication_factor_change,
            error_code: 0,
            error_message: None,
            responses,
        }))
    }
}

/// The partitions an `AlterPartitionReassignments` request actually
/// reassigned.
///
/// A success row is not a mutation. A requested target that already equals the
/// partition's current one plans no record, and the row still answers `NONE`,
/// so the audited set is the intersection of the rows that succeeded with the
/// partitions that contributed a record to the append: `altered` holds those,
/// in the same `"<topic>-<partition>"` spelling the audit resource carries.
fn audited_partitions(
    by_topic: &HashMap<String, Vec<ReassignablePartitionResponse>>,
    altered: &HashSet<String>,
) -> Vec<krabka_audit::AuditResource> {
    by_topic
        .iter()
        .flat_map(|(topic, rows)| {
            rows.iter()
                .filter(|row| row.error_code == NONE)
                .filter_map(move |row| {
                    let name = format!("{topic}-{}", row.partition_index);
                    altered
                        .contains(&name)
                        .then(|| audit_resource("Partition", name))
                })
        })
        .collect()
}
