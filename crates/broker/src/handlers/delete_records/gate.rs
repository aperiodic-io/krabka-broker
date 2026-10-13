//! KFC-9: the break-glass two-person rule over a `DeleteRecords` trim.
//!
//! A trim destroys committed records, so it is gated. This module finds the
//! approved proposal that authorizes one partition's trim, spends it, and
//! builds the refusal row for a trim no proposal covers. The trim itself, and
//! the freeze check that answers ahead of this gate, stay in the module root.

pub(super) use crate::break_glass::gate::consumed_proposal_id;
use {
    super::{TrimEnv, response::error_partition_result},
    crate::{
        break_glass::{
            gate::{self, BreakGlassDenial},
            persistence::spend_before_local_action,
        },
        broker::Broker,
        codes,
        handlers::partition_transition::audit_refusal,
    },
    krabka_metadata::{BreakGlassAction, MetadataRecord},
    krabka_protocol::owned::delete_records_response::DeleteRecordsPartitionResult,
    std::collections::HashSet,
    uuid::Uuid,
};

krabka_macros::partition_gate! {
/// KFC-9: find the approved proposal that authorizes a trim of one partition,
/// and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. A trim then behaves as it does on a cluster with no such section.
pub(super) fn authorize_trim = DeleteRecords;
}

/// Append the consumed proposal, and answer the proposal it names.
///
/// # Errors
///
/// Returns the submit failure when the quorum did not take the consume. No trim
/// runs in that case, so the approval stays unspent.
pub(super) async fn spend_approval(
    broker: &Broker,
    spent: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
    target: &str,
) -> Result<Option<Uuid>, krabka_raft::RaftError> {
    spend_before_local_action(
        broker,
        spent,
        consumed,
        gate::is_gated(&broker.config.break_glass),
        BreakGlassAction::DeleteRecords,
        target,
    )
    .await
}

/// Refuse one partition: count it, audit it, and build its error row.
///
/// The row carries the code alone, because no `DeleteRecords` response version
/// has an `error_message` field to put the reason in.
pub(super) fn refuse_trim(
    env: &TrimEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> DeleteRecordsPartitionResult {
    let message = denial.to_string();
    tracing::warn!(%topic, partition, refusal = %message, "DeleteRecords refused");
    audit_refusal(
        env.broker,
        env.ctx,
        BreakGlassAction::DeleteRecords,
        || trim_target(topic, partition),
        denial,
        &message,
    );
    error_partition_result(partition, codes::POLICY_VIOLATION)
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
pub(super) use crate::handlers::partition_transition::partition_target as trim_target;

#[cfg(test)]
mod tests {
    use {
        super::authorize_trim,
        crate::{
            break_glass::gate::tests::{approved_proposal, image_of},
            config::BreakGlassConfig,
            handlers::delete_records::test_support::gated_config,
        },
        assert2::check,
        krabka_metadata::{BreakGlassAction, MetadataImage},
    };

    #[test]
    fn the_trim_gate_answers_from_the_proposal_registry() {
        /// An image and break-glass config with whether the trim gate admits the trim.
        struct GateCase {
            label: &'static str,
            image: MetadataImage,
            config: BreakGlassConfig,
            expected: bool,
        }

        let cases: [GateCase; 5] = [
            GateCase {
                label: "an approved proposal on the partition",
                image: image_of(&[approved_proposal(
                    BreakGlassAction::DeleteRecords,
                    "orders-3",
                )]),
                config: gated_config(),
                expected: true,
            },
            GateCase {
                label: "an approved proposal on the whole topic",
                image: image_of(&[approved_proposal(BreakGlassAction::DeleteRecords, "orders")]),
                config: gated_config(),
                expected: true,
            },
            GateCase {
                label: "a proposal for another partition",
                image: image_of(&[approved_proposal(
                    BreakGlassAction::DeleteRecords,
                    "orders-4",
                )]),
                config: gated_config(),
                expected: false,
            },
            GateCase {
                label: "no proposal at all",
                image: image_of(&[]),
                config: gated_config(),
                expected: false,
            },
            GateCase {
                label: "no approver set, so nothing is gated",
                image: image_of(&[]),
                config: BreakGlassConfig::default(),
                expected: true,
            },
        ];
        for GateCase {
            label,
            image,
            config,
            expected,
        } in cases
        {
            let authorized = authorize_trim(&image, &config, "orders", 3).is_ok();
            check!(authorized == expected, "case {label}");
        }
    }

    #[test]
    fn a_refused_trim_names_the_partition_it_refused() {
        let denial = authorize_trim(&image_of(&[]), &gated_config(), "orders", 3)
            .expect_err("no proposal covers orders-3");

        check!(denial.action == BreakGlassAction::DeleteRecords);
        check!(denial.target == "orders-3");
    }
}
