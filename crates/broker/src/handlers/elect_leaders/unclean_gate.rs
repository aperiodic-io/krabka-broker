//! The KFC-9 break-glass gate that an unclean election passes, and the refusal
//! row a partition gets when it does not.
//!
//! An unclean election elects a replica that is missing committed records, so
//! the broker looks up the approved proposal that authorizes it in its own
//! metadata image. A cluster whose `[break_glass]` section names no approver
//! gates nothing, and a preferred election never reaches this module at all.

pub(super) use crate::break_glass::gate::consumed_proposal_id;
use {
    super::env::ElectionEnv,
    crate::{
        break_glass::gate::BreakGlassDenial, codes, handlers::partition_transition::audit_refusal,
    },
    krabka_metadata::BreakGlassAction,
    krabka_protocol::owned::elect_leaders_response::PartitionResult,
};

krabka_macros::partition_gate! {
/// KFC-9: find the approved proposal that authorizes an unclean election of one
/// partition, and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. Every transition then behaves as it does on a cluster with no such
/// section, which is what keeps a stock cluster working.
pub(super) fn authorize_unclean = UncleanElectLeaders;
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
pub(super) use crate::handlers::partition_transition::partition_target as unclean_target;

/// Refuse one partition: count it, audit it, and build its error row.
pub(super) fn refuse_unclean(
    env: &ElectionEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> PartitionResult {
    let message = denial.to_string();
    audit_refusal(
        env.broker,
        env.ctx,
        BreakGlassAction::UncleanElectLeaders,
        || unclean_target(topic, partition),
        denial,
        &message,
    );
    PartitionResult {
        partition_id: partition,
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(message),
        ..Default::default()
    }
}
