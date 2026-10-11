//! The break-glass approval gate shared by destructive partition operations.

use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord};

use crate::{
    break_glass::{
        gate::{self, BreakGlassDenial},
        handlers::audit::{GatedTransition, audit_transition},
        metrics::record_refusal,
    },
    broker::Broker,
    config::BreakGlassConfig,
    handlers::RequestContext,
    time_util::now_ms,
};

pub(super) fn partition_target(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

pub(super) fn authorize_partition(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    action: BreakGlassAction,
    topic: &str,
    partition: i32,
) -> Result<Option<MetadataRecord>, BreakGlassDenial> {
    if !gate::is_gated(config) {
        return Ok(None);
    }
    gate::authorize(
        image,
        config,
        action,
        &partition_target(topic, partition),
        now_ms(),
    )
    .map(Some)
}

/// Name a partition operation while sharing its complete approval gate.
macro_rules! authorizer {
    ($(#[$meta:meta])* $visibility:vis fn $name:ident = $action:ident;) => {
        $(#[$meta])*
        $visibility fn $name(
            image: &krabka_metadata::MetadataImage,
            config: &crate::config::BreakGlassConfig,
            topic: &str,
            partition: i32,
        ) -> Result<Option<krabka_metadata::MetadataRecord>, crate::break_glass::gate::BreakGlassDenial> {
            crate::handlers::partition_transition::authorize_partition(image, config, krabka_metadata::BreakGlassAction::$action, topic, partition)
        }
    };
}
pub(super) use authorizer;

/// Count a refusal before resolving its target and emitting the audit event.
pub(super) fn audit_refusal<T: AsRef<str>>(
    broker: &Broker,
    context: &RequestContext<'_>,
    action: BreakGlassAction,
    target: impl FnOnce() -> T,
    denial: &BreakGlassDenial,
    reason: &str,
) {
    record_refusal(&broker.metrics, denial.action);
    audit_transition(
        &broker.audit_log,
        &broker.config.break_glass,
        context,
        &GatedTransition {
            action,
            target: target().as_ref(),
            phase: krabka_audit::PrivilegedPhase::Refused,
            proposal_id: denial.proposal_id(),
            reason,
        },
    );
}
