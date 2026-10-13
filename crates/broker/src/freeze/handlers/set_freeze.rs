//! `SetTopicFreeze`, api key 1015.
//!
//! One request freezes a scope, or removes the freeze on one. A scope is a
//! literal topic name or a topic-name prefix, and `pattern_type` says which.
//! The controller writes the outcome to the metadata log, so a freeze holds
//! after a restart and after a leader change.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On a deny the
//! response carries `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! # A freeze takes one command, and a thaw takes two people
//!
//! A freeze needs no break-glass proposal. An operator must reach it in one
//! command during an incident, on a cluster where nobody installed key material
//! yet, and freezing is the safe direction.
//!
//! A thaw needs an approved proposal, and it needs a signature whatever
//! `freeze.require_signature` says. That asymmetry is the feature. Without it a
//! freeze is only as strong as the one credential that set it, which is the
//! credential an attacker already holds when the incident is a compromise.
//!
//! # The consume and the thaw commit together
//!
//! A thaw prepends the consumed proposal record to its own record and submits
//! one `submit_change` call. That single raft append is what stops one approval
//! from authorizing two thaws across a crash.
//!
//! # The timestamp
//!
//! A signed record keeps the `set_at_ms` that the operator signed, because the
//! broker checks that value against the skew window and against the entry it
//! replaces. An unsigned record takes this broker's clock instead. An unsigned
//! freeze is the broker's attestation rather than the operator's proof, so the
//! broker's clock is the honest stamp, and a client then cannot park an entry
//! in the far future where no later record replaces it.

use {
    self::{
        checks::{FreezeEnv, prepare},
        commit::submit,
        outcome::Refusal,
        response::{FREEZE_ACTION, THAW_ACTION, respond},
    },
    crate::{
        codes,
        freeze::{
            freeze_target,
            handlers::{FreezeAudit, pattern_type_concrete, require_freeze},
        },
        handlers::encode_response,
    },
    krabka_protocol::krabka::freeze::SetTopicFreezeRequest,
    uuid::Uuid,
};

mod checks;
mod commit;
mod outcome;
mod response;

#[cfg(test)]
mod tests;

wire_handler!(
    handle,
    "handle_set_topic_freeze",
    "SetTopicFreeze",
    "info",
    SetTopicFreezeRequest,
    |broker, version, req, ctx| {
        let image = broker.controller.current_image();
        let env = FreezeEnv {
            config: &broker.config,
            image: &image,
            ctx,
        };
        let outcome = match prepare(&env, &req) {
            Err(refusal) => Err(refusal),
            Ok(accepted) => {
                let target = pattern_type_concrete(req.pattern_type)
                    .map_or_else(|| req.scope.clone(), |kind| freeze_target(kind, &req.scope));
                let audit = FreezeAudit {
                    action: if req.frozen {
                        FREEZE_ACTION
                    } else {
                        THAW_ACTION
                    },
                    target,
                    proposal_id: Uuid::from_bytes(req.proposal_id.0),
                    key_id: &req.key_id,
                    signature: &req.signature,
                    signature_verified: accepted.signature_verified,
                    set_at_ms: accepted.record.set_at_ms,
                    reason: req.reason.clone(),
                };
                match require_freeze(
                    broker.audit_log.as_ref(),
                    ctx,
                    &audit,
                    &broker.config.break_glass.approvers,
                )
                .await
                {
                    Ok(()) => submit(broker, accepted).await,
                    Err(error) => Err(Refusal {
                        code: codes::POLICY_VIOLATION,
                        message: format!("privileged action refused: {error}"),
                        signature_verified: accepted.signature_verified,
                    }),
                }
            }
        };
        let response = respond(broker, ctx, &req, &outcome);
        encode_response(&response, version)
    }
);
