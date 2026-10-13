//! `UnregisterBroker` (`api_key=64`).
//!
//! This is the admin RPC an operator uses to drop a permanently dead broker
//! from the cluster's metadata image. Once the change lands through Raft,
//! `Metadata` responses no longer advertise the broker's endpoints, and
//! clients stop routing to it.
//!
//! ## ACL
//!
//! The handler needs `Alter` on `Cluster("kafka-cluster")`. On Deny, the whole
//! response carries `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Idempotency
//!
//! A `broker_id` with no registration, a negative one included, returns
//! `BROKER_ID_NOT_REGISTERED (102)` with the message "Broker ID {id} is not
//! currently registered", as Kafka's
//! `ReplicationControlManager.unregisterBroker` does.
//!
//! ## KFC-9: dropping a broker needs two people
//!
//! Unregistering a broker is one of the transitions the break-glass two-person
//! rule gates. KIP-631 defines the request and it gains no field for this: an
//! operator gets an approval out of band through `krabka-guard`, targeted at
//! the broker id, and then runs the ordinary tool. A request that no approved
//! proposal covers answers `POLICY_VIOLATION (44)` at the top level, which is
//! where this response carries every other whole-request refusal.
//!
//! The consumed proposal rides the same `submit_change` call as the unregister
//! record, so the approval and the transition it authorized commit together.
//! The gate is active only when `[break_glass]` names an approver set.
//!
//! ## Leaving the ISRs first
//!
//! Kafka's `ReplicationControlManager.unregisterBroker` does not only drop the
//! registration. `handleBrokerUnregistered` first removes the broker from every
//! ISR it is in and elects new leaders for the partitions it leads, and writes
//! the `UnregisterBrokerRecord` after them, all in one record list. The same
//! `submit_change` here carries those partition changes ahead of the unregister
//! record, so no partition names an unregistered broker as its leader while the
//! liveness ticker waits for a heartbeat timeout.
//!
//! ## Only the active controller runs it
//!
//! Kafka forwards `UnregisterBroker` from a broker listener to the active
//! controller (`KafkaApis.forwardToController`), and
//! `ReplicationControlManager.unregisterBroker` runs there as one atomic record
//! list. The partition records above are built from the image of the node that
//! runs the handler, and a follower's or an observer's image can trail the
//! leader's. The leader applies a partition record as written, so a record built
//! from a trailing image would roll back a leader, epoch and ISR that the
//! leader committed since. A broker listener therefore forwards the request in
//! an `Envelope` to the active controller, and a request that reaches any other
//! node on the controller listener answers `NOT_CONTROLLER (41)`. The two-person
//! gate and the audit trail then run on the controller as well.
//!
//! This file holds the request flow. The two-person gate and the records it
//! builds live in `gate`, the ISR departures in `leave`, and the
//! wrong-controller refusal in `wire`.

use {
    self::{
        gate::{broker_target, consumed_proposal_id, unregister_records, with_leaves},
        wire::not_controller_refusal,
    },
    crate::{
        break_glass::handlers::audit::{GatedTransition, audit_transition, require_transition},
        codes,
        controller_admin::CONTROLLER_ADMIN_CONNECTION_ID,
        handlers::{
            ErrorResponse as _, cluster_alter_denied, forward_to_controller::to_active_controller,
        },
        time_util::now_ms,
    },
    krabka_audit::PrivilegedPhase,
    krabka_metadata::{BreakGlassAction, NodeId},
    krabka_protocol::owned::{
        unregister_broker_request::{self, UnregisterBrokerRequest},
        unregister_broker_response::UnregisterBrokerResponse,
    },
};

mod gate;
mod leave;
mod wire;

#[cfg(test)]
mod tests;

wire_handler! {
    async (broker, version, req_bytes, ctx), {
        let req = crate::handlers::decode_request::<UnregisterBrokerRequest>(req_bytes, version)?;

        // A broker listener forwards the request untouched, as `KafkaApis` does
        // with `forwardToController`, and the controller authorizes the principal
        // the `Envelope` names. A node that is itself the active controller has
        // nowhere to forward to and answers in place.
        if ctx.connection_id != CONTROLLER_ADMIN_CONNECTION_ID
            && let Some(answer) = to_active_controller(
                broker,
                unregister_broker_request::API_KEY,
                req_bytes,
                version,
                ctx,
                |error_code, message| {
                    crate::handlers::encode_response(
                        &UnregisterBrokerResponse::error(error_code, message.map(str::to_owned)),
                        version,
                    )
                },
            )
            .await
        {
            return answer;
        }

        let image = broker.controller.current_image();

        // Cluster:Alter gate.
        if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            let resp = UnregisterBrokerResponse::error(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("unregister-broker denied".into()),
            );
            return crate::handlers::encode_response(&resp, version);
        }

        // Only the active controller unregisters a broker, as
        // `ControllerWriteEvent.run` says. The records below are built from the
        // image this node holds, and that image is only current on the leader: a
        // follower's or an observer's can trail it, and a partition record built
        // from a trailing image would roll back the leader, epoch and ISR that the
        // leader has committed since.
        let leader = *broker.controller.watch_leader().borrow();
        if let Some(refusal) = not_controller_refusal(leader, broker.config.node_id) {
            return crate::handlers::encode_response(&refusal, version);
        }

        // Existence check, as `ReplicationControlManager.unregisterBroker`: an id
        // with no registration, a negative one included, answers
        // `BROKER_ID_NOT_REGISTERED` with Kafka's message. It runs before the
        // break-glass gate so that a typo in the id does not spend an approval
        // that a real unregistration still needs.
        let Some((node_id, broker_epoch)) = u64::try_from(req.broker_id)
            .ok()
            .map(NodeId)
            .and_then(|id| Some((id, image.broker_epoch(id)?)))
        else {
            let resp = UnregisterBrokerResponse::error(
                codes::BROKER_ID_NOT_REGISTERED,
                Some(format!(
                    "Broker ID {} is not currently registered",
                    req.broker_id
                )),
            );
            return crate::handlers::encode_response(&resp, version);
        };

        // KFC-9: the two-person rule, and the records it makes this append carry.
        let target = broker_target(node_id);
        let records = match unregister_records(
            &image,
            &broker.config.break_glass,
            node_id,
            broker_epoch,
            now_ms(),
        ) {
            Ok(records) => records,
            Err(denial) => {
                let message = denial.to_string();
                crate::handlers::partition_transition::audit_refusal(
                    broker,
                    ctx,
                    BreakGlassAction::UnregisterBroker,
                    || &target,
                    &denial,
                    &message,
                );
                let resp = UnregisterBrokerResponse::error(codes::POLICY_VIOLATION, Some(message));
                return crate::handlers::encode_response(&resp, version);
            }
        };
        let consumed_id = records.first().and_then(consumed_proposal_id);
        let transition = GatedTransition {
            action: BreakGlassAction::UnregisterBroker,
            target: &target,
            phase: PrivilegedPhase::Applied,
            proposal_id: consumed_id,
            reason: "broker unregistration admitted",
        };
        if let Err(error) = require_transition(
            &broker.audit_log,
            &broker.config.break_glass,
            ctx,
            &transition,
        )
        .await
        {
            let resp = UnregisterBrokerResponse::error(
                codes::POLICY_VIOLATION,
                Some(format!("privileged action refused: {error}")),
            );
            return crate::handlers::encode_response(&resp, version);
        }

        // The broker leaves every ISR and every leadership in the same append that
        // drops its registration, as `ReplicationControlManager.unregisterBroker`
        // writes them: the partitions never name a broker that is no longer
        // registered.
        let leaves = leave::leave_isrs(broker, &broker.controller.current_image(), node_id).await;
        let records = with_leaves(records, leaves);

        // Submit the change through Raft. The image apply of the unregister record
        // is idempotent (the `apply` arm calls `brokers.remove`).
        if let Err(e) = broker.controller.submit_change(records).await {
            let resp = UnregisterBrokerResponse::error(
                crate::handlers::submit_failure_code(&e, codes::UNKNOWN_SERVER_ERROR),
                Some(format!("controller submit failed: {e}")),
            );
            return crate::handlers::encode_response(&resp, version);
        }
        audit_transition(
            &broker.audit_log,
            &broker.config.break_glass,
            ctx,
            &GatedTransition {
                action: BreakGlassAction::UnregisterBroker,
                target: &target,
                phase: PrivilegedPhase::Applied,
                proposal_id: consumed_id,
                reason: "broker registration removed",
            },
        );

        crate::handlers::audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "UnregisterBroker",
            vec![crate::handlers::audit_resource(
                "Broker",
                node_id.to_string(),
            )],
        );

        let resp = UnregisterBrokerResponse::error(codes::NONE, None);
        crate::handlers::encode_response(&resp, version)
    }
}
