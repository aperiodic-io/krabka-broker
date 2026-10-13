//! `BrokerHeartbeat` (`api_key=63`). KIP-500 controller-side heartbeat handler.
//!
//! Only the openraft leader handles heartbeats. Non-leaders return
//! `NOT_CONTROLLER` so the broker client can redirect.
//!
//! This file holds the wire handler and the order its stages run in. The
//! registration gate lives in `validation`, the response bodies in `response`,
//! the records that take a broker out of the ISRs in `shutdown`, and the
//! KIP-112 offline-dir
//! failover in `failover`.

use {
    krabka_protocol::owned::{
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse,
    },
    krabka_raft::NodeId,
};

mod failover;
mod response;
mod shutdown;
mod validation;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::failover::failover_offline_dirs;
use {
    self::{
        response::success_response,
        shutdown::{LeaveIsrs, leave_isrs},
        validation::validate_registration,
    },
    crate::{
        codes,
        cordoned_log_dirs::registration_change,
        handlers::{
            ErrorCodeResponse as _, cluster_action_denied,
            forward_to_controller::is_active_controller,
        },
        heartbeat::{
            controller_state::{
                BrokerControlState, ControllerLivenessState, HeartbeatFacts, HeartbeatWants,
                next_broker_state,
            },
            fencing::is_fenced,
        },
        leader_election::compute_unfence_changes,
        metadata_source::MetadataSource,
        metrics::BrokerMetrics,
    },
};

context_handler! {
    BrokerHeartbeatRequest => BrokerHeartbeatResponse,
    (broker, req, _version, ctx),
    {
        let liveness = broker.liveness.clone();
        let controller = broker.controller.clone();
        let metrics = broker.metrics.clone();
        let recovery = broker.unclean_recovery.clone();

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        //
        // Every listener runs it, the controller listener included, as
        // Kafka's `ControllerApis.handleBrokerHeartBeatRequest` does.
        let image = controller.current_image();
        if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return Ok(BrokerHeartbeatResponse::error(
                codes::CLUSTER_AUTHORIZATION_FAILED,
            ));
        }

        // Only the openraft leader handles heartbeats. NOT_CONTROLLER
        // tells the broker client to redirect.
        if !is_active_controller(broker) {
            return Ok(BrokerHeartbeatResponse::error(codes::NOT_CONTROLLER));
        }

        let image = controller.current_image();
        let (broker_id_u64, decision) = match validate_registration(&image, &req) {
            Ok(validated) => validated,
            Err(error_code) => {
                return Ok(BrokerHeartbeatResponse::error(error_code));
            }
        };

        // Record the contact first. If it is a revival, the liveness ticker
        // picks up the transition on its next cycle.
        let _transition = liveness.record_fenced_heartbeat(broker_id_u64).await;
        let next = advance_broker_state(
            &controller,
            &liveness,
            &metrics,
            NodeId(broker_id_u64),
            &req,
            decision.caught_up,
        )
        .await;

        // KIP-112: a broker that reports offline log dirs is still alive, so
        // the liveness `alive→dead` failover never fires. Map the reported
        // offline dir UUIDs to the reporting broker's affected partitions and
        // fail them over (elect from surviving alive ISR, drop the offline
        // replica). Only the controller leader reaches here (NOT_CONTROLLER
        // early-return above), and it's idempotent across repeated heartbeats.
        //
        // Validate the reporting broker id independently of `broker_id_u64`
        // (which falls back to 0 for the liveness path): failing over the
        // wrong broker on a malformed negative id would be harmful.
        if !req.offline_log_dirs.is_empty()
            && let Ok(reporting_broker) = u64::try_from(req.broker_id)
        {
            let offline: std::collections::HashSet<uuid::Uuid> = req
                .offline_log_dirs
                .iter()
                .map(|u| uuid::Uuid::from_bytes(u.0))
                .collect();
            let recoveries = failover_offline_dirs(
                &controller,
                NodeId(reporting_broker),
                &offline,
                &liveness,
                &metrics,
            )
            .await;
            // Fire-and-forget: enqueue logs internally if the recovery manager is gone.
            for (topic, partition, strategy) in recoveries {
                recovery
                    .enqueue(crate::unclean_recovery::RecoveryJob {
                        topic,
                        partition,
                        strategy,
                        reply: None,
                        // KFC-9: a heartbeat that reports an offline log dir
                        // is not a request for an unclean recovery, and the
                        // broker that sent it is not a person who can be asked
                        // for a second signature. The recovery carries no
                        // proposal, and
                        // `break_glass.background_unclean_recovery` decides
                        // what the URM does with it.
                        proposal: None,
                    })
                    .await;
            }
        }

        // KIP-1066: store the directories the broker reports cordoned on its
        // registration, as `ReplicationControlManager.processBrokerHeartbeat`
        // calls `handleDirectoriesCordoned` from `metadata.version` `4.3-IV0`.
        record_cordoned_dirs(&controller, NodeId(broker_id_u64), &req).await;

        Ok(success_response(
            decision.caught_up,
            next.fenced(),
            next.should_shut_down(),
        ))
    }
}

/// Write the cordoned directories a heartbeat reports onto the broker's
/// registration, when the metadata version carries them and the set changed.
///
/// A failed submit is logged and dropped: the broker reports the same set on
/// its next heartbeat, and the next one retries.
async fn record_cordoned_dirs(
    controller: &std::sync::Arc<dyn MetadataSource>,
    broker: NodeId,
    req: &BrokerHeartbeatRequest,
) {
    let image = controller.current_image();
    let Some(record) = cordoned_dirs_change(&image, broker, req) else {
        return;
    };
    if let Err(error) = controller.submit_change(vec![record]).await {
        tracing::warn!(broker = broker.0, %error, "broker heartbeat: cordoned-dirs submit failed");
    }
}

/// The registration change a heartbeat's `cordoned_log_dirs` asks for, or
/// `None` below `metadata.version` `4.3-IV0` or when nothing changed.
fn cordoned_dirs_change(
    image: &krabka_metadata::MetadataImage,
    broker: NodeId,
    req: &BrokerHeartbeatRequest,
) -> Option<krabka_metadata::MetadataRecord> {
    let supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL
    });
    if !supported {
        return None;
    }
    let reported: Option<Vec<uuid::Uuid>> = req.cordoned_log_dirs.as_ref().map(|dirs| {
        dirs.iter()
            .map(|dir| uuid::Uuid::from_bytes(dir.0))
            .collect()
    });
    registration_change(image, broker, reported.as_deref())
}

/// Move `broker` through Kafka's heartbeat state machine
/// (`ReplicationControlManager.processBrokerHeartbeat`), write the records the
/// transition needs, and return the state the broker is left in.
///
/// Fencing a broker, letting it shut down, and moving it into controlled
/// shutdown all take it out of every ISR and every leadership it can hand
/// over. A broker in controlled shutdown may stop once it leads nothing and
/// every active broker reports a metadata offset at or past the end of those
/// records, so no peer still acts on metadata from before the handover.
///
/// Each transition also changes the broker's registration, as Kafka's
/// `handleBrokerFenced`, `handleBrokerUnfenced` and
/// `handleBrokerInControlledShutdown` write a `BrokerRegistrationChangeRecord`:
/// fenced for `Fenced` and `ShutdownNow`, unfenced for `Unfenced`, and in
/// controlled shutdown for `ControlledShutdown`. The registration change of a
/// fence follows the partition changes, and that of a controlled shutdown
/// precedes them, in Kafka's order.
///
/// Kafka writes the drain once. A broker in controlled shutdown is not active
/// here either, so nothing elects it again, but a leadership that came back to
/// it before it entered is written away again on the next heartbeat.
async fn advance_broker_state(
    controller: &std::sync::Arc<dyn MetadataSource>,
    liveness: &std::sync::Arc<ControllerLivenessState>,
    metrics: &BrokerMetrics,
    broker: NodeId,
    req: &BrokerHeartbeatRequest,
    caught_up: bool,
) -> BrokerControlState {
    let image = controller.current_image();
    let current = current_broker_state(liveness.control_state(broker.0).await, &image, broker);
    let asks_for_change = req.want_fence || req.want_shut_down;
    let left = if asks_for_change || current == BrokerControlState::ControlledShutdown {
        leave_isrs(&image, broker, liveness, metrics).await
    } else {
        LeaveIsrs::default()
    };
    let next = next_broker_state(current, HeartbeatFacts {
        wants: HeartbeatWants {
            fence: req.want_fence,
            shut_down: req.want_shut_down,
        },
        caught_up,
        has_leaderships: left.has_leaderships,
        controlled_shutdown_offset: liveness.controlled_shutdown_offset(broker.0).await,
        lowest_active_offset: liveness.lowest_active_offset().await,
    });
    let leaves = match next {
        BrokerControlState::Fenced | BrokerControlState::ShutdownNow => current != next,
        BrokerControlState::ControlledShutdown => current != next || left.has_leaderships,
        BrokerControlState::Unfenced => false,
    };
    // `handleBrokerUnfenced` elects again for every partition with no leader,
    // with the unfencing broker as an acceptable leader.
    let unfences = current != next && next == BrokerControlState::Unfenced;
    let partition_changes = if leaves {
        left.changes
    } else if unfences {
        compute_unfence_changes(&image, broker, liveness, metrics).await
    } else {
        Vec::new()
    };
    let records = transition_records(&image, broker, current, next, partition_changes);
    let wrote = !records.is_empty();
    if wrote && let Err(error) = controller.submit_change(records).await {
        tracing::warn!(broker = broker.0, %error, ?next, "broker heartbeat: submit_change failed");
        // Nothing moved. Stay where the broker was, and let the next
        // heartbeat try again.
        liveness
            .touch(broker.0, current, req.current_metadata_offset)
            .await;
        return current;
    }
    liveness
        .touch(broker.0, next, req.current_metadata_offset)
        .await;
    if next == BrokerControlState::ControlledShutdown
        && (wrote || current != BrokerControlState::ControlledShutdown)
    {
        // The submit returns once the records are committed and applied, so
        // the applied offset is at or past their end.
        liveness
            .enter_controlled_shutdown(broker.0, controller.current_metadata_offset())
            .await;
    }
    next
}

/// The state a heartbeat starts from: `Fenced` whenever the registration is
/// fenced, and the registry's state otherwise.
///
/// Kafka replays every registration and every fence into
/// `BrokerHeartbeatManager.register`, whose `touch` with `fenced = true` also
/// takes the broker out of controlled shutdown, so a fenced registration
/// always starts its next heartbeat from `FENCED`. krabka's registry does not
/// replay the log: a broker's self-registration does not pass through
/// `BrokerRegistration` and its `replace_incarnation`, so the registry can
/// still hold the unfenced session, or the controlled shutdown, of the
/// registration a restart replaced. The registration's fence decides.
fn current_broker_state(
    registry: BrokerControlState,
    image: &krabka_metadata::MetadataImage,
    broker: NodeId,
) -> BrokerControlState {
    if is_fenced(image, broker) {
        BrokerControlState::Fenced
    } else {
        registry
    }
}

/// The records one heartbeat transition writes, in Kafka's order: the
/// partition changes the transition brings -- the ones that take the broker
/// out of its ISRs, or, when it unfences, the elections it makes possible --
/// and the registration change of the transition, if the registration does
/// not already carry it.
fn transition_records(
    image: &krabka_metadata::MetadataImage,
    broker: NodeId,
    current: BrokerControlState,
    next: BrokerControlState,
    mut partition_changes: Vec<krabka_metadata::MetadataRecord>,
) -> Vec<krabka_metadata::MetadataRecord> {
    use crate::heartbeat::fencing::{RegistrationChange, registration_change};
    if current == next {
        return partition_changes;
    }
    let change = match next {
        BrokerControlState::Fenced | BrokerControlState::ShutdownNow => RegistrationChange::FENCE,
        BrokerControlState::Unfenced => RegistrationChange::UNFENCE,
        BrokerControlState::ControlledShutdown => RegistrationChange::CONTROLLED_SHUTDOWN,
    };
    let registration = registration_change(image, broker, change);
    // A fence follows the partition changes; controlled shutdown and unfence
    // precede them, as `handleBrokerInControlledShutdown` and
    // `handleBrokerUnfenced` write them.
    if matches!(
        next,
        BrokerControlState::ControlledShutdown | BrokerControlState::Unfenced
    ) {
        registration.into_iter().chain(partition_changes).collect()
    } else {
        partition_changes.extend(registration);
        partition_changes
    }
}
