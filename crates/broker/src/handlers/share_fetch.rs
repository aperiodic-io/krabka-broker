//! `ShareFetch` (`api_key` 78), from KIP-932.
//!
//! This handler drives the per-`(group, topic, partition)`
//! [`AcquisitionState`] machine that
//! [`crate::share_partition::manager::SharePartitionLeaderManager`] owns. It
//! validates the group id, the member id and the share session. Then, for every
//! requested partition that this broker leads, it applies any piggybacked
//! acknowledgement, expires stale locks, materializes newly produced records
//! up to the high watermark, acquires a batch of `Available` records under a
//! lock, and reads the verbatim bytes of the acquired offset range from the
//! log.
//!
//! On a KFC-1 scheduled topic this path delivers out of offset order, which is
//! the one read path that can. A classic group commits a single position per
//! partition, so a record it steps over is unreachable for it forever, and its
//! fetch stops at the delivery watermark. A share group tracks per-record
//! state, so this handler keeps its window at the high watermark and instead
//! marks the not-yet-due ranges `Deferred`, which acquisition steps over. The
//! group then gets what is due now and picks up the rest on a later pass, in
//! delivery-time order.
//!
//! When it acquired nothing and the client asked to wait, it long-polls on the
//! partitions' append and HW-advance notifies, and runs the acquire pass once
//! more.
//!
//! `network::dispatch` intercepts this request inline, not through the
//! `&Broker`-only handler table, so that the handler receives the
//! per-connection principal and the peer `SocketAddr` for the group `Read` and
//! per-topic `Read` ACL gates.

#[cfg(test)]
use krabka_protocol::owned::share_fetch_request::{AcknowledgementBatch, FetchTopic};
use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, leader_endpoints},
    },
    krabka_protocol::owned::{
        share_fetch_request::{FetchPartition, ShareFetchRequest},
        share_fetch_response::{LeaderIdAndEpoch, ShareFetchResponse},
    },
    krabka_units::convert::ByteSizeExt as _,
    std::collections::{HashMap, HashSet},
};

mod acknowledge;
mod acquire;
mod authorization;
mod leader_hint;
mod long_poll;
mod pending;
mod records;
mod request;
mod resolve;
mod response;
mod tiered;

#[cfg(test)]
mod ack_validation_tests;
#[cfg(test)]
mod byte_limit_tests;
#[cfg(test)]
mod group_authorization_tests;
#[cfg(test)]
mod log_start_lockout_tests;
#[cfg(test)]
mod node_endpoints_tests;
#[cfg(test)]
mod persister_error_tests;
#[cfg(test)]
mod renew_tests;
#[cfg(test)]
mod request_validation_tests;
#[cfg(test)]
mod session_tests;
#[cfg(test)]
mod topic_resolution_tests;

#[cfg(test)]
#[bon::builder]
fn test_fetch_request(
    group_id: String,
    member_id: String,
    topic_id: [u8; 16],
    #[builder(default)] share_session_epoch: i32,
    #[builder(default = 1_048_576)] max_bytes: i32,
    #[builder(default = 10)] max_records: i32,
    #[builder(default = 1_048_576)] partition_max_bytes: i32,
    #[builder(default)] acknowledgement_batches: Vec<AcknowledgementBatch>,
    #[builder(default)] is_renew_ack: bool,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group_id),
        member_id: Some(member_id),
        share_session_epoch,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes,
        max_records,
        batch_size: max_records,
        is_renew_ack,
        topics: vec![FetchTopic {
            topic_id: krabka_protocol::primitives::uuid::Uuid(topic_id),
            partitions: vec![FetchPartition {
                partition_index: 0,
                partition_max_bytes,
                acknowledgement_batches,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) use self::{
    acknowledge::{
        AckApplication, Renewal, acknowledgement_batches_are_valid, apply_acknowledgements,
    },
    leader_hint::{current_leader, names_the_leader},
    request::{ShareRequestIdentity, resolve_share_identity, share_request_identity},
};
use {
    self::{
        acquire::{AcquireContext, acquire_records},
        pending::PendingPartition,
        records::AcquireMode,
        request::has_acknowledgements,
        resolve::{RowContext, resolve_row},
        response::{group_responses, success_response},
    },
    crate::{
        codes,
        handlers::ErrorResponse as _,
        share_partition::session::{FetchPartitions, ResponseRow},
    },
};

/// The longest member id that Kafka accepts: a human-readable UUID.
const MAX_MEMBER_ID_LEN: usize = 36;

/// Kafka's `KafkaApis.isMemberIdValid`: a member id is non-empty and at most
/// 36 characters long. The length is Java's `String.length`, that is UTF-16
/// code units.
pub(crate) fn member_id_is_valid(member_id: &str) -> bool {
    !member_id.is_empty() && member_id.encode_utf16().count() <= MAX_MEMBER_ID_LEN
}

/// Kafka's `PartitionRotateStrategy.rotateRoundRobin`: from session epoch 1
/// on, the partitions rotate left by `epoch % len`, so that a small
/// `MaxRecords` budget reaches every partition in turn.
fn rotate_round_robin<T>(mut partitions: Vec<T>, session_epoch: i32) -> Vec<T> {
    if session_epoch >= 1 && partitions.len() > 1 {
        let by = usize::try_from(session_epoch).unwrap_or(0) % partitions.len();
        partitions.rotate_left(by);
    }
    partitions
}

/// Whether a renew-ack `ShareFetch` asks for no records and no wait:
/// `MaxBytes`, `MinBytes`, `MaxRecords` and `MaxWaitMs` are all 0.
fn renew_fetch_fields_are_zero(req: &ShareFetchRequest) -> bool {
    req.max_bytes == 0 && req.min_bytes == 0 && req.max_records == 0 && req.max_wait_ms == 0
}

pub(crate) async fn handle(
    broker: &Broker,
    req: ShareFetchRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<ShareFetchResponse, BrokerError> {
    share_request_identity!(
        (cfg, image, group, settings, lock_timeout_ms, member),
        ShareFetchResponse,
        broker,
        req,
        ctx
    );

    // KIP-1222: a renew-ack fetch renews locks and fetches no records, so
    // Kafka's `KafkaApis.handleShareFetchRequest` refuses one that asks for
    // records or a wait. The error response carries no message.
    let renew_only = version >= 2 && req.is_renew_ack;
    if renew_only && !renew_fetch_fields_are_zero(&req) {
        return Ok(ShareFetchResponse::error(codes::INVALID_REQUEST, None));
    }

    let mgr = broker.share_partition_leaders.clone();

    let mut requested = HashSet::new();
    let mut requested_order = Vec::new();
    let mut request_rows: HashMap<(uuid::Uuid, i32), FetchPartition> = HashMap::new();
    let has_acknowledgements = has_acknowledgements(&req);
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        for partition in &topic.partitions {
            let key = (topic_id, partition.partition_index);
            if requested.insert(key) {
                requested_order.push(key);
            }
            request_rows.insert(key, partition.clone());
        }
    }
    let forgotten: HashSet<(uuid::Uuid, i32)> = req
        .forgotten_topics_data
        .iter()
        .flat_map(|topic| {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            topic
                .partitions
                .iter()
                .copied()
                .map(move |partition| (topic_id, partition))
        })
        .collect();
    let session = match mgr.update_fetch_session(
        (&group, &member),
        ctx.connection_id,
        req.share_session_epoch,
        FetchPartitions {
            requested: &requested_order,
            forgotten: &forgotten,
        },
        has_acknowledgements,
    ) {
        Ok(session) => session,
        Err(codes::SHARE_SESSION_LIMIT_REACHED) => {
            // Kafka's `createIdleShareFetchTimerTask`: the answer waits out
            // `MaxWaitMs`, so a client that cannot get a session does not
            // spin on the broker.
            let wait = u64::try_from(req.max_wait_ms).unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
            return Ok(ShareFetchResponse::error(
                codes::SHARE_SESSION_LIMIT_REACHED,
                None,
            ));
        }
        Err(code) => return Ok(ShareFetchResponse::error(code, None)),
    };

    // The session's partitions in the session's order, then the request
    // partitions that only acknowledge: those that a final request names and
    // those that a request forgets.
    let fetched: HashSet<(uuid::Uuid, i32)> = session.partitions.iter().copied().collect();
    let mut effective_order =
        rotate_round_robin(session.partitions.clone(), req.share_session_epoch);
    effective_order.extend(
        requested_order
            .iter()
            .copied()
            .filter(|partition| !fetched.contains(partition)),
    );

    // Resolve the complete effective session subscription plus request-only
    // acknowledgement rows into pending partitions.
    let row_context = RowContext {
        broker,
        manager: &mgr,
        image: &image,
        ctx,
        has_acknowledgements,
        supports_renew: version >= 2,
        is_renew_ack: renew_only,
    };
    let mut pending: Vec<PendingPartition> = effective_order
        .into_iter()
        .map(|key| {
            // A renew-ack fetch acquires nothing: a fetch that took longer
            // than the renewed lock would let the lock run out before the
            // response arrives.
            let fetchable = !renew_only && fetched.contains(&key);
            resolve_row(&row_context, key, fetchable, request_rows.get(&key))
        })
        .collect();

    let acquire = AcquireContext {
        broker,
        manager: &mgr,
        group: &group,
        member: &member,
        max_records: req.max_records,
        // Kafka's broker-wide `fetch.max.bytes` and request limit both
        // bound the acquire passes. `acquire_pass` retains the first
        // batch progress exception if that batch itself exceeds this cap.
        max_bytes: req.max_bytes.min(broker.config.fetch_max_bytes.bytes_i32()),
        min_bytes: req.min_bytes,
        mode: AcquireMode::of(
            if version >= 2 {
                req.share_acquire_mode
            } else {
                0
            },
            req.batch_size,
        ),
        renewal: Renewal {
            requested: req.is_renew_ack,
            enabled: settings.renew_acknowledge_enabled,
            lock_duration: settings.record_lock_duration,
        },
        settings,
    };

    let max_wait_ms = if session.final_request || renew_only {
        0
    } else {
        req.max_wait_ms
    };
    acquire_records(&acquire, &mut pending, max_wait_ms).await;
    // Kafka's `releaseSession` runs after the final request's response is
    // built: the member gives its records back.
    if session.final_request {
        mgr.release_session_partitions(&group, &member, &session.released)
            .await;
    }

    // Kafka answers a fetch row for each partition of the share session that
    // it fetched, and an acknowledge row for each request partition when the
    // request carries acknowledgements. A renew-ack fetch runs no fetch, so it
    // answers only the acknowledge rows.
    pending.retain(|p| p.fetchable || (p.in_request && has_acknowledgements));
    // An incremental response carries only the session partitions that have
    // something to say, and every acknowledge row.
    if session.incremental {
        let rows: Vec<ResponseRow> = pending
            .iter()
            .filter(|p| p.fetchable)
            .map(|p| ResponseRow {
                key: (p.topic_id, p.partition_index),
                has_records: !p.out.acquired_records.is_empty(),
                has_error: p.out.error_code != codes::NONE,
            })
            .collect();
        let mut carried = mgr.prune_fetch_response(&group, &member, &rows).into_iter();
        pending.retain(|p| {
            let carried = !p.fetchable || carried.next().unwrap_or(true);
            carried || (p.in_request && has_acknowledgements)
        });
    }

    // Kafka's `processShareFetchResponse`: every row that names another
    // leader carries the current leader and its endpoint.
    for p in &mut pending {
        if names_the_leader(p.out.error_code) {
            let (leader_id, leader_epoch) = current_leader(&mgr, p.topic_id, p.partition_index);
            p.out.current_leader = LeaderIdAndEpoch {
                leader_id,
                leader_epoch,
                ..Default::default()
            };
        }
    }
    let node_endpoints = leader_endpoints(
        &image,
        ctx.connection_listener_name,
        &broker.config.inter_broker_listener_name,
        pending
            .iter()
            .filter(|p| names_the_leader(p.out.error_code))
            .map(|p| p.out.current_leader.leader_id),
    );

    // Group pending rows back into per-topic responses, preserving first-seen
    // topic order.
    let responses = group_responses(pending);

    Ok(success_response(lock_timeout_ms, responses, node_endpoints))
}

#[cfg(test)]
mod rotation_tests {
    use {super::rotate_round_robin, assert2::assert};

    #[test]
    fn partitions_rotate_left_by_the_session_epoch() {
        let rotated: Vec<Vec<i32>> = [-1, 0, 1, 2, 3, 4]
            .into_iter()
            .map(|epoch| rotate_round_robin(vec![1, 2, 3], epoch))
            .collect();
        assert!(
            rotated
                == vec![
                    vec![1, 2, 3],
                    vec![1, 2, 3],
                    vec![2, 3, 1],
                    vec![3, 1, 2],
                    vec![1, 2, 3],
                    vec![2, 3, 1],
                ]
        );
    }
}
