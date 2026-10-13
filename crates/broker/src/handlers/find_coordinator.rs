//! `FindCoordinator` (`api_key=10`). Supports:
//!   - `key_type=0` (GROUP): hashes the group id to its
//!     `__consumer_offsets` partition and returns that partition's leader.
//!   - `key_type=1` (TRANSACTION): hashes the transaction-id to its
//!     `__transaction_state` partition and returns that partition's leader.
//!   - `key_type=2` (SHARE, v6+): checks `ClusterAction` once for the whole
//!     request, validates each `group:topicId:partition` key, and returns the
//!     leader of its `__share_group_state` partition.
//!
//! A state topic that does not exist yet is created on first use. The lookup
//! asks [`crate::auto_topic_creation::AutoTopicCreation`] for it, which sends
//! a `CreateTopics` request to the active controller with its configured
//! partition count and replication factor, and answers
//! `COORDINATOR_NOT_AVAILABLE` for every admitted key.
//! A partition with no leader, or with a leader that is not alive, is
//! `COORDINATOR_NOT_AVAILABLE` too.
//!
//! The handler follows `KafkaApis.getCoordinator`. A refused `ClusterAction`
//! and an unknown key type fail the whole request, as the exception Kafka
//! throws out of its per-key loop does. An ACL denial of a group or
//! transactional id, a malformed share key and an unresolvable coordinator
//! stay on their own key. The handler fills both the legacy single-coordinator
//! form, v0-v3, and the per-key `coordinators` array, v4+.

use {
    crate::{
        broker::Broker,
        coordinator::{bootstrap::OFFSETS_TOPIC, partitioner::partition_for_group},
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, offline_replicas::unavailable_brokers},
        share_coordinator::{
            bootstrap::TOPIC as SHARE_GROUP_STATE_TOPIC, partitioner::partition_for_share_key,
        },
        txn::bootstrap::TOPIC as TRANSACTION_STATE_TOPIC,
    },
    krabka_protocol::owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    },
    std::sync::Arc,
};

mod authz;
mod listener;
mod resolve;
mod response;

#[cfg(test)]
mod tests;

use {
    self::{
        authz::{KeySlot, authorize_keys, cluster_action_allowed},
        listener::local_advertised_for_listener,
        resolve::{
            ResolveTarget, parse_share_key, resolve_partition_coordinator,
            resolve_transaction_keys, unavailable_coordinator,
        },
        response::{coordinators_response, request_error_response},
    },
    crate::codes,
};

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

fn unavailable_for_keys(keys: Vec<String>) -> Vec<Coordinator> {
    keys.into_iter().map(unavailable_coordinator).collect()
}

fn merge_key_slots(key_slots: Vec<KeySlot>, coordinators: Vec<Coordinator>) -> Vec<Coordinator> {
    let mut resolved = coordinators.into_iter();
    key_slots
        .into_iter()
        .map(|slot| match slot {
            KeySlot::Rejected(coordinator) => coordinator,
            KeySlot::Resolve(_) => resolved
                .next()
                .expect("one coordinator result per admitted key"),
        })
        .collect()
}

pub(crate) async fn handle(
    broker: &Broker,
    req: FindCoordinatorRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<FindCoordinatorResponse, BrokerError> {
    // The local broker's advertised `host:port` for the listener this request
    // arrived on (Kafka returns the connection listener's address). Falls back
    // to the legacy top-level `advertised_listener` when the connection
    // listener isn't among this broker's configured listeners.
    let advertised = local_advertised_for_listener(&broker.config, ctx.connection_listener_name);
    let controller = Arc::clone(&broker.controller);

    // For v4+, requests carry `coordinator_keys`. For v0-v3 the single `key`
    // field is what the client cares about; it becomes a one-row list so the
    // resolve path is uniform.
    let keys: Vec<String> = if version < 4 {
        vec![req.key.clone()]
    } else {
        req.coordinator_keys.clone()
    };

    // ── Request-level failures ──────────────────────────────────────
    // Kafka's per-key loop throws out of the handler for these two, and
    // `FindCoordinatorRequest.getErrorResponse` stamps the one error on every
    // key. With no keys the loop never runs, so an empty v4+ batch stays empty.
    if !keys.is_empty() {
        // `authorizeClusterOperation(request, CLUSTER_ACTION)` runs before
        // `SharePartitionKey.validate`, so a malformed key of a refused
        // principal is answered 31 like the others.
        if req.key_type == KEY_TYPE_SHARE
            && version >= 6
            && !cluster_action_allowed(broker, &controller.current_image(), ctx)
        {
            return Ok(request_error_response(
                version,
                codes::CLUSTER_AUTHORIZATION_FAILED,
                keys,
            ));
        }
        // `CoordinatorType.forId` throws `InvalidRequestException`.
        if !matches!(
            req.key_type,
            KEY_TYPE_GROUP | KEY_TYPE_TRANSACTION | KEY_TYPE_SHARE
        ) {
            return Ok(request_error_response(
                version,
                codes::INVALID_REQUEST,
                keys,
            ));
        }
    }

    // ── Per-key admission ───────────────────────────────────────────
    // GROUP and TRANSACTION require `Describe` on their keyed resources; a
    // SHARE key must be well formed and the request v6+. Refused keys retain
    // their original response slots while admitted keys resolve normally.
    let key_slots = authorize_keys(
        broker,
        &controller.current_image(),
        ctx,
        version,
        req.key_type,
        keys,
    );
    let keys: Vec<String> = key_slots
        .iter()
        .filter_map(|slot| match slot {
            KeySlot::Resolve(key) => Some(key.clone()),
            KeySlot::Rejected(_) => None,
        })
        .collect();

    let coordinators: Vec<Coordinator> = if keys.is_empty() {
        Vec::new()
    } else {
        let state_topic = match req.key_type {
            KEY_TYPE_GROUP => OFFSETS_TOPIC,
            KEY_TYPE_TRANSACTION => TRANSACTION_STATE_TOPIC,
            _ => SHARE_GROUP_STATE_TOPIC,
        };
        let image = controller.current_image();
        if image.topic(state_topic).is_none() {
            // Kafka asks `AutoTopicCreationManager.createTopics` for the
            // state topic without a request context, and answers
            // COORDINATOR_NOT_AVAILABLE. The client retries, and a later
            // lookup finds the topic.
            broker.auto_topic_creation.request(state_topic);
            unavailable_for_keys(keys)
        } else {
            let unavailable = unavailable_brokers(broker, &image).await;
            let target = ResolveTarget {
                image: &image,
                unavailable: &unavailable,
                local_node: broker.config.node_id,
                advertised: &advertised,
                listener: ctx.connection_listener_name,
            };
            match req.key_type {
                KEY_TYPE_GROUP => keys
                    .into_iter()
                    .map(|key| {
                        let partition = partition_for_group(&image, &key);
                        resolve_partition_coordinator(&target, state_topic, partition, key)
                    })
                    .collect(),
                KEY_TYPE_TRANSACTION => resolve_transaction_keys(broker, &target, keys),
                _ => keys
                    .into_iter()
                    .map(|key| {
                        // Admission validated the key already, so the parse
                        // cannot fail here.
                        let Some((group, topic_uuid, partition)) = parse_share_key(&key) else {
                            return unavailable_coordinator(key);
                        };
                        let p = partition_for_share_key(
                            group,
                            &topic_uuid,
                            partition,
                            broker.config.share_coordinator.state_topic_num_partitions,
                        );
                        resolve_partition_coordinator(&target, state_topic, p, key)
                    })
                    .collect(),
            }
        }
    };

    // Re-attach rejected entries in their original request slots. Kafka's
    // batched response preserves input order even when authorization and
    // resolution produce different errors for adjacent keys.
    Ok(coordinators_response(
        version,
        merge_key_slots(key_slots, coordinators),
    ))
}
