//! `AllocateProducerIds` (`api_key=67`). Reserves one durable, cluster-wide
//! producer-ID block for a registered broker.

use krabka_metadata::NodeId;
use krabka_protocol::owned::{
    allocate_producer_ids_request::AllocateProducerIdsRequest,
    allocate_producer_ids_response::AllocateProducerIdsResponse,
};

use crate::{
    broker::Broker,
    codes,
    producer_id_manager::{ProducerIdAllocationError, allocate_block},
};

context_handler! {
    /// Checks `ClusterAction` on the cluster, then allocates a block.
    ///
    /// Kafka's `ControllerApis.handleAllocateProducerIdsRequest` calls
    /// `authorizeClusterOperation(request, CLUSTER_ACTION)` before the controller
    /// runs. A denial becomes `AllocateProducerIdsRequest.getErrorResponse`:
    /// `CLUSTER_AUTHORIZATION_FAILED` and the default block fields, and no block
    /// is allocated. A request that arrives in an `Envelope` runs this check
    /// against the principal that the envelope names.
    AllocateProducerIdsRequest => AllocateProducerIdsResponse,
    (broker, request, _version, ctx),
    {
        if crate::handlers::cluster_action_denied(
            broker.config.authorizer.as_ref(),
            &broker.controller.current_image(),
            ctx,
        ) {
            return Ok(AllocateProducerIdsResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            });
        }
        Ok(serve(broker, &request).await)
    }
}

async fn serve(
    broker: &Broker,
    request: &AllocateProducerIdsRequest,
) -> AllocateProducerIdsResponse {
    let controller = broker.controller.clone();
    let result = match u64::try_from(request.broker_id) {
        Ok(broker_id) => allocate_block(&controller, NodeId(broker_id), request.broker_epoch).await,
        Err(_) => Err(ProducerIdAllocationError::BrokerNotRegistered(NodeId(
            u64::MAX,
        ))),
    };

    match result {
        Ok(block) => AllocateProducerIdsResponse {
            error_code: codes::NONE,
            producer_id_start: block.first,
            producer_id_len: block.len,
            ..Default::default()
        },
        Err(error) => {
            // Kafka's `ProducerIdControlManager.generateNextProducerId` opens
            // with `ClusterControlManager.checkBrokerEpoch`, which throws
            // `StaleBrokerEpochException` both for a broker with no
            // registration and for a registration at another epoch.
            let error_code = match error {
                ProducerIdAllocationError::BrokerNotRegistered(_)
                | ProducerIdAllocationError::StaleBrokerEpoch { .. } => codes::STALE_BROKER_EPOCH,
                ProducerIdAllocationError::InvalidFrontier { .. }
                | ProducerIdAllocationError::Exhausted
                | ProducerIdAllocationError::Controller(_) => codes::UNKNOWN_SERVER_ERROR,
            };
            tracing::warn!(%error, "AllocateProducerIds failed");
            AllocateProducerIdsResponse {
                error_code,
                producer_id_start: -1,
                producer_id_len: 0,
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::test_support::{dispatch_wire, peer, principal, request_context};

    /// Serves a request as a principal that the default `AllowAllAuthorizer`
    /// allows.
    async fn handle_allowed(
        broker: &Broker,
        request: AllocateProducerIdsRequest,
    ) -> AllocateProducerIdsResponse {
        request_identity!(
            (user, address, ctx),
            crate::test_support::principal("ANONYMOUS"),
            client_id = "allocate-test",
            address = crate::test_support::peer()
        );
        handle(broker, request, 0, &ctx).await.expect("handle")
    }

    #[tokio::test]
    async fn allocates_consecutive_durable_blocks_and_fences_stale_epochs() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::test_support::start_broker_no_audit()
        );
        let broker_id = i32::try_from(broker.config.node_id.0).unwrap();
        let broker_epoch = broker
            .controller
            .current_image()
            .broker_epoch(broker.config.node_id)
            .expect("registered broker epoch");
        let request = |epoch| AllocateProducerIdsRequest {
            broker_id,
            broker_epoch: epoch,
            ..Default::default()
        };

        let first = handle_allowed(&broker, request(broker_epoch)).await;
        let second = handle_allowed(&broker, request(broker_epoch)).await;
        assert!(first.error_code == codes::NONE);
        assert!(first.producer_id_start == 0);
        assert!(first.producer_id_len == 1_000);
        assert!(second.producer_id_start == 1_000);
        assert!(broker.controller.current_image().next_producer_id() == 2_000);

        // Both calls may observe the same candidate frontier. The controller
        // accepts one exact record, and the loser retries from the committed
        // boundary instead of returning an overlapping block.
        let (third, fourth) = tokio::join!(
            handle_allowed(&broker, request(broker_epoch)),
            handle_allowed(&broker, request(broker_epoch)),
        );
        let mut concurrent_starts = [third.producer_id_start, fourth.producer_id_start];
        concurrent_starts.sort_unstable();
        assert!(concurrent_starts == [2_000, 3_000]);
        assert!(third.producer_id_len == 1_000);
        assert!(fourth.producer_id_len == 1_000);
        assert!(broker.controller.current_image().next_producer_id() == 4_000);

        // Kafka's `ClusterControlManager.checkBrokerEpoch` answers
        // `STALE_BROKER_EPOCH` for a mismatched epoch and for a broker id with
        // no registration alike (#828).
        let refused = AllocateProducerIdsResponse {
            error_code: codes::STALE_BROKER_EPOCH,
            producer_id_start: -1,
            producer_id_len: 0,
            ..Default::default()
        };
        let cases = [
            (
                "registered broker at its epoch",
                broker_id,
                broker_epoch,
                AllocateProducerIdsResponse {
                    error_code: codes::NONE,
                    producer_id_start: 4_000,
                    producer_id_len: 1_000,
                    ..Default::default()
                },
            ),
            (
                "registered broker at a stale epoch",
                broker_id,
                broker_epoch - 1,
                refused.clone(),
            ),
            (
                "unregistered broker id",
                broker_id + 100,
                broker_epoch,
                refused.clone(),
            ),
            ("negative broker id", -1, broker_epoch, refused),
        ];
        for (label, id, epoch, expected) in cases {
            let request = AllocateProducerIdsRequest {
                broker_id: id,
                broker_epoch: epoch,
                ..Default::default()
            };
            let answered = handle_allowed(&broker, request).await;
            check!(answered == expected, "{label}");
        }
        assert!(broker.controller.current_image().next_producer_id() == 5_000);

        // Seed the last frontier that cannot fit another positive block. The
        // adapter must fail without submitting a wrapped or partial range.
        broker
            .controller
            .submit_change(vec![krabka_metadata::MetadataRecord::V1ProducerIds(
                krabka_metadata::ProducerIdsRecord {
                    broker_id: broker.config.node_id,
                    broker_epoch,
                    next_producer_id: i64::MAX - 999,
                },
            )])
            .await
            .expect("seed producer ID limit");
        let exhausted = handle_allowed(&broker, request(broker_epoch)).await;
        assert!(exhausted.error_code == codes::UNKNOWN_SERVER_ERROR);
        assert!(exhausted.producer_id_start == -1);
        assert!(exhausted.producer_id_len == 0);
        assert!(broker.controller.current_image().next_producer_id() == i64::MAX - 999);
        broker_handle.shutdown().await;
    }

    /// Kafka's `ControllerApis.handleAllocateProducerIdsRequest` checks
    /// `ClusterAction` on the cluster before the controller allocates
    /// (#681). A denial is `AllocateProducerIdsRequest.getErrorResponse`, and
    /// the next block start does not move.
    #[tokio::test]
    async fn allocation_needs_cluster_action() {
        broker_fixture!((broker_handle, _dir, broker), principal_grants);
        let request = AllocateProducerIdsRequest {
            broker_id: i32::try_from(broker.config.node_id.0).unwrap(),
            broker_epoch: broker
                .controller
                .current_image()
                .broker_epoch(broker.config.node_id)
                .expect("registered broker epoch"),
            ..Default::default()
        };
        let refused = AllocateProducerIdsResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        let cases = [
            ("none", refused.clone(), 0),
            ("Cluster:Alter+Cluster:Describe", refused, 0),
            (
                "Cluster:ClusterAction",
                AllocateProducerIdsResponse {
                    error_code: codes::NONE,
                    producer_id_start: 0,
                    producer_id_len: 1_000,
                    ..Default::default()
                },
                1_000,
            ),
        ];

        let address = peer();
        for (grants, expected, next_producer_id) in cases {
            let user = principal(grants);
            let ctx = request_context(&user, &address, "allocate-test");
            let answered: AllocateProducerIdsResponse = dispatch_wire(
                &broker,
                krabka_protocol::owned::allocate_producer_ids_request::API_KEY,
                0,
                &request,
                &ctx,
            )
            .await;
            check!(answered == expected, "{grants}");
            check!(
                broker.controller.current_image().next_producer_id() == next_producer_id,
                "{grants}"
            );
        }
        broker_handle.shutdown().await;
    }
}
