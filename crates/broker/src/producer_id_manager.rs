//! Cluster-wide producer-ID block allocation.
//!
//! Brokers reserve non-overlapping blocks through the metadata controller and
//! then serve IDs from their local block without a controller round trip. The
//! committed `ProducerIdsRecord` stores the first ID in the next unassigned
//! block, so broker restarts and controller failover cannot reuse IDs.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use krabka_log::ProducerId;
use krabka_metadata::{MetadataRecord, NodeId, ProducerIdsRecord};
use krabka_verified::{
    ProducerIdBlockAllocationDecision as BlockDecision, producer_id_block_allocation,
};
use tokio::sync::Mutex;

use crate::{error::BrokerError, metadata_source::MetadataSource};

/// Kafka's controller-assigned producer-ID block size.
pub(crate) const PRODUCER_ID_BLOCK_SIZE: i32 = 1_000;

pub(crate) type ProducerIdBlock = krabka_verified::ProducerIdBlockPlan;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProducerIdAllocationError {
    #[error("broker {0} is not registered")]
    BrokerNotRegistered(NodeId),
    #[error("broker {broker_id} epoch is stale: requested {requested}, registered {registered}")]
    StaleBrokerEpoch {
        broker_id: NodeId,
        requested: i64,
        registered: i64,
    },
    #[error("invalid producer ID allocation frontier {first} or block length {len}")]
    InvalidFrontier { first: i64, len: i32 },
    #[error("the signed 64-bit producer ID space is exhausted")]
    Exhausted,
    #[error("controller failed to allocate a producer ID block: {0}")]
    Controller(String),
}

fn plan_block(
    broker_id: NodeId,
    requested_epoch: i64,
    registered_epoch: Option<i64>,
    first: i64,
) -> Result<ProducerIdBlock, ProducerIdAllocationError> {
    let registered = registered_epoch.is_some();
    let registered_epoch = registered_epoch.unwrap_or(-1);
    match producer_id_block_allocation(
        i32::try_from(broker_id.0).is_ok(),
        registered,
        requested_epoch,
        registered_epoch,
        first,
        PRODUCER_ID_BLOCK_SIZE,
    ) {
        BlockDecision::BrokerNotRegistered => {
            Err(ProducerIdAllocationError::BrokerNotRegistered(broker_id))
        }
        BlockDecision::StaleBrokerEpoch => Err(ProducerIdAllocationError::StaleBrokerEpoch {
            broker_id,
            requested: requested_epoch,
            registered: registered_epoch,
        }),
        BlockDecision::InvalidFrontier => Err(ProducerIdAllocationError::InvalidFrontier {
            first,
            len: PRODUCER_ID_BLOCK_SIZE,
        }),
        BlockDecision::Exhausted => Err(ProducerIdAllocationError::Exhausted),
        BlockDecision::Allocate(plan) => Ok(plan),
    }
}

impl From<ProducerIdAllocationError> for BrokerError {
    fn from(error: ProducerIdAllocationError) -> Self {
        Self::ProducerIdBlockUnavailable(error.to_string())
    }
}

/// Atomically reserve one durable block for a registered broker.
///
/// Concurrent brokers can observe the same candidate start. The controller
/// serializes their metadata records; its monotonic validation accepts one and
/// rejects the stale candidate. The loser observes the new image and retries.
pub(crate) async fn allocate_block(
    controller: &Arc<dyn MetadataSource>,
    broker_id: NodeId,
    broker_epoch: i64,
) -> Result<ProducerIdBlock, ProducerIdAllocationError> {
    loop {
        let image = controller.current_image();
        let block = plan_block(
            broker_id,
            broker_epoch,
            image.broker_epoch(broker_id),
            image.next_producer_id(),
        )?;
        let record = MetadataRecord::V1ProducerIds(ProducerIdsRecord {
            broker_id,
            broker_epoch,
            next_producer_id: block.next,
        });
        match controller.submit_change(vec![record]).await {
            Ok(_) => return Ok(block),
            Err(error) => {
                // A competing allocation committed first. Retry from the new
                // durable boundary; return all other controller failures.
                if controller.current_image().next_producer_id() > block.first {
                    continue;
                }
                return Err(ProducerIdAllocationError::Controller(error.to_string()));
            }
        }
    }
}

#[derive(derive_more::Debug)]
pub struct ProducerIdManager {
    #[debug(skip)]
    controller: Option<Arc<dyn MetadataSource>>,
    node_id: NodeId,
    /// The broker epoch this process registered at, which every allocation
    /// names, as Kafka's `RPCProducerIdManager` reads `brokerEpochSupplier`
    /// (`BrokerLifecycleManager.brokerEpoch`). -1 when this node never
    /// registered as a broker.
    #[debug(skip)]
    broker_epoch: i64,
    next: AtomicI64,
    end_exclusive: AtomicI64,
    #[debug(skip)]
    refill: Mutex<()>,
}

impl ProducerIdManager {
    #[must_use]
    pub(crate) fn clustered(
        node_id: NodeId,
        broker_epoch: i64,
        controller: Arc<dyn MetadataSource>,
    ) -> Self {
        Self {
            controller: Some(controller),
            node_id,
            broker_epoch,
            next: AtomicI64::new(0),
            end_exclusive: AtomicI64::new(0),
            refill: Mutex::new(()),
        }
    }

    /// Allocate a fresh `(producer_id, producer_epoch=0)`.
    pub(crate) async fn allocate(&self) -> Result<(ProducerId, i16), ProducerIdAllocationError> {
        loop {
            if let Some(id) = self.claim() {
                return Ok((ProducerId(id), 0));
            }

            let _refill = self.refill.lock().await;
            if let Some(id) = self.claim() {
                return Ok((ProducerId(id), 0));
            }
            let controller = self.controller.as_ref().ok_or_else(|| {
                ProducerIdAllocationError::Controller(
                    "test allocator exhausted its local ID space".into(),
                )
            })?;
            if self.broker_epoch < 0 {
                return Err(ProducerIdAllocationError::BrokerNotRegistered(self.node_id));
            }
            let block = allocate_block(controller, self.node_id, self.broker_epoch).await?;
            self.next.store(block.first, Ordering::Release);
            self.end_exclusive.store(block.next, Ordering::Release);
        }
    }

    fn claim(&self) -> Option<i64> {
        loop {
            let next = self.next.load(Ordering::Acquire);
            if next >= self.end_exclusive.load(Ordering::Acquire) {
                return None;
            }
            if self
                .next
                .compare_exchange_weak(next, next + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(next);
            }
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            controller: None,
            node_id: NodeId(0),
            broker_epoch: -1,
            next: AtomicI64::new(0),
            end_exclusive: AtomicI64::new(i64::MAX),
            refill: Mutex::new(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn local_test_allocator_returns_monotonic_ids() {
        let manager = ProducerIdManager::new();
        for want in 0..3 {
            assert!(manager.allocate().await.unwrap() == (ProducerId(want), 0));
        }
    }

    #[test]
    fn block_plan_adapter_fences_malformed_stale_and_limit_inputs() {
        let broker = NodeId(7);
        assert!(matches!(
            plan_block(NodeId(u64::MAX), 3, Some(3), 0),
            Err(ProducerIdAllocationError::BrokerNotRegistered(_))
        ));
        assert!(matches!(
            plan_block(broker, 3, None, 0),
            Err(ProducerIdAllocationError::BrokerNotRegistered(_))
        ));
        assert!(matches!(
            plan_block(broker, 2, Some(3), 0),
            Err(ProducerIdAllocationError::StaleBrokerEpoch { .. })
        ));
        assert!(matches!(
            plan_block(broker, 3, Some(3), -1),
            Err(ProducerIdAllocationError::InvalidFrontier { .. })
        ));
        assert!(matches!(
            plan_block(broker, 3, Some(3), i64::MAX - 999),
            Err(ProducerIdAllocationError::Exhausted)
        ));
        assert!(
            plan_block(broker, 3, Some(3), i64::MAX - 1_000)
                .is_ok_and(|block| block.first == i64::MAX - 1_000
                    && block.len == 1_000
                    && block.next == i64::MAX)
        );
    }

    #[test]
    fn claim_stops_at_the_exclusive_block_end() {
        let manager = ProducerIdManager {
            controller: None,
            node_id: NodeId(0),
            broker_epoch: -1,
            next: AtomicI64::new(10),
            end_exclusive: AtomicI64::new(12),
            refill: Mutex::new(()),
        };
        assert!(manager.claim() == Some(10));
        assert!(manager.claim() == Some(11));
        assert!(manager.claim() == None);
    }
}
