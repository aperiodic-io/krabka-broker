//! Producer-ID block allocation fencing and exact range construction.

use creusot_std::prelude::*;

#[krabka_macros::model_type]
pub struct ProducerIdBlockPlan {
    pub first: i64,
    pub len: i32,
    pub next: i64,
}

#[krabka_macros::model_type]
pub enum ProducerIdBlockAllocationDecision {
    BrokerNotRegistered,
    StaleBrokerEpoch,
    InvalidFrontier,
    Exhausted,
    Allocate(ProducerIdBlockPlan),
}

/// Fence one broker generation and construct its exact positive contiguous
/// producer-ID block without signed overflow.
#[ensures(match result {
    ProducerIdBlockAllocationDecision::BrokerNotRegistered =>
        !broker_id_valid || !broker_registered,
    ProducerIdBlockAllocationDecision::StaleBrokerEpoch =>
        broker_id_valid && broker_registered
            && (requested_broker_epoch@ < 0
                || registered_broker_epoch@ < 0
                || requested_broker_epoch@ != registered_broker_epoch@),
    ProducerIdBlockAllocationDecision::InvalidFrontier =>
        broker_id_valid && broker_registered
            && requested_broker_epoch@ >= 0
            && requested_broker_epoch@ == registered_broker_epoch@
            && (first@ < 0 || block_len@ <= 0),
    ProducerIdBlockAllocationDecision::Exhausted =>
        broker_id_valid && broker_registered
            && requested_broker_epoch@ >= 0
            && requested_broker_epoch@ == registered_broker_epoch@
            && first@ >= 0 && block_len@ > 0
            && first@ + block_len@ > i64::MAX@,
    ProducerIdBlockAllocationDecision::Allocate(plan) =>
        broker_id_valid && broker_registered
            && requested_broker_epoch@ >= 0
            && requested_broker_epoch@ == registered_broker_epoch@
            && plan.first@ == first@
            && plan.len@ == block_len@
            && plan.next@ == first@ + block_len@
            && plan.first@ >= 0
            && plan.len@ > 0
            && plan.first@ < plan.next@
            && plan.next@ <= i64::MAX@,
})]
#[must_use]
pub fn producer_id_block_allocation(
    broker_id_valid: bool,
    broker_registered: bool,
    requested_broker_epoch: i64,
    registered_broker_epoch: i64,
    first: i64,
    block_len: i32,
) -> ProducerIdBlockAllocationDecision {
    if !broker_id_valid || !broker_registered {
        return ProducerIdBlockAllocationDecision::BrokerNotRegistered;
    }
    if requested_broker_epoch < 0
        || registered_broker_epoch < 0
        || requested_broker_epoch != registered_broker_epoch
    {
        return ProducerIdBlockAllocationDecision::StaleBrokerEpoch;
    }
    if first < 0 || block_len <= 0 {
        return ProducerIdBlockAllocationDecision::InvalidFrontier;
    }
    let Some(next) = first.checked_add(i64::from(block_len)) else {
        return ProducerIdBlockAllocationDecision::Exhausted;
    };
    ProducerIdBlockAllocationDecision::Allocate(ProducerIdBlockPlan {
        first,
        len: block_len,
        next,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{
            ProducerIdBlockAllocationDecision, ProducerIdBlockPlan, producer_id_block_allocation,
        },
        assert2::assert,
    };

    #[test]
    fn allocation_fences_broker_and_builds_only_exact_positive_blocks() {
        use ProducerIdBlockAllocationDecision::{
            Allocate, BrokerNotRegistered, Exhausted, InvalidFrontier, StaleBrokerEpoch,
        };

        assert!(producer_id_block_allocation(false, true, 3, 3, 0, 1_000) == BrokerNotRegistered);
        assert!(producer_id_block_allocation(true, false, 3, 3, 0, 1_000) == BrokerNotRegistered);
        assert!(producer_id_block_allocation(true, true, -1, 3, 0, 1_000) == StaleBrokerEpoch);
        assert!(producer_id_block_allocation(true, true, 1, -1, 0, 1_000) == StaleBrokerEpoch);
        assert!(producer_id_block_allocation(true, true, 2, 3, 0, 1_000) == StaleBrokerEpoch);
        assert!(
            producer_id_block_allocation(true, true, 0, 0, 0, 1_000)
                == Allocate(ProducerIdBlockPlan {
                    first: 0,
                    len: 1_000,
                    next: 1_000,
                })
        );
        assert!(producer_id_block_allocation(true, true, 3, 3, -1, 1_000) == InvalidFrontier);
        assert!(producer_id_block_allocation(true, true, 3, 3, 0, 0) == InvalidFrontier);
        assert!(producer_id_block_allocation(true, true, 3, 3, i64::MAX - 999, 1_000) == Exhausted);
        assert!(
            producer_id_block_allocation(true, true, 3, 3, i64::MAX - 1_000, 1_000)
                == Allocate(ProducerIdBlockPlan {
                    first: i64::MAX - 1_000,
                    len: 1_000,
                    next: i64::MAX,
                })
        );
    }
}
