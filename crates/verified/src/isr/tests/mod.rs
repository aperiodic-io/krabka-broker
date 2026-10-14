use super::*;

/// A registered, unfenced broker at epoch 7 whose last Fetch carried it.
const ELIGIBLE: IsrEligibilityFacts = IsrEligibilityFacts {
    fenced: false,
    shutting_down: false,
    fetch_broker_epoch: Some(7),
    alive_broker_epoch: Some(7),
};

#[test]
fn proposal_change_detection_compares_isr_sizes() {
    assert2::check!(!isr_proposal_changed(0, 0));
    assert2::check!(isr_proposal_changed(1, 0));
    assert2::check!(isr_proposal_changed(0, 1));
    assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
}

mod replica_isr_eligibility_follows_kip_841;

mod leader_high_watermark_follows_kafka;

mod caught_up_credit_follows_kafkas_fetch_state_update;
