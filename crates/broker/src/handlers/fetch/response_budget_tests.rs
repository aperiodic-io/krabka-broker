use assert2::assert;

use super::fetch_response_byte_budget;

#[test]
fn consumer_fetch_uses_the_smallest_request_broker_and_quota_budgets() {
    assert!(fetch_response_byte_budget(100, 80, 90, false) == 80);
    assert!(fetch_response_byte_budget(100, 120, 90, false) == 90);
    assert!(fetch_response_byte_budget(70, 120, 90, false) == 70);
}

#[test]
fn follower_fetch_keeps_its_request_budget_without_consumer_caps() {
    assert!(fetch_response_byte_budget(100, 80, 90, true) == 100);
}
