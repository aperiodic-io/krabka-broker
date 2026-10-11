//! Test-only [`BrokerHandle`] helpers that control the transaction
//! coordinator's marker fan-out, so a test can stop or fail a transaction
//! after its `Prepare*` record is durable.

use crate::{broker::BrokerHandle, txn::coordinator::fanout_gate::MarkerFanoutMode};

impl BrokerHandle {
    /// Test-only: set what every transaction-marker fan-out does at its start.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn set_transaction_marker_fanout_for_test(&self, mode: MarkerFanoutMode) {
        self.broker
            .txn_coordinator
            .marker_fanout_gate
            .set_mode(mode);
    }

    /// Test-only: wait until `count` marker fan-outs reached a gate that was
    /// not open.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_for_transaction_marker_fanouts_for_test(&self, count: usize) {
        use crate::broker::TEST_AWAITER_TIMEOUT;

        let arrived = tokio::time::timeout(
            TEST_AWAITER_TIMEOUT,
            self.broker
                .txn_coordinator
                .marker_fanout_gate
                .wait_for_arrivals(count),
        )
        .await;
        assert2::assert!(
            arrived.is_ok(),
            "{count} transaction marker fan-outs did not reach the test gate"
        );
    }
}
