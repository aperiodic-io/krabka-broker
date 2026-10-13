//! A test gate in front of the transaction-marker fan-out.
//!
//! A crash or a failed write between the `Prepare*` append and the
//! `Complete*` append is the state that transaction recovery must finish.
//! A real broker reaches that window only by chance. This gate lets a test
//! stop every fan-out at its start, or fail it, and then stop the broker or
//! send more requests while the durable state is `PrepareCommit` or
//! `PrepareAbort`.

use {crate::error::BrokerError, tokio::sync::watch};

/// What the transaction-marker fan-out does when it reaches the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarkerFanoutMode {
    /// Continue at once. This is the normal broker behavior.
    #[default]
    Open,
    /// Wait until the mode changes.
    Hold,
    /// Fail the fan-out without writing a marker.
    Fail,
}

/// The gate state that the coordinator and the test share.
#[derive(Debug, krabka_macros::FieldDefaults)]
pub(crate) struct MarkerFanoutGate {
    #[default(watch::Sender::new(MarkerFanoutMode::Open))]
    mode: watch::Sender<MarkerFanoutMode>,
    #[default(watch::Sender::new(0))]
    arrivals: watch::Sender<usize>,
}

impl MarkerFanoutGate {
    pub(crate) fn set_mode(&self, mode: MarkerFanoutMode) { self.mode.send_replace(mode); }

    /// Wait until `count` fan-outs reached the gate while it was not open.
    pub(crate) async fn wait_for_arrivals(&self, count: usize) {
        let mut arrivals = self.arrivals.subscribe();
        // The sender lives as long as `self`, so the wait cannot fail.
        let _ = arrivals.wait_for(|arrived| *arrived >= count).await;
    }

    /// Pass the gate: continue, wait, or fail as the mode says.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] while the mode is
    /// [`MarkerFanoutMode::Fail`].
    pub(crate) async fn pass(&self) -> Result<(), BrokerError> {
        let mut mode = self.mode.subscribe();
        let mut counted = false;
        loop {
            let current = *mode.borrow_and_update();
            if current == MarkerFanoutMode::Open {
                return Ok(());
            }
            if !counted {
                self.arrivals.send_modify(|arrived| *arrived += 1);
                counted = true;
            }
            if current == MarkerFanoutMode::Fail {
                return Err(BrokerError::Txn(
                    "a test gate failed the transaction marker fan-out".into(),
                ));
            }
            // The sender lives as long as `self`, so the wait cannot fail.
            let _ = mode.changed().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::sync::Arc};

    #[tokio::test]
    async fn gate_passes_holds_and_fails_as_its_mode_says() {
        let gate = Arc::new(MarkerFanoutGate::default());
        assert!(gate.pass().await.is_ok());

        gate.set_mode(MarkerFanoutMode::Fail);
        assert!(gate.pass().await.is_err());
        gate.wait_for_arrivals(1).await;

        gate.set_mode(MarkerFanoutMode::Hold);
        let held = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.pass().await.is_ok() }
        });
        gate.wait_for_arrivals(2).await;
        assert!(!held.is_finished());
        gate.set_mode(MarkerFanoutMode::Open);
        assert!(held.await.expect("held fan-out"));
    }
}
