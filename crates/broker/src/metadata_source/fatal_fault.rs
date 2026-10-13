//! Running startup work against the fatal fault of the controller that this
//! node hosts.

use {crate::error::BrokerError, std::future::Future, tokio::sync::watch};

/// Runs `work`, and ends it with [`BrokerError::FatalFault`] as soon as
/// `faults` carries a fault.
///
/// `faults` is [`MetadataSource::watch_fatal`](crate::metadata_source::MetadataSource::watch_fatal).
/// Kafka's `ProcessTerminatingFaultHandler` halts the process the moment the
/// controller replays a fault such as a `FeatureLevelRecord` above its
/// supported range. A start that carries on after that fault only delays the
/// halt: a submit against the stopped controller retries under backoff for
/// seconds, and the wait for the first unfence lasts minutes for a heartbeat
/// that a dead controller cannot answer. The fault ends the work at once
/// instead, and the caller reports it.
///
/// A failure of `work` that comes after a fault reports the fault as well. The
/// controller publishes the fault before it stops, so the failure is a bare
/// `controller shut down` that the fault caused, and the fault names the
/// reason.
///
/// A channel that closes without a fault is a controller that stopped in an
/// orderly way, or a source that has none. That is no fault, and `work` runs
/// on.
pub(crate) async fn or_fatal_fault<T>(
    mut faults: watch::Receiver<Option<String>>,
    work: impl Future<Output = Result<T, BrokerError>>,
) -> Result<T, BrokerError> {
    let published = faults.clone();
    let fault = async move {
        faults
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|fault| fault.clone())
    };
    tokio::select! {
        biased;
        Some(fault) = fault => Err(BrokerError::FatalFault(fault)),
        result = work => result.map_err(|error| {
            published.borrow().clone().map_or(error, BrokerError::FatalFault)
        }),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::time::Duration};

    const FAULT: &str = "Tried to apply FeatureLevelRecord, but this controller only supports 7-30";

    fn fatal_matches(error: &BrokerError, expected: &str) -> bool {
        matches!(error, BrokerError::FatalFault(fault) if fault == expected)
    }

    // Work that never finishes on its own stands for a submit that retries
    // against a stopped controller, or an unfence wait. A test that the fix
    // regressed hangs here, and the timeout turns that into a failure.
    #[tokio::test]
    async fn a_fault_published_mid_work_ends_it_at_once() {
        let (tx, rx) = watch::channel(None);
        let work = or_fatal_fault(rx, std::future::pending::<Result<(), BrokerError>>());
        let publish = async {
            tokio::task::yield_now().await;
            tx.send_replace(Some(FAULT.to_owned()));
        };
        let (result, ()) = tokio::time::timeout(
            Duration::from_secs(30),
            futures_util::future::join(work, publish),
        )
        .await
        .expect("the fault did not end the work");
        let Err(error) = result else {
            panic!("work that a fault ended returned Ok");
        };
        assert!(fatal_matches(&error, FAULT));
        assert!(error.to_string() == format!("Encountered fatal fault: {FAULT}"));
    }

    #[tokio::test]
    async fn a_fault_that_predates_the_work_wins_over_work_that_could_finish() {
        let (_tx, rx) = watch::channel(Some(FAULT.to_owned()));
        let result = or_fatal_fault(rx, async { Ok::<_, BrokerError>(7) }).await;
        let Err(error) = result else {
            panic!("a start over a published fault returned Ok");
        };
        assert!(fatal_matches(&error, FAULT));
    }

    // Each row is a failure of the work with the state of the fault channel
    // behind it, and the message the caller sees.
    #[tokio::test]
    async fn a_failure_reports_the_fault_only_when_there_is_one() {
        let rows = [
            ("a fault caused the failure", Some(FAULT), Some(FAULT)),
            ("no fault", None, None),
        ];
        for (name, published, want_fault) in rows {
            let (tx, rx) = watch::channel(None);
            let result = or_fatal_fault(rx, async {
                // The controller publishes the fault, then stops, and a submit
                // that met the stop fails with a bare shutdown error.
                tx.send_replace(published.map(str::to_owned));
                Err::<(), _>(BrokerError::Shutdown)
            })
            .await;
            let Err(error) = result else {
                panic!("{name}: failed work returned Ok");
            };
            match want_fault {
                Some(fault) => assert!(fatal_matches(&error, fault), "{name}: {error}"),
                None => assert!(matches!(error, BrokerError::Shutdown), "{name}: {error}"),
            }
        }
    }

    #[tokio::test]
    async fn a_channel_that_closes_without_a_fault_lets_the_work_finish() {
        let (tx, rx) = watch::channel(None);
        drop(tx);
        let done = or_fatal_fault(rx, async {
            tokio::task::yield_now().await;
            Ok::<_, BrokerError>(7)
        })
        .await;
        assert!(done.ok() == Some(7));
    }
}
