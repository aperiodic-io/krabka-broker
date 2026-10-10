//! The two shapes the broker's tasks repeat: asking an actor a question over
//! its `mpsc` mailbox, and running a body on a fixed cadence until shutdown.

use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    time::{Interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

/// Start an actor over a bounded mailbox and retain its task handle.
pub(crate) fn spawn_mailbox<M, F: Future<Output = ()> + Send + 'static>(
    capacity: usize,
    run: impl FnOnce(mpsc::Receiver<M>) -> F,
) -> (mpsc::Sender<M>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(capacity);
    (tx, tokio::spawn(run(rx)))
}

/// Why an [`ask`] got no reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AskError {
    /// The actor's mailbox is closed, so the request was never delivered.
    Closed,
    /// The actor took the request but dropped the reply sender unanswered.
    Dropped,
}

/// Send the message `make` builds around a fresh reply sender to `tx`, and
/// await the actor's reply.
///
/// # Errors
///
/// Returns [`AskError::Closed`] when the mailbox is closed and
/// [`AskError::Dropped`] when the actor drops the reply sender.
pub(crate) async fn ask<M, T>(
    tx: &mpsc::Sender<M>,
    make: impl FnOnce(oneshot::Sender<T>) -> M,
) -> Result<T, AskError> {
    let (reply, rx) = oneshot::channel();
    tx.send(make(reply)).await.map_err(|_| AskError::Closed)?;
    rx.await.map_err(|_| AskError::Dropped)
}

/// Send shutdown without a send timeout, then bound only the reply wait.
pub(crate) async fn shutdown_actor<M>(
    tx: &mpsc::Sender<M>,
    shutdown: impl FnOnce(oneshot::Sender<()>) -> M,
    timeout: Duration,
) {
    let (reply, ack) = oneshot::channel();
    if tx.send(shutdown(reply)).await.is_ok() {
        let _ = tokio::time::timeout(timeout, ack).await;
    }
}

/// Clone every registry value before callers perform work that can await.
#[must_use]
pub(crate) fn cloned_registry_values<V: Clone>(registry: &dashmap::DashMap<String, V>) -> Vec<V> {
    registry.iter().map(|entry| entry.value().clone()).collect()
}

/// Run the future `body` makes on every tick of `tick` until `shutdown` is
/// cancelled.
///
/// Missed ticks are skipped, not bunched up. A body already running finishes
/// before cancellation is seen. The function returns on cancellation, so the
/// caller logs its own shutdown line under its own target.
///
/// `body` is a closure returning a future rather than an `AsyncFnMut`: rustc
/// cannot yet prove an `AsyncFnMut` body's future `Send` for every lifetime,
/// and these loops run under `tokio::spawn`.
pub(crate) async fn run_every<F: Future<Output = ()>>(
    mut tick: Interval,
    shutdown: &CancellationToken,
    mut body: impl FnMut() -> F,
) {
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => body().await,
            () = shutdown.cancelled() => return,
        }
    }
}

/// Await already spawned tasks in order, reporting each failed join to its caller.
/// Dropping the handles without calling this function leaves their tasks running.
pub(crate) async fn finish_tasks(
    tasks: Vec<tokio::task::JoinHandle<()>>,
    mut on_error: impl FnMut(tokio::task::JoinError),
) {
    for handle in tasks {
        if let Err(error) = handle.await {
            on_error(error);
        }
    }
}

/// Declare a concrete task collection whose join errors are logged at its caller.
macro_rules! scheduled_tasks_type {
    ($(#[$doc:meta])* $vis:vis struct $name:ident;
        $(#[$finished_doc:meta])* |$error:ident| $on_error:block) => {
        $(#[$doc])*
        #[derive(Debug, Default)]
        $vis struct $name(Vec<tokio::task::JoinHandle<()>>);

        impl $name {
            $(#[$finished_doc])*
            $vis async fn finished(self) {
                $crate::task_util::finish_tasks(self.0, |$error| $on_error).await;
            }
        }
    };
}
pub(crate) use scheduled_tasks_type;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn ask_returns_the_reply() {
        let (tx, mut rx) = mpsc::channel::<oneshot::Sender<u32>>(1);
        tokio::spawn(async move {
            let reply = rx.recv().await.expect("a request");
            let _ = reply.send(7);
        });
        assert!(ask(&tx, |reply| reply).await == Ok(7));
    }

    #[tokio::test]
    async fn ask_reports_a_closed_mailbox_and_a_dropped_reply() {
        let (tx, rx) = mpsc::channel::<oneshot::Sender<u32>>(1);
        drop(rx);
        assert!(ask(&tx, |reply| reply).await == Err(AskError::Closed));

        let (tx, mut rx) = mpsc::channel::<oneshot::Sender<u32>>(1);
        tokio::spawn(async move { drop(rx.recv().await) });
        assert!(ask(&tx, |reply| reply).await == Err(AskError::Dropped));
    }

    #[tokio::test(start_paused = true)]
    async fn run_every_ticks_until_shutdown_and_skips_missed_ticks() {
        let shutdown = CancellationToken::new();
        let period = Duration::from_secs(1);
        let start = tokio::time::Instant::now();
        let mut fired_at = Vec::new();
        run_every(tokio::time::interval(period), &shutdown, || {
            fired_at.push(start.elapsed());
            let runs = fired_at.len();
            if runs == 3 {
                shutdown.cancel();
            }
            async move {
                if runs == 1 {
                    // Overrun two and a half periods.
                    tokio::time::sleep(period * 5 / 2).await;
                }
            }
        })
        .await;
        // Skip ticks once for the overrun, then realigns to the period grid:
        // Burst would tick again at 2.5s and Delay at 3.5s.
        assert!(fired_at == [Duration::ZERO, period * 5 / 2, period * 3]);
    }

    #[tokio::test]
    async fn run_every_returns_at_once_when_already_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let start = tokio::time::Instant::now() + Duration::from_secs(3_600);
        let mut runs = 0_u32;
        run_every(
            tokio::time::interval_at(start, Duration::from_secs(1)),
            &shutdown,
            || {
                runs += 1;
                std::future::ready(())
            },
        )
        .await;
        assert!(runs == 0);
    }
}
