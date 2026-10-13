//! Flat loops preserve binding scopes, timer failure exits and per-tick work.

use {
    assert2::assert,
    std::{
        collections::VecDeque,
        future::{Future, pending, ready},
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Poll, Waker},
        time::Duration,
    },
};

type Events = Arc<Mutex<Vec<String>>>;
type Tick = Pin<Box<dyn Future<Output = bool> + Send>>;

enum Deadline {
    Complete(bool),
    Refuse,
    Pending,
}

struct Timer {
    deadlines: Mutex<VecDeque<Deadline>>,
    events: Events,
    marker: usize,
}

mod time_util {
    use super::{Deadline, Duration, Tick, Timer, pending, ready};

    pub(crate) fn arm(timer: &Timer, delay: Duration, task: &'static str) -> Option<Tick> {
        timer
            .events
            .lock()
            .unwrap()
            .push(format!("arm:{task}:{}", delay.as_millis()));
        match timer
            .deadlines
            .lock()
            .unwrap()
            .pop_front()
            .expect("expected deadline")
        {
            Deadline::Complete(outcome) => Some(Box::pin(ready(outcome))),
            Deadline::Pending => Some(Box::pin(pending())),
            Deadline::Refuse => None,
        }
    }

    pub(crate) fn fired(outcome: bool, task: &'static str) -> bool {
        assert2::assert!(task == "test sweeper");
        outcome
    }
}

struct Cadence {
    first: Duration,
    later: Duration,
}

#[derive(Default)]
struct Shutdown {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Shutdown {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    async fn cancelled(&self) {
        std::future::poll_fn(|context| {
            *self.waker.lock().unwrap() = Some(context.waker().clone());
            if self.cancelled.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

struct Metadata {
    reads: AtomicUsize,
    stop_after: usize,
}

impl Metadata {
    fn current_image(&self) -> usize { self.reads.fetch_add(1, Ordering::SeqCst) + 1 }
}

fn evaluated<T>(events: &Events, name: &str, value: T) -> T {
    events.lock().unwrap().push(name.to_owned());
    value
}

async fn drive(
    timer: Arc<Timer>,
    shutdown: Arc<Shutdown>,
    events: Events,
    metadata: Arc<Metadata>,
    first: Duration,
) {
    let tick = 99;
    krabka_macros::sweep_loop! {
        timer [evaluated(&events, "timer", &*timer)];
        cadence [Cadence { first: evaluated(&events, "first", first), later: evaluated(&events, "later", Duration::from_millis(7)) }];
        shutdown [evaluated(&events, "shutdown", &*shutdown)];
        task [evaluated(&events, "task", "test sweeper")];
        sweep {
            let image = metadata.current_image();
            assert!(tick == 99 && timer.marker == 42);
            events.lock().unwrap().push(format!("image:{image}"));
            if image == metadata.stop_after { return; }
            if image == 2 {
                shutdown.cancel();
            } else {
                std::future::ready(()).await;
            }
        };
        on_shutdown {
            events.lock().unwrap().push("shutdown block".into());
        };
    }
}

async fn scenario(
    deadlines: impl IntoIterator<Item = Deadline>,
    first: Duration,
    cancelled: bool,
    stop_after: usize,
) -> Vec<String> {
    let events = Events::default();
    let timer = Arc::new(Timer {
        deadlines: Mutex::new(deadlines.into_iter().collect()),
        events: Arc::clone(&events),
        marker: 42,
    });
    let shutdown = Arc::new(Shutdown::default());
    if cancelled {
        shutdown.cancel();
    }
    let handle = tokio::spawn(drive(
        timer,
        shutdown,
        Arc::clone(&events),
        Arc::new(Metadata {
            reads: AtomicUsize::new(0),
            stop_after,
        }),
        first,
    ));
    handle.await.unwrap();
    Arc::try_unwrap(events).unwrap().into_inner().unwrap()
}

#[tokio::test]
async fn snapshots_and_delay_are_refreshed_after_each_sweep_with_original_return_scope() {
    for first in [Duration::ZERO, Duration::from_millis(11)] {
        let events = scenario(
            [
                Deadline::Complete(true),
                Deadline::Complete(true),
                Deadline::Pending,
            ],
            first,
            false,
            0,
        )
        .await;
        assert!(
            events
                == [
                    "timer",
                    "first",
                    "later",
                    "shutdown",
                    "task",
                    &format!("arm:test sweeper:{}", first.as_millis()),
                    "image:1",
                    "arm:test sweeper:7",
                    "image:2",
                    "arm:test sweeper:7",
                    "shutdown block"
                ]
        );
    }
}

#[tokio::test]
async fn refusing_or_failing_timers_stop_without_sweeping_or_spinning() {
    for deadline in [Deadline::Refuse, Deadline::Complete(false)] {
        let events = scenario([deadline], Duration::ZERO, false, 0).await;
        assert!(
            events
                == [
                    "timer",
                    "first",
                    "later",
                    "shutdown",
                    "task",
                    "arm:test sweeper:0"
                ]
        );
    }
    let events = scenario(
        [Deadline::Complete(true), Deadline::Refuse],
        Duration::ZERO,
        false,
        0,
    )
    .await;
    assert!(events.last().unwrap() == "arm:test sweeper:7");
    assert!(
        events
            .iter()
            .filter(|event| event.starts_with("image:"))
            .count()
            == 1
    );
}

#[tokio::test]
async fn cancellation_runs_only_the_shutdown_block_and_drops_the_pending_tick() {
    let events = scenario([Deadline::Pending], Duration::from_secs(60), true, 0).await;
    assert!(events.last().unwrap() == "shutdown block");
    assert!(!events.iter().any(|event| event.starts_with("image:")));
}

#[tokio::test]
async fn native_sweep_return_exits_the_enclosing_task_before_rearming() {
    let events = scenario([Deadline::Complete(true)], Duration::ZERO, false, 1).await;
    assert!(events.last().unwrap() == "image:1");
}
