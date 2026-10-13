//! Reaper attributes retain borrowed futures, ordinary signatures and caller-target logs.

use {
    assert2::assert,
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    tracing::{
        Event, Level, Subscriber,
        field::{Field, Visit},
    },
    tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
    },
};

#[derive(Clone, Default)]
struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    fn cancel(&self) { self.cancelled.store(true, Ordering::SeqCst); }

    fn is_cancelled(&self) -> bool { self.cancelled.load(Ordering::SeqCst) }
}

mod task_util {
    use super::CancellationToken;

    pub(crate) async fn run_every<F: Future<Output = ()>>(
        mut tick: tokio::time::Interval,
        shutdown: &CancellationToken,
        mut body: impl FnMut() -> F,
    ) {
        // This observable consumer seam drives the future supplied by the attribute.
        // The real helper's Skip/cancellation policy is tested in the broker crate.
        while !shutdown.is_cancelled() {
            tick.tick().await;
            body().await;
        }
    }
}

#[derive(Default)]
struct Sweeps {
    completed: AtomicUsize,
    expiry_values: Mutex<Vec<Time>>,
}

mod abort_reaper {
    use super::{Arc, CancellationToken, Ordering, Sweeps, Time};

    /// The complete operation stays visible and can borrow owned task inputs.
    #[krabka_macros::reaper_task(shutdown_message = "txn idle-transaction reaper shutting down")]
    pub(super) async fn run(coord: Arc<Sweeps>, interval: Time, shutdown: CancellationToken) {
        if coord.completed.fetch_add(1, Ordering::SeqCst) == 1 {
            shutdown.cancel();
        } else {
            tokio::task::yield_now().await;
        }
    }
}

mod id_reaper {
    use super::{Arc, CancellationToken, Ordering, Sweeps, Time};

    #[krabka_macros::reaper_task(shutdown_message = "transactional-id expiry sweep shutting down")]
    pub(super) async fn run(
        coord: Arc<Sweeps>,
        interval: Time,
        expiration: Time,
        shutdown: CancellationToken,
    ) {
        coord.expiry_values.lock().unwrap().push(expiration);
        tokio::task::yield_now().await;
        coord.completed.fetch_add(1, Ordering::SeqCst);
        shutdown.cancel();
    }
}

struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

struct RecordedEvent {
    level: Level,
    target: &'static str,
    message: String,
}
type RecordedEvents = Arc<Mutex<Vec<RecordedEvent>>>;
struct ShutdownLog {
    events: RecordedEvents,
}

impl<S: Subscriber> Layer<S> for ShutdownLog {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let metadata = event.metadata();
        let mut message = Message(String::new());
        event.record(&mut message);
        self.events.lock().unwrap().push(RecordedEvent {
            level: *metadata.level(),
            target: metadata.target(),
            message: message.0,
        });
    }
}

struct ExpectedShutdown<'a> {
    event: &'a RecordedEvent,
    target: &'static str,
    message: &'static str,
}

fn is_send(_: impl Future<Output = ()> + Send) {}

#[tokio::test(flavor = "current_thread")]
async fn both_reapers_delegate_borrowing_bodies_and_log_at_the_original_targets() {
    let events = RecordedEvents::default();
    let subscriber = tracing_subscriber::registry().with(ShutdownLog {
        events: Arc::clone(&events),
    });
    let _guard = tracing::subscriber::set_default(subscriber);
    let coord = Arc::new(Sweeps::default());
    let interval = Time::from_millis(1);
    is_send(abort_reaper::run(
        Arc::clone(&coord),
        interval,
        CancellationToken::default(),
    ));
    abort_reaper::run(Arc::clone(&coord), interval, CancellationToken::default()).await;
    assert!(coord.completed.load(Ordering::SeqCst) == 2);
    let expiration = Time::from_millis(1_234);
    is_send(id_reaper::run(
        Arc::clone(&coord),
        interval,
        expiration,
        CancellationToken::default(),
    ));
    id_reaper::run(
        Arc::clone(&coord),
        interval,
        expiration,
        CancellationToken::default(),
    )
    .await;
    assert!(coord.completed.load(Ordering::SeqCst) == 3);
    assert!(*coord.expiry_values.lock().unwrap() == [expiration]);

    let shutdown = CancellationToken::default();
    shutdown.cancel();
    abort_reaper::run(Arc::clone(&coord), interval, shutdown).await;
    assert!(coord.completed.load(Ordering::SeqCst) == 3);
    let events = events.lock().unwrap();
    assert!(events.len() == 3);
    for expected in [
        ExpectedShutdown {
            event: &events[0],
            target: concat!(module_path!(), "::abort_reaper"),
            message: "txn idle-transaction reaper shutting down",
        },
        ExpectedShutdown {
            event: &events[1],
            target: concat!(module_path!(), "::id_reaper"),
            message: "transactional-id expiry sweep shutting down",
        },
        ExpectedShutdown {
            event: &events[2],
            target: concat!(module_path!(), "::abort_reaper"),
            message: "txn idle-transaction reaper shutting down",
        },
    ] {
        assert!(
            expected.event.level == Level::INFO
                && expected.event.target == expected.target
                && expected.event.message == expected.message
        );
    }
}
