//! Where the broker runs blocking work.
//!
//! The file IO of the log, the replay of an internal topic, and the calls into
//! the synchronous remote-storage SPIs all hold a thread for a while, and must
//! not stall the async poller. A native runtime takes them off it: a
//! multi-thread runtime runs them in place with
//! [`tokio::task::block_in_place`], which hands the worker's other tasks to
//! another thread, and a current-thread runtime runs them on tokio's blocking
//! pool, because `block_in_place` panics there.
//!
//! `wasm32-wasip1` has no threads. Tokio's blocking pool panics there with
//! "OS can't spawn worker thread", and the only runtime is current-thread. So
//! on that target the work runs inline on the runtime thread, and every other
//! task waits for the length of one call.
//!
//! [`run_blocking`], [`run_blocking_catching`] and [`spawn_blocking`] are the
//! three shapes the broker uses; nothing else in the crate calls
//! `block_in_place` or `spawn_blocking` on a runtime path. A test makes a
//! native thread run its blocking work inline too, with
//! [`inline_blocking_on_this_thread`], and so drives the WASI path on every
//! target.

use {
    std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        task::{Context, Poll},
    },
    tokio::{
        runtime::{Handle, RuntimeFlavor},
        task::JoinHandle,
    },
};

/// Blocking work that did not return its value.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockingError {
    /// The work panicked in place, on a multi-thread runtime.
    #[error("block_in_place panic")]
    PanickedInPlace,
    /// The work panicked inline, on the runtime thread.
    #[error("inline blocking panic")]
    PanickedInline,
    /// The blocking pool did not return the value: the work panicked there,
    /// or the runtime shut down before it ran.
    #[error(transparent)]
    Join(tokio::task::JoinError),
}

/// Where one piece of blocking work runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    /// In place on this worker, with `block_in_place`.
    InPlace,
    /// On tokio's blocking pool.
    Pool,
    /// On the calling thread, which stalls its runtime for the duration.
    Inline,
}

#[derive(Clone, Copy)]
enum InPlacePanicHandling {
    Propagate,
    ReturnError,
}

impl Placement {
    /// The placement of [`run_blocking`] on the current runtime.
    fn for_current_runtime() -> Self {
        if runs_inline() {
            Self::Inline
        } else if Handle::current().runtime_flavor() == RuntimeFlavor::MultiThread {
            Self::InPlace
        } else {
            Self::Pool
        }
    }
}

/// Run `work` off the async poller and wait for its value.
///
/// The work runs in place on a multi-thread runtime, on the blocking pool on
/// a current-thread runtime, and inline on `wasm32-wasip1`. A panic in place
/// unwinds into the caller, as `block_in_place` does.
///
/// # Errors
///
/// Returns [`BlockingError::Join`] when the blocking pool does not return the
/// value, and [`BlockingError::PanickedInline`] when the work panics inline.
pub(crate) async fn run_blocking<R>(
    work: impl FnOnce() -> R + Send + 'static,
) -> Result<R, BlockingError>
where
    R: Send + 'static,
{
    run_blocking_placed(work, InPlacePanicHandling::Propagate).await
}

/// [`run_blocking`], with a panic in place returned as an error rather than
/// unwound into the caller.
///
/// # Errors
///
/// Returns [`BlockingError::PanickedInPlace`] or
/// [`BlockingError::PanickedInline`] when the work panics on the calling
/// thread, and [`BlockingError::Join`] when the blocking pool does not return
/// the value.
pub(crate) async fn run_blocking_catching<R>(
    work: impl FnOnce() -> R + Send + 'static,
) -> Result<R, BlockingError>
where
    R: Send + 'static,
{
    run_blocking_placed(work, InPlacePanicHandling::ReturnError).await
}

async fn run_blocking_placed<R>(
    work: impl FnOnce() -> R + Send + 'static,
    in_place_panic: InPlacePanicHandling,
) -> Result<R, BlockingError>
where
    R: Send + 'static,
{
    match Placement::for_current_runtime() {
        Placement::InPlace => match in_place_panic {
            InPlacePanicHandling::Propagate => Ok(tokio::task::block_in_place(work)),
            InPlacePanicHandling::ReturnError => {
                catch_unwind(AssertUnwindSafe(|| tokio::task::block_in_place(work)))
                    .map_err(|_| BlockingError::PanickedInPlace)
            }
        },
        Placement::Pool => tokio::task::spawn_blocking(work)
            .await
            .map_err(BlockingError::Join),
        Placement::Inline => run_inline(work),
    }
}

/// Start `work` on tokio's blocking pool, or run it inline on
/// `wasm32-wasip1`, and return a handle that yields its value.
///
/// As with [`tokio::task::spawn_blocking`], the work starts when this
/// function is called, not when the handle is first polled; inline, the call
/// itself does the work, so a deadline put around the handle cannot cut it
/// short.
pub(crate) fn spawn_blocking<R>(work: impl FnOnce() -> R + Send + 'static) -> BlockingHandle<R>
where
    R: Send + 'static,
{
    if runs_inline() {
        BlockingHandle::Done(Some(run_inline(work)))
    } else {
        BlockingHandle::Spawned(tokio::task::spawn_blocking(work))
    }
}

/// Run `work` on the calling thread. A panic is the error the blocking pool
/// would have returned for it, so the caller sees the same failure whichever
/// way the work ran. (`wasm32-wasip1` aborts on a panic, so there the panic
/// never comes back at all.)
fn run_inline<R>(work: impl FnOnce() -> R) -> Result<R, BlockingError> {
    catch_unwind(AssertUnwindSafe(work)).map_err(|_| BlockingError::PanickedInline)
}

/// The value of work that [`spawn_blocking`] started.
#[must_use = "a blocking handle does nothing unless polled"]
pub(crate) enum BlockingHandle<R> {
    /// The work runs on the blocking pool.
    Spawned(JoinHandle<R>),
    /// The work ran inline; its outcome until it is taken.
    Done(Option<Result<R, BlockingError>>),
}

// The handle never pins what it holds: `Done` moves its outcome out whole,
// and a `JoinHandle` is `Unpin` whatever its output.
impl<R> Unpin for BlockingHandle<R> {}

impl<R> Future for BlockingHandle<R> {
    type Output = Result<R, BlockingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Spawned(handle) => Pin::new(handle).poll(cx).map_err(BlockingError::Join),
            Self::Done(outcome) => Poll::Ready(
                outcome
                    .take()
                    .expect("a blocking handle is not polled after it completes"),
            ),
        }
    }
}

/// Whether blocking work runs inline on this thread.
fn runs_inline() -> bool { cfg!(target_os = "wasi") || forced_inline::active() }

#[cfg(any(test, feature = "test-helpers"))]
mod forced_inline {
    use std::{cell::Cell, marker::PhantomData};

    thread_local! {
        static ACTIVE: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn active() -> bool { ACTIVE.with(Cell::get) }

    /// Makes the blocking work of this thread run inline until it drops.
    #[must_use = "blocking work runs inline only while the guard lives"]
    pub struct InlineBlockingGuard {
        /// The value the flag had before this guard set it, put back on drop.
        previous: bool,
        /// The flag belongs to one thread, so the guard must stay on it.
        _thread: PhantomData<*const ()>,
    }

    impl Drop for InlineBlockingGuard {
        fn drop(&mut self) { ACTIVE.with(|active| active.set(self.previous)); }
    }

    /// Run the blocking work of this thread inline, as `wasm32-wasip1` does,
    /// until the guard drops.
    ///
    /// A test drives the WASI path on a native target with it. Every task of
    /// a current-thread runtime runs on the thread that drives the runtime,
    /// so the guard covers the whole runtime when that thread holds it.
    pub fn inline_blocking_on_this_thread() -> InlineBlockingGuard {
        InlineBlockingGuard {
            previous: ACTIVE.with(|active| active.replace(true)),
            _thread: PhantomData,
        }
    }
}

#[cfg(not(any(test, feature = "test-helpers")))]
mod forced_inline {
    pub(super) const fn active() -> bool { false }
}

#[cfg(any(test, feature = "test-helpers"))]
pub use self::forced_inline::{InlineBlockingGuard, inline_blocking_on_this_thread};

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            thread::ThreadId,
        },
    };

    /// A runtime that counts the threads it starts. A current-thread runtime
    /// starts no worker, so there the count is the blocking-pool threads.
    fn counting_runtime(multi_thread: bool) -> (tokio::runtime::Runtime, Arc<AtomicUsize>) {
        let started = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&started);
        let mut builder = if multi_thread {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(2);
            builder
        } else {
            tokio::runtime::Builder::new_current_thread()
        };
        let runtime = builder
            .on_thread_start(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .build()
            .expect("build a runtime");
        (runtime, started)
    }

    /// Which thread ran the work, and where the seam put it.
    fn placement_of(multi_thread: bool, inline: bool) -> (Placement, ThreadId, ThreadId) {
        let (runtime, _) = counting_runtime(multi_thread);
        let _guard = inline.then(inline_blocking_on_this_thread);
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let placement = Placement::for_current_runtime();
            let worker = run_blocking(|| std::thread::current().id())
                .await
                .expect("the work returns");
            (placement, caller, worker)
        })
    }

    /// The seam keeps the two native placements, and the forced-inline guard
    /// takes the work onto the calling thread whatever the runtime.
    #[test]
    fn the_placement_follows_the_runtime_and_the_inline_guard() {
        let cases = [
            (true, false, Placement::InPlace, true),
            (false, false, Placement::Pool, false),
            (false, true, Placement::Inline, true),
            (true, true, Placement::Inline, true),
        ];
        for (multi_thread, inline, want, on_caller) in cases {
            let (placement, caller, worker) = placement_of(multi_thread, inline);
            check!(
                placement == want,
                "multi_thread={multi_thread} inline={inline}"
            );
            check!(
                (caller == worker) == on_caller,
                "multi_thread={multi_thread} inline={inline}: the work ran on {worker:?}, the \
                 caller is {caller:?}"
            );
        }
    }

    /// Blocking work that panics.
    fn panics() -> u8 { panic!("the blocking work panics") }

    /// Inline, no shape of the seam starts a blocking-pool thread, which is
    /// the property the WASI build needs: its runtime cannot start one.
    #[test]
    fn inline_work_starts_no_blocking_thread() {
        let (runtime, started) = counting_runtime(false);
        let guard = inline_blocking_on_this_thread();
        let values = runtime.block_on(async {
            let ran = run_blocking(|| 1).await.expect("run_blocking");
            let caught = run_blocking_catching(|| 2)
                .await
                .expect("run_blocking_catching");
            let spawned = spawn_blocking(|| 3).await.expect("spawn_blocking");
            [ran, caught, spawned]
        });
        check!(values == [1, 2, 3]);
        check!(started.load(Ordering::SeqCst) == 0);

        // The control: without the guard the same runtime does start one.
        drop(guard);
        runtime.block_on(async { spawn_blocking(|| ()).await.expect("spawn_blocking") });
        check!(started.load(Ordering::SeqCst) == 1);
    }

    /// Inline, a panic comes back as the error a pooled panic would have
    /// been, from every shape of the seam, instead of unwinding into the
    /// task that asked for the work.
    #[test]
    fn an_inline_panic_is_an_error_not_an_unwind() {
        let (runtime, _) = counting_runtime(false);
        let _guard = inline_blocking_on_this_thread();
        let outcomes = runtime.block_on(async {
            [
                run_blocking(panics).await,
                run_blocking_catching(panics).await,
                spawn_blocking(panics).await,
            ]
        });
        for outcome in outcomes {
            assert!(let Err(BlockingError::PanickedInline) = outcome);
        }
    }

    /// Natively a panic keeps the shape each placement always gave it: an
    /// error from the pool, and from `run_blocking_catching` in place an error
    /// with the message the callers already log.
    #[test]
    fn a_native_panic_keeps_its_shape() {
        let (current_thread, _) = counting_runtime(false);
        let pooled = current_thread.block_on(run_blocking(panics));
        check!(let Err(BlockingError::Join(_)) = pooled);

        let (multi_thread, _) = counting_runtime(true);
        let in_place = multi_thread.block_on(run_blocking_catching(panics));
        let error = in_place.expect_err("the panic is caught");
        check!(error.to_string() == "block_in_place panic");
    }

    /// The guard sets the flag for its own thread only, and restores what was
    /// there when it drops, so guards nest.
    #[test]
    fn the_inline_guard_is_per_thread_and_nests() {
        check!(!runs_inline());
        let outer = inline_blocking_on_this_thread();
        let inner = inline_blocking_on_this_thread();
        check!(runs_inline());
        check!(!std::thread::spawn(runs_inline).join().expect("join"));
        drop(inner);
        check!(runs_inline(), "the outer guard still holds");
        drop(outer);
        check!(!runs_inline());
    }
}
