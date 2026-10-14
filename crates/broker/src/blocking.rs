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
//! [`run_blocking`] and [`spawn_blocking`] are the two shapes the broker uses
//! for work that may block; nothing else in the crate calls
//! `block_in_place` or `spawn_blocking` on a runtime path. A test makes a
//! native thread run its blocking work inline too, with
//! [`inline_blocking_on_this_thread`], and so drives the WASI path on every
//! target.
//!
//! [`run_page_cache_io`] is the exception: file work that only touches the
//! page cache runs on the calling task, because handing it off costs more
//! than the work. Work that may wait on a lock or the disk still goes
//! through [`run_blocking`].

use {
    std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError},
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
    match Placement::for_current_runtime() {
        Placement::InPlace => Ok(tokio::task::block_in_place(work)),
        Placement::Pool => tokio::task::spawn_blocking(work)
            .await
            .map_err(BlockingError::Join),
        Placement::Inline => run_inline(work),
    }
}

/// Run `work` on the value behind `lock`, on the calling task when that only
/// touches the page cache, and through [`run_blocking`] otherwise. A panic is
/// returned as an error rather than unwound into the caller.
///
/// This is for the log's hot paths: a produce append, a fetch read, and a
/// follower's replicated append. Most of those are a copy into or out of the
/// page cache that takes microseconds, and handing it to another thread, as
/// [`run_blocking`] does, costs a thread handoff and a futex wake, which is
/// more than the work. But one of them can also wait: on the lock, which other
/// paths hold across an fsync, or on the disk, for an append that is due a
/// flush or a segment roll, or a read the page cache does not hold. A wait on
/// a runtime worker stalls every task that worker would have run, so `work`
/// runs here only when the lock is free and `stays_in_cache` says, under that
/// lock, that it will not wait. The guard is held from that answer through
/// `work`, so no other thread can take the lock in between.
///
/// # Errors
///
/// Returns [`BlockingError::PanickedInline`] when the work panics, and
/// [`BlockingError::Join`] when the blocking pool does not return it.
pub(crate) async fn run_page_cache_io<T, R>(
    lock: &Arc<Mutex<T>>,
    stays_in_cache: impl FnOnce(&mut T) -> bool,
    work: impl FnOnce(&mut T) -> R + Send + 'static,
) -> Result<R, BlockingError>
where
    T: Send + 'static,
    R: Send + 'static,
{
    let work = match run_if_cached(lock, stays_in_cache, work) {
        Ok(ran) => return ran,
        Err(work) => work,
    };
    let lock = Arc::clone(lock);
    run_blocking(move || {
        let mut guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
        run_inline(|| work(&mut guard))
    })
    .await?
}

/// How long [`run_if_cached`] keeps trying a held lock before it gives up on
/// running inline. The hot paths hold the log lock for a page-cache copy, a
/// few microseconds; a holder still there after this is likely fsyncing or
/// reading the disk.
const LOCK_SPIN: std::time::Duration = std::time::Duration::from_micros(20);

/// Runs `work` here under `lock` when the lock comes free within
/// [`LOCK_SPIN`] and `stays_in_cache` agrees, and hands `work` back
/// otherwise. The guard never outlives this call, which keeps it out of the
/// caller's future.
fn run_if_cached<T, R, W: FnOnce(&mut T) -> R>(
    lock: &Mutex<T>,
    stays_in_cache: impl FnOnce(&mut T) -> bool,
    work: W,
) -> Result<Result<R, BlockingError>, W> {
    let Some(mut guard) = lock_within(lock, LOCK_SPIN) else {
        return Err(work);
    };
    if stays_in_cache(&mut guard) {
        Ok(run_inline(|| work(&mut guard)))
    } else {
        Err(work)
    }
}

/// `lock`'s guard, if it comes free within `spin`. A poisoned lock is
/// recovered, as the log's own lock helper does.
fn lock_within<T>(lock: &Mutex<T>, spin: std::time::Duration) -> Option<MutexGuard<'_, T>> {
    let started = std::time::Instant::now();
    loop {
        match lock.try_lock() {
            Ok(guard) => return Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => return Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) if started.elapsed() < spin => std::hint::spin_loop(),
            Err(TryLockError::WouldBlock) => return None,
        }
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
    fn counting_runtime(multi_thread: bool) -> CountingRuntime {
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
        CountingRuntime { runtime, started }
    }

    /// A runtime, and the number of threads it has started.
    struct CountingRuntime {
        runtime: tokio::runtime::Runtime,
        started: Arc<AtomicUsize>,
    }

    /// Which thread ran the work, and where the seam put it.
    fn placement_of(multi_thread: bool, inline: bool) -> ObservedPlacement {
        let CountingRuntime { runtime, .. } = counting_runtime(multi_thread);
        let _guard = inline.then(inline_blocking_on_this_thread);
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let placement = Placement::for_current_runtime();
            let worker = run_blocking(|| std::thread::current().id())
                .await
                .expect("the work returns");
            ObservedPlacement {
                placement,
                caller,
                worker,
            }
        })
    }

    /// Where the seam put the work, and the thread that asked and the thread
    /// that ran it.
    struct ObservedPlacement {
        placement: Placement,
        caller: ThreadId,
        worker: ThreadId,
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
            let ObservedPlacement {
                placement,
                caller,
                worker,
            } = placement_of(multi_thread, inline);
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
        let CountingRuntime { runtime, started } = counting_runtime(false);
        let guard = inline_blocking_on_this_thread();
        let values = runtime.block_on(async {
            let ran = run_blocking(|| 1).await.expect("run_blocking");
            let caught = run_page_cache_io(&Arc::new(Mutex::new(2)), |_| true, |value| *value)
                .await
                .expect("run_page_cache_io");
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
        let CountingRuntime { runtime, .. } = counting_runtime(false);
        let _guard = inline_blocking_on_this_thread();
        let outcomes = runtime.block_on(async {
            [
                run_blocking(panics).await,
                run_page_cache_io(&Arc::new(Mutex::new(())), |()| true, |()| panics()).await,
                spawn_blocking(panics).await,
            ]
        });
        for outcome in outcomes {
            assert!(let Err(BlockingError::PanickedInline) = outcome);
        }
    }

    /// Natively a panic keeps the shape each placement always gave it: an
    /// error from the pool, and from `run_page_cache_io` on the calling task
    /// an error with the message the callers log.
    #[test]
    fn a_native_panic_keeps_its_shape() {
        let CountingRuntime {
            runtime: current_thread,
            ..
        } = counting_runtime(false);
        let pooled = current_thread.block_on(run_blocking(panics));
        check!(let Err(BlockingError::Join(_)) = pooled);

        let CountingRuntime {
            runtime: multi_thread,
            ..
        } = counting_runtime(true);
        let in_place = multi_thread.block_on(run_page_cache_io(
            &Arc::new(Mutex::new(())),
            |()| true,
            |()| panics(),
        ));
        let error = in_place.expect_err("the panic is caught");
        check!(error.to_string() == "inline blocking panic");
    }

    /// Page-cache work runs on the calling task only when its lock is free
    /// and it says it will not wait. Otherwise it takes the blocking path,
    /// which on a current-thread runtime is a pool thread.
    #[test]
    fn page_cache_io_runs_inline_only_on_a_free_lock_and_work_that_will_not_wait() {
        let CountingRuntime { runtime, .. } = counting_runtime(false);
        let caller = std::thread::current().id();
        let lock = Arc::new(Mutex::new(()));
        let ran_on = |stays_in_cache: bool| {
            runtime
                .block_on(run_page_cache_io(
                    &lock,
                    move |()| stays_in_cache,
                    |()| std::thread::current().id(),
                ))
                .expect("the work returns")
        };
        check!(ran_on(true) == caller, "a free lock and page-cache work");
        check!(ran_on(false) != caller, "work that may wait");

        let (holding, held) = std::sync::mpsc::channel::<()>();
        let (taken, released) = std::sync::mpsc::channel::<()>();
        let holder_lock = Arc::clone(&lock);
        let contended = std::thread::scope(|scope| {
            scope.spawn(move || {
                let _held = holder_lock.lock().expect("lock");
                holding.send(()).expect("the test waits");
                let _ = released.recv();
            });
            held.recv().expect("the holder takes the lock");
            let outcome = runtime.block_on(async {
                let work = run_page_cache_io(&lock, |()| true, |()| std::thread::current().id());
                // `join!` polls the work first: it finds the lock held and
                // goes to the pool before the holder lets go.
                let (worker, ()) = tokio::join!(work, async { drop(taken) });
                worker
            });
            outcome.expect("the work returns")
        });
        check!(contended != caller, "a held lock");
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
