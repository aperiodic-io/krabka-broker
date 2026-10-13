//! The reader pool's two guarantees: no more than `threads` reads run at once,
//! and a read that arrives with the pending queue already full is refused
//! rather than queued behind the running ones.

use {super::*, assert2::check};

/// The idle percentage is a ratio of two small integers, so every value these
/// tests expect is exactly representable. `check!` on `f64` equality is a
/// clippy error even then, so the comparison names its own tolerance.
fn is_percent(got: f64, want: f64) -> bool { (got - want).abs() < f64::EPSILON }

#[tokio::test]
async fn only_thread_count_reads_hold_a_permit_at_once() {
    let pool = ReaderPool::new(2, 100);

    let first = pool.acquire().await.expect("first permit");
    let second = pool.acquire().await.expect("second permit");
    check!(is_percent(pool.idle_percent(), 0.0));

    // A third read has to wait; it is queued, not refused.
    let waiting = pool.acquire();
    tokio::pin!(waiting);
    check!(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "the third read must wait while both slots are held"
    );
    check!(pool.queue_size() == 1);

    drop(first);
    let third = waiting.await.expect("third permit after a slot frees");
    check!(pool.queue_size() == 0);
    drop((second, third));
    check!(is_percent(pool.idle_percent(), 100.0));
}

#[tokio::test]
async fn a_read_past_the_pending_cap_is_refused_instead_of_queued() {
    // One slot, room for one waiter.
    let pool = ReaderPool::new(1, 1);
    let held = pool.acquire().await.expect("the one slot");

    let queued = pool.acquire();
    tokio::pin!(queued);
    check!(futures_util::poll!(queued.as_mut()).is_pending());
    check!(pool.queue_size() == 1);

    // The queue is full, so this one does not park -- it comes back refused.
    check!(pool.acquire().await.is_err());
    check!(pool.rejected() == 1);

    drop(held);
    queued.await.expect("the queued read still gets its slot");
}

#[tokio::test]
async fn an_unbounded_pool_never_refuses() {
    let pool = ReaderPool::unbounded();
    let mut permits = Vec::new();
    for _ in 0..64 {
        permits.push(pool.acquire().await.expect("unbounded pool never refuses"));
    }
    check!(pool.rejected() == 0);
    check!(pool.queue_size() == 0);
}

#[tokio::test]
async fn idle_percent_reports_the_share_of_free_slots() {
    let pool = ReaderPool::new(4, 100);
    check!(is_percent(pool.idle_percent(), 100.0));
    let one = pool.acquire().await.expect("permit");
    check!(is_percent(pool.idle_percent(), 75.0));
    let two = pool.acquire().await.expect("permit");
    check!(is_percent(pool.idle_percent(), 50.0));
    drop((one, two));
    check!(is_percent(pool.idle_percent(), 100.0));
}

#[tokio::test]
async fn a_zero_thread_pool_still_serves_one_read_at_a_time() {
    let pool = ReaderPool::new(0, 100);
    let permit = pool
        .acquire()
        .await
        .expect("a pool must never refuse everything");
    drop(permit);
}
