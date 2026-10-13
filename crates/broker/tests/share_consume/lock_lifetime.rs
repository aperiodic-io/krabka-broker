//! The lifetime of the acquisition lock a `ShareFetch` takes. The background
//! sweep re-delivers a lock that expires unacknowledged, a record that
//! exhausts `max_delivery_attempts` is archived rather than re-delivered, and
//! a renew-ack extends the deadline so the sweep leaves the record acquired.
//! The last test is the un-renewed control for the renew case.

use {
    crate::{
        NONE,
        harness::initialize_consumption,
        share_rpc::{
            acquired_count, fetch_empty, fetch_redelivered, fetch_until_acquired, share_fetch,
            share_renew,
        },
        support::share::{
            ConsumptionSetup, ShareFetchSetup, ShareRecordCount, ShareSessionEpoch,
            ShareSessionSetup, permitted_topic_fixture,
        },
    },
    assert2::assert,
    std::time::{Duration, Instant},
};

/// The background sweep reverts an acquired-but-unacknowledged lock that
/// expires, so the next fetch re-delivers at an incremented count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_timeout_redelivers() {
    let LockFixture {
        permit: _permit,
        broker,
        client,
        dir: _dir,
        tid,
        member,
    } = Box::pin(lock_fixture(Duration::from_millis(200), None)).await;
    let session = ShareSessionSetup::joined(&member, tid);

    // Fetch but DO NOT acknowledge.
    acquire_initial_record(&client, &member, tid).await;

    // Wait until the lock expires and the background sweeper reverts the
    // record to Available (acquired-batch count drops to 0).
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Next fetch (epoch 1) re-acquires the same offset at delivery_count 2.
    fetch_redelivered(&client, ShareFetchSetup::from(session)).await;
}

/// The broker archives a record that exhausts `max_delivery_attempts` without
/// an Accept (poison pill). Later fetches acquire nothing and the SPSO advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_limit_archives() {
    let LockFixture {
        permit: _permit,
        broker,
        client,
        dir: _dir,
        tid,
        member,
    } = Box::pin(lock_fixture(Duration::from_millis(150), Some(2))).await;
    let session = ShareSessionSetup::joined(&member, tid);

    // Delivery 1 (no ack).
    let row1 = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
    assert!(row1.acquired_records[0].delivery_count == 1);

    // Wait until the lock expires and the sweeper reverts the record to Available
    // (acquired-batch count drops to 0), then re-fetch for delivery 2.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Delivery 2 (no ack).
    let row2 = share_fetch(&client, ShareFetchSetup::from(session)).await;
    assert!(
        acquired_count(&row2) == 1 && row2.acquired_records[0].delivery_count == 2,
        "second delivery must be count 2, got {:?}",
        row2.acquired_records
    );

    // Wait until that lock expires too — the sweeper reverts the record back to
    // Available (delivery_count=2, which equals max_delivery_attempts). The
    // archiving (dcc increment) happens during the next acquire call when the
    // broker detects the poison pill.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Subsequent fetch: the acquire path detects delivery_count >= max_attempts
    // and archives the record — SPSO advances, nothing is returned.
    fetch_empty(&client, session.fetch_at(ShareSessionEpoch(2))).await;
}

/// F1 (renew): a renew-ack extends the acquisition lock. The broker does NOT
/// re-acquire a record that it would otherwise re-deliver after the lock expires.
///
/// Config: `record_lock_duration = 500ms` (sweeper ticks at 250ms). Acquire
/// offset 0 with a 500ms lock, then send a renew-ack about 200ms in. The
/// renew-ack resets the deadline to renew-time + 500ms (≈ T0+700ms). Then check
/// at about T0+600ms, which is PAST the original 500ms deadline but BEFORE the
/// renewed 700ms deadline. The sweeper would already have swept and re-delivered
/// an un-renewed lock. The renew kept the record Acquired, so the fetch acquires
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_extends_lock_not_redelivered() {
    let LockFixture {
        permit: _permit,
        broker: _broker,
        client,
        dir: _dir,
        tid,
        member,
    } = Box::pin(lock_fixture(Duration::from_millis(500), None)).await;
    // Acquire offset 0 (lock 500ms, delivery_count 1). Epoch is now 1.
    let (session, acquire_at) = acquire_initial_session(&client, &member, tid).await;

    // Intentional calibrated timing: renew ~200ms in (before the 500ms lock
    // expires) to reset the deadline to renew-time + 500ms ≈ T0+700ms. Epoch
    // is now 2. This sleep proves renew timing; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let renew = share_renew()
        .client(&client)
        .session(ShareSessionSetup::joined(&member, tid))
        .call()
        .await;
    assert!(
        renew.error_code == NONE,
        "renew must succeed for an acquired offset, got {}",
        renew.error_code
    );

    // Intentional calibrated timing: wait until ~600ms after the ORIGINAL
    // acquire — past the original 500ms deadline (un-renewed lock would have
    // been swept) but before the renewed ~700ms deadline. The remaining sleep
    // is computed from the real acquire instant so scheduling jitter doesn't
    // overshoot the renewed window. This proves the renew suppressed redelivery;
    // it is NOT a flaky state-guess.
    let target = acquire_at + Duration::from_millis(600);
    if let Some(rem) = target.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(rem).await;
    }
    fetch_empty(&client, session.fetch_at(ShareSessionEpoch(2))).await;
}

/// F1 (control): the SAME timing WITHOUT a renew re-acquires the offset after
/// the lock expires, at `delivery_count` 2. This proves the renew above
/// suppressed the redelivery, and that slack in the timing did not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_renew_redelivers_after_lock_expiry() {
    let fixture = Box::pin(lock_fixture(Duration::from_millis(500), None)).await;
    let (client, tid, member) = (&fixture.client, fixture.tid, &fixture.member);
    let (session, _) = acquire_initial_session(client, member, tid).await;

    // Intentional calibrated timing: no renew — wait 800ms (well past the 500ms
    // lock + a sweeper tick) so the record is reverted to Available and
    // re-delivered. This sleep mirrors the renew test's timing to prove that
    // WITHOUT a renew the lock IS swept; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(800)).await;
    fetch_redelivered(client, ShareFetchSetup::from(session)).await;
}

/// A joined share-group member and the owners that keep its broker running.
struct LockFixture {
    permit: tokio::sync::OwnedSemaphorePermit,
    broker: krabka_broker::BrokerHandle,
    client: std::sync::Arc<krabka_client_core::Client>,
    dir: tempfile::TempDir,
    tid: uuid::Uuid,
    member: String,
}

/// Hold the broker permit, materialize t, and join its single-record share group.
async fn lock_fixture(lock_duration: Duration, max_delivery_attempts: Option<i16>) -> LockFixture {
    let (permit, broker, client, dir, tid) = Box::pin(
        permitted_topic_fixture()
            .customize(Box::new(move |config| {
                config.share_group.record_lock_duration = lock_duration;
                if let Some(attempts) = max_delivery_attempts {
                    config.share_group.max_delivery_attempts = attempts;
                }
            }))
            .call(),
    )
    .await;
    let (member, _) = initialize_consumption(&broker, &client, ConsumptionSetup {
        topic_id: tid,
        count: ShareRecordCount(1),
    })
    .await;

    LockFixture {
        permit,
        broker,
        client,
        dir,
        tid,
        member,
    }
}

async fn acquire_initial_record(
    client: &krabka_client_core::Client,
    member: &str,
    tid: uuid::Uuid,
) {
    let row = fetch_until_acquired(client, ShareSessionSetup::opening(member, tid)).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);
}

async fn acquire_initial_session<'a>(
    client: &krabka_client_core::Client,
    member: &'a str,
    tid: uuid::Uuid,
) -> (ShareSessionSetup<'a>, Instant) {
    let session = ShareSessionSetup::joined(member, tid);
    let acquire_at = Instant::now();
    acquire_initial_record(client, member, tid).await;
    (session, acquire_at)
}
