//! Unit tests for the partition's high-watermark and ISR bookkeeping.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_log::Offset;
use tokio::sync::Notify;

use crate::partition::test_support::{append_records, test_partition};

async fn install_local_isr(partition: &crate::partition::Partition) {
    let local = krabka_audit::NodeId(1);
    partition.install_isr(&[local], &[local], local).await;
}

#[tokio::test]
async fn high_watermark_reads_cached_value() {
    let (p, _dir) = test_partition(Arc::new(Notify::new()));
    p.replica_state.lock().await.hw = Offset(42);
    assert!(p.high_watermark().await == 42);
}

#[tokio::test]
async fn install_isr_populates_replica_state() {
    let (p, _dir) = test_partition(Arc::new(Notify::new()));
    crate::partition::test_support::install_three_replica_isr(&p).await;
    let st = p.replica_state.lock().await;
    check!(
        st.isr
            == [
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3)
            ]
            .into_iter()
            .collect()
    );
    check!(st.per_follower.get(&krabka_audit::NodeId(2)).map(|f| f.leo) == Some(Offset(0)));
}

#[tokio::test]
async fn install_isr_advances_and_notifies_only_synced_storage() {
    for diskless in [false, true] {
        let hw_advance_notify = Arc::new(Notify::new());
        let (mut partition, _dir) = test_partition(hw_advance_notify.clone());
        partition.diskless = diskless;
        append_records(
            &partition,
            crate::test_support::PartitionRecordsSetup {
                count: crate::test_support::RecordCount(3),
                ..Default::default()
            },
        );
        assert!(partition.high_watermark().await == 0);

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );
        install_local_isr(&partition).await;
        let expected_watermark = if diskless { 0 } else { 3 };
        assert!(
            partition.high_watermark().await == expected_watermark,
            "diskless={diskless}"
        );
        assert!(
            futures_util::poll!(&mut waiter).is_ready() == !diskless,
            "only synced storage may advance HW and release waiters; diskless={diskless}"
        );
    }
}

#[tokio::test]
async fn install_isr_same_high_watermark_does_not_notify() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _td) = test_partition(hw_advance_notify.clone());
    append_records(
        &p,
        crate::test_support::PartitionRecordsSetup {
            count: crate::test_support::RecordCount(2),
            ..Default::default()
        },
    );
    install_local_isr(&p).await;
    assert!(p.high_watermark().await == 2);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    install_local_isr(&p).await;

    assert!(p.high_watermark().await == 2);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "unchanged HW must not wake waiters"
    );
}

#[tokio::test]
async fn await_hw_returns_immediately_if_already_satisfied() {
    let (p, _dir) = test_partition(Arc::new(Notify::new()));
    p.replica_state.lock().await.hw = Offset(100);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    p.await_hw_at_least(Offset(50), deadline)
        .await
        .expect("immediate");
}

#[tokio::test]
async fn await_hw_returns_timeout_when_unreached() {
    let (p, _dir) = test_partition(Arc::new(Notify::new()));
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
    let result = p.await_hw_at_least(Offset(100), deadline).await;
    assert!(matches!(result, Err(crate::partition::HwTimeout)));
}

#[tokio::test]
async fn set_follower_hw_clamps_advances_and_notifies() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _dir) = test_partition(hw_advance_notify.clone());

    // Append a 3-record batch so log_end_offset() == 3.
    append_records(
        &p,
        crate::test_support::PartitionRecordsSetup {
            count: crate::test_support::RecordCount(3),
            ..Default::default()
        },
    );
    assert!(p.log_end_offset() == 3);

    // reported_hw below log_end: stored verbatim, notify fires.
    // A `Notified` future does not register with the `Notify` until it is
    // first polled, and `notify_waiters()` only wakes already-registered
    // waiters — so poll once (Pending) to register BEFORE advancing HW.
    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );
    p.set_follower_hw(Offset(2)).await;
    assert!(p.high_watermark().await == 2);
    assert!(
        futures_util::poll!(&mut waiter).is_ready(),
        "notify should fire when HW advances"
    );

    // reported_hw above log_end: clamped to log_end (3).
    p.set_follower_hw(Offset(100)).await;
    assert!(p.high_watermark().await == 3);

    // reported_hw below current HW: no regression.
    p.set_follower_hw(Offset(1)).await;
    assert!(p.high_watermark().await == 3);
}

#[tokio::test]
async fn set_follower_hw_same_high_watermark_does_not_notify() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _td) = test_partition(hw_advance_notify.clone());
    assert!(p.high_watermark().await == 0);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    p.set_follower_hw(Offset(0)).await;

    assert!(p.high_watermark().await == 0);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "unchanged HW must not wake waiters"
    );
}

#[tokio::test]
async fn await_hw_wakes_on_advance() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _dir) = test_partition(hw_advance_notify.clone());
    let replica_state = p.replica_state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        replica_state.lock().await.hw = Offset(100);
        hw_advance_notify.notify_waiters();
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    p.await_hw_at_least(Offset(50), deadline)
        .await
        .expect("woke on advance");
}
