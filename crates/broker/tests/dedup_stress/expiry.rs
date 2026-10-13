//! An idle partition and the share of the broker budget it holds.
//!
//! The index measures expiry against the newest record timestamp it has seen,
//! and sweeps only when a record is inserted. A partition that stops taking
//! produces therefore never advances its own expiry reference and never
//! sweeps, so every entry it holds stays counted against the broker-wide
//! `dedup_max_entries` long after the window that could still match it has
//! passed. Another partition on the same broker is then starved of budget by
//! entries that no longer protect anything.

use {
    crate::{
        Target,
        support::{
            dedup::{Rec, create_dedup_topic, keyed_batch},
            start_configured,
        },
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::records::RecordBatch,
    std::time::{Duration, Instant},
};

/// A short window, so the idle partition's entries fall outside it quickly.
const WINDOW_MS: i64 = 2_000;

/// The whole broker may hold this many identities; one partition fills it.
const BROKER_LIMIT: u64 = 1_000;

/// Produce `batch` to `target` and return its error code.
async fn produce(client: &Client, target: Target<'_>, batch: RecordBatch) -> i16 {
    target.send(client, batch).await.error_code
}

/// A batch of records keyed `{prefix}-0` upwards, aged `age_ms` before now.
fn keys(prefix: &str, count: u64, age_ms: i64) -> RecordBatch {
    let records: Vec<Rec> = (0..count)
        .map(|n| Rec::key(&format!("{prefix}-{n}")).aged(age_ms))
        .collect();
    keyed_batch().records(&records).call()
}

async fn broker_entries(broker: &BrokerHandle) -> u64 {
    crate::whole(crate::support::dedup::broker_metric(broker, "dedup_broker_index_entries").await)
}

/// A partition that fills the broker budget and then goes idle must not keep
/// its entries counted against the budget once its window has passed: the
/// broker's periodic idle sweep reclaims them, so a second partition on the
/// same broker can index its own records. Without that sweep, the idle
/// partition starves the busy one forever.
///
/// Target: coordinator findings 2 and 3 (broker-clock expiry plus a periodic
/// bounded idle sweep). The test waits, by polling the broker gauge rather
/// than by a fixed sleep, for the idle partition's entries to be reclaimed,
/// then checks that the busy partition can index. Before the fix the gauge
/// never dropped and the poll timed out with the busy partition at zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_partition_releases_its_budget_once_its_window_passes() {
    let p = start_configured(|config| {
        config.dedup_max_entries = usize::try_from(BROKER_LIMIT).unwrap();
        config.dedup_max_entries_per_partition = usize::try_from(BROKER_LIMIT).unwrap();
    })
    .await;
    let topic_id = create_dedup_topic()
        .broker(&p.broker)
        .client(&p.client)
        .topic("idle")
        .window_ms(WINDOW_MS)
        .partitions(2)
        .call()
        .await;
    let idle = Target {
        topic: "idle",
        topic_id,
        partition: 0,
    };
    let busy = Target {
        topic: "idle",
        topic_id,
        partition: 1,
    };

    // Partition 0 fills the whole broker budget, then takes no more produces.
    assert!(produce(&p.client, idle, keys("idle", BROKER_LIMIT, 0)).await == 0);
    let full = broker_entries(&p.broker).await;

    // Wait, past the window, for the idle sweep to reclaim partition 0's
    // entries. The sweep runs about once a second, so poll the broker gauge
    // rather than guess a sleep. A broker without the sweep never drops below
    // the full budget and the poll times out.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut reclaimed = full;
    while Instant::now() < deadline {
        reclaimed = broker_entries(&p.broker).await;
        if reclaimed < BROKER_LIMIT / 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Partition 1 now produces 500 new identities. With partition 0's expired
    // entries reclaimed, the budget has room to index them.
    assert!(produce(&p.client, busy, keys("busy", 500, 0)).await == 0);
    let busy_indexed = busy.metric(&p.broker, "dedup_index_entries").await;

    eprintln!(
        "idle partition held {full} entries; after the sweep the broker held {reclaimed}; busy \
         partition indexed {busy_indexed} of 500"
    );
    assert!(full == BROKER_LIMIT);
    assert!(
        reclaimed < BROKER_LIMIT / 2,
        "idle partition never released its budget: {reclaimed}"
    );
    assert!(
        busy_indexed >= 500,
        "busy partition starved by an idle one: {busy_indexed}"
    );
    p.broker.shutdown().await;
}
