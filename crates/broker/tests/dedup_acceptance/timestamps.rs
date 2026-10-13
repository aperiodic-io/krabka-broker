//! What the window measures: the distance between two records' timestamps,
//! which is `CreateTime` clamped to the broker clock, or the append time on a
//! `LogAppendTime` topic.

use {
    crate::{
        harness::{appended_at, recs, stored_key, topic},
        support::{self, dedup::Rec},
    },
    assert2::{assert, check},
    krabka_broker::codes,
    krabka_protocol::records::TimestampType,
    std::time::Duration,
};

const MINUTE_MS: i64 = 60_000;

/// Requests in order, each one the ages of its records in milliseconds.
type Requests = Vec<Vec<i64>>;

/// A duplicate that arrives after the window has passed is kept, and from
/// then on it is the copy that later duplicates are measured against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_after_the_window_is_admitted() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("short-window")
        .window_ms(Some(500))
        .call()
        .await;

    let first = t.send(&recs(&["a"])).await;
    let inside = t.send(&recs(&["a"])).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = t.send(&recs(&["a"])).await;
    let again = t.send(&recs(&["a"])).await;
    let contents = t.contents().await;

    assert!(
        [first, inside, after, again]
            == [
                appended_at(0),
                appended_at(1),
                appended_at(2),
                appended_at(3)
            ]
    );
    assert!(contents.records == [stored_key(0, "a"), stored_key(2, "a")]);
    assert!(contents.high_watermark == 4);
    p.broker.shutdown().await;
}

/// Two records are duplicates only when their timestamps are at most the
/// window apart, whenever they arrive: records produced back to back whose
/// `CreateTime` differs by more than the window are both kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_further_apart_than_the_window_are_not_duplicates() {
    let p = support::start().await;
    let two_minutes = 2 * MINUTE_MS;
    let half_window = MINUTE_MS / 2;
    // Each case sends its requests in order, each request a list of the ages
    // of records keyed `a`, and names the offsets kept.
    let cases: [(&str, Requests, &[i64]); 5] = [
        (
            "an old copy, then a current one",
            vec![vec![two_minutes], vec![0]],
            &[0, 1],
        ),
        (
            "a current copy, then an old one",
            vec![vec![0], vec![two_minutes]],
            &[0, 1],
        ),
        ("both in one request", vec![vec![two_minutes, 0]], &[0, 1]),
        ("half a window apart", vec![vec![half_window], vec![0]], &[
            0,
        ]),
        (
            "an old copy, a current one, and a current duplicate",
            vec![vec![two_minutes], vec![0], vec![0]],
            &[0, 1],
        ),
    ];
    for (index, (name, requests, kept)) in cases.into_iter().enumerate() {
        let topic_name = format!("apart-{index}");
        let t = topic().p(&p).name(&topic_name).call().await;
        for ages in &requests {
            let records: Vec<Rec> = ages.iter().map(|age| Rec::key("a").aged(*age)).collect();
            check!(t.send(&records).await.error_code == 0, "{name}");
        }
        let offsets: Vec<i64> = t
            .contents()
            .await
            .records
            .iter()
            .map(|record| record.offset)
            .collect();
        check!(offsets == kept, "{name}");
    }
    p.broker.shutdown().await;
}

/// A record dated ahead of the broker clock is clamped to it before it
/// reaches the index. Unclamped, it would move the index's notion of now
/// half an hour forward, the sweep would expire every later entry as soon as
/// it was written, and duplicates of later records would be kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_future_dated_record_is_clamped_and_does_not_disable_dedup() {
    let p = support::start().await;
    let t = topic().p(&p).name("future").call().await;

    // Inside `message.timestamp.after.max.ms`, which is an hour by default.
    let future = t.send(&[Rec::key("x").aged(-30 * MINUTE_MS)]).await;
    let y = t.send(&recs(&["y"])).await;
    // Each new identity advances the expiry sweep over the index, so these
    // give an unclamped index the chance to drop `y`.
    for filler in 0..16 {
        let row = t.send(&recs(&[&format!("f{filler}")])).await;
        check!(row.error_code == 0);
    }
    let y_again = t.send(&recs(&["y"])).await;
    let x_now = t.send(&recs(&["x"])).await;
    let contents = t.contents().await;

    assert!([future.error_code, y.error_code] == [0, 0]);
    assert!([y_again, x_now] == [appended_at(18), appended_at(19)]);
    assert!(contents.records.len() == 18);
    assert!(contents.high_watermark == 20);
    assert!(t.metric("dedup_records_skew_clamped_total").await == 1);
    assert!(t.metric("dedup_records_dropped_total").await == 2);
    p.broker.shutdown().await;
}

/// On a `LogAppendTime` topic the window measures the append time, so a
/// record whose `CreateTime` is hours old is still a duplicate of a recent
/// one. The same requests on a `CreateTime` topic keep both records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_append_time_topic_measures_the_append_time() {
    let p = support::start().await;
    let two_hours = 120 * MINUTE_MS;
    // Each case names the offsets kept and how many records were dropped.
    let cases: [(TimestampType, &str, &[i64], u64); 2] = [
        (TimestampType::CreateTime, "CreateTime", &[0, 1, 2, 3], 0),
        (TimestampType::LogAppendTime, "LogAppendTime", &[0, 2], 2),
    ];
    for (stamping, timestamp_type, kept, dropped) in cases {
        let topic_name = format!("timestamps-{timestamp_type}");
        let t = topic()
            .p(&p)
            .name(&topic_name)
            .extra_configs(&[("message.timestamp.type", timestamp_type)])
            .call()
            .await;
        // The append time is the log's, so the override must reach the
        // partition's own `LogConfig` before the first produce.
        t.await_log_config(|config| config.message_timestamp_type == stamping)
            .await;

        let mut rows = Vec::new();
        for record in [
            Rec::key("a"),
            Rec::key("a").aged(two_hours),
            Rec::key("b").aged(two_hours),
            Rec::key("b"),
        ] {
            let row = t.send(&[record]).await;
            rows.push((row.error_code, row.base_offset));
        }
        let offsets: Vec<i64> = t
            .contents()
            .await
            .records
            .iter()
            .map(|record| record.offset)
            .collect();

        check!(rows == [(0, 0), (0, 1), (0, 2), (0, 3)], "{timestamp_type}");
        check!(offsets == kept, "{timestamp_type}");
        check!(
            t.metric("dedup_records_dropped_total").await == dropped,
            "{timestamp_type}"
        );
    }
    p.broker.shutdown().await;
}

/// A batch that `message.timestamp.before.max.ms` refuses appends nothing and
/// indexes nothing: its other records are new to the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_refused_for_its_timestamps_indexes_nothing() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("timestamp-refused")
        .extra_configs(&[("message.timestamp.before.max.ms", "3600000")])
        .call()
        .await;

    let refused = t
        .send(&[Rec::key("a"), Rec::key("b").aged(120 * MINUTE_MS)])
        .await;
    let retry = t.send(&recs(&["a", "b"])).await;
    let contents = t.contents().await;

    assert!(refused.error_code == codes::INVALID_TIMESTAMP);
    assert!(retry == appended_at(0));
    assert!(contents.records == [stored_key(0, "a"), stored_key(1, "b")]);
    p.broker.shutdown().await;
}
