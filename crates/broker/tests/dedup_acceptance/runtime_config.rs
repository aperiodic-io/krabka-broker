//! Config changes on a live topic. The index is a function of the log and
//! the config together, so each change that the old index cannot serve
//! rebuilds it from the last window of the log under the new config.

use {
    crate::{
        harness::{
            Topic, appended_at, broker_gauge, rec, recs, settled_broker_gauge, stored, stored_key,
            topic,
        },
        support::{
            self,
            dedup::{DEDUP_HEADER, Rec},
        },
    },
    assert2::{assert, check},
};

const WINDOW: &str = "dedup.window.ms";
const HEADER: &str = "dedup.key.header";

/// Enabling deduplication on a topic that already holds records indexes the
/// records of the last window: a duplicate of one of them is dropped by the
/// first produce after the change. A record older than the window is not a
/// duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enabling_dedup_indexes_the_records_already_in_the_window() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("enable-late")
        .window_ms(None)
        .call()
        .await;

    let old = t.send(&[Rec::key("old").aged(5 * 60_000)]).await;
    let before = t.send(&recs(&["a", "b", "a"])).await;
    t.set_config(WINDOW, "60000").await;
    let after = t.send(&recs(&["a", "c", "b", "old"])).await;
    let contents = t.contents().await;

    assert!([old, before, after] == [appended_at(0), appended_at(1), appended_at(4)]);
    assert!(
        contents.records
            == [
                stored_key(0, "old"),
                stored_key(1, "a"),
                stored_key(2, "b"),
                stored_key(3, "a"),
                stored_key(5, "c"),
                stored_key(7, "old"),
            ]
    );
    assert!(t.metric("dedup_records_dropped_total").await == 2);
    assert!(t.metric("dedup_index_entries").await == 4);
    assert!(t.metric("dedup_index_rebuilds_total").await >= 1);
    p.broker.shutdown().await;
}

/// Switching the identity rule rebuilds the index from the log under the new
/// rule, in both directions: the records already in the log count under the
/// header rule once it is set, and under the key rule once it is removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switching_the_identity_rule_reindexes_the_log_under_the_new_rule() {
    let p = support::start().await;
    let t = topic().p(&p).name("switch-rule").call().await;
    let tagged = |key: &str, id: &str, value: &str| Rec::key(key).dedup_id(id).value(value);

    let key_mode = t
        .send(&[tagged("k1", "h1", "first"), tagged("k2", "h2", "second")])
        .await;
    let rebuilds_before = t.metric("dedup_index_rebuilds_total").await;
    t.set_config(HEADER, DEDUP_HEADER).await;
    let header_mode = t
        .send(&[
            tagged("k9", "h1", "same-header"),
            tagged("k1", "h3", "same-key"),
        ])
        .await;
    let rebuilds_after_header = t.metric("dedup_index_rebuilds_total").await;
    t.delete_config(HEADER).await;
    let back_to_keys = t
        .send(&[
            tagged("k1", "h4", "key-again"),
            tagged("k9", "h5", "new-key"),
        ])
        .await;
    let contents = t.contents().await;

    assert!(
        [key_mode, header_mode, back_to_keys] == [appended_at(0), appended_at(2), appended_at(4)]
    );
    assert!(
        contents.records
            == [
                stored(0, "k1", "first"),
                stored(1, "k2", "second"),
                stored(3, "k1", "same-key"),
                stored(5, "k9", "new-key"),
            ]
    );
    assert!(rebuilds_after_header > rebuilds_before);
    p.broker.shutdown().await;
}

/// A wider window takes effect at once: the rebuild re-reads the records
/// that the narrower window had already expired from the index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wider_window_takes_effect_at_once() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("widen")
        .window_ms(Some(10_000))
        .call()
        .await;
    let aged = |key: &str| Rec::key(key).aged(30_000);

    let old = t.send(&[aged("x1"), aged("x2")]).await;
    // A current record moves the index's clock past both, so the sweep can
    // expire them under the 10 s window.
    let current = t.send(&recs(&["y"])).await;
    let outside = t.send(&recs(&["x1"])).await;
    t.set_config(WINDOW, "60000").await;
    let inside = t.send(&recs(&["x2", "y"])).await;
    let contents = t.contents().await;

    assert!(
        [old, current, outside, inside]
            == [
                appended_at(0),
                appended_at(2),
                appended_at(3),
                appended_at(4)
            ]
    );
    assert!(
        contents.records
            == [
                stored_key(0, "x1"),
                stored_key(1, "x2"),
                stored_key(2, "y"),
                stored_key(3, "x1"),
            ]
    );
    assert!(contents.high_watermark == 6);
    p.broker.shutdown().await;
}

/// A narrower window applies to the next comparison, also against the
/// entries the index already holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_narrower_window_takes_effect_at_once() {
    let p = support::start().await;
    let t = topic().p(&p).name("narrow").call().await;

    let old = t.send(&[Rec::key("a").aged(30_000)]).await;
    let inside = t.send(&recs(&["a"])).await;
    t.set_config(WINDOW, "10000").await;
    let outside = t
        .send(&[Rec::key("a"), Rec::key("b").aged(30_000), Rec::key("b")])
        .await;
    let contents = t.contents().await;

    assert!([old, inside, outside] == [appended_at(0), appended_at(1), appended_at(2)]);
    assert!(
        contents.records
            == [
                stored_key(0, "a"),
                stored_key(2, "a"),
                stored_key(3, "b"),
                stored_key(4, "b"),
            ]
    );
    p.broker.shutdown().await;
}

/// Turning deduplication off admits duplicates again and frees the index:
/// the partition's gauge goes to zero and the broker-wide gauge drops by what
/// the partition held, while another topic's index stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabling_dedup_admits_duplicates_and_frees_the_index() {
    for (how, disable) in [("set to -1", Disable::SetOff), ("deleted", Disable::Delete)] {
        let p = support::start().await;
        let t = topic().p(&p).name("disabled").call().await;
        let other = topic().p(&p).name("other").call().await;

        check!(
            t.send(&recs(&["a", "b", "c"])).await == appended_at(0),
            "{how}"
        );
        check!(
            other.send(&recs(&["x", "y"])).await == appended_at(0),
            "{how}"
        );
        let before = (
            t.metric("dedup_index_entries").await,
            broker_gauge(&p, "dedup_broker_index_entries").await,
        );
        disable.apply(t).await;
        let duplicate = t.send(&[rec("a", "again")]).await;
        let partition_gauge = t.settled_metric("dedup_index_entries", 0).await;
        let broker_gauge = settled_broker_gauge(&p, "dedup_broker_index_entries", 2).await;
        let contents = t.contents().await;

        check!(before == (3, 5), "{how}");
        check!(duplicate == appended_at(3), "{how}");
        check!((partition_gauge, broker_gauge) == (0, 2), "{how}");
        check!(
            contents.records
                == [
                    stored_key(0, "a"),
                    stored_key(1, "b"),
                    stored_key(2, "c"),
                    stored(3, "a", "again"),
                ],
            "{how}"
        );
        check!(other.metric("dedup_index_entries").await == 2, "{how}");
        p.broker.shutdown().await;
    }
}

/// The two ways an operator turns deduplication off.
#[derive(Clone, Copy)]
enum Disable {
    SetOff,
    Delete,
}

impl Disable {
    async fn apply(self, t: Topic<'_>) {
        match self {
            Self::SetOff => t.set_config(WINDOW, "-1").await,
            Self::Delete => t.delete_config(WINDOW).await,
        }
    }
}
