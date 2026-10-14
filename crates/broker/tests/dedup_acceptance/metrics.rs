//! The KFC-10 series, read back after a scripted produce sequence whose
//! drops, rebuilds and identities are known in advance.

use {
    crate::{
        harness::{DedupMetrics, broker_gauge, recs, topic},
        support::{self, dedup::Rec},
    },
    assert2::{assert, check},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_series_count_drops_rebuilds_and_live_identities() {
    let p = support::start().await;
    let t = topic().p(&p).name("metered").call().await;
    // Each step sends one request and names the records it drops, whether it
    // rebuilds its batch, and the identities the index holds after it.
    let steps: [(&str, Vec<Rec>, u64, u64, u64); 6] = [
        ("fresh keys", recs(&["a", "b", "c"]), 0, 0, 3),
        ("a partial drop", recs(&["a", "d", "a", "e"]), 2, 1, 5),
        ("all duplicates", recs(&["b", "c", "d", "e"]), 4, 0, 5),
        (
            "null keys",
            vec![Rec::keyless("n"), Rec::keyless("n")],
            0,
            0,
            5,
        ),
        ("one new key twice", recs(&["f", "f"]), 1, 1, 6),
        ("one more fresh key", recs(&["g"]), 0, 0, 7),
    ];
    let mut expected = DedupMetrics {
        // The index of a new topic is built from its empty log on the first
        // append.
        index_rebuilds: 1,
        ..DedupMetrics::default()
    };
    for (step, records, dropped, rebuilt, entries) in steps {
        check!(t.send(&records).await.error_code == 0, "{step}");
        expected.records_dropped += dropped;
        expected.batches_rebuilt += rebuilt;
        expected.index_entries = entries;
        check!(t.metrics_of(0).await == expected, "{step}");
        check!(
            broker_gauge(&p, "dedup_broker_index_entries").await == entries,
            "{step}"
        );
    }
    assert!(expected.records_dropped == 7);
    assert!(broker_gauge(&p, "dedup_broker_index_max_entries").await == 16_000_000);
    p.broker.shutdown().await;
}
