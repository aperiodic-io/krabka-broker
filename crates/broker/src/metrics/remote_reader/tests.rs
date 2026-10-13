//! The sampler advances each counter by the increment the reader accrued
//! since the last sample, and leaves the gauges at the newest reading.

use {super::*, assert2::check};

#[test]
fn counters_advance_by_the_increment_since_the_last_sample() {
    let metrics = BrokerMetrics::new();
    let levels = RemoteReaderLevels {
        task_queue_size: 3,
        idle_percent: 40.0,
        index_cache_bytes: 4096,
        index_cache_entries: 2,
    };

    let first = RemoteReaderTotals {
        index_cache_hits: 5,
        index_cache_misses: 2,
        index_cache_evictions: 1,
        rejected_reads: 0,
    };
    metrics.observe_remote_reader(&RemoteReaderTotals::default(), first, levels);
    let second = RemoteReaderTotals {
        index_cache_hits: 9,
        index_cache_misses: 3,
        index_cache_evictions: 1,
        rejected_reads: 7,
    };
    metrics.observe_remote_reader(&first, second, levels);

    check!(metrics.remote_index_cache_hits_total.get() == 9);
    check!(metrics.remote_index_cache_misses_total.get() == 3);
    check!(metrics.remote_index_cache_evictions_total.get() == 1);
    check!(metrics.remote_log_reader_rejected_total.get() == 7);
    check!(metrics.remote_log_reader_task_queue_size.get() == 3);
    check!(metrics.remote_index_cache_bytes.get() == 4096);
    check!(metrics.remote_index_cache_entries.get() == 2);
}

#[test]
fn a_total_that_went_backwards_leaves_its_counter_where_it_was() {
    let metrics = BrokerMetrics::new();
    let levels = RemoteReaderLevels {
        task_queue_size: 0,
        idle_percent: 100.0,
        index_cache_bytes: 0,
        index_cache_entries: 0,
    };
    let reported = RemoteReaderTotals {
        index_cache_hits: 10,
        ..RemoteReaderTotals::default()
    };
    metrics.observe_remote_reader(&RemoteReaderTotals::default(), reported, levels);

    metrics.observe_remote_reader(&reported, RemoteReaderTotals::default(), levels);

    check!(metrics.remote_index_cache_hits_total.get() == 10);
}

#[test]
fn each_cold_read_is_recorded_in_the_fetch_duration_histogram() {
    let metrics = BrokerMetrics::new();

    metrics.observe_remote_reader_fetch(std::time::Duration::from_millis(12));
    metrics.observe_remote_reader_fetch(std::time::Duration::from_millis(30));

    let mut body = String::new();
    prometheus_client::encoding::text::encode(
        &mut body,
        &metrics.registry.try_lock().expect("registry"),
    )
    .expect("encode");
    check!(body.contains("remote_log_reader_fetch_duration_seconds_count 2"));
}
