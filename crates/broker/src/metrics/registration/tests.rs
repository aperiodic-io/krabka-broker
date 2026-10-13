//! Tests for the registration plumbing: that every family reaches the
//! registry under the `krabka_broker` prefix, and that the families with no
//! recorder method of their own start at their documented default.

use {
    crate::{
        break_glass::ALL_ACTIONS,
        metrics::{
            BarrierGroupLabel, BreakGlassAction, BreakGlassState, BrokerMetrics, DedupCounterDelta,
            DirectoryLabel, PartitionLabel, RemoteTierPath, ShareGroupLabel, TopicLabel,
        },
    },
    assert2::assert,
    krabka_metadata::BreakGlassAction as GatedAction,
    krabka_units::{convert::TimeExt as _, millis, secs},
    std::sync::Arc,
};

/// The gauge exists so an alert can read the bound the broker relies on
/// instead of carrying a copy of it, so what matters is that the exported
/// value is the configured extent in seconds.
#[tokio::test]
async fn declared_clock_bound_is_exported_in_seconds() {
    for bound in [millis(250), millis(750), secs(2), millis(1)] {
        let m = BrokerMetrics::new();
        m.delivery_clock_uncertainty_seconds.set(bound.secs_f64());

        crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
        drop(r);

        let name = "krabka_broker_delivery_clock_uncertainty_seconds ";
        let line = buf
            .lines()
            .find(|line| line.starts_with(name))
            .expect("the declared bound is registered and exported");
        let exported: f64 = line[name.len()..]
            .trim()
            .parse()
            .expect("the gauge encodes a number");

        // Bit equality rather than `==`: the assertion is that the
        // encode/parse round trip returns the same `f64`, not that two
        // computed values are near each other, and `clippy::float_cmp`
        // rejects the latter shape. Comparing bits says exactly what is
        // meant and needs no suppression.
        assert!(exported.to_bits() == bound.secs_f64().to_bits());
    }
}

/// Materialises the KIP-405 tier families and the KIP-73 replication
/// byte-rate counters, so the name check below runs against a registry that
/// actually carries them (#420). `prometheus-client` writes no `# TYPE` line
/// for a family with no live label set, so a family nothing has touched is
/// invisible to a name check.
fn seed_remote_tier_and_replication_throttle(m: &BrokerMetrics) {
    for path in [
        RemoteTierPath::Copy,
        RemoteTierPath::Fetch,
        RemoteTierPath::Delete,
    ] {
        m.record_remote_request(path, "topic-a");
        m.record_remote_error(path, "topic-a");
        m.record_remote_bytes(path, "topic-a", 64);
    }
    m.set_remote_copy_lag("topic-a", 2, 200);
    m.set_remote_delete_lag("topic-a", 1, 100);
    m.record_replication_throttled_out(1_024);
    m.record_replication_throttled_in(512);
    m.record_replication_throttle_sleep();
}

fn seed_dedup(m: &BrokerMetrics, topic: &Arc<str>) {
    m.record_dedup_partition(
        topic,
        0,
        DedupCounterDelta {
            records_dropped: 1,
            records_unindexed: 1,
            records_skew_clamped: 1,
            capacity_evicted_entries: 1,
            batches_rebuilt: 1,
            index_rebuilds: 1,
        },
        5,
    );
    m.set_dedup_broker_index(5, 1_000);
}

#[tokio::test]
async fn dedup_metrics_are_registered_with_the_broker_prefix() {
    let m = BrokerMetrics::new();
    seed_dedup(&m, &Arc::from("topic-a"));

    crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
    for needle in [
        "krabka_broker_dedup_records_dropped_total",
        "krabka_broker_dedup_records_unindexed_total",
        "krabka_broker_dedup_records_skew_clamped_total",
        "krabka_broker_dedup_capacity_evicted_entries_total",
        "krabka_broker_dedup_batches_rebuilt_total",
        "krabka_broker_dedup_index_rebuilds_total",
        "krabka_broker_dedup_index_entries",
        "krabka_broker_dedup_broker_index_entries",
        "krabka_broker_dedup_broker_index_max_entries",
    ] {
        assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
    }
}

#[tokio::test]
async fn registry_has_broker_prefix_and_all_metrics() {
    let m = BrokerMetrics::new();
    let topic_a: Arc<str> = Arc::from("topic-a");
    m.record_produce(&topic_a, 100);
    m.record_produce_messages(&topic_a, 5);
    m.record_fetch(&topic_a, 50);
    m.record_partition_produce(&topic_a, 0, 100);
    m.record_partition_fetch(&topic_a, 0, 50);
    m.record_partition_cpu_micros(&topic_a, 0, 250);
    m.record_replication_in(&topic_a, 0, 4096);
    m.record_replication_out(&topic_a, 0, 8192);
    m.record_cleaner_run();
    seed_remote_tier_and_replication_throttle(&m);
    m.record_compaction("topic-a", 0);
    let barrier_group = BarrierGroupLabel {
        group: "orders-cut".into(),
    };
    m.barrier_epochs_started_total
        .get_or_create(&barrier_group)
        .inc();
    m.barrier_epochs_committed_total
        .get_or_create(&barrier_group)
        .inc();
    m.barrier_epochs_published_partial_total
        .get_or_create(&barrier_group)
        .inc();
    m.barrier_injection_duration_seconds
        .get_or_create(&barrier_group)
        .observe(0.02);
    m.barrier_latest_epoch.get_or_create(&barrier_group).set(7);
    m.barrier_markers_written_total
        .get_or_create(&TopicLabel {
            topic: "topic-a".into(),
        })
        .inc_by(3);
    m.barrier_groups_coordinated.set(1);
    m.record_produce_message_conversion(&topic_a);
    m.record_fetch_message_conversion(&topic_a);
    m.record_failed_produce(&topic_a);
    m.record_failed_fetch(&topic_a);
    m.record_schema_validation_rejection("topic-a", "unknown_id");
    m.record_schema_cache_hit();
    m.record_schema_cache_miss();
    m.record_topic_freeze_rejection("topic-a");
    m.record_topic_freezes_active(1);
    m.record_break_glass_proposals(BreakGlassState::Pending, 1);
    m.record_break_glass_refusal(BreakGlassAction(GatedAction::DeleteTopic));
    m.record_break_glass_bypass(BreakGlassAction(GatedAction::UncleanRecovery));
    m.record_authentication("PLAIN", true);
    m.record_authentication("SCRAM-SHA-512", false);
    m.record_authentication("Unknown", false);
    m.record_unclean_leader_election();
    m.record_api_request(0); // Produce
    m.record_api_request(999); // unknown → "Unknown" label
    m.record_unsupported_api_request(999);
    m.observe_request_duration(0, 0.002); // Produce latency sample
    m.observe_request_duration(999, 1.5); // unknown → "Unknown" label
    m.record_request_error(1); // Fetch handler error
    m.in_flight_requests.set(3);
    m.active_connections.set(11);
    m.partition_disk_bytes
        .get_or_create(&PartitionLabel {
            topic: "topic-a".into(),
            partition: 0,
        })
        .set(42);
    m.share_group_backlog
        .get_or_create(&ShareGroupLabel {
            group_id: "workers".into(),
            topic: "topic-a".into(),
            partition: 0,
        })
        .set(9);
    m.record_share_dlq_produce("workers");
    m.record_share_dlq_records("workers", 2);
    m.record_share_dlq_produce_failed("workers");
    m.partitions_led.set(7);
    m.partitions_total.set(42);
    m.under_replicated_partitions.set(3);
    m.under_min_isr_partition_count.set(2);
    m.offline_partitions_count.set(1);
    m.active_controller.set(1);
    m.ignored_static_voters.set(3);
    m.witness_role.set(1);
    m.leader_site_drift_partitions.set(4);
    m.voted_directory
        .get_or_create(&DirectoryLabel {
            directory_id: "00000000-0000-0000-0000-000000000001".into(),
        })
        .set(1);
    m.controller_leader_changes_total.inc();
    m.isr_shrinks_total.inc();
    m.isr_expands_total.inc_by(2);

    crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
    // Spot-check every metric is present and prefixed.
    for needle in [
        "krabka_broker_topic_bytes_in_total",
        "krabka_broker_topic_bytes_out_total",
        "krabka_broker_topic_produce_requests_total",
        "krabka_broker_topic_fetch_requests_total",
        "krabka_broker_partitions_led",
        "krabka_broker_partitions_total",
        "krabka_broker_under_replicated_partitions",
        "krabka_broker_under_min_isr_partition_count",
        "krabka_broker_offline_partitions_count",
        "krabka_broker_active_controller",
        "krabka_broker_ignored_static_voters",
        "krabka_broker_witness_role",
        "krabka_broker_leader_site_drift_partitions",
        "krabka_broker_voted_directory",
        "krabka_broker_controller_leader_changes_total",
        "krabka_broker_isr_shrinks_total",
        "krabka_broker_isr_expands_total",
        "krabka_broker_partition_bytes_in_total",
        "krabka_broker_partition_bytes_out_total",
        "krabka_broker_partition_disk_bytes",
        "krabka_broker_share_group_backlog",
        "krabka_broker_share_group_dlq_records_total",
        "krabka_broker_share_group_dlq_produce_requests_total",
        "krabka_broker_share_group_dlq_failed_produce_requests_total",
        "krabka_broker_partition_cpu_micros_total",
        "krabka_broker_incremental_fetch_sessions",
        "krabka_broker_incremental_fetch_session_evictions_total",
        "krabka_broker_incremental_fetch_partitions_cached",
        "krabka_broker_replication_bytes_in_total",
        "krabka_broker_replication_bytes_out_total",
        "krabka_broker_tiered_storage_rlmm_topic_backed",
        "krabka_broker_produce_message_conversions_total",
        "krabka_broker_fetch_message_conversions_total",
        "krabka_broker_unclean_leader_elections_total",
        "krabka_broker_log_cleaner_runs_total",
        "krabka_broker_log_retention_runs_total",
        "krabka_broker_log_compactions_total",
        "krabka_broker_api_requests_total",
        "krabka_broker_unsupported_api_requests_total",
        "krabka_broker_request_duration_seconds_bucket",
        "krabka_broker_request_duration_seconds_sum",
        "krabka_broker_request_duration_seconds_count",
        "krabka_broker_in_flight_requests",
        "krabka_broker_active_connections",
        "krabka_broker_request_errors_total",
        "krabka_broker_messages_in_total",
        "krabka_broker_topic_failed_produce_requests_total",
        "krabka_broker_topic_failed_fetch_requests_total",
        "krabka_broker_successful_authentication_total",
        "krabka_broker_failed_authentication_total",
        "krabka_broker_barrier_epochs_started_total",
        "krabka_broker_barrier_epochs_committed_total",
        "krabka_broker_barrier_epochs_published_partial_total",
        "krabka_broker_barrier_injection_duration_seconds_bucket",
        "krabka_broker_barrier_injection_duration_seconds_sum",
        "krabka_broker_barrier_injection_duration_seconds_count",
        "krabka_broker_barrier_latest_epoch",
        "krabka_broker_barrier_markers_written_total",
        "krabka_broker_barrier_groups_coordinated",
        "krabka_broker_schema_validation_rejections_total",
        "krabka_broker_schema_validation_cache_hits_total",
        "krabka_broker_schema_validation_cache_misses_total",
        "krabka_broker_topic_freeze_rejections_total",
        "krabka_broker_topic_freezes_active",
        "krabka_broker_break_glass_proposals",
        "krabka_broker_break_glass_refusals_total",
        "krabka_broker_break_glass_bypassed_total",
        "krabka_broker_remote_copy_bytes_total",
        "krabka_broker_remote_fetch_bytes_total",
        "krabka_broker_remote_copy_requests_total",
        "krabka_broker_remote_fetch_requests_total",
        "krabka_broker_remote_delete_requests_total",
        "krabka_broker_remote_copy_errors_total",
        "krabka_broker_remote_fetch_errors_total",
        "krabka_broker_remote_delete_errors_total",
        "krabka_broker_remote_copy_lag_bytes",
        "krabka_broker_remote_copy_lag_segments",
        "krabka_broker_remote_delete_lag_bytes",
        "krabka_broker_remote_delete_lag_segments",
        "krabka_broker_replication_throttled_bytes_out_total",
        "krabka_broker_replication_throttled_bytes_in_total",
        "krabka_broker_replication_throttle_sleeps_total",
        "krabka_broker_remote_log_reader_task_queue_size",
        "krabka_broker_remote_log_reader_avg_idle_percent",
        "krabka_broker_remote_log_reader_fetch_duration_seconds",
        "krabka_broker_remote_log_reader_rejected_total",
        "krabka_broker_remote_index_cache_hits_total",
        "krabka_broker_remote_index_cache_misses_total",
        "krabka_broker_remote_index_cache_evictions_total",
        "krabka_broker_remote_index_cache_bytes",
        "krabka_broker_remote_index_cache_entries",
    ] {
        assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
    }
    // Topic label and values made it through.
    for (needle, what) in [
        ("topic=\"topic-a\"", "topic label"),
        ("100", "bytes_in=100"),
        ("50", "bytes_out=50"),
        ("7", "partitions_led=7"),
    ] {
        assert!(buf.contains(needle), "expected {what} in:\n{buf}");
    }
}

#[test]
fn tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set() {
    let m = BrokerMetrics::new();
    // Default for a fresh broker (in-memory placeholder, or no
    // tiered-storage at all) is `0`.
    assert!(m.tiered_storage_rlmm_topic_backed.get() == 0);
    // The bootstrap task bumps it to `1` after a successful
    // SwappableRlmm swap.
    m.tiered_storage_rlmm_topic_backed.set(1);
    assert!(m.tiered_storage_rlmm_topic_backed.get() == 1);
}

#[test]
fn tiered_storage_rlmm_bootstrap_attempts_counts_up() {
    let m = BrokerMetrics::new();
    assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 0);
    m.tiered_storage_rlmm_bootstrap_attempts.inc();
    m.tiered_storage_rlmm_bootstrap_attempts.inc();
    assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 2);
}

#[test]
fn audit_counters_present() {
    let m = BrokerMetrics::new();
    m.audit_events.inc();
    m.audit_write_failures.inc();
    assert2::check!(m.audit_events.get() == 1);
    assert2::check!(m.audit_write_failures.get() == 1);
}

#[test]
fn audit_spool_metrics_present() {
    let m = BrokerMetrics::new();
    m.audit_records_spooled_total.inc();
    m.audit_records_replayed_total.inc();
    m.audit_records_dropped_total.inc();
    m.audit_spool_depth.set(7);
    m.audit_spool_bytes.set(123);
    assert2::check!(m.audit_records_spooled_total.get() == 1);
    assert2::check!(m.audit_spool_depth.get() == 7);
    assert2::check!(m.audit_spool_bytes.get() == 123);
}

#[tokio::test]
async fn kfc9_families_scrape_under_their_names_with_their_labels() {
    // The registered name plus the counter suffix is what an alert rule
    // spells, and the label name is what it groups by, so both belong in
    // the assertion. Every value here is the movement one call makes.
    let m = BrokerMetrics::new();
    m.record_topic_freeze_rejection("orders");
    m.record_topic_freezes_active(2);
    m.record_break_glass_proposals(BreakGlassState::Pending, 3);
    m.record_break_glass_refusal(BreakGlassAction(GatedAction::DeleteTopic));
    m.record_break_glass_bypass(BreakGlassAction(GatedAction::UncleanRecovery));

    crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
    drop(r);

    let cases = [
        (
            "freeze rejections",
            "krabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
        ),
        ("freezes active", "krabka_broker_topic_freezes_active 2"),
        (
            "proposals",
            "krabka_broker_break_glass_proposals{state=\"pending\"} 3",
        ),
        (
            "refusals",
            "krabka_broker_break_glass_refusals_total{action=\"delete_topic\"} 1",
        ),
        (
            "bypassed",
            "krabka_broker_break_glass_bypassed_total{action=\"unclean_recovery\"} 1",
        ),
    ];
    for (what, needle) in cases {
        assert!(buf.contains(needle), "{what}: missing {needle} in:\n{buf}");
    }
}

#[tokio::test]
async fn break_glass_state_label_covers_every_state() {
    let cases = [
        ("pending", BreakGlassState::Pending, 1),
        ("approved", BreakGlassState::Approved, 2),
        ("expired", BreakGlassState::Expired, 3),
        ("consumed", BreakGlassState::Consumed, 4),
    ];
    // A new state needs a row here, so the closed label set stays covered.
    assert!(cases.len() == BreakGlassState::ALL.len());

    let m = BrokerMetrics::new();
    for (_, state, count) in cases {
        m.record_break_glass_proposals(state, count);
    }

    crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
    drop(r);

    for (label, _, count) in cases {
        let needle = format!("krabka_broker_break_glass_proposals{{state=\"{label}\"}} {count}");
        assert!(buf.contains(&needle), "missing {needle} in:\n{buf}");
    }
}

#[tokio::test]
async fn break_glass_action_label_covers_every_gated_transition() {
    // The expected label text is spelled out here rather than read back
    // from `action_name`, so renaming an action fails this test instead of
    // silently renaming the series an alert rule groups by.
    let cases = [
        ("thaw_topic_freeze", GatedAction::ThawTopicFreeze),
        ("unclean_elect_leaders", GatedAction::UncleanElectLeaders),
        ("unclean_recovery", GatedAction::UncleanRecovery),
        ("unregister_broker", GatedAction::UnregisterBroker),
        ("cancel_reassignment", GatedAction::CancelReassignment),
        ("delete_topic", GatedAction::DeleteTopic),
        ("delete_records", GatedAction::DeleteRecords),
    ];
    // An action added to the metadata enum needs a row here, so the closed
    // label set stays covered.
    assert!(cases.len() == crate::break_glass::ALL_ACTIONS.len());
    for action in ALL_ACTIONS {
        assert!(
            cases.iter().any(|(_, cased)| *cased == action),
            "no expected label for {action:?}"
        );
    }

    // Each action gets a distinct refusal count, so a label that resolves
    // to the wrong series shows up as the wrong number rather than as a
    // still-passing test.
    let m = BrokerMetrics::new();
    for (i, (_, action)) in cases.into_iter().enumerate() {
        for _ in 0..=i {
            m.record_break_glass_refusal(BreakGlassAction(action));
        }
        m.record_break_glass_bypass(BreakGlassAction(action));
    }

    crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
    drop(r);

    for (i, (label, _)) in cases.into_iter().enumerate() {
        let refused = format!(
            "krabka_broker_break_glass_refusals_total{{action=\"{label}\"}} {}",
            i + 1
        );
        let bypassed = format!("krabka_broker_break_glass_bypassed_total{{action=\"{label}\"}} 1");
        assert!(buf.contains(&refused), "missing {refused} in:\n{buf}");
        assert!(buf.contains(&bypassed), "missing {bypassed} in:\n{buf}");
    }
}
