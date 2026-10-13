//! The published metrics contract against the registry that emits it.
//!
//! `docs/operations/grafana-dashboard.json` and
//! `docs/operations/alert-rules.yaml` name series. Every one of those names
//! must be a series the broker exports, or the dashboard shows an empty panel
//! and the alert never fires. This suite builds the registry the broker
//! starts with, encodes it the way the `/metrics` route does, and checks each
//! name the two files spell against that body.
//!
//! `docs/operations/metrics-body.txt` is a checked-in copy of the same body.
//! The CI docs job has no Rust toolchain, so `aspect check-metrics-contract`
//! reads that copy instead of a live broker. This suite keeps the copy fresh:
//! it fails when the copy differs from the body a new registry encodes. To
//! regenerate the copy, run the suite with `KRABKA_METRICS_BODY_OUT` set to
//! the path to write:
//!
//! ```text
//! KRABKA_METRICS_BODY_OUT=docs/operations/metrics-body.txt \
//!     cargo nextest run -p krabka-broker --test metrics_contract
//! ```

use {
    assert2::assert,
    krabka_broker::metrics::{
        ApiKeyLabel, AuthorizationDeniedLabel, BarrierGroupLabel, BreakGlassAction,
        BreakGlassActionLabel, BreakGlassState, BreakGlassStateLabel, BrokerMetrics,
        CleanerFailureLabel, CleanerFailureReason, ClientSoftwareLabel, ConnectionCloseReason,
        ConnectionCloseReasonLabel, ConsumerGroupLabel, DirectoryLabel, PartitionLabel,
        QuotaEntityLabel, QuotaType, QuotaTypeLabel, RaftStateLabel, ReplicaLagLabel,
        SaslMechanismLabel, SchemaRejectionLabel, ShareGroupIdLabel, ShareGroupLabel, TopicLabel,
        WalShardLabel, WalVoterLabel,
    },
    krabka_metadata::BreakGlassAction as GatedAction,
    prometheus_client::metrics::family::Family,
    std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    },
};

mod support;

krabka_macros::metric_registry_fixture!(render_registry);

/// The dashboard and rules files, relative to the repository root.
const CONTRACT_FILES: [&str; 2] = [
    "docs/operations/grafana-dashboard.json",
    "docs/operations/alert-rules.yaml",
];

/// The checked-in `/metrics` body, relative to the repository root.
const BODY_FILE: &str = "docs/operations/metrics-body.txt";

/// Environment variable naming a path to write the fresh body to.
const BODY_OUT_ENV: &str = "KRABKA_METRICS_BODY_OUT";

/// The repository root, under Cargo and under Bazel.
fn repo_root() -> PathBuf {
    let crate_dir = support::manifest_dir();
    crate_dir
        .parent()
        .and_then(Path::parent)
        .expect("crates/broker sits two levels below the repository root")
        .to_path_buf()
}

/// The `/metrics` body of a registry in which every family carries one label
/// set, encoded exactly as `metrics_server::metrics` encodes it.
///
/// `prometheus-client` writes no `# TYPE` line for a family with no live
/// label set, so a fresh broker exports only its label-free series. One
/// placeholder label set per family makes every registered name appear.
async fn fresh_body() -> String {
    let metrics = BrokerMetrics::new();
    seed_grouped_families(&metrics);
    seed_single_families(&metrics);

    render_registry!(metrics, body, registry; expect("registry encodes"));
    canonical(&body)
}

/// `body` with the sample lines of every counter and gauge family sorted.
///
/// A `Family` keeps its label sets in a hash map, so a family with more than
/// one live series, such as the three `path` series `fetch_response_drain`
/// registers at startup, encodes them in an order that changes from one run
/// to the next. The checked-in copy needs one order. A histogram's lines are
/// left as encoded: its `le` buckets are already in a fixed order, and a
/// lexical sort would scatter them.
fn canonical(body: &str) -> String {
    let mut out = String::new();
    let mut kind = "";
    let mut samples: Vec<&str> = Vec::new();
    let flush = |samples: &mut Vec<&str>, kind: &str, out: &mut String| {
        if kind != "histogram" {
            samples.sort_unstable();
        }
        for line in samples.drain(..) {
            out.push_str(line);
            out.push('\n');
        }
    };
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            flush(&mut samples, kind, &mut out);
            kind = rest.split_whitespace().nth(1).unwrap_or("");
        } else if line.starts_with('#') {
            flush(&mut samples, kind, &mut out);
        } else {
            samples.push(line);
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    flush(&mut samples, kind, &mut out);
    out
}

/// Gives one label set to every family that shares its label type with
/// others. The destructure names every field without `..`, so a family added
/// to [`BrokerMetrics`] fails to compile here until it is seeded, either in
/// one of these groups or in [`seed_single_families`]. A `_` marks a
/// label-free series, which the encoder writes on its own, or a family that
/// [`seed_single_families`] covers.
/// The one label set of each shape that [`seed_grouped_families`] gives every
/// family it seeds. The values are arbitrary; what matters is that each family
/// carries exactly one series so the encoder writes its `# TYPE` line.
struct SeedLabels {
    topic: TopicLabel,
    partition: PartitionLabel,
    api_key: ApiKeyLabel,
    group: BarrierGroupLabel,
    shard: WalShardLabel,
}

impl SeedLabels {
    fn new() -> Self {
        Self {
            topic: TopicLabel {
                topic: "orders".into(),
            },
            partition: PartitionLabel {
                topic: "orders".into(),
                partition: 0,
            },
            api_key: ApiKeyLabel {
                api_key: "Produce".into(),
            },
            group: BarrierGroupLabel {
                group: "orders-cut".into(),
            },
            shard: WalShardLabel {
                topic_id: "00000000-0000-0000-0000-000000000001".into(),
                partition: 0,
            },
        }
    }
}

/// Materializes one series in each of `families` under `label`.
fn seed<L, M, C>(label: &L, families: &[&Family<L, M, C>])
where
    L: Clone + std::hash::Hash + Eq + prometheus_client::encoding::EncodeLabelSet,
    C: prometheus_client::metrics::family::MetricConstructor<M>,
{
    for family in families {
        drop(family.get_or_create(label));
    }
}

/// Fails to compile when a family is added to [`BrokerMetrics`] and not
/// accounted for by this suite.
///
/// The destructure names every field and uses no `..`, so a new family is a
/// compile error here until its author decides where it is seeded: in one of
/// [`seed_grouped_families`]'s groups, in [`seed_single_families`], or
/// nowhere, because the encoder writes a label-free series on its own.
fn every_family_is_accounted_for(metrics: &BrokerMetrics) {
    let BrokerMetrics {
        registry: _,
        topic_bytes_in: _,
        topic_bytes_out: _,
        topic_messages_in: _,
        topic_produce_requests: _,
        topic_fetch_requests: _,
        topic_failed_produce_requests: _,
        topic_failed_fetch_requests: _,
        partition_bytes_in: _,
        partition_bytes_out: _,
        replication_bytes_in: _,
        replication_bytes_out: _,
        partition_disk_bytes: _,
        replica_lag: _,
        replica_lag_max: _,
        consumer_group_lag: _,
        share_group_backlog: _,
        share_group_dlq_records: _,
        share_group_dlq_produce_requests: _,
        share_group_dlq_failed_produce_requests: _,
        partition_cpu_micros: _,
        partitions_led: _,
        partitions_total: _,
        under_replicated_partitions: _,
        under_min_isr_partition_count: _,
        offline_partitions_count: _,
        active_controller: _,
        ignored_static_voters: _,
        witness_role: _,
        leader_site_drift_partitions: _,
        voted_directory: _,
        controller_leader_changes_total: _,
        controller_fencing_publications_total: _,
        isr_shrinks_total: _,
        isr_expands_total: _,
        fetch_response_drain: _,
        ktls_enabled: _,
        incremental_fetch_sessions: _,
        incremental_fetch_session_evictions_total: _,
        incremental_fetch_partitions_cached: _,
        client_software_versions: _,
        successful_authentication: _,
        failed_authentication: _,
        authorization_denied: _,
        api_requests: _,
        unsupported_api_requests: _,
        request_duration_seconds: _,
        request_local_duration_seconds: _,
        request_remote_duration_seconds: _,
        request_throttle_duration_seconds: _,
        quota_throttle_duration_seconds: _,
        in_flight_requests: _,
        active_connections: _,
        connection_closes: _,
        request_errors: _,
        tiered_storage_rlmm_topic_backed: _,
        tiered_storage_rlmm_bootstrap_attempts: _,
        worm_manifests_sealed_total: _,
        worm_manifest_seal_failures_total: _,
        produce_message_conversions: _,
        fetch_message_conversions: _,
        unclean_leader_elections_total: _,
        audit_events: _,
        audit_write_failures: _,
        audit_spool_depth: _,
        audit_spool_bytes: _,
        audit_records_spooled_total: _,
        audit_records_replayed_total: _,
        audit_records_dropped_total: _,
        client_metrics_otlp_dropped_total: _,
        client_metrics_otlp_failed_total: _,
        log_cleaner_runs_total: _,
        log_cleaner_failures: _,
        log_retention_runs_total: _,
        log_retention_failures: _,
        log_cleaner_uncleanable_partitions: _,
        offline_log_dirs: _,
        metadata_load_error_count: _,
        log_compactions_total: _,
        barrier_epochs_started_total: _,
        barrier_epochs_committed_total: _,
        barrier_epochs_published_partial_total: _,
        barrier_injection_duration_seconds: _,
        barrier_latest_epoch: _,
        barrier_markers_written_total: _,
        barrier_groups_coordinated: _,
        delivery_watermark: _,
        delivery_pending_records: _,
        delivery_activation_lateness_seconds: _,
        delivery_scheduler_wakeups_total: _,
        schema_validation_rejections: _,
        schema_validation_cache_hits: _,
        schema_validation_cache_misses: _,
        delivery_clock_uncertainty_seconds: _,
        topic_freeze_rejections: _,
        topic_freezes_active: _,
        break_glass_proposals: _,
        break_glass_refusals: _,
        break_glass_bypassed: _,
        dedup_records_dropped: _,
        dedup_records_unindexed: _,
        dedup_records_expired_on_arrival: _,
        dedup_records_skew_clamped: _,
        dedup_capacity_evicted_entries: _,
        dedup_batches_rebuilt: _,
        dedup_index_rebuilds: _,
        dedup_index_entries: _,
        dedup_index_heap_bytes: _,
        dedup_broker_index_entries: _,
        dedup_broker_index_max_entries: _,
        diskless_wal_durable_watermark: _,
        diskless_wal_voter_lag: _,
        diskless_wal_quorum_loss_events_total: _,
        diskless_wal_flush_attempts_total: _,
        diskless_wal_flush_bytes_total: _,
        diskless_wal_flush_failures_total: _,
        diskless_wal_index_decode_failures_total: _,
        diskless_wal_index_projection_lag: _,
        diskless_wal_trim_frontier: _,
        diskless_wal_expired_ranges_total: _,
        diskless_wal_cold_read_hits_total: _,
        diskless_wal_cold_read_misses_total: _,
        diskless_wal_cold_read_errors_total: _,
        raft_current_state: _,
        raft_current_epoch: _,
        raft_high_watermark: _,
        raft_log_end_offset: _,
        raft_voters: _,
        raft_observers: _,
        metadata_last_applied_offset: _,
        metadata_last_applied_timestamp_ms: _,
        metadata_last_applied_lag_ms: _,
        pending_record_processing_time_us_avg: _,
        pending_record_processing_time_us_max: _,
        metadata_lag_records: _,
        broker_state: _,
        active_brokers: _,
        fenced_brokers: _,
        global_topics: _,
        global_partitions: _,
        at_min_isr_partition_count: _,
        reassigning_partitions: _,
        preferred_replica_imbalance: _,
        remote_copy_bytes_total: _,
        remote_fetch_bytes_total: _,
        remote_copy_requests_total: _,
        remote_fetch_requests_total: _,
        remote_delete_requests_total: _,
        remote_copy_errors_total: _,
        remote_fetch_errors_total: _,
        remote_delete_errors_total: _,
        remote_copy_lag_bytes: _,
        remote_copy_lag_segments: _,
        remote_delete_lag_bytes: _,
        remote_delete_lag_segments: _,
        replication_throttled_bytes_out_total: _,
        replication_throttled_bytes_in_total: _,
        replication_throttle_sleeps_total: _,
        quota_entity_throttle_seconds_total: _,
        queued_requests: _,
        queued_request_bytes: _,
        remote_log_reader_task_queue_size: _,
        remote_log_reader_avg_idle_percent: _,
        remote_log_reader_fetch_duration_seconds: _,
        remote_log_reader_rejected_total: _,
        remote_index_cache_hits_total: _,
        remote_index_cache_misses_total: _,
        remote_index_cache_evictions_total: _,
        remote_index_cache_bytes: _,
        remote_index_cache_entries: _,
        lag_series: _,
        metric_series: _,
    } = metrics;
}

fn seed_grouped_families(metrics: &BrokerMetrics) {
    every_family_is_accounted_for(metrics);
    let SeedLabels {
        topic,
        partition,
        api_key,
        group,
        shard,
    } = SeedLabels::new();

    seed(&topic, &[
        &metrics.topic_bytes_in,
        &metrics.topic_bytes_out,
        &metrics.topic_messages_in,
        &metrics.topic_produce_requests,
        &metrics.topic_fetch_requests,
        &metrics.topic_failed_produce_requests,
        &metrics.topic_failed_fetch_requests,
        &metrics.produce_message_conversions,
        &metrics.fetch_message_conversions,
        &metrics.barrier_markers_written_total,
        &metrics.topic_freeze_rejections,
        &metrics.remote_copy_bytes_total,
        &metrics.remote_fetch_bytes_total,
        &metrics.remote_copy_requests_total,
        &metrics.remote_fetch_requests_total,
        &metrics.remote_delete_requests_total,
        &metrics.remote_copy_errors_total,
        &metrics.remote_fetch_errors_total,
        &metrics.remote_delete_errors_total,
    ]);
    seed(&topic, &[
        &metrics.remote_copy_lag_bytes,
        &metrics.remote_copy_lag_segments,
        &metrics.remote_delete_lag_bytes,
        &metrics.remote_delete_lag_segments,
    ]);
    seed(&partition, &[
        &metrics.partition_bytes_in,
        &metrics.partition_bytes_out,
        &metrics.replication_bytes_in,
        &metrics.replication_bytes_out,
        &metrics.partition_cpu_micros,
        &metrics.log_compactions_total,
        &metrics.dedup_records_dropped,
        &metrics.dedup_records_unindexed,
        &metrics.dedup_records_expired_on_arrival,
        &metrics.dedup_records_skew_clamped,
        &metrics.dedup_capacity_evicted_entries,
        &metrics.dedup_batches_rebuilt,
        &metrics.dedup_index_rebuilds,
    ]);
    seed(&partition, &[
        &metrics.partition_disk_bytes,
        &metrics.delivery_watermark,
        &metrics.delivery_pending_records,
        &metrics.dedup_index_entries,
        &metrics.dedup_index_heap_bytes,
    ]);
    seed(&api_key, &[
        &metrics.api_requests,
        &metrics.unsupported_api_requests,
        &metrics.request_errors,
    ]);
    seed(&api_key, &[
        &metrics.request_duration_seconds,
        &metrics.request_local_duration_seconds,
        &metrics.request_remote_duration_seconds,
        &metrics.request_throttle_duration_seconds,
    ]);
    seed(&group, &[
        &metrics.barrier_epochs_started_total,
        &metrics.barrier_epochs_committed_total,
        &metrics.barrier_epochs_published_partial_total,
    ]);
    seed(&group, &[&metrics.barrier_injection_duration_seconds]);
    seed(&group, &[&metrics.barrier_latest_epoch]);
    seed(&shard, &[
        &metrics.diskless_wal_durable_watermark,
        &metrics.diskless_wal_index_projection_lag,
        &metrics.diskless_wal_trim_frontier,
    ]);
}

/// Gives one label set to every family whose label type no other family
/// shares. [`seed_grouped_families`] marks each of these with `_`.
///
/// `fetch_response_drain` is also marked `_`, but needs no seed: registration
/// creates all three of its `path` series at zero.
fn seed_single_families(metrics: &BrokerMetrics) {
    drop(metrics.replica_lag.get_or_create(&ReplicaLagLabel {
        topic: "orders".into(),
        partition: 0,
        replica: 2,
    }));
    drop(
        metrics
            .consumer_group_lag
            .get_or_create(&ConsumerGroupLabel {
                group_id: "billing".into(),
                topic: "orders".into(),
                partition: 0,
            }),
    );
    drop(metrics.share_group_backlog.get_or_create(&ShareGroupLabel {
        group_id: "workers".into(),
        topic: "orders".into(),
        partition: 0,
    }));
    seed(
        &ShareGroupIdLabel {
            group_id: "workers".into(),
        },
        &[
            &metrics.share_group_dlq_records,
            &metrics.share_group_dlq_produce_requests,
            &metrics.share_group_dlq_failed_produce_requests,
        ],
    );
    drop(metrics.voted_directory.get_or_create(&DirectoryLabel {
        directory_id: "00000000-0000-0000-0000-000000000001".into(),
    }));
    drop(
        metrics
            .client_software_versions
            .get_or_create(&ClientSoftwareLabel {
                software_name: "apache-kafka-java".into(),
                software_version: "4.0.0".into(),
            }),
    );
    for family in [
        &metrics.successful_authentication,
        &metrics.failed_authentication,
    ] {
        drop(family.get_or_create(&SaslMechanismLabel {
            mechanism: "SCRAM-SHA-512".into(),
        }));
    }
    drop(
        metrics
            .authorization_denied
            .get_or_create(&AuthorizationDeniedLabel {
                operation: "Write".into(),
                resource_type: "Topic".into(),
            }),
    );
    drop(
        metrics
            .quota_throttle_duration_seconds
            .get_or_create(&QuotaTypeLabel {
                quota_type: QuotaType::Produce,
            }),
    );
    drop(
        metrics
            .connection_closes
            .get_or_create(&ConnectionCloseReasonLabel {
                reason: ConnectionCloseReason::Idle,
            }),
    );
    drop(
        metrics
            .schema_validation_rejections
            .get_or_create(&SchemaRejectionLabel {
                topic: "orders".into(),
                reason: "unframed".into(),
            }),
    );
    drop(
        metrics
            .log_cleaner_failures
            .get_or_create(&CleanerFailureLabel {
                topic: "orders".into(),
                partition: 0,
                reason: CleanerFailureReason::Io,
            }),
    );
    drop(
        metrics
            .log_retention_failures
            .get_or_create(&CleanerFailureLabel {
                topic: "orders".into(),
                partition: 0,
                reason: CleanerFailureReason::Io,
            }),
    );
    drop(
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel {
                state: BreakGlassState::Pending,
            }),
    );
    for family in [&metrics.break_glass_refusals, &metrics.break_glass_bypassed] {
        drop(family.get_or_create(&BreakGlassActionLabel {
            action: BreakGlassAction(GatedAction::UncleanRecovery),
        }));
    }
    drop(
        metrics
            .diskless_wal_voter_lag
            .get_or_create(&WalVoterLabel {
                topic_id: "00000000-0000-0000-0000-000000000001".into(),
                partition: 0,
                voter: 1,
            }),
    );
    for state in RaftStateLabel::ALL {
        drop(metrics.raft_current_state.get_or_create(&state));
    }
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: Some("producer-1".into()),
            }),
    );
}

/// Every sample name the body can carry, expanded from its `# TYPE` lines.
///
/// A family with no live label set writes no sample line, so the `# TYPE`
/// line is the one place every registered family appears. `prometheus-client`
/// suffixes a counter with `_total` and a histogram with `_bucket`, `_sum` and
/// `_count`; a gauge keeps its bare name.
fn exported_sample_names(body: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in body.lines() {
        let Some(rest) = line.strip_prefix("# TYPE ") else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let (Some(name), Some(kind)) = (parts.next(), parts.next()) else {
            continue;
        };
        match kind {
            "counter" => {
                names.insert(format!("{name}_total"));
            }
            "gauge" | "unknown" => {
                names.insert(name.to_string());
            }
            "histogram" => {
                for suffix in ["_bucket", "_sum", "_count"] {
                    names.insert(format!("{name}{suffix}"));
                }
            }
            other => panic!("unexpected metric type {other} on {name}"),
        }
    }
    names
}

/// Every `krabka_broker_*` token a contract file spells.
///
/// A token is the metric name and nothing else, so a `PromQL` expression such
/// as `rate(krabka_broker_api_requests_total{api_key="Produce"}[5m])` yields
/// one name and a label value never does.
fn referenced_names(text: &str) -> BTreeSet<String> {
    let token = regex::Regex::new(r"\bkrabka_broker_[A-Za-z0-9_]+").expect("valid pattern");
    token
        .find_iter(text)
        .map(|found| found.as_str().to_string())
        .collect()
}

#[tokio::test]
async fn every_dashboard_and_rule_series_is_one_the_registry_exports() {
    let exported = exported_sample_names(&fresh_body().await);
    assert!(
        exported.contains("krabka_broker_api_requests_total"),
        "the registry exports the request counter"
    );

    let root = repo_root();
    for file in CONTRACT_FILES {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|error| panic!("read {file}: {error}"));
        let referenced = referenced_names(&text);
        assert!(
            referenced.len() >= 10,
            "{file} names too few series to be the contract: {referenced:?}"
        );
        let missing: Vec<&String> = referenced
            .iter()
            .filter(|name| !exported.contains(*name))
            .collect();
        assert!(
            missing.is_empty(),
            "{file} names series the registry does not export: {missing:?}"
        );
    }
}

#[tokio::test]
async fn checked_in_body_matches_a_fresh_registry() {
    let body = fresh_body().await;
    if let Ok(out) = std::env::var(BODY_OUT_ENV) {
        std::fs::write(&out, &body).unwrap_or_else(|error| panic!("write {out}: {error}"));
    }

    let checked_in = std::fs::read_to_string(repo_root().join(BODY_FILE))
        .unwrap_or_else(|error| panic!("read {BODY_FILE}: {error}"));
    assert!(
        checked_in == body,
        "{BODY_FILE} is stale; regenerate it with {BODY_OUT_ENV}={BODY_FILE}"
    );
}

/// The name expansion is what the Python check repeats, so its rules are
/// pinned here on a body small enough to read.
#[test]
fn sample_names_expand_by_metric_type() {
    let body = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total 1
# HELP krabka_broker_b A gauge.
# TYPE krabka_broker_b gauge
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
# EOF
";
    let expected: BTreeSet<String> = [
        "krabka_broker_a_total",
        "krabka_broker_b",
        "krabka_broker_c_bucket",
        "krabka_broker_c_sum",
        "krabka_broker_c_count",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert!(exported_sample_names(body) == expected);
}

/// Counter and gauge samples take one order whatever the encoder's hash map
/// produced; histogram buckets keep the order the encoder wrote.
#[test]
fn canonical_body_sorts_counter_samples_and_keeps_bucket_order() {
    let body = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total{path=\"vectored\"} 0
krabka_broker_a_total{path=\"pread\"} 0
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
krabka_broker_c_bucket{le=\"0.5\"} 0
krabka_broker_c_bucket{le=\"+Inf\"} 0
krabka_broker_c_sum 0.0
krabka_broker_c_count 0
# EOF
";
    let expected = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total{path=\"pread\"} 0
krabka_broker_a_total{path=\"vectored\"} 0
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
krabka_broker_c_bucket{le=\"0.5\"} 0
krabka_broker_c_bucket{le=\"+Inf\"} 0
krabka_broker_c_sum 0.0
krabka_broker_c_count 0
# EOF
";
    assert!(canonical(body) == expected);
}

#[test]
fn referenced_names_are_metric_tokens_only() {
    let text = r#"sum by (topic) (rate(krabka_broker_topic_bytes_in_total{topic!="__x"}[5m]))
        / histogram_quantile(0.99, rate(krabka_broker_request_duration_seconds_bucket[5m]))"#;
    let expected: BTreeSet<String> = [
        "krabka_broker_topic_bytes_in_total",
        "krabka_broker_request_duration_seconds_bucket",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert!(referenced_names(text) == expected);
}
