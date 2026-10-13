//! `DescribeLogDirs` (`api_key=35`, KIP-113).
//!
//! The handler reports, for each configured log directory, the partitions that
//! the directory physically holds and their log segment sizes, as Kafka's
//! `UnifiedLog.size` counts them. It backs the
//! `kafka-log-dirs --describe` admin tool.
//!
//! The handler reports both current logs and the future logs of in-progress
//! KIP-113 intra-broker moves. It reports a future-log entry under the
//! destination dir, with `is_future_key = true` and an `offset_lag` equal to
//! `current_log.LEO − future_log.LEO`.
//!
//! This file holds the wire entry point, its ACL Deny response, and the
//! per-directory scan loop. The request's topic filter lives in `filter`, the
//! two lag readings in `lag`, and everything the handler reports about a
//! directory itself in `dirs`.

use {
    krabka_protocol::owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
    },
    krabka_units::prelude::ByteSizeExt,
    std::collections::BTreeMap,
};

mod dirs;
mod filter;
mod lag;

use {
    self::{
        dirs::{absolute_path, cordoned_log_dirs_reported, log_dir_capacity, offline_result},
        filter::request_filter,
        lag::{future_offset_lag, offset_lag_for},
    },
    crate::{
        codes,
        disk_scanner::scan::sum_log_segments,
        handlers::cluster_describe_denied,
        log_dir::{self, DiscoveredPartition},
    },
};

context_handler! {
    DescribeLogDirsRequest => DescribeLogDirsResponse,
    (broker, req, _version, ctx),
    {
        let log_dirs = broker.config.all_log_dirs();
        let partitions = broker.partitions.clone();
        let future_logs = broker.future_logs.clone();
        let log_dir_status = broker.log_dir_status.clone();

        // ── ACL preamble ────────────────────────────────────────────
        // `Describe` on `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        if cluster_describe_denied(
            broker.config.authorizer.as_ref(),
            &broker.controller.current_image(),
            ctx,
        ) {
            let resp = DescribeLogDirsResponse {
                throttle_time_ms: 0,
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                results: Vec::new(),
                ..Default::default()
            };
            return Ok(resp);
        }

        let filter = request_filter(req);
        // KIP-1066: `IsCordoned` goes out only from `metadata.version`
        // `4.3-IV0`, as `ReplicaManager.describeLogDirs` gates it on
        // `isCordonedLogDirsSupported`. Below that level every directory
        // answers `false` whatever `cordoned.log.dirs` says.
        let cordoned = cordoned_log_dirs_reported(
            &broker.controller.current_image(),
            partitions.cordoned_log_dirs(),
        );

        let mut results = Vec::with_capacity(log_dirs.len());
        for dir in &log_dirs {
            // KIP-113 offline-dir handling: a dir the startup probe
            // flagged unwritable is reported with
            // `error_code = KAFKA_STORAGE_ERROR`, no partition scan,
            // and `-1` for capacity. The JVM `kafka-log-dirs` tool
            // expects this shape — it prints the dir as
            // "OFFLINE: …" rather than a row of zeros.
            if log_dir_status.is_offline(dir) {
                results.push(offline_result(dir));
                continue;
            }
            // Group the partitions physically present in this dir by topic.
            let mut by_topic: BTreeMap<String, Vec<DescribeLogDirsPartition>> = BTreeMap::new();
            // KIP-113: surface in-progress future logs (one per
            // `<topic>-<partition>-future` subdir) with
            // `is_future_key = true`. `offset_lag` is the gap between
            // the future log and the source log; while the move is
            // running this shrinks toward zero, then the directory
            // rename turns the entry into a regular current log.
            for is_future_key in [false, true] {
                let discovered = if is_future_key {
                    log_dir::scan_future(dir)
                } else {
                    log_dir::scan(dir)
                };
                for DiscoveredPartition { topic, partition } in discovered.unwrap_or_default() {
                    if !filter.allows(&topic, partition) {
                        continue;
                    }
                    let part_dir = if is_future_key {
                        log_dir::future_partition_dir(dir, &topic, partition)
                    } else {
                        log_dir::partition_dir(dir, &topic, partition)
                    };
                    // A log loaded from this directory knows its size: the
                    // bytes of its batches, Kafka's `UnifiedLog.size`. Its
                    // files can hold more -- the block of padding an
                    // `O_DIRECT` active segment carries under `preallocate`
                    // -- so they are summed only where no log here owns them.
                    let loaded = if is_future_key {
                        None
                    } else {
                        partitions
                            .get(&topic, krabka_ids::PartitionIndex(partition))
                            .and_then(|part| part.log_size_in(&part_dir))
                    };
                    let size = loaded.map_or_else(
                        || sum_log_segments(&part_dir).unwrap_or(0),
                        ByteSizeExt::bytes_u64,
                    );
                    let offset_lag = if is_future_key {
                        future_offset_lag(
                            &partitions,
                            &future_logs,
                            &topic,
                            krabka_ids::PartitionIndex(partition),
                        )
                    } else {
                        offset_lag_for(&partitions, &topic, partition).await
                    };
                    by_topic
                        .entry(topic)
                        .or_default()
                        .push(DescribeLogDirsPartition {
                            partition_index: partition,
                            partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                            offset_lag,
                            is_future_key,
                            ..Default::default()
                        });
                }
            }

            let topics = by_topic
                .into_iter()
                .map(|(name, partitions)| DescribeLogDirsTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect();

            let (total_bytes, usable_bytes) = log_dir_capacity(dir);

            results.push(DescribeLogDirsResult {
                error_code: codes::NONE,
                log_dir: absolute_path(dir),
                topics,
                // KIP-827 (Kafka 3.3+): v4 surfaces per-dir filesystem
                // capacity. We query the underlying filesystem via
                // `statvfs` on unix and report `-1` (Kafka's "unknown"
                // sentinel) on non-unix; the JVM admin tools tolerate
                // `-1` and skip the column.
                total_bytes,
                usable_bytes,
                is_cordoned: cordoned.iter().any(|cordoned| cordoned == dir),
                ..Default::default()
            });
        }

        Ok(DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{DenyAll, start_broker_with_authorizer, test_ctx},
        assert2::assert,
        krabka_protocol::owned::describe_log_dirs_response,
        std::sync::Arc,
    };

    crate::test_support::context_helper!(client_id = "client-a");

    /// A principal without `Describe` on the cluster gets a whole-response
    /// `CLUSTER_AUTHORIZATION_FAILED` and no directories.
    #[tokio::test]
    async fn handle_denies_cluster_describe_for_whole_request() {
        let version = describe_log_dirs_response::MAX_VERSION;
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker_with_authorizer(Arc::new(DenyAll))
        );
        test_ctx!(ctx, "ANONYMOUS");

        let resp = handle(&broker, DescribeLogDirsRequest::default(), version, &ctx)
            .await
            .expect("handle");

        let expected = DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            results: Vec::new(),
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
