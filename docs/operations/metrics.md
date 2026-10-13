# Metrics

Every series the broker exports on `/metrics`, with the JMX name it replaces.
The registry in `crates/broker/src/metrics.rs` is the source; this table is
derived from its doc comments and the registration help strings. When the two
disagree, the code wins, and the [contract check](#the-contract-check) fails
the build until this directory catches up.

## Reading the table

- The broker serves `/metrics` on `--metrics-listen-addr`, which defaults to
  `0.0.0.0:9404`. That is the port `jmx_prometheus_javaagent` uses on a JVM
  Kafka broker, so an existing scrape config applies unchanged.
- Every series carries the `krabka_broker_` prefix. The table writes the
  exported sample name, which is what a PromQL expression spells.
- Kafka's `*PerSec` meters become monotonic counters. Compute a rate at query
  time with `rate(<series>[5m])`. The counter suffix `_total` is what the
  encoder adds to the registered name.
- A histogram exports `_bucket`, `_sum` and `_count`. Read a quantile with
  `histogram_quantile(0.99, sum by (le, <label>) (rate(<series>_bucket[5m])))`.
- **Replaces** is the JMX MBean and attribute the series stands in for. A
  dash means Kafka has no counterpart.
- The `instance` label is the scrape target's, and every series carries it.
  The **Labels** column lists only the labels the broker adds.

A series is one per broker unless its labels say otherwise. Cardinality is
bounded by the metadata image or a closed enum in every family but one.
`client_software_versions_total` takes its `software_name` and
`software_version` labels from the client's own `ApiVersions` request. The
broker checks only the character set, not the value, and `ApiVersions` is
allowed before authentication. Nothing caps or releases the series, so a client
can grow that family without bound. Drop or relabel it at the scrape if a
listener is open to peers you do not control.

## Topics and partitions

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_topic_bytes_in_total` | counter | `topic` | `BrokerTopicMetrics,name=BytesInPerSec` | Bytes received from producers. |
| `krabka_broker_topic_bytes_out_total` | counter | `topic` | `BrokerTopicMetrics,name=BytesOutPerSec` | Bytes delivered to fetchers, consumers and followers both. |
| `krabka_broker_messages_in_total` | counter | `topic` | `BrokerTopicMetrics,name=MessagesInPerSec` | Records received from producers. Legacy v0 and v1 batches are not counted; `produce_message_conversions` tracks their arrival. |
| `krabka_broker_topic_produce_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=TotalProduceRequestsPerSec` | One increment per topic per Produce request. |
| `krabka_broker_topic_fetch_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=TotalFetchRequestsPerSec` | One increment per topic per Fetch request. |
| `krabka_broker_topic_failed_produce_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=FailedProduceRequestsPerSec` | Produce partition responses with a non-zero error code, one per failed partition. Alert on `rate(...) > 0`. |
| `krabka_broker_topic_failed_fetch_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=FailedFetchRequestsPerSec` | Fetch partition responses with a non-zero error code. |
| `krabka_broker_produce_message_conversions_total` | counter | `topic` | `BrokerTopicMetrics,name=ProduceMessageConversionsPerSec` | v0 or v1 to v2 up-conversions on the Produce path. |
| `krabka_broker_fetch_message_conversions_total` | counter | `topic` | `BrokerTopicMetrics,name=FetchMessageConversionsPerSec` | v2 to v0 or v1 down-conversions for a `Fetch v < 4` client. |
| `krabka_broker_partition_bytes_in_total` | counter | `topic`, `partition` | - | Bytes received from producers, per partition. |
| `krabka_broker_partition_bytes_out_total` | counter | `topic`, `partition` | - | Bytes delivered to fetchers, consumers and followers both, per partition. Subtract `replication_bytes_out_total` for the consumer share. |
| `krabka_broker_partition_disk_bytes` | gauge | `topic`, `partition` | `kafka.log:type=Log,name=Size` | On-disk size of the partition's log directory. Sampled every `--partition-disk-scan-interval` (default 60s); `0s` disables the scanner and the series stops. |
| `krabka_broker_partition_cpu_micros_total` | counter | `topic`, `partition` | - | Handler-thread microseconds spent on the partition. `rate(...) / 1000000` is the core occupancy. |
| `krabka_broker_log_cleaner_runs_total` | counter | - | `kafka.log:type=LogCleaner` (run accounting) | Clean compaction sweeps, one per pass that failed no partition, whether or not a partition was eligible. A sweep that failed a partition is not counted here, so the pass rate never reports success during a compaction outage. |
| `krabka_broker_log_compactions_total` | counter | `topic`, `partition` | - | Compaction passes completed on the partition. |
| `krabka_broker_log_cleaner_failures_total` | counter | `topic`, `partition`, `reason` | - | Compaction passes that failed. `reason` is one of `io` (a storage failure, already reported to the log-dir registry), `writer` (the partition's writer actor is gone) and `other`. Compaction and local retention stop together on a compacted topic, so alert on `rate(...) > 0`. [Runbook](runbooks/log-cleaner-stalled.md). |
| `krabka_broker_log_cleaner_uncleanable_partitions` | gauge | - | `kafka.log:type=LogCleanerManager,name=uncleanable-partitions-count` | Partitions this broker hosts whose most recent compaction attempt failed and which have not compacted since. The cleaner sweeps every hosted replica, leader or follower, as Kafka's `LogCleanerManager` does, so a follower replica counts here too. Republished each sweep, so it falls when a pass succeeds, when this broker stops hosting the replica, or when the topic stops being compacted. [Runbook](runbooks/log-cleaner-stalled.md). |
| `krabka_broker_log_retention_runs_total` | counter | - | `kafka.log:type=LogManager` (cleanupLogs accounting) | Clean local-retention sweeps, one per pass that failed no partition, whether or not any segment was evicted. The `delete` half of log maintenance: `retention.ms`, `retention.bytes` and `segment.ms`, applied on `log_retention_check_interval` to every log this broker hosts, leader or follower. |
| `krabka_broker_log_retention_failures_total` | counter | `topic`, `partition`, `reason` | - | Local-retention passes that failed. `reason` is read exactly as it is on `log_cleaner_failures_total`: `io`, `writer`, `other`. Nothing else reports this: a broker that never evicts a segment looks like one with nothing to evict until the disk fills, so alert on `rate(...) > 0`. |
| `krabka_broker_offline_log_dirs` | gauge | - | `kafka.log:type=LogManager,name=OfflineLogDirectoryCount` | Log directories this broker has marked offline, by the startup writability probe or by a live write or fsync failure. Partitions on an offline dir answer `KAFKA_STORAGE_ERROR` until the broker restarts. Alert on `> 0`. [Runbook](runbooks/offline-log-dir.md). |

## Replication and partition state

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_replication_bytes_in_total` | counter | `topic`, `partition` | `BrokerTopicMetrics,name=ReplicationBytesInPerSec` | Bytes this broker took from a leader as a follower. |
| `krabka_broker_replication_bytes_out_total` | counter | `topic`, `partition` | `BrokerTopicMetrics,name=ReplicationBytesOutPerSec` | Bytes this broker served to followers as the leader. |
| `krabka_broker_partitions_led` | gauge | - | `ReplicaManager,name=LeaderCount` | Partitions this broker leads. Sampled once a second. |
| `krabka_broker_partitions_total` | gauge | - | `ReplicaManager,name=PartitionCount` | Partitions this broker hosts, leader and follower replicas both. |
| `krabka_broker_under_replicated_partitions` | gauge | - | `ReplicaManager,name=UnderReplicatedPartitions` | Partitions this broker leads whose ISR is smaller than the replica set. Alert on `> 0`. [Runbook](runbooks/under-replicated-partitions.md). |
| `krabka_broker_under_min_isr_partition_count` | gauge | - | `ReplicaManager,name=UnderMinIsrPartitionCount` | Partitions this broker leads whose ISR is below `min.insync.replicas`. They reject `acks=all` with `NOT_ENOUGH_REPLICAS`. Alert on `> 0`. [Runbook](runbooks/under-min-isr-partitions.md). |
| `krabka_broker_offline_partitions_count` | gauge | - | `ReplicaManager,name=OfflinePartitionsCount` | Partitions with no live leader. Alert on `> 0`. [Runbook](runbooks/offline-partitions.md). |
| `krabka_broker_replica_lag_records` | gauge | `topic`, `partition`, `replica` | `FetcherLagMetrics,name=ConsumerLag,clientId=ReplicaFetcherThread-*,topic=<t>,partition=<p>` | Records a follower of a partition this broker leads has yet to fetch: the leader's log end offset minus the follower's last-fetched offset. `replica` is the follower's node id. Kafka reports this on the follower; krabka reports it on the leader, one series per follower. Where `under_replicated_partitions` says only that a follower left the ISR, this says how far behind it is while it is still in. [Runbook](runbooks/replica-lag.md). |
| `krabka_broker_replication_throttled_bytes_out_total` | counter | - | `LeaderReplication,name=byte-rate` | KIP-73: replication bytes the leader-side throttle granted to a throttled follower. Flat at the configured rate during a reassignment means the throttle is biting; flat at zero with a reassignment in flight means something else is. |
| `krabka_broker_replication_throttled_bytes_in_total` | counter | - | `FollowerReplication,name=byte-rate` | KIP-73: replication bytes the follower-side throttle granted for a throttled partition. |
| `krabka_broker_replication_throttle_sleeps_total` | counter | - | - | KIP-73 rounds the throttle held back rather than merely capped: a follower fetch whose bucket had nothing left. krabka drops the partition from the round instead of delaying it, so there is no `throttle_time_ms` to observe and this is the series that says the throttle refused. |
| `krabka_broker_replica_lag_max_records` | gauge | - | `ReplicaFetcherManager,name=MaxLag,clientId=Replica` | The largest value `replica_lag_records` carries on this broker, or zero when it leads no partition with a follower. Alert on this series rather than on an aggregate of the per-follower family. [Runbook](runbooks/replica-lag.md). |
| `krabka_broker_isr_shrinks_total` | counter | - | `ReplicaManager,name=IsrShrinksPerSec` | ISR shrinks this broker's maintenance loop proposed. |
| `krabka_broker_isr_expands_total` | counter | - | `ReplicaManager,name=IsrExpandsPerSec` | ISR expands this broker's maintenance loop proposed. |
| `krabka_broker_leader_site_drift_partitions` | gauge | - | - | Partitions this broker leads from a site other than the stretch cluster's preferred leader site. Zero with no `[stretch]` section. Alert on `> 0`. [Runbook](runbooks/leader-site-drift.md). |
| `krabka_broker_witness_role` | gauge | - | - | 1 when the metadata image records `broker.witness` for this node. Confirms the role reached the controller. |

The lag families are sampled every 30 seconds, not incremented. The leader
samples replica lag, and the group coordinator samples consumer-group lag.
Each pass publishes the whole set of series it can justify and releases the
rest, so a follower that left the replica set, or a partition this broker no
longer leads, loses its series at the next pass. Shutdown clears the
families. The metric comments name no threshold for any of them.

## Controller

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_active_controller` | gauge | - | `KafkaController,name=ActiveControllerCount` | 1 on the raft leader, 0 elsewhere. |
| `krabka_broker_controller_leader_changes_total` | counter | - | `KafkaController,name=LeaderElectionRateAndTimeMs` | Controller-leader transitions this broker observed. Alert on a sustained `rate(...) > 0`. [Runbook](runbooks/controller-leader-flapping.md). |
| `krabka_broker_controller_fencing_publications_total` | counter | - | - | Completed broker-fencing publication passes on the controller leader, one per liveness tick. |
| `krabka_broker_unclean_leader_elections_total` | counter | - | `ControllerStats,name=UncleanLeaderElectionsPerSec` | KIP-841 unclean elections this controller drove. A KIP-966 recovery from an eligible leader replica is not counted. Alert on `rate(...) > 0`. [Runbook](runbooks/unclean-leader-election.md). |
| `krabka_broker_ignored_static_voters` | gauge | - | `raft-metrics,name=ignored-static-voters` | Static voters in the configuration that the quorum ignores at `kraft.version` 1. |
| `krabka_broker_voted_directory` | gauge | `directory_id` | `raft-metrics,name=current-vote-directory-id` | One-hot series for the directory identity voted for in this epoch. |

## Requests

The `api_key` label is the `ApiKey` variant name: `Produce`, `Fetch`,
`DescribeQuorum`. A krabka-private RPC carries its own name. An unknown key
lands under `Unknown`.

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_api_requests_total` | counter | `api_key` | `RequestMetrics,name=RequestsPerSec,request=<api>` | Requests the dispatcher handled. |
| `krabka_broker_unsupported_api_requests_total` | counter | `api_key` | - | Requests rejected because the version is outside the registered range. Alert on `rate(...) > 0`. [Runbook](runbooks/unsupported-api-requests.md). |
| `krabka_broker_request_errors_total` | counter | `api_key` | `RequestMetrics,name=ErrorsPerSec` | Requests whose handler returned an error and the connection was closed. Disjoint from the unsupported-version series. Alert on `rate(...) > 0`. [Runbook](runbooks/request-errors.md). |
| `krabka_broker_request_duration_seconds` | histogram | `api_key` | `RequestMetrics,name=TotalTimeMs` | Full handler round trip: decode, handle, encode. `_count` pairs with `api_requests`. Alert on a sustained p99. [Runbook](runbooks/broker-saturation.md). |
| `krabka_broker_request_local_duration_seconds` | histogram | `api_key` | `RequestMetrics,name=LocalTimeMs` | Time on this broker's own log: the Produce writer round trip summed over partitions, or the Fetch read of every planned partition. |
| `krabka_broker_request_remote_duration_seconds` | histogram | `api_key` | `RequestMetrics,name=RemoteTimeMs` | Time waiting on something other than the local log: the `acks=all` high-watermark gate, the Fetch long poll, and the object-store round trip of a KIP-405 tiered or diskless cold read. High here and low in the local phase is replication or the tier, not disk. |
| `krabka_broker_request_throttle_duration_seconds` | histogram | `api_key` | `RequestMetrics,name=ThrottleTimeMs` | Time asleep in the KIP-219 quota throttle or the inline KIP-599 mutation throttle. Observed once per request the broker accounts for, with an explicit zero when no quota applied. |
| `krabka_broker_quota_throttle_duration_seconds` | histogram | `quota_type` | `kafka.server:type=<Produce,Fetch,Request,ControllerMutation>,name=throttle-time` | Throttle the broker applied, under the quota that produced the largest delay. Unthrottled requests are not observed, so `_count` is the throttled request count. `quota_type` is Kafka's own spelling. Alert on a sustained p99. [Runbook](runbooks/broker-saturation.md). |
| `krabka_broker_in_flight_requests` | gauge | - | `RequestChannel,name=RequestQueueSize` (closest) | Requests being handled now. A sustained climb is a handler stall or a wedged controller. Alert on a floor that stops falling. [Runbook](runbooks/broker-saturation.md). |
| `krabka_broker_fetch_response_drain_total` | counter | `path` | - | Drained Fetch responses by the path their records took to the socket. `path` is one of `sendfile`, `pread`, `vectored`. All three series exist from startup. On a plaintext cluster, `rate(...{path="sendfile"}[5m]) == 0` is a zero-copy regression. [Runbook](runbooks/fetch-zero-copy-regression.md). |
| `krabka_broker_ktls_enabled` | gauge | - | - | 1 when the startup probe found working Linux kTLS, so TLS fetches drain through `sendfile`. 0 with no TLS listener and on every non-Linux target. Constant for the life of the process. |

The three phase families are disjoint and do not cover the total.
`local + remote + throttle <= request_duration_seconds`. The remainder is the
work no phase names: decode, authorization, record validation and response
encode. Compare the `_sum` streams; a remainder that grows is handler CPU. Do
not subtract `_bucket` streams: each family observes its own durations, so one
bucket of a phase says nothing about the same bucket of the total.

## Connections and clients

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_active_connections` | gauge | - | `socket-server-metrics,name=connection-count` | Client connections open now. |
| `krabka_broker_connection_closes_total` | counter | `reason` | `Selector,name=connection-close-total`; `expired-connections-killed-count` for the idle arm | Connections the broker closed on its own. `reason` is one of `idle`, `sasl_session_expired`, `decode_error`, `peer_closed`. A connection dropped because of a request it did read is counted by that request's family instead. [Runbook](runbooks/idle-connection-closes.md). |
| `krabka_broker_client_software_versions_total` | counter | `software_name`, `software_version` | `socket-server-metrics,clientSoftwareName=...,clientSoftwareVersion=...,name=connections` | KIP-511 accepted v3+ `ApiVersions` handshakes. |
| `krabka_broker_successful_authentication_total` | counter | `mechanism` | `Selector,name=successful-authentication-total` | `SaslAuthenticate` frames that reached an authenticated state. `mechanism` is the wire name: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `GSSAPI`. [Runbook](runbooks/authentication-failures.md). |
| `krabka_broker_failed_authentication_total` | counter | `mechanism` | `Selector,name=failed-authentication-total` | `SaslAuthenticate` frames that returned an error. The `mechanism` label carries the same set, `GSSAPI` included. `ILLEGAL_SASL_STATE` rejects with no prior handshake land under `Unknown`. Alert on a sustained `rate(...)`. [Runbook](runbooks/authentication-failures.md). |
| `krabka_broker_incremental_fetch_sessions` | gauge | - | `FetchSessionCache,name=NumIncrementalFetchSessions` | KIP-227 live fetch sessions. |
| `krabka_broker_incremental_fetch_session_evictions_total` | counter | - | `FetchSessionCache,name=IncrementalFetchSessionEvictionsPerSec` | Sessions evicted to make room. |
| `krabka_broker_incremental_fetch_partitions_cached` | gauge | - | `FetchSessionCache,name=NumIncrementalFetchPartitionsCached` | Partitions held across every live session. |
| `krabka_broker_client_metrics_otlp_dropped_total` | counter | - | - | KIP-714 client-metric batches dropped because the OTLP queue was full or closed. |
| `krabka_broker_client_metrics_otlp_failed_total` | counter | - | - | KIP-714 export attempts the collector rejected or the transport failed. |

## Tiered storage

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_tiered_storage_rlmm_topic_backed` | gauge | - | - | KIP-405: 1 once the topic-backed `RemoteLogMetadataManager` is in place, 0 on the fail-closed placeholder. Alert on `min_over_time(...[5m]) == 0` for a cluster that asked for it. [Runbook](runbooks/rlmm-bootstrap-stuck.md). |
| `krabka_broker_tiered_storage_rlmm_bootstrap_attempts_total` | counter | - | - | Bootstrap attempts. Climbs while stuck, flat once the gauge flips to 1. |
| `krabka_broker_worm_manifests_sealed_total` | counter | - | - | KFC-5: WORM manifests the archive sealed and returned a valid chain receipt for, one per segment copied into a write-once archive. Flat while a WORM cluster tiers means the chain has stopped growing. |
| `krabka_broker_worm_manifest_seal_failures_total` | counter | - | - | KFC-5: write-once segment copies that ended with no usable manifest -- the copy failed, the task panicked, or the receipt was missing or did not match the requested chain position. Alert on `rate(...[5m]) > 0`. |
| `krabka_broker_remote_copy_bytes_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteCopyBytesPerSec` | Bytes of finished segment copies that reached the remote tier. |
| `krabka_broker_remote_fetch_bytes_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteFetchBytesPerSec` | Bytes the remote tier served to a `Fetch` the local log could not answer. |
| `krabka_broker_remote_copy_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteCopyRequestsPerSec` | Segment copies attempted, whatever their outcome. |
| `krabka_broker_remote_fetch_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteFetchRequestsPerSec` | Cold-tier reads attempted, whatever their outcome. |
| `krabka_broker_remote_delete_requests_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteDeleteRequestsPerSec` | Remote segment deletions attempted, whatever their outcome. |
| `krabka_broker_remote_copy_errors_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteCopyErrorsPerSec` | Copies that did not finish. Alert on `rate(...[5m]) > 0`. [Runbook](runbooks/remote-copy-lag.md). |
| `krabka_broker_remote_fetch_errors_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteFetchErrorsPerSec` | Cold-tier reads the tier refused or failed. A consumer reading history sees these as `OFFSET_OUT_OF_RANGE`. |
| `krabka_broker_remote_delete_errors_total` | counter | `topic` | `BrokerTopicMetrics,name=RemoteDeleteErrorsPerSec` | Deletions that did not finish, including a metadata listing the pass could not read. |
| `krabka_broker_remote_copy_lag_segments` | gauge | `topic` | `BrokerTopicMetrics,name=RemoteCopyLagSegments` | Sealed local segments this broker has not finished copying. Climbing means the tier is not keeping up. [Runbook](runbooks/remote-copy-lag.md). |
| `krabka_broker_remote_copy_lag_bytes` | gauge | `topic` | `BrokerTopicMetrics,name=RemoteCopyLagBytes` | Total size of those segments; the local disk the tier is not yet relieving. |
| `krabka_broker_remote_delete_lag_segments` | gauge | `topic` | `BrokerTopicMetrics,name=RemoteDeleteLagSegments` | Remote segments a retention pass has decided to remove and not yet removed. |
| `krabka_broker_remote_delete_lag_bytes` | gauge | `topic` | `BrokerTopicMetrics,name=RemoteDeleteLagBytes` | Total size of those segments; object-store space retention has not reclaimed. |
| `krabka_broker_remote_log_reader_task_queue_size` | gauge | - | `RemoteLogManager,name=RemoteLogReaderTaskQueueSize` | Cold-tier reads waiting for a slot in the bounded reader pool. |
| `krabka_broker_remote_log_reader_avg_idle_percent` | gauge | - | `RemoteLogManager,name=RemoteLogReaderAvgIdlePercent` | Share of the reader pool's slots that are free. Zero for long means every cold read is queueing. |
| `krabka_broker_remote_log_reader_fetch_duration_seconds` | histogram | - | `RemoteLogManager,name=RemoteLogReaderFetchRateAndTimeMs` | How long each cold read held its reader slot. |
| `krabka_broker_remote_log_reader_rejected_total` | counter | - | - | Cold reads refused because the reader pool's pending queue was full. The partition is answered with an error rather than queued behind the tier. |
| `krabka_broker_remote_index_cache_hits_total` | counter | - | - | Segment-index lookups served from `<log.dir>/remote-log-index-cache` without a download. |
| `krabka_broker_remote_index_cache_misses_total` | counter | - | - | Segment-index lookups that downloaded the index object. A hit ratio near zero means the cache is too small for the working set. |
| `krabka_broker_remote_index_cache_evictions_total` | counter | - | - | Cache entries dropped to stay inside `remote_storage.index_cache_size`. |
| `krabka_broker_remote_index_cache_bytes` | gauge | - | - | Bytes the index cache currently holds. |
| `krabka_broker_remote_index_cache_entries` | gauge | - | - | Entries the index cache currently holds. |

## Diskless WAL

A `krabka.diskless=true` topic replicates its write-ahead log to a quorum of
WAL voters and flushes it to the object store. `topic_id` is the topic UUID,
so a delete and recreate cycle gets a new series. Kafka has no counterpart for
any of these.

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_diskless_wal_durable_watermark` | gauge | `topic_id`, `partition` | - | Quorum-durable offset of each shard this broker leads. |
| `krabka_broker_diskless_wal_voter_lag` | gauge | `topic_id`, `partition`, `voter` | - | Leader log end minus the voter's durable offset. [Runbook](runbooks/diskless-wal-voter-lag.md). |
| `krabka_broker_diskless_wal_quorum_loss_events_total` | counter | - | - | Leader-side acknowledgements that could not form a quorum. Alert on `rate(...) > 0`. [Runbook](runbooks/diskless-wal-quorum-loss.md). |
| `krabka_broker_diskless_wal_flush_attempts_total` | counter | - | - | Non-empty WAL objects submitted to the object store. |
| `krabka_broker_diskless_wal_flush_bytes_total` | counter | - | - | Bytes written as WAL objects. |
| `krabka_broker_diskless_wal_flush_failures_total` | counter | - | - | Flushes that failed after an attempt began. [Runbook](runbooks/diskless-wal-flush-failures.md). |
| `krabka_broker_diskless_wal_index_decode_failures_total` | counter | - | - | Index records rejected because the payload or format version is invalid. Refuse reads until replay is rebuilt. |
| `krabka_broker_diskless_wal_index_projection_lag` | gauge | `topic_id`, `partition` | - | Durable offsets the committed object index does not cover yet. |
| `krabka_broker_diskless_wal_trim_frontier` | gauge | `topic_id`, `partition` | - | Local log-start offset after trimming. The gap to the durable watermark is the hot tail. |
| `krabka_broker_diskless_wal_expired_ranges_total` | counter | - | - | Committed index ranges tombstoned by `retention.ms`, `retention.bytes`, or a `DeleteRecords` floor. |
| `krabka_broker_diskless_wal_cold_read_hits_total` | counter | - | - | Cold reads served from the object store. |
| `krabka_broker_diskless_wal_cold_read_misses_total` | counter | - | - | Cold reads with no matching committed index entry. |
| `krabka_broker_diskless_wal_cold_read_errors_total` | counter | - | - | Cold reads that failed in the object store. [Runbook](runbooks/diskless-wal-flush-failures.md). |

## Coordinators

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_consumer_group_lag_records` | gauge | `group_id`, `topic`, `partition` | - | Records a consumer group this broker coordinates has yet to consume from one partition: the high watermark minus the group's committed offset. Classic and KIP-848 groups both report. JVM Kafka does not export this from the broker; it is the `LAG` column `kafka-consumer-groups --describe` computes on the client. A series exists only for a partition the group committed an offset for, and is released when the group is deleted, moves coordinator, or the topic is deleted. [Runbook](runbooks/consumer-group-lag.md). |
| `krabka_broker_share_group_backlog` | gauge | `group_id`, `topic`, `partition` | - | KIP-932 records waiting for acquisition in the share-group partition. |
| `krabka_broker_share_group_dlq_records_total` | counter | `group_id` | `ShareGroupMetrics,name=DeadLetterQueueRecordCount` | KIP-1191 dead-letter records this broker wrote to the group's dead-letter queue. Only a group that sets `errors.deadletterqueue.topic.name`, at `share.version` 2, has a series. It is released when `DeleteGroups` deletes the group on the broker that coordinates it. A broker that only leads the group's share partitions keeps its series until it restarts, as Kafka keeps its meters until the broker closes, and a broker that stops coordinating the group keeps them too. |
| `krabka_broker_share_group_dlq_produce_requests_total` | counter | `group_id` | `ShareGroupMetrics,name=DeadLetterQueueTotalProduceRequestsPerSec` | Attempts to produce the group's dead-letter records: one for each round of each write for each attempt, counted when a request takes the round, as Kafka counts it, not one for each coalesced produce request. |
| `krabka_broker_share_group_dlq_failed_produce_requests_total` | counter | `group_id` | `ShareGroupMetrics,name=DeadLetterQueueFailedProduceRequestsPerSec` | Dead-letter writes of the group that failed for good: a produce that a broker refused, or that ran out of its five attempts. The record is archived regardless. Alert on `rate(...) > 0`. |
| `krabka_broker_barrier_epochs_started_total` | counter | `group` | - | Barrier epochs the coordinator started. |
| `krabka_broker_barrier_epochs_committed_total` | counter | `group` | - | Epochs whose marker reached every partition of the group. |
| `krabka_broker_barrier_epochs_published_partial_total` | counter | `group` | - | Epochs whose cut names a partition that got no marker. Alert on `rate(...) > 0`. [Runbook](runbooks/barrier-partial-cut.md). |
| `krabka_broker_barrier_injection_duration_seconds` | histogram | `group` | - | Seconds from the injection-start record to the published cut. Graph the p99 against `barrier_injection_timeout`. |
| `krabka_broker_barrier_latest_epoch` | gauge | `group` | - | Epoch of the newest published cut. Flat beside a live injection interval means injection stopped. |
| `krabka_broker_barrier_markers_written_total` | counter | `topic` | - | Barrier markers this broker appended. Markers survive compaction. |
| `krabka_broker_barrier_groups_coordinated` | gauge | - | - | Barrier groups this broker coordinates. |
| `krabka_broker_delivery_watermark` | gauge | `topic`, `partition` | - | KFC-1 first offset not yet visible on a `delivery.mode=scheduled` partition. Only scheduled partitions report. |
| `krabka_broker_delivery_pending_records` | gauge | `topic`, `partition` | - | KFC-1 records durable but not visible: log end minus the watermark. |
| `krabka_broker_delivery_activation_lateness_seconds` | histogram | - | - | KFC-1 seconds past a batch's activation deadline before it became visible. A healthy broker reports zero. A rising tail says the bound is not honest, or that the scheduler is starved of CPU. [Runbook](runbooks/delivery-activation-lateness.md). |
| `krabka_broker_delivery_scheduler_wakeups_total` | counter | - | - | KFC-1 scheduler wakeups. Separates "never ran" from "ran late". |
| `krabka_broker_delivery_clock_uncertainty_seconds` | gauge | - | - | KFC-8 clock bound the broker declares and adds before a batch activates. Compare a measured clock error against it. |

## Policies and audit

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_schema_validation_rejections_total` | counter | `topic`, `reason` | - | KFC-7 records rejected, one per record. `reason` is one of `unframed`, `unknown_id`, `wrong_subject`, `body_mismatch`, `registry_unavailable`. Alert on `rate(...{reason="registry_unavailable"}[5m]) > 0`: with the default `fail_open = false` those produces are refused. [Runbook](runbooks/schema-registry-unavailable.md). |
| `krabka_broker_schema_validation_cache_hits_total` | counter | - | - | KFC-7 schema lookups answered from the local cache. |
| `krabka_broker_schema_validation_cache_misses_total` | counter | - | - | KFC-7 lookups that cost a registry round trip. |
| `krabka_broker_topic_freeze_rejections_total` | counter | `topic` | - | KFC-9 Produce partition rows refused by a write freeze, before the batch is parsed. |
| `krabka_broker_topic_freezes_active` | gauge | - | - | KFC-9 live freeze registry entries. One prefix entry covers a namespace. |
| `krabka_broker_break_glass_proposals` | gauge | `state` | - | KFC-9 proposals by state: `pending`, `approved`, `expired`, `consumed`. |
| `krabka_broker_break_glass_refusals_total` | counter | `action` | - | KFC-9 privileged transitions refused for want of an approved proposal. A steady rate is normal. |
| `krabka_broker_break_glass_bypassed_total` | counter | `action` | - | KFC-9 privileged transitions that ran without an approved proposal. Alert on `rate(...) > 0`. [Runbook](runbooks/break-glass-bypassed.md). |
| `krabka_broker_authorization_denied_total` | counter | `operation`, `resource_type` | the `Principal ... is Denied Operation ...` line on the `kafka.authorizer.logger` log4j logger; Kafka exports no metric | Authorization decisions that came back Deny. Both labels are the `AclOperation` / `ResourceType` spelling, so cardinality is bounded. Counted whether or not audit is enabled, so it is the denial signal a cluster with `audit.enabled=false` still has. Alert on `rate(...) > 0` sustained. [Runbook](runbooks/authorization-denials.md). |
| `krabka_broker_audit_events_total` | counter | - | - | Audit records written to `__krabka_audit`. |
| `krabka_broker_audit_write_failures_total` | counter | - | - | Audit records that failed to write. Alert on `rate(...) > 0`. [Runbook](runbooks/audit-write-failures.md). |
| `krabka_broker_audit_spool_depth` | gauge | - | - | Audit records buffered in the durable spool. |
| `krabka_broker_audit_spool_bytes` | gauge | - | - | Bytes buffered in the durable spool. |
| `krabka_broker_audit_records_spooled_total` | counter | - | - | Records diverted to the spool on a write failure. |
| `krabka_broker_audit_records_replayed_total` | counter | - | - | Records drained from the spool back to the topic. |
| `krabka_broker_audit_records_dropped_total` | counter | - | - | Records lost because the channel or the spool was full. Alert on `rate(...) > 0`. |

The `action` label on the break-glass families is one of `thaw_topic_freeze`,
`unclean_elect_leaders`, `unclean_recovery`, `unregister_broker`,
`cancel_reassignment`, `delete_topic` and `delete_records`.

## Produce deduplication

KFC-10 drops records at produce time whose identity is already in the
partition's dedup window. A per-partition gauge appears when the partition's
dedup state first reports, and a counter series on its first non-zero
increment. A partition's series are released when this broker stops hosting
it, with the rest of its per-partition series.

| Series | Type | Labels | Replaces | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `krabka_broker_dedup_records_dropped_total` | counter | `topic`, `partition` | - | KFC-10 records dropped as duplicates. A dropped record never reaches the log, so this is its only trace. |
| `krabka_broker_dedup_records_unindexed_total` | counter | `topic`, `partition` | - | KFC-10 records admitted without being indexed because the partition index or the broker-wide budget was full. Their duplicates are not detected. Alert on `rate(...) > 0` and raise `dedup_max_entries_per_partition` or `dedup_max_entries`. |
| `krabka_broker_dedup_records_skew_clamped_total` | counter | `topic`, `partition` | - | KFC-10 records whose timestamp was ahead of the broker clock and was clamped before reaching the index. Non-zero means producer clock skew. |
| `krabka_broker_dedup_capacity_evicted_entries_total` | counter | `topic`, `partition` | - | KFC-10 still-live identities dropped to make room at the entry limits. Non-zero means the effective window is narrower than configured. |
| `krabka_broker_dedup_batches_rebuilt_total` | counter | `topic`, `partition` | - | KFC-10 produce batches re-encoded and recompressed because some but not all of their records were duplicates. This rate drives dedup's produce-path cost. |
| `krabka_broker_dedup_index_rebuilds_total` | counter | `topic`, `partition` | - | KFC-10 rebuilds of the partition's index from the log, after a restart, a truncation, or a dedup config change. |
| `krabka_broker_dedup_index_entries` | gauge | `topic`, `partition` | - | KFC-10 identities in this partition replica's dedup index. |
| `krabka_broker_dedup_broker_index_entries` | gauge | - | - | KFC-10 identities across every dedup index on this broker. At `dedup_broker_index_max_entries`, records are admitted unindexed. |
| `krabka_broker_dedup_broker_index_max_entries` | gauge | - | - | KFC-10 broker-wide limit on dedup index identities, `dedup_max_entries`. |

## The contract check

[`grafana-dashboard.json`](grafana-dashboard.json) and
[`alert-rules.yaml`](alert-rules.yaml) name only series in this table. Two
checks enforce that:

- `crates/broker/tests/metrics_contract.rs` builds the registry, gives every
  family one label set, encodes the body the way `/metrics` does, and checks
  every `krabka_broker_*` token in the two files against it. It also checks
  that [`metrics-body.txt`](metrics-body.txt) equals that body, so the
  checked-in copy cannot go stale. `bazel test //crates/broker:metrics_contract_test`
  runs it.
- `aspect check-metrics-contract` does the
  same name check in the CI docs job, which has no Rust toolchain, against
  the checked-in copy.

When a family is added or renamed, run the suite with
`KRABKA_METRICS_BODY_OUT=docs/operations/metrics-body.txt` to regenerate the
copy, then update this table and the two files.
