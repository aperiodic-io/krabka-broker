//! KIP-405 tiered-storage traffic and lag, and the KIP-73 replication
//! byte-rate counters.
//!
//! Kafka publishes the tier's traffic under
//! `kafka.server:type=BrokerTopicMetrics`, per topic and in aggregate:
//! `RemoteCopyBytesPerSec`, `RemoteFetchBytesPerSec`, the three
//! `Remote*RequestsPerSec` meters, the three `Remote*ErrorsPerSec` meters and
//! the four `Remote*Lag*` gauges. The names are in
//! `org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics`. Without
//! them, an operator whose object store starts returning 503s learns about it
//! from consumer lag rather than from the broker.
//!
//! Lag is what the tier has not caught up on: for the copy path, the sealed
//! local segments past the highest finished copy; for the delete path, the
//! remote segments a retention pass has decided to remove and not yet
//! removed. Kafka records both at the top of the task that will do the work,
//! which is where these are recorded too, so a stuck tier shows a lag that
//! climbs rather than a rate that merely stops.
//!
//! The KIP-73 counters are the byte-rate `ReplicationQuotaManager` publishes
//! as `kafka.server:type=LeaderReplication,name=byte-rate` and its
//! `FollowerReplication` twin: the measured throttled-replication rate an
//! operator watches during a reassignment to see whether the throttle is
//! biting. They are counters here rather than rates because Prometheus takes
//! the derivative itself.

use {
    super::{BrokerMetrics, TopicLabel},
    std::sync::Arc,
};

/// Which of the tier's three paths a count belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteTierPath {
    /// A segment going out to the remote tier.
    Copy,
    /// A read served from the remote tier.
    Fetch,
    /// A segment being removed from the remote tier.
    Delete,
}

impl BrokerMetrics {
    /// Counts one attempt on one of the tier's paths, whatever its outcome.
    ///
    /// Kafka's `Remote{Copy,Fetch,Delete}RequestsPerSec`. Recorded on entry,
    /// so the errors below are a subset of these and the ratio of the two is
    /// the failure rate.
    pub fn record_remote_request(&self, path: RemoteTierPath, topic: &str) {
        let family = match path {
            RemoteTierPath::Copy => &self.remote_copy_requests_total,
            RemoteTierPath::Fetch => &self.remote_fetch_requests_total,
            RemoteTierPath::Delete => &self.remote_delete_requests_total,
        };
        self.count_topic(family, Arc::from(topic), 1);
    }

    /// Counts one failed attempt on one of the tier's paths.
    ///
    /// Kafka's `Remote{Copy,Fetch,Delete}ErrorsPerSec`.
    pub fn record_remote_error(&self, path: RemoteTierPath, topic: &str) {
        let family = match path {
            RemoteTierPath::Copy => &self.remote_copy_errors_total,
            RemoteTierPath::Fetch => &self.remote_fetch_errors_total,
            RemoteTierPath::Delete => &self.remote_delete_errors_total,
        };
        self.count_topic(family, Arc::from(topic), 1);
    }

    /// Accounts bytes moved on the copy or fetch path.
    ///
    /// Kafka's `RemoteCopyBytesPerSec` and `RemoteFetchBytesPerSec`. The
    /// delete path moves no bytes and has no counterpart; a zero-byte move is
    /// not recorded, so an empty segment cannot materialise a series.
    pub fn record_remote_bytes(&self, path: RemoteTierPath, topic: &str, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let family = match path {
            RemoteTierPath::Copy => &self.remote_copy_bytes_total,
            RemoteTierPath::Fetch => &self.remote_fetch_bytes_total,
            RemoteTierPath::Delete => return,
        };
        self.count_topic(family, Arc::from(topic), bytes);
    }

    /// Sets what the copy path has left to do for one topic.
    ///
    /// Kafka's `RemoteCopyLagBytes` / `RemoteCopyLagSegments`, recorded by
    /// `RLMCopyTask.copyLogSegmentsToRemote` before each round: the sealed
    /// local segments past the highest `CopySegmentFinished` offset, and their
    /// total size.
    pub fn set_remote_copy_lag(&self, topic: &str, segments: u64, bytes: u64) {
        let label = TopicLabel {
            topic: Arc::from(topic),
        };
        self.remote_copy_lag_segments
            .get_or_create(&label)
            .set(i64::try_from(segments).unwrap_or(i64::MAX));
        self.remote_copy_lag_bytes
            .get_or_create(&label)
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.track_topic_series(&label);
    }

    /// Sets what the delete path has left to do for one topic.
    ///
    /// Kafka's `RemoteDeleteLagBytes` / `RemoteDeleteLagSegments`, recorded by
    /// `RLMExpirationTask`: the remote segments a retention pass has decided
    /// to remove, and their total size.
    pub fn set_remote_delete_lag(&self, topic: &str, segments: u64, bytes: u64) {
        let label = TopicLabel {
            topic: Arc::from(topic),
        };
        self.remote_delete_lag_segments
            .get_or_create(&label)
            .set(i64::try_from(segments).unwrap_or(i64::MAX));
        self.remote_delete_lag_bytes
            .get_or_create(&label)
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.track_topic_series(&label);
    }

    /// Accounts replication bytes the leader-side KIP-73 throttle granted.
    ///
    /// Kafka's `kafka.server:type=LeaderReplication,name=byte-rate`. A
    /// reassignment that is not moving is either a throttle that is biting --
    /// this counter flat at the configured rate -- or something else, and the
    /// two are indistinguishable without it.
    pub fn record_replication_throttled_out(&self, bytes: u64) {
        self.replication_throttled_bytes_out_total.inc_by(bytes);
    }

    /// Accounts replication bytes the follower-side KIP-73 throttle granted.
    ///
    /// Kafka's `kafka.server:type=FollowerReplication,name=byte-rate`.
    pub fn record_replication_throttled_in(&self, bytes: u64) {
        self.replication_throttled_bytes_in_total.inc_by(bytes);
    }

    /// Counts one replication fetch the KIP-73 throttle held back entirely,
    /// because its bucket had nothing left to grant.
    ///
    /// Kafka's throttle delays a fetch; krabka's drops the partition from the
    /// round and retries, so there is no delay to observe and this is the
    /// series that says the throttle refused rather than merely capped.
    pub fn record_replication_throttle_sleep(&self) {
        self.replication_throttle_sleeps_total.inc();
    }
}

#[cfg(test)]
mod tests;
