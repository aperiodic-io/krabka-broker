//! Expiry of quota buckets, and of the metric series they carry.
//!
//! A bucket is created the first time a principal and client id are charged
//! against a quota, and nothing removed one. A cluster that sees ten thousand
//! distinct client ids over a week therefore kept ten thousand buckets and, as
//! of KIP-599's per-entity throttle series, ten thousand
//! `quota_entity_throttle_seconds` label sets, for the life of the process --
//! long after the clients behind them stopped connecting.
//!
//! Kafka expires a client's quota sensors after an hour of inactivity
//! (`ClientQuotaManager`'s `InactiveSensorExpirationTimeSeconds`). This is that
//! sweep: it drops every bucket untouched for [`INACTIVE_EXPIRATION`] and
//! releases the metric series each one had materialised, so an inactive
//! tenant's label set leaves the `/metrics` body instead of growing it forever.
//!
//! Reviving a client that was expired costs one bucket allocation and starts it
//! at full burst, which is the same state a first-seen client gets. An hour of
//! silence is longer than any quota window, so the burst it is handed back is
//! one it would have refilled to anyway.

use {
    super::buckets::QuotaBuckets,
    crate::metrics::{BrokerMetrics, QuotaType},
    krabka_units::{Time, convert::TimeExt as _, minutes},
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
    tracing::debug,
};

/// How long a bucket may go untouched before the sweep drops it. Kafka's
/// `ClientQuotaManager.InactiveSensorExpirationTimeSeconds`.
pub(crate) const INACTIVE_EXPIRATION: Time = minutes(60);

/// How often the sweep runs.
///
/// It is not a configuration knob, and Kafka does not expose one either: the
/// cadence only decides how long an expired label set lingers past its hour,
/// and a minute of lingering costs one map entry.
const SWEEP_INTERVAL: Time = minutes(1);

/// Drops inactive quota buckets and their metric series until cancelled.
pub(crate) async fn run(
    buckets: Arc<QuotaBuckets>,
    metrics: BrokerMetrics,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(SWEEP_INTERVAL.to_std());
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            () = shutdown.cancelled() => return,
        }
        sweep(&buckets, &metrics, INACTIVE_EXPIRATION);
    }
}

/// One sweep: expire the idle buckets and release what they published.
fn sweep(buckets: &QuotaBuckets, metrics: &BrokerMetrics, max_age: Time) {
    for (quota_key, user, client_id) in buckets.expire_inactive(max_age.to_std()) {
        // A quota key the metric has no `QuotaType` for published no series,
        // so there is nothing to release. It cannot happen today -- every key
        // a bucket is created under is one of the five -- and dropping the
        // bucket is still right if it ever does.
        if let Some(quota_type) = QuotaType::from_config_key(&quota_key) {
            metrics.evict_quota_entity_series(quota_type, user.clone(), client_id.clone());
        }
        debug!(
            quota_key,
            ?user,
            ?client_id,
            "quota expiry: dropped an inactive bucket"
        );
    }
}

#[cfg(test)]
mod tests;
