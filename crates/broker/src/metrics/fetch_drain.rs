//! Zero-copy fetch accounting: the per-path drain counter the fetch writer
//! bumps once per response, and the startup gauge that says whether kTLS is
//! carrying the TLS half of it.
//!
//! The two belong together because neither answers the operator's question
//! alone. `ktls_enabled` says the offload is available; the drain counter says
//! it is being used.

use super::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel};

impl BrokerMetrics {
    /// Account one drained Fetch response on the path its records regions took.
    ///
    /// Called from the fetch writer's drain, after the last op reached the
    /// socket, with the path the drain observed rather than the one the
    /// handler intended. That is the point of the series: the two disagreeing
    /// is exactly the regression it exists to catch.
    pub fn record_fetch_response_drain(&self, path: FetchDrainPath) {
        self.fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .inc();
    }

    /// Publish the result of the startup kTLS probe. Called once, during
    /// startup, with `Broker::ktls_enabled`. It is `false` on a broker with no
    /// TLS listener and on every non-Linux target.
    pub fn set_ktls_enabled(&self, enabled: bool) { self.ktls_enabled.set(i64::from(enabled)); }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// Encode the registry to the Prometheus text format an operator scrapes.
    async fn scrape(metrics: &BrokerMetrics) -> String {
        let mut buf = String::new();
        let registry = metrics.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &registry)
            .expect("encode the registry");
        buf
    }

    /// A dashboard that names one path must find its series on a broker that
    /// has served no fetch, and on a target where that path is unreachable.
    #[tokio::test]
    async fn every_drain_path_scrapes_at_zero_before_the_first_fetch() {
        let metrics = BrokerMetrics::new();

        let buf = scrape(&metrics).await;

        for path in ["sendfile", "vectored", "pread"] {
            let needle = format!("krabka_broker_fetch_response_drain_total{{path=\"{path}\"}} 0");
            assert!(buf.contains(&needle), "missing {needle} in:\n{buf}");
        }
    }

    /// The registered name plus the counter suffix is what an alert rule
    /// spells and the label value is what it groups by, so both belong in the
    /// assertion. Each path gets a distinct count, so a label that resolves to
    /// the wrong series reads as the wrong number rather than as a passing
    /// test.
    #[tokio::test]
    async fn each_drain_path_counts_under_its_own_label() {
        let cases = [
            ("sendfile", FetchDrainPath::Sendfile, 3),
            ("vectored", FetchDrainPath::Vectored, 1),
            ("pread", FetchDrainPath::Pread, 2),
        ];
        // A path added to the enum needs a row here, so the closed label set
        // stays covered.
        assert!(cases.len() == FetchDrainPath::ALL.len());

        let metrics = BrokerMetrics::new();
        for (_, path, count) in cases {
            for _ in 0..count {
                metrics.record_fetch_response_drain(path);
            }
        }

        let buf = scrape(&metrics).await;

        for (label, _, count) in cases {
            let needle =
                format!("krabka_broker_fetch_response_drain_total{{path=\"{label}\"}} {count}");
            assert!(buf.contains(&needle), "missing {needle} in:\n{buf}");
        }
    }

    /// The gauge reads `0` on a broker that never probed kTLS, which is every
    /// broker with no TLS listener and every non-Linux target, and `1` only
    /// after a probe that succeeded.
    #[tokio::test]
    async fn ktls_gauge_starts_at_zero_and_follows_the_probe() {
        for (enabled, expected) in [(false, 0), (true, 1)] {
            let metrics = BrokerMetrics::new();
            assert!(metrics.ktls_enabled.get() == 0);

            metrics.set_ktls_enabled(enabled);

            assert!(metrics.ktls_enabled.get() == expected);
            let buf = scrape(&metrics).await;
            let needle = format!("krabka_broker_ktls_enabled {expected}");
            assert!(buf.contains(&needle), "missing {needle} in:\n{buf}");
        }
    }
}
