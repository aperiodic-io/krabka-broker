//! Per-API request accounting: the dispatched, unsupported and errored
//! request counters, the request-latency histograms — the end-to-end total and
//! the local, remote and throttle phases beside it — the applied-throttle
//! histogram keyed by quota, and the resolution of a wire `api_key` to the
//! bounded label those families share.

use {
    super::{
        ApiKeyLabel, BrokerMetrics, ConnectionCloseReason, ConnectionCloseReasonLabel, QuotaType,
        QuotaTypeLabel, UNKNOWN_LABEL,
    },
    crate::{
        handlers::{
            ALTER_BARRIER_GROUPS_API_KEY, ApiKeyCode, DESCRIBE_BARRIER_GROUPS_API_KEY,
            KRABKA_PRIVATE_API_KEY_FLOOR, LIST_BARRIER_CUTS_API_KEY, TRIGGER_BARRIER_API_KEY,
            WRITE_BARRIER_MARKERS_API_KEY,
        },
        quota::QuotaDelay,
    },
    krabka_units::{Time, convert::TimeExt},
};

/// The `api_key` label every per-API family in this module shares.
fn api_key_label(api_key: ApiKeyCode) -> ApiKeyLabel {
    ApiKeyLabel {
        api_key: api_key_label_name(api_key).to_string(),
    }
}

impl BrokerMetrics {
    /// Account one dispatched request for `api_key`. The label is the
    /// human-readable name from `api_key_label_name`; unknown keys fold under
    /// `"Unknown"`.
    pub fn record_api_request(&self, api_key: ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.api_requests.get_or_create(&lbl).inc();
    }

    /// Account one request the dispatcher rejected because its version is
    /// outside the registered range: by closing the connection, or for
    /// `ApiVersions` with `UNSUPPORTED_VERSION`. Mirrors the labelling of
    /// `record_api_request`.
    pub fn record_unsupported_api_request(&self, api_key: ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.unsupported_api_requests.get_or_create(&lbl).inc();
    }

    /// Observe the wall-clock handling latency for one dispatched
    /// request on the `request_duration_seconds{api}` histogram. `api_key`
    /// is resolved to the same human-readable label as
    /// `record_api_request` (unknown keys fold under `"Unknown"`), so the
    /// two families share one label set. Called from the dispatch path once
    /// per frame with the elapsed seconds of the full handler round-trip.
    pub fn observe_request_duration(&self, api_key: i16, seconds: f64) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_duration_seconds
            .get_or_create(&lbl)
            .observe(seconds);
    }

    /// Account one closed client connection under the reason the
    /// per-connection serve loop stopped reading frames. The label set is the
    /// closed [`ConnectionCloseReason`] enum, so the family holds at most one
    /// series per reason.
    pub fn record_connection_close(&self, reason: ConnectionCloseReason) {
        self.connection_closes
            .get_or_create(&ConnectionCloseReasonLabel { reason })
            .inc();
    }

    /// Observe the seconds one request spent on this broker's own log, on
    /// `request_local_duration_seconds{api_key}`. Labelled exactly like
    /// `observe_request_duration`, so the phase and the total share one label
    /// set and an operator can divide one by the other.
    pub fn observe_request_local_duration(&self, api_key: ApiKeyCode, seconds: f64) {
        self.request_local_duration_seconds
            .get_or_create(&api_key_label(api_key))
            .observe(seconds);
    }

    /// Observe the seconds one request spent waiting on another broker, on
    /// `request_remote_duration_seconds{api_key}`. Labelled like
    /// `observe_request_duration`.
    pub fn observe_request_remote_duration(&self, api_key: ApiKeyCode, seconds: f64) {
        self.request_remote_duration_seconds
            .get_or_create(&api_key_label(api_key))
            .observe(seconds);
    }

    /// Observe the seconds one request slept in the KIP-219 quota throttle, on
    /// `request_throttle_duration_seconds{api_key}`. Callers observe on every
    /// request they account a quota for, passing zero when no quota delayed
    /// it, so the family counts accounted requests rather than only throttled
    /// ones.
    pub fn observe_request_throttle_duration(&self, api_key: ApiKeyCode, seconds: f64) {
        self.request_throttle_duration_seconds
            .get_or_create(&api_key_label(api_key))
            .observe(seconds);
    }

    /// Observe one applied throttle on `quota_throttle_duration_seconds`,
    /// attributed to the [`QuotaType`] whose delay the broker slept for.
    ///
    /// A zero delay is *not* observed: this family answers "which quota is
    /// holding my clients back, and for how long", so its `_count` is the
    /// number of throttled requests. The caller checks the delay before
    /// deciding which quota won, so it also decides whether to call at all.
    pub fn observe_quota_throttle(&self, quota_type: QuotaType, seconds: f64) {
        self.quota_throttle_duration_seconds
            .get_or_create(&QuotaTypeLabel { quota_type })
            .observe(seconds);
    }
}

/// A single quota charge evaluated by [`BrokerMetrics::record_applied_throttle`].
#[derive(Debug, Clone)]
pub(crate) struct QuotaCharge {
    pub quota_type: QuotaType,
    pub delay: Time,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

impl From<(QuotaType, Time)> for QuotaCharge {
    fn from((quota_type, delay): (QuotaType, Time)) -> Self {
        Self {
            quota_type,
            delay,
            user: None,
            client_id: None,
        }
    }
}

impl From<(QuotaType, QuotaDelay)> for QuotaCharge {
    fn from((quota_type, qd): (QuotaType, QuotaDelay)) -> Self {
        Self {
            quota_type,
            delay: qd.delay,
            user: qd.user,
            client_id: qd.client_id,
        }
    }
}

impl BrokerMetrics {
    /// Resolve and record the throttle one request applies, and return the
    /// delay the caller must sleep for.
    ///
    /// `charged` is every quota the request was charged, paired with the delay
    /// that quota asked for. KIP-219 makes the request sleep for the largest
    /// of them, so that delay lands on
    /// `request_throttle_duration_seconds{api_key}` — with an explicit zero
    /// when no quota asked for anything — and the quota that produced it
    /// labels `quota_throttle_duration_seconds`. Equal delays resolve to the
    /// earlier entry, which is why callers list the quota specific to their
    /// api ahead of the request quota that every api shares.
    ///
    /// The `max` lives here rather than at each call site so that the delay
    /// the broker sleeps for, the delay it reports in `ThrottleTimeMs`, and
    /// the delay it records cannot drift apart. An api that is charged one
    /// quota still calls with a one-entry slice, for that reason: the
    /// KIP-599 admin apis pass only their controller-mutation delay.
    pub(crate) fn record_applied_throttle(
        &self,
        api_key: ApiKeyCode,
        charged: &[QuotaCharge],
    ) -> Time {
        let mut applied: Option<&QuotaCharge> = None;
        for c in charged {
            if applied.is_none_or(|largest| c.delay > largest.delay) {
                applied = Some(c);
            }
        }
        let fallback = QuotaCharge {
            quota_type: QuotaType::Request,
            delay: <Time as TimeExt>::ZERO,
            user: None,
            client_id: None,
        };
        let winning = applied.unwrap_or(&fallback);
        // Kafka's throttle time is a whole number of milliseconds
        // (`QuotaUtils.throttleTime` returns `Math.round`), and it mutes the
        // channel only when that is above zero. A sub-half-millisecond delay
        // is therefore no throttle at all, on the wire and on the socket.
        let delay = <Time as TimeExt>::from_millis(winning.delay.millis_i64());
        self.observe_request_throttle_duration(api_key, delay.secs_f64());
        if delay > <Time as TimeExt>::ZERO {
            self.observe_quota_throttle(winning.quota_type, delay.secs_f64());
            let entity_label = super::labels::QuotaEntityLabel {
                quota_type: winning.quota_type,
                user: winning.user.clone(),
                client_id: winning.client_id.clone(),
            };
            self.quota_entity_throttle_seconds_total
                .get_or_create(&entity_label)
                .inc_by(delay.secs_f64());
        }
        delay
    }

    /// Account one request whose handler returned an error (the
    /// dispatcher closed the connection). Labelled like
    /// `record_api_request`; disjoint from the
    /// `unsupported_api_requests` family.
    pub fn record_request_error(&self, api_key: i16) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_errors.get_or_create(&lbl).inc();
    }
}

/// Resolve a wire `api_key` to the name used as the metric label.
///
/// An api key the generated `ApiKey` enum knows resolves to its variant name.
/// That covers every Kafka key and `GetReplicaLogInfo` (1020), the one
/// krabka-private key with a generated schema. Any other krabka-private api
/// key resolves through [`krabka_private_api_key_label_name`]. Anything else
/// folds under [`UNKNOWN_LABEL`].
fn api_key_label_name(api_key: ApiKeyCode) -> &'static str {
    match krabka_protocol::api_key::ApiKey::from_i16(api_key) {
        Some(k) => k.into(),
        None if api_key >= KRABKA_PRIVATE_API_KEY_FLOOR => {
            krabka_private_api_key_label_name(api_key)
        }
        None => UNKNOWN_LABEL,
    }
}

/// Resolve a krabka-private wire `api_key` to its RPC name.
///
/// Without this arm every krabka-private request shares one
/// `api_requests{api_key="Unknown"}` series with genuine garbage traffic, and
/// an operator cannot tell the two apart. Cardinality stays bounded: the range
/// holds one label per krabka-private RPC, plus [`UNKNOWN_LABEL`].
fn krabka_private_api_key_label_name(api_key: ApiKeyCode) -> &'static str {
    match api_key {
        ALTER_BARRIER_GROUPS_API_KEY => "AlterBarrierGroups",
        DESCRIBE_BARRIER_GROUPS_API_KEY => "DescribeBarrierGroups",
        TRIGGER_BARRIER_API_KEY => "TriggerBarrier",
        LIST_BARRIER_CUTS_API_KEY => "ListBarrierCuts",
        WRITE_BARRIER_MARKERS_API_KEY => "WriteBarrierMarkers",
        _ => UNKNOWN_LABEL,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::{
            ALTER_BARRIER_GROUPS_API_KEY, DESCRIBE_BARRIER_GROUPS_API_KEY,
            KRABKA_PRIVATE_API_KEY_FLOOR, LIST_BARRIER_CUTS_API_KEY, TRIGGER_BARRIER_API_KEY,
            WRITE_BARRIER_MARKERS_API_KEY,
        },
        assert2::assert,
        krabka_units::{micros, millis},
    };

    /// The applied throttle is the largest delay charged, and it is attributed
    /// to the quota that asked for it — the one an operator has to raise.
    #[tokio::test]
    async fn record_applied_throttle_returns_and_attributes_the_largest_delay() {
        let cases = [
            (
                "byte rate dominates",
                vec![
                    (QuotaType::Produce, millis(400)),
                    (QuotaType::Request, millis(100)),
                ],
                millis(400),
                Some(QuotaType::Produce),
            ),
            (
                "request quota dominates",
                vec![
                    (QuotaType::Fetch, millis(50)),
                    (QuotaType::Request, millis(250)),
                ],
                millis(250),
                Some(QuotaType::Request),
            ),
            (
                "a tie goes to the api-specific quota listed first",
                vec![
                    (QuotaType::Produce, millis(200)),
                    (QuotaType::Request, millis(200)),
                ],
                millis(200),
                Some(QuotaType::Produce),
            ),
            (
                "no quota fired: the phase is observed, the quota family is not",
                vec![
                    (QuotaType::Produce, <Time as TimeExt>::ZERO),
                    (QuotaType::Request, <Time as TimeExt>::ZERO),
                ],
                <Time as TimeExt>::ZERO,
                None,
            ),
            (
                "the KIP-599 admin apis charge one quota and pass one entry",
                vec![(QuotaType::ControllerMutation, millis(750))],
                millis(750),
                Some(QuotaType::ControllerMutation),
            ),
            (
                "the delay is rounded to the nearest millisecond, as Kafka's throttle time is",
                vec![(QuotaType::Produce, micros(1_600))],
                millis(2),
                Some(QuotaType::Produce),
            ),
            (
                "a delay under half a millisecond rounds to no throttle",
                vec![(QuotaType::Produce, micros(400))],
                <Time as TimeExt>::ZERO,
                None,
            ),
        ];

        for (label, charged, want_delay, want_quota) in cases {
            let metrics = BrokerMetrics::new();
            let charges: Vec<QuotaCharge> = charged.into_iter().map(Into::into).collect();
            let applied = metrics.record_applied_throttle(0, &charges);
            assert!(applied == want_delay, "{label}");

            let rendered = render(&metrics).await;
            // Every call observes the per-api throttle phase, the zero
            // included, so its `_count` tracks the request count.
            let phase = format!(
                "krabka_broker_request_throttle_duration_seconds_sum{{api_key=\"Produce\"}} {}",
                want_delay.secs_f64()
            );
            assert!(rendered.contains(&phase), "{label}: missing {phase}");
            assert!(
                rendered.contains(
                    "krabka_broker_request_throttle_duration_seconds_count{api_key=\"Produce\"} 1"
                ),
                "{label}: throttle phase must be observed once"
            );

            // The quota family carries a series only for the quota that won,
            // and only when a quota actually delayed the request.
            for quota_type in QuotaType::ALL {
                let series = format!(
                    "krabka_broker_quota_throttle_duration_seconds_sum{{quota_type=\"{}\"}} {}",
                    quota_type.as_str(),
                    want_delay.secs_f64()
                );
                let present = rendered.contains(&series);
                assert!(
                    present == (want_quota == Some(quota_type)),
                    "{label}: {} series presence {present} in:\n{rendered}",
                    quota_type.as_str()
                );
            }
        }
    }

    /// Renders the registry as the exposition text an operator scrapes.
    ///
    /// `Histogram::sum` and `Histogram::count` are behind prometheus-client's
    /// `test-util` feature, which this workspace does not turn on, so a test
    /// reads a histogram the way Prometheus does.
    async fn render(metrics: &BrokerMetrics) -> String {
        crate::metrics::test_support::render_registry!(metrics, out, registry; expect("encode registry"));
        out
    }

    #[test]
    fn api_key_label_name_names_every_krabka_private_api() {
        let cases = [
            (ALTER_BARRIER_GROUPS_API_KEY, "AlterBarrierGroups"),
            (DESCRIBE_BARRIER_GROUPS_API_KEY, "DescribeBarrierGroups"),
            (TRIGGER_BARRIER_API_KEY, "TriggerBarrier"),
            (LIST_BARRIER_CUTS_API_KEY, "ListBarrierCuts"),
            (WRITE_BARRIER_MARKERS_API_KEY, "WriteBarrierMarkers"),
            // A Kafka api key still resolves through the generated enum, and
            // so does the one krabka-private key that has a generated schema.
            (0, "Produce"),
            (1020, "GetReplicaLogInfo"),
            // Kafka trunk's key 93 is no longer GetReplicaLogInfo.
            (93, "StreamsGroupTopologyDescriptionUpdate"),
            // Garbage inside the krabka-private range, and outside it, both
            // fold under the sentinel.
            (KRABKA_PRIVATE_API_KEY_FLOOR, UNKNOWN_LABEL),
            (9999, UNKNOWN_LABEL),
            (999, UNKNOWN_LABEL),
        ];

        for (api_key, want) in cases {
            assert!(api_key_label_name(api_key) == want, "api_key {api_key}");
        }
    }

    #[test]
    fn unsupported_api_requests_counter_is_disjoint_from_api_requests() {
        let m = BrokerMetrics::new();
        // Invariant: `record_unsupported_api_request` bumps
        // only the `unsupported_api_requests` family — operators
        // expect `api_requests` to count *every* dispatched frame and
        // `unsupported_api_requests` to count just the ones that hit
        // the unsupported-version arm.
        m.record_unsupported_api_request(0); // Produce, unsupported
        m.record_unsupported_api_request(999); // unknown label coverage

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        // `record_unsupported_api_request` does NOT also bump
        // `api_requests`; the dispatcher already did that for the
        // request in question via `record_api_request`.
        let cases = [
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &produce,
                1,
            ),
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &unknown,
                1,
            ),
            ("api_requests", &m.api_requests, &produce, 0),
            ("api_requests", &m.api_requests, &unknown, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.api_key);
        }
    }

    #[test]
    fn api_requests_label_resolves_known_keys_and_folds_unknown() {
        let m = BrokerMetrics::new();
        // Three known + one unknown api_key. Verify per-label tallies.
        m.record_api_request(0); // Produce
        m.record_api_request(0); // Produce again
        m.record_api_request(1); // Fetch
        m.record_api_request(12_345); // out-of-range → Unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        for (label, want) in [(&produce, 2), (&fetch, 1), (&unknown, 1)] {
            let got = m.api_requests.get_or_create(label).get();
            assert!(got == want, "api_key {:?}", label.api_key);
        }
    }

    #[tokio::test]
    async fn request_duration_errors_and_gauges_render() {
        let m = BrokerMetrics::new();
        // Two Produce latency samples + one unknown-key sample.
        m.observe_request_duration(0, 0.0008);
        m.observe_request_duration(0, 0.04);
        m.observe_request_duration(12_345, 2.0); // → "Unknown" label
        m.record_request_error(1); // Fetch handler fault
        m.record_request_error(1);
        m.in_flight_requests.inc();
        m.in_flight_requests.inc();
        m.in_flight_requests.dec();
        m.active_connections.set(5);

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        // Histogram Family exposes sample count via the encoded `_count`;
        // assert the render + the error/gauge values here.
        assert!(m.request_errors.get_or_create(&fetch).get() == 2);
        assert!(m.in_flight_requests.get() == 1);
        assert!(m.active_connections.get() == 5);

        crate::metrics::test_support::render_registry!(m, buf, r; unwrap());
        assert!(
            buf.contains("krabka_broker_request_duration_seconds_count{api_key=\"Produce\"} 2"),
            "expected 2 Produce latency samples in:\n{buf}"
        );
        assert!(
            buf.contains("krabka_broker_request_errors_total{api_key=\"Fetch\"} 2"),
            "expected 2 Fetch request errors in:\n{buf}"
        );
        assert!(buf.contains("krabka_broker_in_flight_requests 1"));
        assert!(buf.contains("krabka_broker_active_connections 5"));
        // Unknown api_key folds under the shared "Unknown" label.
        assert!(buf.contains("api_key=\"Unknown\""), "unknown label missing");
        // Keep `produce` referenced to document the intended label.
        let _ = produce;
    }

    /// Each reason is its own series, and the four together are the whole
    /// label set: a reason the enum does not name cannot reach the counter.
    #[tokio::test]
    async fn connection_closes_render_one_series_per_reason() {
        let m = BrokerMetrics::new();
        for reason in ConnectionCloseReason::ALL {
            m.record_connection_close(reason);
        }
        m.record_connection_close(ConnectionCloseReason::Idle);

        crate::metrics::test_support::render_registry!(m, buf, r; unwrap());

        // A `Family` renders its series in map order, so sort before
        // comparing: what matters is the set of series and their values.
        let mut rendered: Vec<&str> = buf
            .lines()
            .filter(|line| line.starts_with("krabka_broker_connection_closes_total{"))
            .collect();
        rendered.sort_unstable();
        assert!(
            rendered
                == vec![
                    "krabka_broker_connection_closes_total{reason=\"decode_error\"} 1",
                    "krabka_broker_connection_closes_total{reason=\"idle\"} 2",
                    "krabka_broker_connection_closes_total{reason=\"max_connections\"} 1",
                    "krabka_broker_connection_closes_total{reason=\"max_connections_per_ip\"} 1",
                    "krabka_broker_connection_closes_total{reason=\"peer_closed\"} 1",
                    "krabka_broker_connection_closes_total{reason=\"sasl_session_expired\"} 1",
                ]
        );
    }
}
