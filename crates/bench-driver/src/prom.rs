//! Prometheus query client. The driver issues a small set of instant
//! queries at scenario end to capture resource usage on the broker pods.
//! For Strimzi only, it also captures JVM heap and non-heap from the JMX
//! exporter.

use {
    crate::{
        ids::{MessageCount, TimeOffsetMs},
        numeric::{event_rate, nonnegative_f64_to_u64, to_f64},
        scenario::{BrokerSample, Resource, Stack},
    },
    anyhow::{Context, Result, anyhow},
    krabka_units::prelude::*,
    serde::Deserialize,
    std::collections::BTreeMap,
};

/// Default HTTP request timeout for Prometheus queries.
pub const DEFAULT_PROMETHEUS_REQUEST_TIMEOUT: Time = secs(15);

pub struct PromClient {
    base_url: String,
    http: reqwest::Client,
}

impl PromClient {
    /// `base_url` is the Prometheus root, for example
    /// `http://prom.monitoring.svc:9090`. A trailing slash is not needed.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn new(base_url: impl Into<String>, request_timeout: Time) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(request_timeout.to_std())
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        })
    }

    /// Runs a `PromQL` instant query. Returns the sum of the first scalar
    /// value across all returned series. Returns `None` when the result
    /// vector is empty.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub async fn query_scalar_sum(&self, query: &str) -> Result<Option<f64>> {
        self.query_scalar_sum_at(query, None).await
    }

    /// [`Self::query_scalar_sum`], evaluated at an explicit instant.
    ///
    /// `at_unix_s` is the Prometheus `time` parameter. A range selector inside
    /// the query then looks back from that instant and not from now, which is
    /// what lets a caller name the window it measured rather than the window
    /// that happens to end when the client got round to asking.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub async fn query_scalar_sum_at(
        &self,
        query: &str,
        at_unix_s: Option<f64>,
    ) -> Result<Option<f64>> {
        let url = format!("{}/api/v1/query", self.base_url);
        let mut params: Vec<(&str, String)> = vec![("query", query.to_owned())];
        if let Some(at) = at_unix_s {
            params.push(("time", format!("{at}")));
        }
        let body: PromResp = self
            .http
            .get(&url)
            .query(&params)
            .send()
            .await
            .with_context(|| format!("GET {url} query={query}"))?
            .error_for_status()
            .with_context(|| "prometheus non-2xx")?
            .json()
            .await
            .context("decode prometheus json")?;

        if body.status != "success" {
            return Err(anyhow!(
                "prometheus query failed: status={} err={:?}",
                body.status,
                body.error
            ));
        }
        let Some(data) = body.data else {
            return Ok(None);
        };
        if data.result.is_empty() {
            return Ok(None);
        }
        let mut sum = 0.0_f64;
        let mut had = false;
        for r in &data.result {
            if let Some((_, v)) = r.value.as_ref()
                && let Ok(parsed) = v.parse::<f64>()
            {
                sum += parsed;
                had = true;
            }
        }
        Ok(had.then_some(sum))
    }

    /// Runs a `PromQL` range query and sums across all returned series per
    /// timestamp. Returns `(unix_seconds, value)` points on the step grid.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub async fn query_range_sum(
        &self,
        query: &str,
        start_s: f64,
        end_s: f64,
        step_s: u64,
    ) -> Result<Vec<(f64, f64)>> {
        let url = format!("{}/api/v1/query_range", self.base_url);
        let start = format!("{start_s}");
        let end = format!("{end_s}");
        let step = format!("{step_s}s");
        let body: PromResp = self
            .http
            .get(&url)
            .query(&[
                ("query", query),
                ("start", start.as_str()),
                ("end", end.as_str()),
                ("step", step.as_str()),
            ])
            .send()
            .await
            .with_context(|| format!("GET {url} (range) query={query}"))?
            .error_for_status()
            .with_context(|| "prometheus non-2xx (range)")?
            .json()
            .await
            .context("decode prometheus range json")?;

        if body.status != "success" {
            return Err(anyhow!(
                "prometheus range query failed: status={} err={:?}",
                body.status,
                body.error
            ));
        }
        let Some(data) = body.data else {
            return Ok(Vec::new());
        };
        // Sum across series per timestamp (keyed by ms to dedupe float ts).
        let mut by_ts: BTreeMap<u64, f64> = BTreeMap::new();
        for r in &data.result {
            if let Some(vals) = &r.values {
                for (ts, v) in vals {
                    if let Ok(parsed) = v.parse::<f64>() {
                        *by_ts
                            .entry(nonnegative_f64_to_u64((ts * 1000.0).round()))
                            .or_insert(0.0) += parsed;
                    }
                }
            }
        }
        Ok(by_ts
            .into_iter()
            .map(|(ts_ms, v)| (to_f64(ts_ms) / 1000.0, v))
            .collect())
    }

    /// Captures a broker CPU and memory **time series** over `[start_s, end_s]`,
    /// so a graph can show the values over the test. CPU is a 1-minute `rate()`
    /// in cores, and memory is the summed working set. Both align onto a single
    /// `step_s` grid. `t_offset_ms` is relative to `start_s`.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub async fn capture_resource_series(
        &self,
        stack: Stack,
        namespace: &str,
        start_s: f64,
        end_s: f64,
        step_s: u64,
    ) -> Result<Vec<BrokerSample>> {
        let pod_re = format!("{}.*", stack.broker_pod_regex().trim_start_matches('^'));
        let cpu_q = format!(
            "sum(rate(container_cpu_usage_seconds_total{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}}[1m]))"
        );
        let mem_q = format!(
            "sum(container_memory_working_set_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}})"
        );
        let cpu = self
            .query_range_sum(&cpu_q, start_s, end_s, step_s)
            .await
            .unwrap_or_default();
        let mem = self
            .query_range_sum(&mem_q, start_s, end_s, step_s)
            .await
            .unwrap_or_default();

        let range_start_ms = nonnegative_f64_to_u64((start_s * 1000.0).round());
        let mut by_ts: BTreeMap<u64, (f64, u64)> = BTreeMap::new();
        for (ts, v) in cpu {
            by_ts
                .entry(nonnegative_f64_to_u64((ts * 1000.0).round()))
                .or_default()
                .0 = v;
        }
        for (ts, v) in mem {
            by_ts
                .entry(nonnegative_f64_to_u64((ts * 1000.0).round()))
                .or_default()
                .1 = nonnegative_f64_to_u64(v);
        }
        Ok(by_ts
            .into_iter()
            .map(|(ts_ms, (cpu_cores, mem))| BrokerSample {
                t_offset_ms: TimeOffsetMs(ts_ms.saturating_sub(range_start_ms)),
                cpu_cores,
                mem_working_set: ByteSize::from_bytes(mem),
            })
            .collect())
    }

    /// Captures broker resource usage for the given stack over a window that
    /// ends now. Make `window` a little larger than the measured scenario
    /// duration, so the `rate()` window does not tail off.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    /// `measured_until_unix_s` is the instant the measurement window closed.
    /// The queries are evaluated there rather than at call time, so the
    /// `window`-long lookback covers the window that was measured. Without it,
    /// the time the driver spends joining its tasks and draining its producers
    /// shifts the lookback forward by that much: the start of the measurement
    /// falls out of the window and the drain falls into it, while the
    /// efficiency figure still divides the measurement-only message count by
    /// that shifted CPU interval.
    pub async fn capture_resource(
        &self,
        stack: Stack,
        namespace: &str,
        window: Time,
        measured_until_unix_s: f64,
        msgs_produced: MessageCount,
    ) -> Result<Resource> {
        // `broker_pod_regex()` returns a `^`-anchored *prefix* (e.g.
        // `^demo-broker`) shared with `failover.rs`, which strips the `^` and
        // uses it via `starts_with`. PromQL `=~` matchers are *fully* anchored
        // (`^demo-broker` is compiled as `^(?:^demo-broker)$`), so the bare
        // prefix matches a pod named *exactly* `demo-broker` and never the real
        // `demo-broker-0-0` — every series came back empty → all resource
        // numbers 0. Rebuild it as an unanchored prefix-glob: drop the `^` and
        // append `.*` so `demo-broker.*` matches the ordinal-suffixed pods.
        let pod_re = format!("{}.*", stack.broker_pod_regex().trim_start_matches('^'));
        let pod_re = pod_re.as_str();
        // PromQL range selectors take whole units, and need at least one full
        // scrape interval to have anything to rate over.
        let win = window.secs_i64().max(15);

        // GKE's kubelet-cadvisor series carry `pod`/`namespace` but NO
        // `container` label — so the old `container!=""` filter matched nothing
        // and every resource number came back 0. Each pod instead has one
        // pod-level cgroup series (cgroup `id` ends in `.slice`) plus a
        // per-container series (`id` ends in `.scope`). Match only the pod-level
        // rollup to get the pod's true total (all containers + pause) without
        // double-counting. The matcher is fully anchored, so `.*slice` keeps the
        // `…pod<uid>.slice` rollup and drops the `…/cri-…scope` children
        // (verified: pod-level ≈ Σ containers, vs ~2× when summing everything).

        let cpu_query = format!(
            "sum(rate(container_cpu_usage_seconds_total{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}}[{win}s]) * {win})"
        );
        let rss_query = format!(
            "max_over_time(sum(container_memory_working_set_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}})[{win}s:15s])"
        );

        // An empty result vector is not a zero. It means the selector matched
        // no series: a scrape that had not started, a renamed label, a wrong
        // namespace. Recording it as zero publishes 0 bytes of memory as a
        // real and exceptionally good result, and derives an efficiency figure
        // from a CPU measurement that does not exist. Both queries below have
        // already been wrong in exactly that way, as the comments above say, and
        // the only symptom was a run of suspiciously good numbers. Fail instead:
        // `workload::capture_resources` turns this into a
        // `prometheus-capture-failed` note on the run.
        let at = Some(measured_until_unix_s);
        let broker_cpu = Time::from_secs_f64(require_series(
            self.query_scalar_sum_at(&cpu_query, at).await?,
            "broker CPU",
            &cpu_query,
        )?);
        let mem_working = nonnegative_f64_to_u64(require_series(
            self.query_scalar_sum_at(&rss_query, at).await?,
            "broker memory",
            &rss_query,
        )?);

        let mut res = Resource {
            broker_cpu,
            mem_cgroup_working_set: ByteSize::from_bytes(mem_working),
            jvm_heap_used: None,
            jvm_nonheap_used: None,
            kafka_page_cache_approx: None,
            // Messages per second of burnt CPU: the same count-over-window shape
            // as a throughput, with CPU time standing in for wallclock. A run
            // that burnt no CPU (nothing scraped) reports no rate.
            msgs_per_cpu_second: event_rate(msgs_produced.0, broker_cpu),
        };

        if matches!(stack, Stack::Kafka) {
            // Strimzi 0.46 ships the new Prometheus Java client (1.x), whose
            // built-in JVM collector publishes `jvm_memory_used_bytes{area=...}`
            // — note the `used_bytes` suffix order, NOT the legacy simpleclient
            // `jvm_memory_bytes_used`. (Querying the old name returned empty, so
            // the JVM heap/non-heap split was silently 0.) The `area` label is
            // lowercase `heap`/`nonheap`, matching the selectors below.
            let heap_q = format!(
                "max_over_time(sum(jvm_memory_used_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",area=\"heap\"}})[{win}s:15s])"
            );
            let nonheap_q = format!(
                "max_over_time(sum(jvm_memory_used_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",area=\"nonheap\"}})[{win}s:15s])"
            );
            // These three are already `Option`, so an empty vector has a way to
            // say "not measured" without inventing a zero. A JVM that exports
            // no heap series is a real state -- a metrics port that is not
            // scraped yet, or a collector under another name -- and the page
            // cache figure is derived from both, so it is unavailable whenever
            // either half is.
            let heap = self.query_scalar_sum_at(&heap_q, at).await?;
            let nonheap = self.query_scalar_sum_at(&nonheap_q, at).await?;
            res.jvm_heap_used = heap.map(|v| ByteSize::from_bytes(nonnegative_f64_to_u64(v)));
            res.jvm_nonheap_used = nonheap.map(|v| ByteSize::from_bytes(nonnegative_f64_to_u64(v)));
            res.kafka_page_cache_approx = heap.zip(nonheap).map(|(heap, nonheap)| {
                let page_cache = mem_working
                    .saturating_sub(nonnegative_f64_to_u64(heap))
                    .saturating_sub(nonnegative_f64_to_u64(nonheap));
                ByteSize::from_bytes(page_cache)
            });
        }

        Ok(res)
    }
}

/// The value of a query that must have matched a series, or an error naming
/// the query that matched none.
///
/// This is the seam that keeps "no series" from becoming a zero. It is a free
/// function so its rule is testable without an HTTP server.
fn require_series(value: Option<f64>, what: &str, query: &str) -> Result<f64> {
    value.ok_or_else(|| anyhow!("no series matched the {what} query: {query}"))
}

// ── Prometheus HTTP API types (minimal) ─────────────────────────────────────

#[derive(Debug, Deserialize)]
struct PromResp {
    status: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data: Option<PromData>,
}

#[derive(Debug, Deserialize)]
struct PromData {
    #[serde(default)]
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    /// `[<unix-ts>, "<string-value>"]`. Prometheus encodes the scalar
    /// portion as a JSON string. The timestamp is f64 in seconds.
    #[serde(default)]
    value: Option<(f64, String)>,
    /// The matrix payload of a range query: `[[ts, "val"], ...]`.
    #[serde(default)]
    values: Option<Vec<(f64, String)>>,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn prometheus_request_timeout_constructs_prom_client() {
        assert!(PromClient::new("http://prometheus.example", secs(1)).is_ok());
    }

    /// A matched series keeps its value, whatever that value is. A real zero
    /// is a measurement and passes through.
    #[test]
    fn require_series_passes_a_matched_value_through() {
        for value in [12.5_f64, 0.0_f64] {
            let kept = require_series(Some(value), "broker CPU", "q").expect("a matched series");
            assert!((kept - value).abs() < f64::EPSILON);
        }
    }

    /// An empty vector is not a zero. The error names the query, because the
    /// cause is nearly always the selector.
    #[test]
    fn require_series_rejects_an_empty_vector() {
        let error = require_series(None, "broker memory", "sum(container_memory{pod=~\"x.*\"})")
            .expect_err("an empty vector is an error");
        let message = format!("{error}");
        assert!(message.contains("no series matched the broker memory query"));
        assert!(message.contains("container_memory"));
    }

    #[test]
    fn parses_success_with_one_result() {
        let json = r#"{
          "status": "success",
          "data": {
            "resultType": "vector",
            "result": [
              { "metric": {}, "value": [1234.0, "12.5"] }
            ]
          }
        }"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert2::assert!(p.status == "success");
        assert2::assert!(p.data.unwrap().result.len() == 1);
    }

    #[test]
    fn parses_empty_result_set() {
        let json = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert2::assert!(p.data.unwrap().result.is_empty());
    }

    #[test]
    fn parses_error_response() {
        let json = r#"{"status":"error","error":"bad query"}"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert2::assert!(p.status == "error");
        assert2::assert!(p.error.as_deref() == Some("bad query"));
    }
}
