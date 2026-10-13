//! The live snapshot itself: the keyed store of the newest point per series,
//! the ingest path that folds deltas into it, and the staleness bound that
//! decides which points a scrape still sees.
//!
//! Ingest is where the store is written and where stale entries are dropped,
//! so the mutex, the key shape, and the time-to-live sit here; `render` only
//! reads the same state at scrape time.

use {
    super::{DataPoint, PointValue},
    std::{
        collections::HashMap,
        sync::Mutex,
        time::{Duration, Instant},
    },
};

#[derive(Debug)]
pub(super) struct StoredPoint {
    pub(super) attributes: Vec<(String, String)>,
    pub(super) value: PointValue,
    delta_start: Option<u64>,
    pub(super) at: Instant,
}

type SeriesKey = (String, String, String, Vec<(String, String)>);

#[derive(Debug)]
pub(crate) struct ClientMetricsCollector {
    pub(super) points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    pub(super) ttl: Duration,
}

impl ClientMetricsCollector {
    pub(super) fn is_live(age: Duration, ttl: Duration) -> bool { age < ttl }

    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Records the latest value for each point, replaces any earlier value,
    /// and removes stale points.
    pub(crate) fn ingest(&self, points: &[DataPoint]) {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        if self.ttl.is_zero() {
            guard.clear();
            return;
        }
        for p in points {
            let key = (
                p.metric.clone(),
                p.client_instance_id.clone(),
                p.client_id.clone(),
                p.attributes.clone(),
            );
            if let Some(start) = p.delta_start
                && let Some(stored) = guard.get_mut(&key)
                && stored
                    .delta_start
                    .is_some_and(|previous| previous == 0 || start == 0 || previous == start)
                && stored.value.accumulate(&p.value)
            {
                stored.delta_start = Some(start);
                stored.at = now;
                continue;
            }
            guard.insert(key, StoredPoint {
                attributes: p.attributes.clone(),
                value: p.value.clone(),
                delta_start: p.delta_start,
                at: now,
            });
        }
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
    }

    /// The count of points that are not stale. This method also removes the
    /// stale entries in place.
    #[cfg(test)]
    pub(crate) fn live_point_count(&self) -> usize {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::test_support::data_point, *},
        std::time::Duration,
    };

    #[test]
    fn delta_points_accumulate_per_series() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        for (counter, count, sum, buckets) in [
            (5.0, 2, 4.0, vec![(1.0, 1), (f64::MAX, 1)]),
            (3.0, 3, 6.0, vec![(1.0, 2), (f64::MAX, 1)]),
        ] {
            sink.ingest(&[
                data_point()
                    .metric("requests".into())
                    .value(PointValue::Counter(counter))
                    .delta_start(7)
                    .call(),
                data_point()
                    .metric("latency".into())
                    .value(PointValue::Histogram {
                        count,
                        sum,
                        buckets,
                    })
                    .delta_start(7)
                    .call(),
            ]);
        }
        for (metric, first_start, second_start, expected) in [
            ("unknown_previous", 0, 8, 8.0),
            ("unknown_current", 8, 0, 8.0),
            ("reset", 7, 8, 3.0),
        ] {
            for (value, start) in [(5.0, first_start), (3.0, second_start)] {
                sink.ingest(&[DataPoint {
                    metric: metric.into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                    value: PointValue::Counter(value),
                    delta_start: Some(start),
                }]);
            }
            let guard = sink.points.lock().unwrap();
            let point = guard
                .get(&(metric.into(), "i".into(), "c".into(), vec![]))
                .unwrap();
            assert2::assert!(
                matches!(point.value, PointValue::Counter(value) if (value - expected).abs() < f64::EPSILON)
            );
        }

        let guard = sink.points.lock().unwrap();
        assert2::assert!(guard.values().any(
            |point| matches!(point.value, PointValue::Counter(value) if (value - 8.0).abs() < f64::EPSILON)
        ));
        assert2::assert!(guard.values().any(|point| matches!(
            &point.value,
            PointValue::Histogram { count: 5, sum, buckets }
                if (*sum - 10.0).abs() < f64::EPSILON
                    && buckets.as_slice() == [(1.0, 3), (f64::MAX, 2)]
        )));

        let mut total = PointValue::Histogram {
            count: 2,
            sum: 4.0,
            buckets: vec![(1.0, 1), (f64::MAX, 1)],
        };
        assert2::assert!(!total.accumulate(&PointValue::Histogram {
            count: 3,
            sum: 6.0,
            buckets: vec![(2.0, 2), (f64::MAX, 1)],
        }));
    }

    #[test]
    fn stale_points_evicted_on_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_millis(0));
        sink.ingest(&[DataPoint {
            metric: "m".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            attributes: vec![],
            value: PointValue::Gauge(1.0),
            delta_start: None,
        }]);
        assert2::assert!((sink.live_point_count()) == (0));
        assert2::assert!(ClientMetricsCollector::is_live(
            Duration::from_nanos(9),
            Duration::from_nanos(10)
        ));
        assert2::assert!(!ClientMetricsCollector::is_live(
            Duration::from_nanos(10),
            Duration::from_nanos(10)
        ));
    }
}
