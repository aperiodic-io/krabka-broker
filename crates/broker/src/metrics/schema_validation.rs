//! KFC-7 schema-validation accounting: the per-topic and per-reason rejection
//! counter, and the cache hit and miss counters that say what the feature
//! costs at steady state.

use super::{BrokerMetrics, SchemaRejectionLabel};

impl BrokerMetrics {
    /// KFC-7: account one record that failed schema validation on `topic`.
    ///
    /// Callers bump once per rejected record, so a Produce request with three
    /// bad records makes three calls. `reason` should be one of the five fixed
    /// label values the schema validator returns, because the label set stays
    /// bounded only while it is.
    pub fn record_schema_validation_rejection(&self, topic: &str, reason: &str) {
        let lbl = SchemaRejectionLabel {
            topic: topic.to_string(),
            reason: reason.to_string(),
        };
        self.schema_validation_rejections.get_or_create(&lbl).inc();
        self.track_topic_name(topic);
    }

    /// KFC-7: account one schema lookup the broker answered from its local
    /// cache.
    pub fn record_schema_cache_hit(&self) { self.schema_validation_cache_hits.inc(); }

    /// KFC-7: account one schema lookup that cost a registry round trip on the
    /// produce path.
    pub fn record_schema_cache_miss(&self) { self.schema_validation_cache_misses.inc(); }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn schema_validation_helpers_accumulate_per_topic_and_reason() {
        // KFC-7: rejections are keyed by (topic, reason), so a run of
        // `unframed` on one topic must not move any other pair. The two cache
        // counters carry no labels and must stay independent of each other.
        let m = BrokerMetrics::new();
        m.record_schema_validation_rejection("orders", "unframed");
        m.record_schema_validation_rejection("orders", "unframed");
        m.record_schema_validation_rejection("orders", "wrong_subject");
        m.record_schema_validation_rejection("payments", "unframed");
        m.record_schema_validation_rejection("payments", "registry_unavailable");
        m.record_schema_cache_hit();
        m.record_schema_cache_hit();
        m.record_schema_cache_hit();
        m.record_schema_cache_miss();

        // A pair that saw no rejection reads 0: `get_or_create` materializes
        // the series at read time, which is what
        // `rate(schema_validation_rejections_total{...}[1m])` computes over.
        let cases = [
            ("orders", "unframed", 2),
            ("orders", "wrong_subject", 1),
            ("orders", "registry_unavailable", 0),
            ("orders", "body_mismatch", 0),
            ("payments", "unframed", 1),
            ("payments", "wrong_subject", 0),
            ("payments", "registry_unavailable", 1),
        ];
        for (topic, reason, want) in cases {
            let lbl = SchemaRejectionLabel {
                topic: topic.to_string(),
                reason: reason.to_string(),
            };
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = m.schema_validation_rejections.get_or_create(&lbl).get();
            assert!(got == want, "rejections for {topic} / {reason}");
        }

        assert!(m.schema_validation_cache_hits.get() == 3);
        assert!(m.schema_validation_cache_misses.get() == 1);
    }
}
