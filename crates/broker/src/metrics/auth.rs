//! Connection-identity accounting: the KIP-511 client software fingerprint
//! counter and the per-mechanism SASL authentication outcome counters.

use super::{AuthorizationDeniedLabel, BrokerMetrics, ClientSoftwareLabel, SaslMechanismLabel};

impl BrokerMetrics {
    /// KIP-511: bump the per-(name, version) handshake counter.
    /// Caller guarantees both inputs already passed
    /// `handlers::api_versions::is_valid_client_info` so the label
    /// values stay bounded.
    pub fn record_client_software(&self, name: &str, version: &str) {
        let lbl = ClientSoftwareLabel {
            software_name: name.to_string(),
            software_version: version.to_string(),
        };
        self.client_software_versions.get_or_create(&lbl).inc();
    }

    /// Account one completed `SaslAuthenticate` frame on
    /// `mechanism`. `success = true` increments
    /// `successful_authentication_total`; `success = false`
    /// increments `failed_authentication_total`. The mechanism
    /// label is the canonical Kafka wire name; pass `"Unknown"`
    /// for the `ILLEGAL_SASL_STATE` reject (no prior handshake)
    /// to keep cardinality bounded.
    pub fn record_authentication(&self, mechanism: &str, success: bool) {
        let lbl = SaslMechanismLabel {
            mechanism: mechanism.to_string(),
        };
        if success {
            self.successful_authentication.get_or_create(&lbl).inc();
        } else {
            self.failed_authentication.get_or_create(&lbl).inc();
        }
    }

    /// Account one Deny decision from the authorizer. `operation` and
    /// `resource_type` are the `Debug` spellings of
    /// `krabka_metadata::AclOperation` and `krabka_metadata::ResourceType`,
    /// so the label set stays bounded by the two enums. Kafka has no
    /// equivalent metric; the closest it offers is the
    /// `kafka.authorizer.logger` Denied Operation line.
    pub fn record_authorization_denied(&self, operation: &str, resource_type: &str) {
        let lbl = AuthorizationDeniedLabel {
            operation: operation.to_string(),
            resource_type: resource_type.to_string(),
        };
        self.authorization_denied.get_or_create(&lbl).inc();
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn record_authentication_splits_success_and_failure_per_mechanism() {
        let m = BrokerMetrics::new();
        let plain = SaslMechanismLabel {
            mechanism: "PLAIN".to_string(),
        };
        let scram = SaslMechanismLabel {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        let unknown = SaslMechanismLabel {
            mechanism: "Unknown".to_string(),
        };
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", false);
        m.record_authentication("SCRAM-SHA-256", true);
        m.record_authentication("Unknown", false);
        // PLAIN: 2 successes, 1 failure. SCRAM-SHA-256: 1 success, 0
        // failures (must not lazily allocate a failure entry from the
        // success bump). ILLEGAL_SASL_STATE: 0 successes, 1 failure
        // under the `Unknown` sentinel.
        let cases = [
            ("successful", &m.successful_authentication, &plain, 2),
            ("failed", &m.failed_authentication, &plain, 1),
            ("successful", &m.successful_authentication, &scram, 1),
            ("failed", &m.failed_authentication, &unknown, 1),
            ("successful", &m.successful_authentication, &unknown, 0),
        ];
        for (outcome, family, label, want) in cases {
            // Each read is its own statement: `get_or_create` returns a
            // read guard, and a first-materialization on the same family
            // takes the write lock — holding several guards in one
            // expression self-deadlocks.
            let got = family.get_or_create(label).get();
            assert!(got == want, "{outcome} auth for {:?}", label.mechanism);
        }
    }

    #[test]
    fn record_authorization_denied_counts_per_operation_and_resource_type() {
        let m = BrokerMetrics::new();
        let write_topic = AuthorizationDeniedLabel {
            operation: "Write".to_string(),
            resource_type: "Topic".to_string(),
        };
        let read_group = AuthorizationDeniedLabel {
            operation: "Read".to_string(),
            resource_type: "Group".to_string(),
        };

        m.record_authorization_denied("Write", "Topic");
        m.record_authorization_denied("Write", "Topic");
        m.record_authorization_denied("Read", "Group");

        for (label, want) in [(&write_topic, 2), (&read_group, 1)] {
            let got = m.authorization_denied.get_or_create(label).get();
            assert!(got == want, "label {label:?}");
        }
    }

    #[test]
    fn record_client_software_accumulates_per_name_version() {
        let m = BrokerMetrics::new();
        let krabka_100 = ClientSoftwareLabel {
            software_name: "krabka".to_string(),
            software_version: "1.0.0".to_string(),
        };
        let krabka_101 = ClientSoftwareLabel {
            software_name: "krabka".to_string(),
            software_version: "1.0.1".to_string(),
        };
        let other = ClientSoftwareLabel {
            software_name: "other-lib".to_string(),
            software_version: "1.0.0".to_string(),
        };

        m.record_client_software("krabka", "1.0.0");
        m.record_client_software("krabka", "1.0.0");
        m.record_client_software("krabka", "1.0.1");
        m.record_client_software("other-lib", "1.0.0");

        for (label, want) in [(&krabka_100, 2), (&krabka_101, 1), (&other, 1)] {
            let got = m.client_software_versions.get_or_create(label).get();
            assert!(got == want, "label {label:?}");
        }
    }

    #[tokio::test]
    async fn record_client_software_renders_labelled_openmetrics_counter() {
        let m = BrokerMetrics::new();

        m.record_client_software("render-lib", "2.0.0");

        krabka_macros::metric_registry_fixture!(m, body, registry; unwrap());
        assert!(body.contains(
            "krabka_broker_client_software_versions_total{software_name=\"render-lib\",software_version=\"2.0.0\"} 1"
        ));
    }
}
