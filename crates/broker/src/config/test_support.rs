//! Fixtures the configuration unit tests share: the invalid-runtime
//! assertion, a well-formed two-listener config, and the witness role set.

use assert2::assert;
use krabka_security::{ListenerProtocol, SaslMechanism};

use crate::{
    BrokerError,
    config::{BrokerConfig, ListenerSpec, NodeRole},
};

pub type RuntimeInvalidator = (&'static str, fn(&mut BrokerConfig));

pub fn assert_invalid_runtime(config: &BrokerConfig, expected: &str) {
    let Err(BrokerError::InvalidRuntimeConfig(actual)) = config.validate() else {
        panic!("expected invalid runtime config");
    };
    assert!(actual == expected);
}

/// Exercise each scalar rejection from an independently initialized default config.
pub fn assert_invalid_runtime_cases(cases: impl IntoIterator<Item = RuntimeInvalidator>) {
    for (expected, invalidate) in cases {
        let mut config = BrokerConfig::default();
        invalidate(&mut config);
        assert_invalid_runtime(&config, expected);
    }
}

/// A well-formed two-listener config used as the base for validation
/// tests.
pub fn base() -> BrokerConfig {
    BrokerConfig {
        listeners: vec![
            ListenerSpec {
                name: "INTERNAL".to_string(),
                bind_addr: "127.0.0.1:9093".parse().unwrap(),
                advertised: "127.0.0.1:9093".to_string(),
                protocol: ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: crate::SslPrincipalMapper::default(),
            },
            ListenerSpec {
                name: "EXTERNAL".to_string(),
                bind_addr: "0.0.0.0:9092".parse().unwrap(),
                advertised: "host.docker.internal:9092".to_string(),
                protocol: ListenerProtocol::SaslSsl,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: crate::SslPrincipalMapper::default(),
            },
        ],
        inter_broker_listener_name: "INTERNAL".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
        // PLAIN is enabled above, and `validate` refuses that with an empty
        // credential table.
        plain_credentials: [("admin".to_string(), "admin-secret".to_string())]
            .into_iter()
            .collect(),
        ..BrokerConfig::default()
    }
}

/// Controller + broker + witness, the only valid witness role set.
pub fn witness_roles() -> Vec<NodeRole> {
    vec![NodeRole::Controller, NodeRole::Broker, NodeRole::Witness]
}
