//! Named listeners and inter-broker credentials: what the broker binds and
//! advertises, what it authenticates outbound connections with, and the
//! check that the two agree.

use std::{net::SocketAddr, path::PathBuf};

use krabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

use crate::{BrokerError, config::BrokerConfig};

/// A single named listener: the port the broker binds and the address it
/// gives to clients.
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    /// Listener name (e.g. `"PLAINTEXT"`, `"SSL"`, `"SASL_SSL"`).
    pub name: String,
    /// Local address to bind.
    pub bind_addr: SocketAddr,
    /// `host:port` advertised to clients in `Metadata` responses.
    pub advertised: String,
    /// Wire protocol (Plaintext / Ssl / `SaslPlaintext` / `SaslSsl`).
    pub protocol: ListenerProtocol,
    /// Per-listener TLS material. When `Some`, overrides the top-level
    /// `BrokerConfig::tls_config` for this listener's accept loop.
    pub tls_config: Option<TlsConfig>,
    /// SASL mechanisms enabled on this listener. When `Some`, overrides
    /// the top-level `BrokerConfig::enabled_sasl_mechanisms`.
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
    /// KIP-371 `ssl.principal.mapping.rules` for this listener, parsed. It
    /// rewrites the Subject DN of an mTLS peer certificate into the principal
    /// name ACLs are written against. The default is an empty rule list, which
    /// passes the DN through exactly as Kafka's `DEFAULT` rule does.
    pub principal_mapper: crate::network::auth::SslPrincipalMapper,
}

/// Credentials the broker uses to connect *to* other brokers.
///
/// There is one variant for each SASL mechanism the inter-broker client
/// supports.
///
/// `Debug` renders the `Plain` and `Scram` passwords as `<redacted>`;
/// `BrokerConfig` derives `Debug`, and this enum sits inside it.
#[derive(Clone, PartialEq, Eq, derive_more::Debug)]
pub enum InterBrokerCredentials {
    /// SASL/PLAIN: `\0username\0password`.
    Plain {
        username: String,
        #[debug("{:?}", "<redacted>")]
        password: String,
    },
    /// SASL/SCRAM (SHA-256 or SHA-512).
    Scram {
        mechanism: SaslMechanism,
        username: String,
        #[debug("{:?}", "<redacted>")]
        password: String,
    },
    /// SASL/GSSAPI: authenticate as `client_principal` with the long-term key
    /// in `keytab_path`. This mechanism uses no password. `service_name` is
    /// the target broker's SPN primary; the client combines it with the
    /// dialed host into `service_name/host` at connect time. `kdc_url` is the
    /// KDC endpoint, for example `tcp://kdc:88`.
    Gssapi {
        keytab_path: PathBuf,
        client_principal: String,
        service_name: String,
        kdc_url: String,
    },
    /// SASL/OAUTHBEARER. The token file is read on every new outbound
    /// connection so credential rotation does not require a broker restart.
    OAuthBearer { token_path: PathBuf },
}

impl InterBrokerCredentials {
    /// The SASL mechanism this credential set authenticates with.
    #[must_use]
    pub fn mechanism(&self) -> SaslMechanism {
        match self {
            Self::Plain { .. } => SaslMechanism::Plain,
            Self::Scram { mechanism, .. } => *mechanism,
            Self::Gssapi { .. } => SaslMechanism::Gssapi,
            Self::OAuthBearer { .. } => SaslMechanism::OAuthBearer,
        }
    }
}

impl BrokerConfig {
    /// Returns the effective listener list.
    ///
    /// When [`listeners`][Self::listeners] is empty (the default), this
    /// method builds a single `PLAINTEXT` listener from the legacy
    /// `listen_addr` and `advertised_listener` fields.
    #[must_use]
    pub fn effective_listeners(&self) -> Vec<ListenerSpec> {
        if !self.listeners.is_empty() {
            return self.listeners.clone();
        }
        vec![ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: self.listen_addr,
            advertised: self.advertised_listener.clone(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: crate::SslPrincipalMapper::default(),
        }]
    }

    /// The broker-wide idle window in force: what the operator configured, or
    /// Kafka's `connections.max.idle.ms` default when they configured
    /// nothing. Every path that only wants the window reads it here, so that
    /// `connections_max_idle` can keep carrying the provenance
    /// `DescribeConfigs` reports.
    #[must_use]
    pub fn effective_connections_max_idle(&self) -> krabka_units::Time {
        self.connections_max_idle
            .unwrap_or(crate::config::DEFAULT_CONNECTIONS_MAX_IDLE)
    }

    /// The idle window a connection accepted on `listener_name` is held to.
    ///
    /// A per-listener override wins over the broker-wide
    /// `connections_max_idle`; names are matched without ASCII case, because
    /// Kafka spells the override key with a lowercased listener name
    /// (`listener.name.plaintext.connections.max.idle.ms`) while the listener
    /// itself is conventionally uppercase.
    ///
    /// `None` means this listener expires no connection. A non-positive
    /// configured value asks for that, the way Kafka's `Selector` arms no
    /// `IdleExpiryManager` when `connections.max.idle.ms` is not positive.
    #[must_use]
    pub fn connections_max_idle_for(&self, listener_name: &str) -> Option<std::time::Duration> {
        use krabka_units::convert::TimeExt as _;

        let configured = self
            .connections_max_idle_overrides
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(listener_name))
            .map_or_else(
                || self.effective_connections_max_idle(),
                |(_, value)| *value,
            );
        (configured.millis_i64() > 0).then(|| configured.to_std())
    }

    /// How long a failed SASL authentication holds its response and its close:
    /// Kafka's `connection.failed.authentication.delay.ms`. A non-positive
    /// value is no delay, as Kafka's `Selector` then closes at once.
    #[must_use]
    pub fn failed_authentication_delay(&self) -> std::time::Duration {
        use krabka_units::convert::TimeExt as _;

        std::time::Duration::from_millis(
            u64::try_from(self.connection_failed_authentication_delay.millis_i64()).unwrap_or(0),
        )
    }

    /// Which of the three [`ListenerKind`][crate::api_catalog::ListenerKind]
    /// the listener named `listener_name` is.
    ///
    /// A Kafka broker never advertises the control-plane keys tagged
    /// `controller` on any listener it binds -- `ApiKeys.brokerApis()` drops
    /// them whatever `inter.broker.listener.name` points at -- but krabka
    /// still has to *dispatch* the RPCs in
    /// [`INTER_BROKER_ONLY_APIS`][crate::api_catalog::INTER_BROKER_ONLY_APIS]
    /// somewhere: `isr_maintenance` sends `AlterPartition` to a peer's
    /// inter-broker endpoint on every ISR shrink and expand, and that peer has
    /// to answer it. The listener
    /// [`inter_broker_listener_name`][Self::inter_broker_listener_name] names
    /// is where krabka accepts them.
    ///
    /// Whether that same listener also *advertises* those keys turns on
    /// whether a client can reach it too, which
    /// [`effective_listeners`][Self::effective_listeners] answers: on the
    /// default single-listener broker the one `PLAINTEXT` listener is the
    /// inter-broker listener and the only listener there is, so it is also
    /// what a client dials, and it withholds the control-plane keys the same
    /// as [`ListenerKind::Client`][crate::api_catalog::ListenerKind::Client]
    /// does. An operator who separates the two -- the way a Kafka deployment
    /// does anyway, `apache/kafka` ships `PLAINTEXT` for clients and `BROKER`
    /// for inter-broker traffic -- gets the dedicated inter-broker listener
    /// that keeps advertising them, because nothing else negotiates the RPC
    /// version against that table.
    #[must_use]
    pub fn listener_kind(&self, listener_name: &str) -> crate::api_catalog::ListenerKind {
        use crate::api_catalog::ListenerKind;

        if !listener_name.eq_ignore_ascii_case(&self.inter_broker_listener_name) {
            return ListenerKind::Client;
        }
        if self.effective_listeners().len() <= 1 {
            ListenerKind::ClientAndInterBroker
        } else {
            ListenerKind::InterBroker
        }
    }

    /// Whether this broker has somewhere to send KIP-714 client metrics, and
    /// so whether it advertises the two RPCs that invite a client to push.
    ///
    /// Either the explicit
    /// [`client_metrics_enable`][Self::client_metrics_enable] key or a
    /// configured
    /// [`client_metrics_otlp_endpoint`][Self::client_metrics_otlp_endpoint]
    /// counts as a receiver. Both are off by default, which is what a Kafka
    /// broker with no `ClientTelemetry` metric reporter advertises.
    #[must_use]
    pub fn client_metrics_receiver(&self) -> crate::api_catalog::ClientMetricsReceiver {
        use crate::api_catalog::ClientMetricsReceiver;

        if self.client_metrics_enable || self.client_metrics_otlp_endpoint.is_some() {
            ClientMetricsReceiver::Configured
        } else {
            ClientMetricsReceiver::Absent
        }
    }

    /// The KIP-368 re-authentication window a SASL session on `listener_name`
    /// is held to, or `None` when that listener expires no session.
    ///
    /// A per-listener override wins over the broker-wide
    /// `connections_max_reauth`; names are matched without ASCII case, for the
    /// same reason [`BrokerConfig::connections_max_idle_for`] does.
    #[must_use]
    pub fn connections_max_reauth_for(&self, listener_name: &str) -> Option<krabka_units::Time> {
        use krabka_units::convert::TimeExt as _;

        self.connections_max_reauth_overrides
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(listener_name))
            .map(|(_, value)| *value)
            .or(self.connections_max_reauth)
            .filter(|value| value.millis_i64() > 0)
    }

    pub(super) fn validate_outbound_sasl(
        &self,
        inter_broker: &ListenerSpec,
    ) -> Result<(), BrokerError> {
        let Some(credentials) = self.inter_broker_credentials.as_ref() else {
            return Ok(());
        };
        let mechanism = credentials.mechanism();

        if inter_broker.protocol.requires_sasl() {
            let enabled = inter_broker
                .sasl_mechanisms
                .as_deref()
                .unwrap_or(&self.enabled_sasl_mechanisms);
            if !enabled.contains(&mechanism) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "inter-broker credential mechanism {} is not enabled on listener {}",
                    mechanism.wire_name(),
                    inter_broker.name
                )));
            }
        }
        if self.controller_listener_protocol.requires_sasl()
            && !self.enabled_sasl_mechanisms.contains(&mechanism)
        {
            return Err(BrokerError::InvalidRuntimeConfig(format!(
                "inter-broker credential mechanism {} is not enabled on the controller listener",
                mechanism.wire_name()
            )));
        }
        if let InterBrokerCredentials::OAuthBearer { token_path } = credentials {
            let token = std::fs::read(token_path).map_err(|error| {
                BrokerError::InvalidRuntimeConfig(format!(
                    "cannot read inter-broker OAUTHBEARER token {}: {error}",
                    token_path.display()
                ))
            })?;
            let token = token.trim_ascii();
            if token.is_empty() || token.contains(&b'\x01') {
                return Err(BrokerError::InvalidRuntimeConfig(
                    "inter-broker OAUTHBEARER token must be non-empty and contain no RFC 7628 separator"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{BrokerError as BrokerStartError, config::test_support::base};

    #[test]
    fn accepts_distinct_listener_bind_addresses() {
        let c = base();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_bind_collision() {
        let mut c = base();
        c.listeners[1].bind_addr = c.listeners[0].bind_addr;
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::ListenerConflict { .. })
        ));
    }

    #[test]
    fn the_inter_broker_listener_is_the_only_one_that_widens_the_api_table() {
        use crate::api_catalog::ListenerKind;

        let c = base();
        assert!(c.listener_kind("INTERNAL") == ListenerKind::InterBroker);
        // Kafka spells the override key with a lowercased listener name, so
        // the match ignores ASCII case here as elsewhere.
        assert!(c.listener_kind("internal") == ListenerKind::InterBroker);
        assert!(c.listener_kind("EXTERNAL") == ListenerKind::Client);
        assert!(c.listener_kind("NONESUCH") == ListenerKind::Client);
    }

    #[test]
    fn a_single_listener_broker_is_client_and_inter_broker() {
        use crate::api_catalog::ListenerKind;

        // The default: no declared listeners, so `effective_listeners` builds
        // one `PLAINTEXT` spec and `inter_broker_listener_name` names it. That
        // listener carries client and inter-broker traffic together, so it
        // still dispatches the control-plane keys -- krabka's own peers reach
        // it there because there is nowhere else -- but, like `Client`, it
        // withholds them from `ApiVersions`: a client can reach this listener
        // too, and no Kafka broker listener ever advertises them.
        let c = BrokerConfig::default();
        assert!(c.inter_broker_listener_name == "PLAINTEXT");
        assert!(c.effective_listeners().len() == 1);
        assert!(c.listener_kind("PLAINTEXT") == ListenerKind::ClientAndInterBroker);
    }

    #[test]
    fn a_client_metrics_receiver_comes_from_either_the_key_or_the_otlp_endpoint() {
        use crate::api_catalog::ClientMetricsReceiver;

        let mut c = base();
        assert!(c.client_metrics_receiver() == ClientMetricsReceiver::Absent);

        c.client_metrics_enable = true;
        assert!(c.client_metrics_receiver() == ClientMetricsReceiver::Configured);

        c.client_metrics_enable = false;
        c.client_metrics_otlp_endpoint = Some("http://collector:4317".to_string());
        assert!(c.client_metrics_receiver() == ClientMetricsReceiver::Configured);
    }

    #[test]
    fn rejects_missing_inter_broker_listener() {
        let mut c = base();
        c.inter_broker_listener_name = "NONESUCH".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::InvalidInterBrokerListener { .. })
        ));
    }

    #[test]
    fn rejects_sasl_listener_without_mechanisms() {
        let mut c = base();
        c.enabled_sasl_mechanisms.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn legacy_default_passes() {
        let c = BrokerConfig::default();
        c.validate().expect("legacy default must validate");
    }

    #[test]
    fn defaults_listen_on_localhost_9092() {
        let c = BrokerConfig::default();
        assert!(c.listen_addr.port() == 9092);
        assert!(c.broker_id == 1);
    }

    #[test]
    fn rejects_controller_tls_without_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Ssl,
            tls_config: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::Tls(_))));
    }

    #[test]
    fn rejects_controller_sasl_without_mechanisms() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::SaslListenerNoMechanisms { .. })
        ));
    }

    #[test]
    fn outbound_oauthbearer_credentials_validate_for_data_and_controller_listeners() {
        let (_dir, token_path) = crate::test_support::oauth_token_file(
            crate::test_support::OAuthTokenContents::Nonempty,
        );
        let credentials = Some(InterBrokerCredentials::OAuthBearer { token_path });
        let data_listener = ListenerSpec {
            name: "OAUTH".into(),
            bind_addr: "127.0.0.1:9094".parse().unwrap(),
            advertised: "broker:9094".into(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: Some(vec![SaslMechanism::OAuthBearer]),
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        let data = BrokerConfig {
            listeners: vec![data_listener],
            inter_broker_listener_name: "OAUTH".into(),
            inter_broker_credentials: credentials.clone(),
            ..BrokerConfig::default()
        };
        assert!(data.validate().is_ok());

        let controller = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::OAuthBearer],
            inter_broker_credentials: credentials,
            ..BrokerConfig::default()
        };
        assert!(controller.validate().is_ok());
    }

    #[test]
    fn rejects_outbound_credential_mechanism_not_enabled_on_sasl_targets() {
        let data = BrokerConfig {
            listeners: vec![ListenerSpec {
                name: "INTERNAL".into(),
                bind_addr: "127.0.0.1:9094".parse().unwrap(),
                advertised: "broker:9094".into(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: Some(vec![SaslMechanism::Plain]),
                principal_mapper: crate::SslPrincipalMapper::default(),
            }],
            inter_broker_listener_name: "INTERNAL".into(),
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer {
                token_path: "/unused/token".into(),
            }),
            plain_credentials: [("admin".to_string(), "admin-secret".to_string())]
                .into_iter()
                .collect(),
            ..BrokerConfig::default()
        };
        let error = data
            .validate()
            .expect_err("data-listener mechanism mismatch is rejected");
        assert!(
            error
                .to_string()
                .contains("not enabled on listener INTERNAL")
        );

        let controller = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            // A controller listener offering PLAIN needs a credential table to
            // reach the outbound-mismatch error this case is about.
            plain_credentials: [("admin".to_string(), "admin-secret".to_string())]
                .into_iter()
                .collect(),
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer {
                token_path: "/unused/token".into(),
            }),
            ..BrokerConfig::default()
        };
        let error = controller
            .validate()
            .expect_err("controller-listener mechanism mismatch is rejected");
        assert!(
            error
                .to_string()
                .contains("not enabled on the controller listener")
        );
    }

    #[test]
    fn rejects_empty_outbound_oauthbearer_token() {
        let (_dir, token_path) =
            crate::test_support::oauth_token_file(crate::test_support::OAuthTokenContents::Empty);
        let c = BrokerConfig {
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer { token_path }),
            ..BrokerConfig::default()
        };
        let error = c.validate().expect_err("empty OAuth token is rejected");
        assert!(error.to_string().contains("token must be non-empty"));
    }

    #[test]
    fn legacy_default_still_passes() {
        BrokerConfig::default()
            .validate()
            .expect("legacy default validates");
    }

    #[test]
    fn per_listener_sasl_mechanisms_satisfy_validation_without_broker_default() {
        let tls = TlsConfig {
            cert_chain_path: std::path::PathBuf::from("/tls/c"),
            private_key_path: std::path::PathBuf::from("/tls/k"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: krabka_security::ClientAuthMode::Disabled,
        };
        let listener = ListenerSpec {
            name: "scram".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "broker-0:9094".into(),
            protocol: ListenerProtocol::SaslSsl,
            tls_config: Some(tls.clone()),
            sasl_mechanisms: Some(vec![SaslMechanism::ScramSha512]),
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        let c = BrokerConfig {
            listeners: vec![listener],
            inter_broker_listener_name: "scram".into(),
            enabled_sasl_mechanisms: vec![],
            tls_config: Some(tls),
            controller_listener_protocol: ListenerProtocol::Plaintext,
            ..BrokerConfig::default()
        };
        c.validate()
            .expect("per-listener mechanisms satisfy SASL validation");
    }

    /// `BrokerConfig` derives `Debug` and this enum sits inside it, so any
    /// `{:?}` of the config -- a startup log line, a panic message, a test
    /// failure -- would print the inter-broker password if this rendering
    /// regressed to a derive.
    #[test]
    fn debug_renders_the_username_but_never_the_password() {
        let cases = [
            InterBrokerCredentials::Plain {
                username: "broker-user".into(),
                password: "hunter2-plain".into(),
            },
            InterBrokerCredentials::Scram {
                mechanism: SaslMechanism::ScramSha512,
                username: "broker-user".into(),
                password: "hunter2-scram".into(),
            },
        ];
        for credentials in cases {
            let rendered = format!("{credentials:?}");
            assert!(
                rendered.contains("broker-user"),
                "username must stay visible: {rendered}"
            );
            assert!(
                !rendered.contains("hunter2"),
                "password leaked into Debug: {rendered}"
            );
            assert!(
                rendered.contains("<redacted>"),
                "the password field must still be shown as redacted: {rendered}"
            );
        }
    }

    /// The passwordless variants carry operational detail an operator reads
    /// out of a log, so redaction must not have swallowed them too.
    #[test]
    fn debug_keeps_the_passwordless_variants_legible() {
        let gssapi = format!(
            "{:?}",
            InterBrokerCredentials::Gssapi {
                keytab_path: "/etc/krabka/broker.keytab".into(),
                client_principal: "krabka/broker-0@EXAMPLE.COM".into(),
                service_name: "kafka".into(),
                kdc_url: "tcp://kdc:88".into(),
            }
        );
        assert!(gssapi.contains("broker.keytab"), "{gssapi}");
        assert!(gssapi.contains("krabka/broker-0@EXAMPLE.COM"), "{gssapi}");
        assert!(gssapi.contains("kafka"), "{gssapi}");
        assert!(gssapi.contains("tcp://kdc:88"), "{gssapi}");

        let oauth = format!(
            "{:?}",
            InterBrokerCredentials::OAuthBearer {
                token_path: "/var/run/krabka/token".into(),
            }
        );
        assert!(oauth.contains("/var/run/krabka/token"), "{oauth}");
    }

    /// The mechanism the outbound client negotiates with, and what
    /// `validate_outbound_sasl` checks the listener's enabled list against: a
    /// wrong arm here silently authenticates as the wrong mechanism.
    #[test]
    fn mechanism_reports_the_credential_variants_sasl_mechanism() {
        let cases = [
            (
                InterBrokerCredentials::Plain {
                    username: "u".into(),
                    password: "p".into(),
                },
                SaslMechanism::Plain,
            ),
            (
                InterBrokerCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha256,
                    username: "u".into(),
                    password: "p".into(),
                },
                SaslMechanism::ScramSha256,
            ),
            (
                InterBrokerCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha512,
                    username: "u".into(),
                    password: "p".into(),
                },
                SaslMechanism::ScramSha512,
            ),
            (
                InterBrokerCredentials::Gssapi {
                    keytab_path: "/k".into(),
                    client_principal: "c".into(),
                    service_name: "kafka".into(),
                    kdc_url: "tcp://kdc:88".into(),
                },
                SaslMechanism::Gssapi,
            ),
            (
                InterBrokerCredentials::OAuthBearer {
                    token_path: "/t".into(),
                },
                SaslMechanism::OAuthBearer,
            ),
        ];
        for (credentials, expected) in cases {
            assert!(credentials.mechanism() == expected, "{credentials:?}");
        }
    }

    /// With no `listeners` configured the broker still has to bind and
    /// advertise something; the synthesised entry is what clients get back in
    /// `Metadata`, so it must carry the legacy fields verbatim.
    #[test]
    fn an_empty_listener_list_synthesises_one_from_the_legacy_fields() {
        let c = BrokerConfig {
            listeners: vec![],
            listen_addr: "127.0.0.1:19092".parse().unwrap(),
            advertised_listener: "broker-0:19092".to_string(),
            ..BrokerConfig::default()
        };

        let listeners = c.effective_listeners();

        assert!(listeners.len() == 1);
        assert!(listeners[0].name == "PLAINTEXT");
        assert!(listeners[0].bind_addr == c.listen_addr);
        assert!(listeners[0].advertised == "broker-0:19092");
        assert!(listeners[0].protocol == ListenerProtocol::Plaintext);
        assert!(listeners[0].tls_config.is_none());
        assert!(listeners[0].sasl_mechanisms.is_none());
    }

    /// A configured list must be returned as-is rather than replaced by the
    /// legacy single-listener fallback.
    #[test]
    fn a_configured_listener_list_is_returned_unchanged() {
        let c = base();

        let listeners = c.effective_listeners();

        let names: Vec<&str> = listeners.iter().map(|l| l.name.as_str()).collect();
        let configured: Vec<&str> = c.listeners.iter().map(|l| l.name.as_str()).collect();
        assert!(names == configured);
        assert!(listeners.len() > 1);
    }

    #[test]
    fn rejects_gssapi_mechanism_without_gssapi_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Plaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::Gssapi],
            gssapi: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::GssapiConfigMissing)
        ));
    }
}

#[cfg(test)]
mod connections_max_idle_tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt as _, millis, secs};

    use crate::config::{BrokerConfig, DEFAULT_CONNECTIONS_MAX_IDLE};

    /// A broker-wide window of `idle` with the named per-listener overrides.
    fn config(idle: Time, overrides: &[(&str, Time)]) -> BrokerConfig {
        BrokerConfig {
            connections_max_idle: Some(idle),
            connections_max_idle_overrides: crate::test_support::listener_time_overrides(overrides),
            ..BrokerConfig::default()
        }
    }

    #[test]
    fn an_unnamed_listener_gets_kafkas_ten_minute_default() {
        // Kafka spells the default 600000, which is the ten minutes below.
        assert!(DEFAULT_CONNECTIONS_MAX_IDLE.millis_i64() == 600_000);
        assert!(
            BrokerConfig::default().connections_max_idle_for("PLAINTEXT")
                == Some(std::time::Duration::from_mins(10))
        );
    }

    #[test]
    fn a_listener_override_wins_over_the_broker_wide_window() {
        let config = config(secs(600), &[("EXTERNAL", secs(20))]);
        assert!(
            config.connections_max_idle_for("EXTERNAL") == Some(std::time::Duration::from_secs(20))
        );
        assert!(
            config.connections_max_idle_for("INTERNAL") == Some(std::time::Duration::from_mins(10))
        );
    }

    /// Kafka lowercases the listener name in `listener.name.<name>.…`, so an
    /// override written the Kafka way still has to find an uppercase listener.
    #[test]
    fn an_override_matches_its_listener_without_ascii_case() {
        let config = config(secs(600), &[("external", secs(20))]);
        assert!(
            config.connections_max_idle_for("EXTERNAL") == Some(std::time::Duration::from_secs(20))
        );
    }

    /// Kafka arms no idle-expiry manager for a non-positive
    /// `connections.max.idle.ms`, so neither does krabka -- broker-wide and
    /// per-listener alike.
    #[test]
    fn a_non_positive_window_expires_nothing() {
        assert!(
            config(millis(0), &[])
                .connections_max_idle_for("PLAINTEXT")
                .is_none()
        );
        assert!(
            config(Time::from_millis(-1), &[])
                .connections_max_idle_for("PLAINTEXT")
                .is_none()
        );
        assert!(
            config(secs(600), &[("EXTERNAL", millis(0))])
                .connections_max_idle_for("EXTERNAL")
                .is_none()
        );
    }
}

#[cfg(test)]
mod connections_max_reauth_tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt as _, millis, secs};

    use crate::config::BrokerConfig;

    /// A broker-wide KIP-368 window with the named per-listener overrides.
    fn config(broker_wide: Option<Time>, overrides: &[(&str, Time)]) -> BrokerConfig {
        BrokerConfig {
            connections_max_reauth: broker_wide,
            connections_max_reauth_overrides: crate::test_support::listener_time_overrides(
                overrides,
            ),
            ..BrokerConfig::default()
        }
    }

    /// Kafka's `connections.max.reauth.ms` defaults to 0, which means sessions
    /// never expire. A default that started expiring sessions would disconnect
    /// every SASL client on a timer nobody asked for.
    #[test]
    fn nothing_configured_expires_no_session() {
        assert!(
            BrokerConfig::default()
                .connections_max_reauth_for("SASL_SSL")
                .is_none()
        );
    }

    /// The whole point of the override: one listener re-authenticates on a
    /// different cadence from the rest, and a listener with no override still
    /// gets the broker-wide window.
    #[test]
    fn a_listener_override_wins_over_the_broker_wide_window() {
        let config = config(Some(secs(3600)), &[("EXTERNAL", secs(60))]);

        assert!(config.connections_max_reauth_for("EXTERNAL") == Some(secs(60)));
        assert!(config.connections_max_reauth_for("INTERNAL") == Some(secs(3600)));
    }

    /// An override with no broker-wide window configured still applies: the
    /// `or(connections_max_reauth)` fallback must not swallow the match.
    #[test]
    fn a_listener_override_applies_without_a_broker_wide_window() {
        let config = config(None, &[("EXTERNAL", secs(60))]);

        assert!(config.connections_max_reauth_for("EXTERNAL") == Some(secs(60)));
        assert!(config.connections_max_reauth_for("INTERNAL").is_none());
    }

    /// Kafka spells the property `listener.name.<lowercased>.connections.max.
    /// reauth.ms` while the listener itself is conventionally uppercase, so an
    /// override written the Kafka way still has to find its listener.
    #[test]
    fn an_override_matches_its_listener_without_ascii_case() {
        let config = config(Some(secs(3600)), &[("sasl_ssl", secs(60))]);

        assert!(config.connections_max_reauth_for("SASL_SSL") == Some(secs(60)));
    }

    /// A non-positive window means "expire nothing", broker-wide and
    /// per-listener alike -- and a zero override must not silently fall back
    /// to the broker-wide window it was written to disable.
    #[test]
    fn a_non_positive_window_expires_nothing() {
        assert!(
            config(Some(millis(0)), &[])
                .connections_max_reauth_for("SASL_SSL")
                .is_none()
        );
        assert!(
            config(Some(Time::from_millis(-1)), &[])
                .connections_max_reauth_for("SASL_SSL")
                .is_none()
        );
        assert!(
            config(Some(secs(3600)), &[("SASL_SSL", millis(0))])
                .connections_max_reauth_for("SASL_SSL")
                .is_none()
        );
        assert!(
            config(Some(secs(3600)), &[("SASL_SSL", Time::from_millis(-1))])
                .connections_max_reauth_for("SASL_SSL")
                .is_none()
        );
    }
}
