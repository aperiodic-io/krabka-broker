//! The broker the containers dial, and what a case changes about it before it
//! starts.
//!
//! [`start_jvm_broker`] boots one broker on the host and hands back both
//! addresses it answers on: loopback, which the host-side control plane dials,
//! and `host.docker.internal`, which the JVM tools inside containers bootstrap
//! against. [`sasl_listener`] and [`gate_on`] are the adjustments the
//! break-glass cases make and the freeze cases do not, and [`sasl_props`] is
//! the client-side half of the first of them.

use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::{
    support,
    vocabulary::{SASL_LISTENER, approver_set},
};

/// One broker on the host, addressed the two ways this suite needs.
pub(super) struct JvmBroker {
    /// Dropping the handle stops the broker, so the case owns it to the end.
    _handle: BrokerHandle,
    /// The log directory, which outlives the broker only by a moment.
    _dir: TempDir,
    /// Loopback. The host-side clients in this suite dial this.
    pub(super) host: String,
    /// `host.docker.internal`. The containers bootstrap against this, and the
    /// broker advertises it in `Metadata`.
    pub(super) container: String,
}

/// Boot one broker that the cp-kafka containers can reach.
///
/// The shape follows [`crate::jvm_acceptance::start_host_broker_with`], with one
/// difference: the listeners come from [`support::JvmListeners::allocate`]
/// rather than from the process-wide set, so two cases in this binary never
/// contend for a port.
///
/// `adjust` sees a config whose addresses are already filled in, which is what
/// lets [`sasl_listener`] build a listener spec on the same bind and
/// advertised addresses.
pub(super) async fn start_jvm_broker(adjust: impl FnOnce(&mut BrokerConfig)) -> JvmBroker {
    support::init_tracing();
    let listeners = support::JvmListeners::allocate();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen: SocketAddr = listeners
        .listen
        .parse()
        .expect("an allocated listen address");
    let controller: SocketAddr = listeners
        .controller
        .parse()
        .expect("an allocated controller address");

    let mut config = support::jvm_broker_config(
        dir.path().to_path_buf(),
        crate::support::JvmBrokerSetup {
            listen,
            controller,
            advertised: listeners.advertised.clone(),
            voters: crate::support::controller_voters(&[(1, controller)]),
            ..Default::default()
        },
    );
    adjust(&mut config);

    // `Broker::start` waits for a metadata leader before it returns, so the
    // control-plane writes below need no retry loop of their own.
    let handle = Broker::start(config).await.expect("start broker");
    JvmBroker {
        _handle: handle,
        _dir: dir,
        host: format!("127.0.0.1:{}", listen.port()),
        container: listeners.advertised,
    }
}

/// Turn the broker's one listener into `SASL_PLAINTEXT`/`PLAIN` over the same
/// addresses, and install `users` as PLAIN credentials.
///
/// The break-glass case needs this and the other three do not. Over a
/// plaintext listener every connection authenticates as one anonymous
/// principal, which can prove that the gate refuses and can never prove that
/// two distinct people got past it.
pub(super) fn sasl_listener(config: &mut BrokerConfig, users: &[(&str, &str)]) {
    config.listeners = vec![ListenerSpec {
        name: SASL_LISTENER.to_owned(),
        bind_addr: config.listen_addr,
        advertised: config.advertised_listener.clone(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    SASL_LISTENER.clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (user, pass) in users {
        config
            .plain_credentials
            .insert((*user).to_owned(), (*pass).to_owned());
    }
}

/// Turn the break-glass two-person rule on, with no signature demanded.
///
/// An empty `approvers` turns the whole workflow off, so naming the set is
/// what puts the five gated transitions behind an approval. `signed_actions`
/// is emptied explicitly rather than left to the default: the file-config
/// default names three actions, and this suite has no operator key material to
/// sign with. What it asserts is what the JVM tool sees, and a signature
/// changes nothing about that.
pub(super) fn gate_on(config: &mut BrokerConfig) {
    config.break_glass.approvers = approver_set();
    config.break_glass.signed_actions = Vec::new();
}

/// The JVM client properties for one PLAIN operator.
pub(super) fn sasl_props(user: &str, pass: &str) -> String {
    crate::jvm_acceptance::plain_client_properties(user, pass)
}
