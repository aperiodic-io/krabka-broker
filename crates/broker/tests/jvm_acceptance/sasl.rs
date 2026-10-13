//! Single-broker `SASL_PLAINTEXT` bring-up and the JAAS strings that reach it.
//!
//! The JVM tools authenticate with a JAAS login-module entry, so the builders
//! for those entries live beside the brokers that accept them.

use {
    super::{broker::HostBroker, ports::broker0_advertised},
    krabka_broker::BrokerConfig,
};

pub(crate) const ADMIN: &str = "admin";
pub(crate) const ADMIN_PASS: &str = "admin-secret";
pub(crate) const ALICE: &str = "alice";
pub(crate) const ALICE_PASS: &str = "alice-secret";

/// Build a JAAS config string for the `PlainLoginModule`. The trailing `;`
/// is mandatory. Kafka's JAAS parser rejects the entry without it.
pub(crate) fn plain_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Build a JAAS config string for the `ScramLoginModule`. The
/// SCRAM-SHA-512 acceptance test uses it.
pub(crate) fn scram_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener on
/// an allocated port, advertised as an allocated port. The listener
/// starts with the given PLAIN `users` already installed. Mirrors
/// [`start_host_broker`] otherwise.
pub(crate) fn start_sasl_plaintext_broker(
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = HostBroker> {
    start_sasl_with_users(users, None)
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms, plus a single PLAIN
/// super-user (`admin` / `admin_pass`). The super-user designation grants
/// the admin principal `CLUSTER_AUTHORIZATION` on
/// `AlterUserScramCredentials` (51). The admin runs the JVM `kafka-configs
/// --alter --entity-type users` tool over PLAIN, so that tool can provision
/// SCRAM credentials for other users.
///
/// `jvm_sasl_scram_sha512_produce_consume` and
/// `jvm_sasl_scram_sha256_produce_consume` use this broker.
pub(crate) fn start_dual_mech_broker(
    admin: &str,
    admin_pass: &str,
) -> impl std::future::Future<Output = HostBroker> {
    start_dual_mech_broker_with_reauth(admin, admin_pass, None)
}

/// [`start_dual_mech_broker`] with the KIP-368 `connections.max.reauth.ms`
/// window set, so a JVM client on the listener must re-authenticate in band
/// before `max_reauth` elapses or have its connection closed.
pub(crate) fn start_dual_mech_broker_with_reauth(
    admin: &str,
    admin_pass: &str,
    max_reauth: Option<krabka_units::Time>,
) -> impl std::future::Future<Output = HostBroker> {
    super::broker::start_host_broker_with(move |config| {
        configure_sasl(config, &[(admin, admin_pass)], Some(admin));
        config.enabled_sasl_mechanisms.extend([
            krabka_security::SaslMechanism::ScramSha256,
            krabka_security::SaslMechanism::ScramSha512,
        ]);
        config.connections_max_reauth = max_reauth;
    })
}

/// JAAS config for the JVM `OAuthBearerLoginModule` built-in *unsecured*
/// token issuer. `unsecuredLoginStringClaim_sub` mints an
/// `alg:none` JWS with `sub=<user>`, `iat=now`, `exp=now+3600s`. That is
/// exactly the token shape Krabka's
/// [`krabka_security::UnsecuredJwsValidator`] accepts. It pairs with
/// `OAuthBearerUnsecuredLoginCallbackHandler` on the client.
pub(crate) fn oauthbearer_jaas(sub: &str) -> String {
    format!(
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
         unsecuredLoginStringClaim_sub=\"{sub}\";",
    )
}

/// Spawn a single `SASL_PLAINTEXT` broker that enables **only** OAUTHBEARER.
/// The broker validates the JVM client's unsecured JWS with the default
/// validator (principal claim `sub`). Mirrors [`start_sasl_plaintext_broker`].
pub(crate) async fn start_oauthbearer_broker() -> HostBroker {
    super::broker::start_host_broker_with(|config| {
        configure_sasl(config, &[], None);
        config.enabled_sasl_mechanisms = vec![krabka_security::SaslMechanism::OAuthBearer];
    })
    .await
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// PLAIN, plus a configured PLAIN super-user. Mirrors
/// [`start_sasl_plaintext_broker`] otherwise. The ACL JVM acceptance tests
/// use it: the super-user authenticates with PLAIN and runs
/// `kafka-acls --add/--remove/--list`. Those flags hit `CreateAcls (30)`,
/// `DeleteAcls (31)`, and `DescribeAcls (29)`, which all need the
/// `Cluster Alter` or `Cluster Describe` operation. The super-user bypass
/// in `authorize()` short-circuits that check.
pub(crate) fn start_sasl_plaintext_broker_with_super_user<'a>(
    super_user: &'a str,
    users: &'a [(&'a str, &'a str)],
) -> impl std::future::Future<Output = HostBroker> + 'a {
    start_sasl_with_users(users, Some(super_user))
}

pub(crate) fn configure_sasl(
    config: &mut BrokerConfig,
    users: &[(&str, &str)],
    super_user: Option<&str>,
) {
    use {
        krabka_broker::config::ListenerSpec,
        krabka_security::{ListenerProtocol, SaslMechanism},
    };

    config.listeners = vec![ListenerSpec {
        advertised: broker0_advertised().into(),
        ..crate::support::listeners::listener(
            "SASL_PLAINTEXT",
            config.listen_addr,
            ListenerProtocol::SaslPlaintext,
        )
    }];
    config.inter_broker_listener_name = "SASL_PLAINTEXT".into();
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (user, password) in users {
        config
            .plain_credentials
            .insert((*user).into(), (*password).into());
    }
    if let Some(user) = super_user {
        // The node's own heartbeat reaches the PLAINTEXT controller as ANONYMOUS.
        config.super_users.extend([user.into(), "ANONYMOUS".into()]);
        config.authorizer = std::sync::Arc::new(
            krabka_broker::authorizer::SimpleAclAuthorizer::new(config.super_users.clone()),
        );
    }
}

/// ACL fixture with an admin-owned topic and a provisioned PLAIN user.
pub(crate) async fn start_plain_acl_topic(
    topic: &str,
    user: &str,
    password: &str,
) -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    super::docker::ClientPropsFile,
) {
    let (broker, dir) = start_sasl_plaintext_broker_with_super_user("admin", &[
        ("admin", "admin-secret"),
        (user, password),
    ])
    .await;
    super::docker::nc_check_connectivity();
    let props = super::docker::write_plain_props("admin", "admin-secret");
    super::docker::create_console_topic(
        super::docker::KAFKA_IMAGE_TXN,
        &[&props.mount_str()],
        topic,
        1,
        1,
    );
    (broker, dir, props)
}

fn start_sasl_with_users<'a>(
    users: &'a [(&'a str, &'a str)],
    super_user: Option<&'a str>,
) -> impl std::future::Future<Output = HostBroker> + 'a {
    super::broker::start_host_broker_with(move |config| configure_sasl(config, users, super_user))
}

/// Admin-created topic with the user's exact literal topic operations.
pub(crate) async fn start_plain_acl_topic_with_ops(
    topic: &str,
    user: &str,
    password: &str,
    operations: &[&str],
) -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    super::docker::ClientPropsFile,
) {
    let fixture = start_plain_acl_topic(topic, user, password).await;
    super::docker::add_console_acl(
        super::docker::KAFKA_IMAGE_TXN,
        &[&fixture.2.mount_str()],
        &format!("User:{user}"),
        operations,
        "--topic",
        topic,
    );
    fixture
}
