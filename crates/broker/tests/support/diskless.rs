//! Peer identity and distinct-rack placement for native diskless WAL clusters.

use {crate::support::NodeIndex, krabka_broker::NodeId};

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct WalVoterCount(pub usize);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct PeerCredentialsSetup<'a> {
    #[default(WalVoterCount(3))]
    pub voters: WalVoterCount,
    #[default("diskless-e2e")]
    pub password: &'a str,
}

pub fn broker_principal(node: NodeId) -> String { format!("broker-{node}") }

/// Preserve the lazy ordered peer credential construction and checked node ids.
///
/// # Panics
/// Panics if the original peer index arithmetic or checked node id is out of range.
#[bon::builder]
pub fn peer_credentials(
    #[builder(into, default = WalVoterCount (3))] voters: WalVoterCount,
    #[builder(default = "diskless-e2e")] password: &'_ str,
) -> impl Iterator<Item = (String, String)> + '_ {
    let setup = PeerCredentialsSetup { voters, password };
    (0..setup.voters.0).map(move |peer| {
        (
            broker_principal(NodeId(u64::try_from(peer + 1).expect("small cluster"))),
            setup.password.to_owned(),
        )
    })
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct DisklessIdentitySetup<'a> {
    pub index: NodeIndex,
    #[default(NodeId(1))]
    pub node: NodeId,
    #[default(WalVoterCount(3))]
    pub voters: WalVoterCount,
    #[default("diskless-e2e")]
    pub password: &'a str,
}

/// Set the broker's authenticated identity before its distinct rack and replica count.
/// `select_voters` picks one broker per unused rack; a shared rack would yield
/// two voters and make the WAL reconcile loop refuse the three-voter placement.
///
/// # Panics
/// Panics if the rack index does not fit the original byte arithmetic.
#[bon::builder]
pub fn configure_identity(
    config: &mut krabka_broker::BrokerConfig,
    #[builder(into, default)] index: NodeIndex,
    #[builder(into, default = NodeId (1))] node: NodeId,
    #[builder(into, default = WalVoterCount (3))] voters: WalVoterCount,
    #[builder(default = "diskless-e2e")] password: &'_ str,
) {
    let setup = DisklessIdentitySetup {
        index,
        node,
        voters,
        password,
    };
    config.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: broker_principal(setup.node),
        password: setup.password.to_owned(),
    });
    config.rack = Some(format!(
        "rack-{}",
        char::from(b'a' + u8::try_from(setup.index.0).expect("small cluster"))
    ));
    config.diskless_wal_local_replica_count = setup.voters.0;
}

#[derive(krabka_macros::FieldDefaults)]
pub struct DisklessAuthenticationSetup<'a> {
    #[default("SASL_PLAINTEXT")]
    pub listener: &'a str,
    pub identity: DisklessIdentitySetup<'a>,
    pub additional_credentials: Vec<(String, String)>,
}

/// Install peer credentials, then this node's identity and distinct rack.
#[bon::builder]
pub fn configure_authentication(
    config: &mut krabka_broker::BrokerConfig,
    #[builder(default = "SASL_PLAINTEXT")] listener: &'_ str,
    #[builder(default)] identity: DisklessIdentitySetup<'_>,
    #[builder(default)] additional_credentials: Vec<(String, String)>,
) {
    let setup = DisklessAuthenticationSetup {
        listener,
        identity,
        additional_credentials,
    };
    setup
        .listener
        .clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![krabka_security::SaslMechanism::Plain];
    config.plain_credentials = peer_credentials()
        .voters(setup.identity.voters)
        .password(setup.identity.password)
        .call()
        .chain(setup.additional_credentials)
        .collect();
    configure_identity()
        .config(config)
        .index(setup.identity.index.0)
        .node(setup.identity.node.0)
        .voters(setup.identity.voters.0)
        .password(setup.identity.password)
        .call();
}
