//! Data-plane listener binding and the resumption of interrupted KIP-113
//! log-dir moves. Both run at the point where the broker is ready to accept
//! traffic, and both need the resolved listener set, so they share a module.
//!
//! Only a node with the broker role opens data-plane listeners. Kafka's
//! `ControllerServer` opens only the listeners that `controller.listener.names`
//! names, and `KafkaConfig` refuses a controller-only node whose `listeners`
//! name another one. A controller-only node serves the controller APIs on its
//! controller listener, which the metadata phase binds.

use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{Broker, accept::accept_loop},
    config::BrokerConfig,
    error::BrokerError,
    log_dir,
    partition_registry::PartitionRegistry,
    platform::Sockets,
};

pub(super) struct ListenerStartup {
    pub(super) bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>,
    /// The bound address of the inter-broker listener, or of the first
    /// listener when none has that name. `None` on a node without the broker
    /// role, which binds no data-plane listener.
    pub(super) listen_addr: Option<SocketAddr>,
    pub(super) future_logs:
        Arc<DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>>,
}

/// Binds the default data-plane listener before the broker registers itself,
/// when the config asks for an OS-assigned port and the caller supplied no
/// data-plane listener of its own.
///
/// `register_broker` reads `advertised_listener` to build the
/// `BrokerRegistrationRecord` this node publishes to the controller, and it
/// runs inside `start_metadata_phase`, before [`bind_listeners_and_recover_moves`]
/// binds the data plane. A `:0` config would therefore publish port 0 to every
/// other broker's `Metadata` and `ListGroups` routing. This binds the listener
/// now and writes the port it got into `listen_addr` and `advertised_listener`
/// first, exactly as `bind_ephemeral_controller_listener` does for the
/// controller listener. The bound socket is kept in `data_plane_listeners`, so
/// `bind_listeners_and_recover_moves` adopts it later instead of binding
/// again, and no other process can take the port in between.
///
/// A caller-supplied data-plane listener, a concrete port, and a
/// `config.listeners` (KIP-113 multi-listener) setup all keep their config as
/// it is. A node without the broker role binds nothing: it opens no
/// data-plane listener and does not register as a broker.
pub(super) async fn bind_ephemeral_data_plane_listener(
    config: &mut BrokerConfig,
    data_plane_listeners: &mut Vec<TcpListener>,
) -> Result<(), BrokerError> {
    if !config.is_broker()
        || !data_plane_listeners.is_empty()
        || !config.listeners.is_empty()
        || config.listen_addr.port() != 0
    {
        return Ok(());
    }
    let listener = crate::platform::bind_listener(config.listen_addr).await?;
    let bound = Sockets::TARGET.listener_address(&listener, config.listen_addr)?;
    config.listen_addr = bound;
    if let Some((host, _)) = config.advertised_listener.rsplit_once(':') {
        config.advertised_listener = format!("{host}:{}", bound.port());
    }
    data_plane_listeners.push(listener);
    Ok(())
}

/// The data-plane listeners that this node opens: every listener of the
/// config on a node with the broker role, and none on a node without it.
fn data_plane_listener_specs(config: &BrokerConfig) -> Vec<crate::config::ListenerSpec> {
    if config.is_broker() {
        config.effective_listeners()
    } else {
        Vec::new()
    }
}

/// Binds the data-plane listeners of [`data_plane_listener_specs`] and resumes
/// the interrupted log-dir moves.
///
/// A supplied listener that serves no spec is closed. On a node without the
/// broker role that is every supplied listener.
pub(super) async fn bind_listeners_and_recover_moves(
    config: &mut BrokerConfig,
    supplied_listeners: Vec<TcpListener>,
    partitions: &Arc<PartitionRegistry>,
    throttle_state: &Arc<crate::throttle::ThrottleState>,
) -> Result<ListenerStartup, BrokerError> {
    let bound = adopt_or_bind_listeners(
        Sockets::TARGET,
        data_plane_listener_specs(config),
        supplied_listeners,
    )
    .await?;
    let listen_addr = bound
        .iter()
        .find(|(spec, _, _)| spec.name == config.inter_broker_listener_name)
        .or_else(|| bound.first())
        .map(|(_, _, address)| *address);
    if let Some(listen_addr) = listen_addr
        && config.advertised_listener.ends_with(":0")
        && let Some((host, _)) = config.advertised_listener.rsplit_once(':')
    {
        config.advertised_listener = format!("{host}:{}", listen_addr.port());
    }
    let future_logs = Arc::new(DashMap::new());
    for log_dir in config.all_log_dirs() {
        for (topic, partition_id) in log_dir::scan_future(&log_dir).unwrap_or_default() {
            let partition = PartitionIndex(partition_id);
            if !partitions.contains(&topic, partition) {
                let path = log_dir::future_partition_dir(&log_dir, &topic, partition_id);
                if let Err(error) = std::fs::remove_dir_all(&path) {
                    tracing::warn!(path = %path.display(), %error, "failed to remove stranded future log");
                }
                continue;
            }
            if let Err(error) = crate::future_log::resume_move(
                partitions,
                &future_logs,
                &log_dir,
                &config.log_config,
                &topic,
                partition,
                crate::future_log::MovePolicy {
                    retry_backoff: config.future_log_move_retry_backoff,
                    read_chunk: config.future_log_move_read_chunk,
                    throttle: throttle_state.alter_log_dirs.clone(),
                },
            ) {
                tracing::warn!(%topic, partition = partition_id, ?error,
                    "failed to resume interrupted log-dir move");
            }
        }
    }
    Ok(ListenerStartup {
        bound,
        listen_addr,
        future_logs,
    })
}

/// Pairs every listener spec with a listener: a supplied one when one serves
/// the spec, and a fresh bind of the spec's address otherwise.
///
/// A native supplied listener serves the spec whose `bind_addr` equals its
/// local address. A preopened listener cannot report its address, so the
/// supplied listeners serve the specs in order, and each one is taken to be
/// bound on its spec's `bind_addr`.
async fn adopt_or_bind_listeners(
    sockets: Sockets,
    specs: Vec<crate::config::ListenerSpec>,
    mut supplied: Vec<TcpListener>,
) -> std::io::Result<Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>> {
    let mut bound = Vec::with_capacity(specs.len());
    for spec in specs {
        let serving = match sockets {
            Sockets::Native => supplied.iter().position(|listener| {
                listener
                    .local_addr()
                    .is_ok_and(|addr| addr == spec.bind_addr)
            }),
            Sockets::Preopened => (!supplied.is_empty()).then_some(0),
        };
        let listener = match serving {
            Some(index) => supplied.remove(index),
            None => crate::platform::bind_listener(spec.bind_addr).await?,
        };
        let address = sockets.listener_address(&listener, spec.bind_addr)?;
        bound.push((spec, listener, address));
    }
    Ok(bound)
}

pub(super) fn spawn_listener_tasks(
    broker: &Arc<Broker>,
    bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>,
) -> (CancellationToken, Vec<JoinHandle<()>>) {
    let shutdown = CancellationToken::new();
    let tasks = bound
        .into_iter()
        .map(|(spec, listener, _)| {
            tokio::spawn(accept_loop(
                Arc::clone(broker),
                listener,
                spec,
                shutdown.clone(),
            ))
        })
        .collect();
    (shutdown, tasks)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::config::{
        ListenerSpec,
        NodeRole::{Broker, Controller},
    };

    fn spec(name: &str, bind_addr: SocketAddr) -> ListenerSpec {
        ListenerSpec {
            name: name.to_owned(),
            bind_addr,
            advertised: bind_addr.to_string(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: crate::SslPrincipalMapper::default(),
        }
    }

    /// `(spec name, address the broker records, address the socket is bound
    /// to)` for each adopted listener.
    fn adopted(
        bound: &[(ListenerSpec, TcpListener, SocketAddr)],
    ) -> Vec<(String, SocketAddr, SocketAddr)> {
        bound
            .iter()
            .map(|(spec, listener, address)| {
                (
                    spec.name.clone(),
                    *address,
                    listener.local_addr().expect("local address"),
                )
            })
            .collect()
    }

    async fn two_listeners() -> (TcpListener, TcpListener, (SocketAddr, SocketAddr)) {
        let first = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let second = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let locals = (
            first.local_addr().expect("local address"),
            second.local_addr().expect("local address"),
        );
        (first, second, locals)
    }

    /// A preopened listener cannot report its address, so the supplied
    /// listeners serve the specs in order and take the spec's address. The
    /// specs name an address that no local interface has, so a bind of
    /// either would fail: the adoption binds nothing.
    #[tokio::test]
    async fn preopened_listeners_serve_the_specs_in_order_without_a_bind() {
        let (first, second, locals) = two_listeners().await;
        let data: SocketAddr = "10.255.0.1:9092".parse().expect("literal");
        let internal: SocketAddr = "10.255.0.1:9094".parse().expect("literal");

        let bound = adopt_or_bind_listeners(
            Sockets::Preopened,
            vec![spec("PLAINTEXT", data), spec("INTERNAL", internal)],
            vec![first, second],
        )
        .await
        .expect("adopted");

        assert!(
            adopted(&bound)
                == vec![
                    ("PLAINTEXT".to_owned(), data, locals.0),
                    ("INTERNAL".to_owned(), internal, locals.1),
                ]
        );
    }

    /// A native listener serves the spec whose address it is bound to,
    /// whatever the order of the supplied listeners.
    #[tokio::test]
    async fn native_listeners_serve_the_spec_with_their_address() {
        let (first, second, locals) = two_listeners().await;

        let bound = adopt_or_bind_listeners(
            Sockets::Native,
            vec![spec("PLAINTEXT", locals.1), spec("INTERNAL", locals.0)],
            vec![first, second],
        )
        .await
        .expect("adopted");

        assert!(
            adopted(&bound)
                == vec![
                    ("PLAINTEXT".to_owned(), locals.1, locals.1),
                    ("INTERNAL".to_owned(), locals.0, locals.0),
                ]
        );
    }

    /// What a node opens of its data plane, for each `process.roles`: the
    /// names of the bound listeners, the address `listen_addr` reports, and
    /// whether the port of the supplied listener still answers a connect.
    type DataPlane = (Vec<String>, Option<SocketAddr>, bool);

    /// A node with the broker role serves its data plane on the listener the
    /// caller supplied. A node without it closes that listener and binds
    /// nothing, as Kafka's controller-only node opens only the listeners that
    /// `controller.listener.names` names.
    #[tokio::test]
    async fn only_a_node_with_the_broker_role_opens_the_data_plane() {
        // (roles, whether the node opens the data plane)
        let cases = [
            (vec![Controller], false),
            (vec![Broker], true),
            (vec![Controller, Broker], true),
        ];
        for (roles, opens) in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let supplied = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = supplied.local_addr().expect("local address");
            let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
            config.listen_addr = address;
            config.advertised_listener = address.to_string();
            config.roles.clone_from(&roles);

            let startup = bind_listeners_and_recover_moves(
                &mut config,
                vec![supplied],
                &Arc::new(PartitionRegistry::new()),
                &Arc::new(crate::throttle::ThrottleState::new()),
            )
            .await
            .expect("bind the data plane");
            let names = startup
                .bound
                .iter()
                .map(|(spec, _, _)| spec.name.clone())
                .collect();
            let answers = tokio::net::TcpStream::connect(address).await.is_ok();

            let expected: DataPlane = if opens {
                (vec!["PLAINTEXT".to_owned()], Some(address), true)
            } else {
                (Vec::new(), None, false)
            };
            check!(
                (names, startup.listen_addr, answers) == expected,
                "{roles:?}"
            );
        }
    }

    /// A configured port 0 binds an ephemeral data-plane listener before the
    /// broker registers, and only a node with the broker role registers. A
    /// controller-only node binds nothing and keeps its config.
    #[tokio::test]
    async fn only_a_node_with_the_broker_role_binds_an_ephemeral_data_plane_port() {
        // (roles, whether the node binds a listener)
        let cases = [
            (vec![Controller], false),
            (vec![Broker], true),
            (vec![Controller, Broker], true),
        ];
        for (roles, binds) in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
            config.listen_addr = "127.0.0.1:0".parse().expect("literal");
            config.advertised_listener = "127.0.0.1:0".to_owned();
            config.roles.clone_from(&roles);
            let mut listeners = Vec::new();

            bind_ephemeral_data_plane_listener(&mut config, &mut listeners)
                .await
                .expect("bind the ephemeral port");

            let bound: Vec<SocketAddr> = listeners
                .iter()
                .map(|listener| listener.local_addr().expect("local address"))
                .collect();
            let expected = if binds {
                (
                    vec![config.listen_addr],
                    format!("127.0.0.1:{}", config.listen_addr.port()),
                )
            } else {
                (Vec::new(), "127.0.0.1:0".to_owned())
            };
            check!((bound, config.advertised_listener) == expected, "{roles:?}");
        }
    }
}
