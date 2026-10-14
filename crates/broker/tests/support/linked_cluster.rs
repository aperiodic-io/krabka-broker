//! A three-broker cluster whose peers reach each other only through relays.
//!
//! Every node binds its real controller and client listeners and advertises
//! addresses that a [`Network`] puts in front of them: the controller one in
//! the static voter set, the client one as its inter-broker endpoint. A peer
//! learns a node only through what that node advertises, so taking a relay
//! away takes the node off the network *as its peers see it*, while every
//! broker keeps running. A test's own clients can still dial a broker's real
//! listen address, the way a client next to a partitioned broker still
//! reaches it.
//!
//! # Two networks
//!
//! [`SiteLinks`] puts one [`SiteLink`] in front of each node. Cutting it stops
//! the peers from reaching that node, but the node's own dials still land,
//! because they go to the *other* nodes' relays. The cut is one-way.
//!
//! [`PeerLinks`] puts a [`SiteLink`] on every *ordered pair* of nodes, so a cut
//! can be two-way. Every node dials a given peer at the same advertised
//! address -- it comes from the committed voter set and the metadata image,
//! not from the dialer's own configuration -- so the relays of the pairs
//! cannot be told apart by address. A front listener at the advertised address
//! tells them apart by the dialer instead. It reads the `client_id` of the
//! first request on each connection, which every inter-broker client of a
//! krabka broker names its node in (`krabka-broker-2-replicator`,
//! `krabka-broker-2-controller`, `krabka-broker-txn-2`), and splices the
//! connection onto the relay of that pair. A connection whose client id names
//! no node is an outside client's, and takes a path that no cut touches.
//!
//! The front reads only the request header, which has the same shape in
//! header versions 1 and 2: a 4-byte size, the api key, the api version, the
//! correlation id, and the client id as an `int16`-length string.

use {
    crate::support::{self, poll::within, relay::SiteLink},
    krabka_broker::{Broker, BrokerConfig, BrokerError, BrokerHandle},
    std::{
        collections::BTreeSet,
        future::Future,
        io,
        net::{Ipv4Addr, SocketAddr},
        ops::RangeInclusive,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tempfile::TempDir,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

/// The number of nodes of a relayed cluster.
pub const NODES: usize = 3;

/// The longest any one broker or relay may take to stop.
const STOP_TIMEOUT: Duration = Duration::from_secs(60);

/// The two listeners of one node, by address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Listeners {
    pub controller: SocketAddr,
    pub client: SocketAddr,
}

/// The relays a [`RelayedCluster`] puts between its nodes.
pub trait Network: Sized {
    /// Put relays in front of the real listeners of each node, in node order.
    fn start(upstreams: &[Listeners]) -> impl Future<Output = Self>;

    /// The addresses node `index` advertises to its peers.
    fn advertised(&self, index: usize) -> Listeners;

    /// Stop every relay and await its tasks.
    fn shutdown(self) -> impl Future<Output = ()>;
}

// ---------------------------------------------------------------------------
// The cluster
// ---------------------------------------------------------------------------

/// A running three-node combined controller and broker quorum behind a
/// [`Network`].
pub struct RelayedCluster<N> {
    handles: Vec<BrokerHandle>,
    configs: Vec<BrokerConfig>,
    network: N,
    _dirs: Vec<TempDir>,
}

impl<N: Network> RelayedCluster<N> {
    /// Boot the cluster, with `customize` applied to each node's config last,
    /// and wait until every node sees all three brokers registered. A boot
    /// that does not elect a controller is retried on fresh ports, as short
    /// raft timings sometimes split the vote on a loaded runner.
    pub async fn start(customize: impl Fn(usize, &mut BrokerConfig)) -> Self {
        let mut failures = Vec::new();
        while failures.len() < 3 {
            match Self::try_start(&customize).await {
                Ok(cluster) => return cluster,
                Err(error) => {
                    tracing::warn!(%error, "relayed cluster start failed; retrying");
                    failures.push(error);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        panic!("relayed cluster start failed three times: {failures:?}");
    }

    async fn try_start(customize: &impl Fn(usize, &mut BrokerConfig)) -> Result<Self, BrokerError> {
        support::init_tracing();
        // Each listener is held from here until its broker adopts it, so no
        // other test can take the port in between. The controller listener
        // is bound on the unspecified address, as a node behind a proxy binds
        // it: such a node advertises its voter-set entry, the relay, in the
        // KIP-853 `UpdateRaftVoter` every controller sends its leader. Bound
        // on loopback it would advertise the loopback address instead, and
        // teach the leader a route around every relay.
        let mut client_listeners = Vec::with_capacity(NODES);
        let mut controller_listeners = Vec::with_capacity(NODES);
        let mut upstreams = Vec::with_capacity(NODES);
        for _ in 0..NODES {
            let client = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let controller = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
            upstreams.push(Listeners {
                controller: (Ipv4Addr::LOCALHOST, controller.local_addr()?.port()).into(),
                client: client.local_addr()?,
            });
            client_listeners.push(client);
            controller_listeners.push(controller);
        }
        let client_addrs: Vec<SocketAddr> = upstreams.iter().map(|node| node.client).collect();
        let controller_addrs = controller_listeners
            .iter()
            .map(TcpListener::local_addr)
            .collect::<io::Result<Vec<_>>>()?;
        // The brokers adopt the real listeners; only the advertised addresses
        // go through the relays.
        let network = N::start(&upstreams).await;
        let voters = support::controller_voters(
            &(0..NODES)
                .map(|index| (node_id(index), network.advertised(index).controller))
                .collect::<Vec<_>>(),
        );

        let mut starts = Vec::with_capacity(NODES);
        let mut prepared = Vec::with_capacity(NODES);
        for (index, (data, controller)) in
            support::listener_pairs(client_listeners, controller_listeners)
        {
            let dir = TempDir::new().expect("a log directory");
            // A static three-voter bootstrap. Peers learn this node only
            // through its relays; the broker binds the real listeners
            // behind them.
            let mut node_config = support::broker_config()
                .log_dir(dir.path())
                .index(index)
                .client_addrs(&client_addrs)
                .controller_addrs(&controller_addrs)
                .voters(voters.clone())
                .call();
            node_config.advertised_listener = network.advertised(index).client.to_string();
            node_config.directory_id = uuid::Uuid::from_u128(u128::from(node_config.node_id.0));
            node_config.auto_join = false;
            node_config.bootstrap_servers = vec![];
            customize(index, &mut node_config);
            let boot = node_config.clone();
            starts.push(tokio::spawn(async move {
                Broker::start_with_listeners(boot, Some(controller), Some(data)).await
            }));
            prepared.push((node_config, dir));
        }

        let mut handles = Vec::with_capacity(NODES);
        let mut configs = Vec::with_capacity(NODES);
        let mut dirs = Vec::with_capacity(NODES);
        for (start, (config, dir)) in starts.into_iter().zip(prepared) {
            handles.push(support::await_broker_start(start).await?);
            configs.push(config);
            dirs.push(dir);
        }
        for handle in &handles {
            handle.wait_until_brokers_registered(NODES).await;
        }
        Ok(Self {
            handles,
            configs,
            network,
            _dirs: dirs,
        })
    }
}

impl<N: Network> RelayedCluster<N> {
    /// The broker at position `index`, whose node id is `index + 1`.
    pub fn handle(&self, index: usize) -> &BrokerHandle { &self.handles[index] }

    /// The configuration broker `index` runs with.
    pub fn config(&self, index: usize) -> &BrokerConfig { &self.configs[index] }

    /// The real client listener of broker `index`, which no relay fronts.
    pub fn addr(&self, index: usize) -> SocketAddr { self.configs[index].listen_addr }

    /// The relays between the nodes.
    pub fn network(&self) -> &N { &self.network }

    /// Stop every broker, then every relay.
    pub async fn shutdown(self) {
        for handle in self.handles {
            within("a relayed broker stops", STOP_TIMEOUT, handle.shutdown()).await;
        }
        within("the relays stop", STOP_TIMEOUT, self.network.shutdown()).await;
    }
}

/// The node id of the broker at position `index`.
fn node_id(index: usize) -> u64 { u64::try_from(index + 1).expect("a node id") }

// ---------------------------------------------------------------------------
// One relay per node: a one-way cut
// ---------------------------------------------------------------------------

/// One [`SiteLink`] in front of each node. See the module documentation for
/// why a cut of it is one-way.
pub struct SiteLinks {
    links: Vec<SiteLink>,
    /// The nodes whose link is cut.
    cut: Mutex<BTreeSet<usize>>,
}

impl SiteLinks {
    /// Stop node `index`'s peers from reaching it, including over the
    /// connections they already hold open.
    pub fn cut(&self, index: usize) {
        self.cut.lock().expect("cut links").insert(index);
        self.links[index].cut();
    }

    /// Let them reach it again. A link that is not cut is left alone: healing
    /// replaces a relay's link generation, and the connections opened under
    /// the generation it replaced would no longer be reached by the next cut.
    pub fn heal(&self, index: usize) {
        if self.cut.lock().expect("cut links").remove(&index) {
            self.links[index].heal();
        }
    }
}

impl Network for SiteLinks {
    async fn start(upstreams: &[Listeners]) -> Self {
        let mut links = Vec::with_capacity(upstreams.len());
        for upstream in upstreams {
            links.push(SiteLink::start(upstream.controller, upstream.client).await);
        }
        Self {
            links,
            cut: Mutex::new(BTreeSet::new()),
        }
    }

    fn advertised(&self, index: usize) -> Listeners {
        Listeners {
            controller: self.links[index].controller_addr(),
            client: self.links[index].client_addr(),
        }
    }

    async fn shutdown(self) {
        for link in self.links {
            link.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// One relay per ordered pair: a two-way cut
// ---------------------------------------------------------------------------

/// A [`SiteLink`] for every ordered pair of nodes, behind a front listener per
/// node listener that sorts each connection onto its pair. See the module
/// documentation.
///
/// Nodes are named by their node ids, 1 to 3, here. The faults compose: a
/// path between two nodes is cut while either of them is isolated, or while
/// the pair is split, or while that one direction is cut, and it is healed
/// only once none of those holds.
pub struct PeerLinks {
    /// `paths[to][from]` carries what node index `from` sends to node index
    /// `to`. `from == NODES` is the path of an outside client, which no cut
    /// touches, and `from == to` a node's path to itself.
    paths: Vec<Vec<SiteLink>>,
    /// The controller front, then the client front, of each node.
    fronts: Vec<[Front; 2]>,
    /// The faults in force, and which paths they cut.
    faults: Mutex<Faults>,
    /// Client ids that looked like a krabka broker's but named no node.
    unattributed: Arc<Mutex<BTreeSet<String>>>,
}

/// The faults a [`PeerLinks`] holds, by node id.
#[derive(Default)]
struct Faults {
    isolated: BTreeSet<u64>,
    /// Directions `(from, to)` cut one by one, or both at once by a split.
    one_way: BTreeSet<(u64, u64)>,
    /// The directions whose relay is cut right now.
    cut: BTreeSet<(u64, u64)>,
}

impl Faults {
    fn cuts(&self, from: u64, to: u64) -> bool {
        self.isolated.contains(&from)
            || self.isolated.contains(&to)
            || self.one_way.contains(&(from, to))
    }
}

impl PeerLinks {
    /// Cut node `node` off from both of its peers, both ways, including the
    /// connections already open. Outside clients still reach it.
    pub fn isolate(&self, node: u64) {
        self.change(|faults| {
            faults.isolated.insert(node);
        });
    }

    /// Lift every fault that names node `node`. It is reconnected to each
    /// peer that is not itself isolated.
    pub fn heal(&self, node: u64) {
        self.change(|faults| {
            faults.isolated.remove(&node);
            faults
                .one_way
                .retain(|(from, to)| *from != node && *to != node);
        });
    }

    /// Cut the two directions between nodes `a` and `b`, leaving both
    /// connected to the third node.
    pub fn cut_between(&self, a: u64, b: u64) {
        self.change(|faults| {
            faults.one_way.extend([(a, b), (b, a)]);
        });
    }

    /// Stop node `from` from opening connections to node `to`. Kafka traffic
    /// is request and response, so `to` can still reach `from` over the
    /// connections it opens itself.
    pub fn cut_path(&self, from: u64, to: u64) {
        self.change(|faults| {
            faults.one_way.insert((from, to));
        });
    }

    /// Lift every fault.
    pub fn heal_all(&self) {
        self.change(|faults| {
            faults.isolated.clear();
            faults.one_way.clear();
        });
    }

    /// The broker-like client ids the fronts could not attribute to a node.
    /// A connection with one of them went around every cut, so a test that
    /// relies on a partition can assert this is empty.
    pub fn unattributed_client_ids(&self) -> Vec<String> {
        self.unattributed
            .lock()
            .expect("unattributed client ids")
            .iter()
            .cloned()
            .collect()
    }

    /// Apply `edit` to the faults, then cut each path they now cut and heal
    /// each path they no longer do.
    ///
    /// A relay is healed only while it is cut: healing replaces its link
    /// generation, and a connection opened under the generation it replaces
    /// would no longer be reached by the next cut.
    fn change(&self, edit: impl FnOnce(&mut Faults)) {
        let mut faults = self.faults.lock().expect("relay faults");
        edit(&mut faults);
        for from in node_ids() {
            for to in peers_of(from) {
                let link = &self.paths[index_of(to)][index_of(from)];
                let cut = faults.cuts(from, to);
                if cut && faults.cut.insert((from, to)) {
                    link.cut();
                } else if !cut && faults.cut.remove(&(from, to)) {
                    link.heal();
                }
            }
        }
    }
}

impl Network for PeerLinks {
    async fn start(upstreams: &[Listeners]) -> Self {
        let unattributed = Arc::new(Mutex::new(BTreeSet::new()));
        let mut paths = Vec::with_capacity(upstreams.len());
        let mut fronts = Vec::with_capacity(upstreams.len());
        for upstream in upstreams {
            let mut sources = Vec::with_capacity(NODES + 1);
            for _ in 0..=NODES {
                sources.push(SiteLink::start(upstream.controller, upstream.client).await);
            }
            let controller_routes = sources.iter().map(SiteLink::controller_addr).collect();
            let client_routes = sources.iter().map(SiteLink::client_addr).collect();
            fronts.push([
                Front::start(controller_routes, Arc::clone(&unattributed)).await,
                Front::start(client_routes, Arc::clone(&unattributed)).await,
            ]);
            paths.push(sources);
        }
        Self {
            paths,
            fronts,
            faults: Mutex::new(Faults::default()),
            unattributed,
        }
    }

    fn advertised(&self, index: usize) -> Listeners {
        let [controller, client] = &self.fronts[index];
        Listeners {
            controller: controller.addr,
            client: client.addr,
        }
    }

    async fn shutdown(self) {
        for front in self.fronts.into_iter().flatten() {
            front.shutdown().await;
        }
        for link in self.paths.into_iter().flatten() {
            link.shutdown().await;
        }
    }
}

/// Every node id of the cluster.
fn node_ids() -> RangeInclusive<u64> { 1..=node_id(NODES - 1) }

/// Every node id but `node`.
fn peers_of(node: u64) -> impl Iterator<Item = u64> { node_ids().filter(move |peer| *peer != node) }

/// The node index a node id names.
fn index_of(node: u64) -> usize {
    usize::try_from(node)
        .ok()
        .and_then(|node| node.checked_sub(1))
        .filter(|index| *index < NODES)
        .unwrap_or_else(|| panic!("node {node} is not in a {NODES}-node cluster"))
}

/// The node, by node id, that a krabka broker's inter-broker `client_id`
/// names, or `None` for any other client id.
///
/// A broker names itself either right after `krabka-broker-`
/// (`krabka-broker-2-replicator`) or as the last dash-separated part
/// (`krabka-broker-txn-2`, `krabka-consumer-lag-2`).
pub fn peer_of_client_id(client_id: &str) -> Option<u64> {
    let rest = client_id.strip_prefix("krabka-")?;
    let number = |part: &str| {
        (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| part.parse::<u64>().ok())
            .flatten()
    };
    let leading = rest
        .strip_prefix("broker-")
        .and_then(|named| named.split('-').next())
        .and_then(number);
    let node = leading.or_else(|| rest.rsplit('-').next().and_then(number))?;
    node_ids().contains(&node).then_some(node)
}

/// A listener at a node's advertised address that sorts each connection onto
/// the relay of the pair it belongs to.
struct Front {
    addr: SocketAddr,
    stop: CancellationToken,
    tasks: TaskTracker,
}

impl Front {
    /// `routes[source]` is where a connection from node index `source` goes;
    /// `routes[NODES]` is where every other connection goes.
    async fn start(routes: Vec<SocketAddr>, unattributed: Arc<Mutex<BTreeSet<String>>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a front listener");
        let addr = listener.local_addr().expect("the front listener's address");
        let front = Self {
            addr,
            stop: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let sorter = Sorter {
            routes: routes.into(),
            unattributed,
        };
        front.tasks.spawn(serve(
            listener,
            sorter,
            front.stop.clone(),
            front.tasks.clone(),
        ));
        front
    }

    async fn shutdown(self) {
        self.stop.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl Drop for Front {
    fn drop(&mut self) {
        // A test that unwinds never reaches `shutdown`; stop accepting so the
        // port does not outlive the cluster. Nothing can be awaited here.
        self.stop.cancel();
    }
}

/// What a front needs to place one connection.
#[derive(Clone)]
struct Sorter {
    routes: Arc<[SocketAddr]>,
    unattributed: Arc<Mutex<BTreeSet<String>>>,
}

/// Accept until the front stops, placing each connection on its own task.
async fn serve(listener: TcpListener, sorter: Sorter, stop: CancellationToken, tasks: TaskTracker) {
    while let Some(accepted) = stop.run_until_cancelled(listener.accept()).await {
        let inbound = match accepted {
            Ok((inbound, _)) => inbound,
            Err(error) => {
                // One aborted handshake must not end the front; yield so a
                // repeating error cannot spin.
                tracing::debug!(%error, "front accept failed");
                tokio::task::yield_now().await;
                continue;
            }
        };
        let sorter = sorter.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
            stop.run_until_cancelled(sorter.splice(inbound)).await;
        });
    }
}

impl Sorter {
    /// Read the first request header, then join the connection to the relay
    /// of its pair, replaying the bytes already read.
    async fn splice(self, mut inbound: TcpStream) {
        let Ok((head, client_id)) = read_request_head(&mut inbound).await else {
            return;
        };
        let peer = client_id.as_deref().and_then(peer_of_client_id);
        if peer.is_none()
            && let Some(id) = client_id.filter(|id| id.starts_with("krabka"))
        {
            self.unattributed
                .lock()
                .expect("unattributed client ids")
                .insert(id);
        }
        let source = peer.map_or(NODES, index_of);
        let Ok(mut outbound) = TcpStream::connect(self.routes[source]).await else {
            return;
        };
        if outbound.write_all(&head).await.is_ok() {
            // A cut relay closes its side. The copy passes that on to the
            // dialer, which sees its connection end and closes it, and the
            // copy ends.
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        }
    }
}

/// The bytes of a request header up to the end of its client id, and the
/// client id, `None` when it is null.
async fn read_request_head(stream: &mut TcpStream) -> io::Result<(Vec<u8>, Option<String>)> {
    // size (4), api key (2), api version (2), correlation id (4), client id
    // length (2).
    let mut head = vec![0_u8; 14];
    stream.read_exact(&mut head).await?;
    let length = i16::from_be_bytes([head[12], head[13]]);
    let Ok(length) = usize::try_from(length) else {
        return Ok((head, None));
    };
    let mut client_id = vec![0_u8; length];
    stream.read_exact(&mut client_id).await?;
    head.extend_from_slice(&client_id);
    Ok((head, Some(String::from_utf8_lossy(&client_id).into_owned())))
}
