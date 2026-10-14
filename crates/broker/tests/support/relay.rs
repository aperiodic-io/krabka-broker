//! A test-only TCP relay that simulates a network partition between brokers.
//!
//! # Why a relay exists
//!
//! Broker integration tests speak real loopback TCP, and a broker builds its
//! outbound dialer internally from its own config (see `broker.rs`), so a test
//! cannot slot a fault-injecting transport underneath it without a production
//! change made solely for tests. The raft crate's simulation harness does have
//! a partition/heal injector, but it works on raft events inside one process
//! and never touches a socket, so it cannot fault the broker-level links.
//!
//! A relay closes that gap from the outside. Bind a listener on loopback, tell
//! the brokers that their peer lives *there*, and forward every byte on to the
//! peer's real address. Cutting the relay is then indistinguishable, from the
//! broker's point of view, from the peer becoming unreachable.
//!
//! # Why this is not the same as stopping a broker
//!
//! A stopped broker and a partitioned-away broker are different failures, and
//! only the second one leaves a *live* minority that can misbehave — accept a
//! write it should refuse, or elect a second leader. A test that shuts a broker
//! down cannot observe any of that, because the suspect node is not running.
//! The relay leaves every node running and takes away only the network.
//!
//! # Tearing down connections that are already open
//!
//! [`Relay::cut`] must do two things, and the second is the subtle one:
//!
//! 1. stop forwarding *new* connections, and
//! 2. tear down the connections that are *already open*.
//!
//! Blocking only new connections is not a partition. Brokers hold long-lived
//! replication and heartbeat connections, so a "partitioned" node whose
//! established sockets keep working would go on replicating, the test would
//! pass, and it would have proved nothing.
//!
//! So every forwarding task holds a *link token* — a [`CancellationToken`] that
//! stands for one healthy generation of the link — and selects on it while it
//! copies. `cut` cancels that token, which wakes every in-flight copy at once;
//! each task then returns, dropping both of its sockets, and both endpoints see
//! their connection close. [`Relay::heal`] installs a fresh generation, because
//! a cancelled token never un-cancels.
//!
//! The token is kept in a `std::sync::Mutex`, which is the simplest thing that
//! is correct here. It is *both* the state flag the accept loop reads
//! (`is_cancelled`) and the wake-up that aborts the copies, so one field does
//! both jobs: an `AtomicBool` would need a second mechanism to interrupt the
//! copies, and a `watch` channel would need every connection task to hold a
//! receiver and match on values. The lock is held only long enough to clone or
//! replace a token, never across an `.await`, so a blocking mutex cannot stall
//! the runtime — and being blocking is what lets `cut`/`heal` take `&self` and
//! stay callable from synchronous code holding an `Arc<Relay>`.
//!
//! Each link token is a *child* of the relay's shutdown token, so cancelling
//! the shutdown token also aborts every in-flight copy, without `shutdown`
//! having to reach for the link.
//!
//! # What a cut looks like on the wire
//!
//! While cut, the relay stays bound and accepts an inbound connection, then
//! immediately drops it. That is the simplest way to make a connection attempt
//! *fail* rather than hang: the dialer's handshake completes but its first read
//! ends at once, so the broker sees a dead peer and retries, which is what a
//! real partition looks like to it. A silently ignored connection would instead
//! leave the dialer blocked until its own timeout, which is slower and looks
//! like a hang rather than a partition.
//!
//! # Ports
//!
//! The relay binds `127.0.0.1:0` and reads back the assigned port, and it keeps
//! that listener for its whole life. It therefore has none of the bind-and-drop
//! TOCTOU problem that `super::bind_and_hold_ports` exists to solve: the port is
//! never released for a concurrently running test binary to steal, because it is
//! never released at all until the relay shuts down. There is nothing to "fix"
//! here.

use {
    std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
    },
    tokio::{
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

/// A loopback TCP forwarder that a test can cut and heal.
pub struct Relay {
    addr: SocketAddr,
    state: Arc<State>,
    /// Taken by [`Relay::shutdown`]; `Some` at every other moment. It is an
    /// `Option` only so that `shutdown` can await the handle without moving out
    /// of a type that implements `Drop`.
    accept: Option<JoinHandle<()>>,
}

/// What the accept loop and the per-connection tasks share.
struct State {
    /// Where accepted connections are forwarded.
    upstream: SocketAddr,
    /// The current link generation. Cancelled means "cut"; healing replaces it.
    link: Mutex<CancellationToken>,
    /// The parent of every link token. Cancelling it stops the accept loop and,
    /// through the parent-child relation, every in-flight copy.
    shutdown: CancellationToken,
    /// The live forwarding tasks, so `shutdown` can await them instead of
    /// leaking them into the rest of the test binary's run.
    conns: TaskTracker,
}

impl Relay {
    /// Bind a relay on an ephemeral loopback port that forwards to `upstream`.
    pub async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let addr = listener.local_addr().expect("relay listener local addr");
        let shutdown = CancellationToken::new();
        let state = Arc::new(State {
            upstream,
            link: Mutex::new(shutdown.child_token()),
            shutdown,
            conns: TaskTracker::new(),
        });
        let accept = tokio::spawn(accept_loop(listener, Arc::clone(&state)));
        Self {
            addr,
            state,
            accept: Some(accept),
        }
    }

    /// The address brokers should be pointed at.
    #[must_use]
    pub fn addr(&self) -> SocketAddr { self.addr }

    /// Drop every live connection and refuse new ones until [`Relay::heal`].
    pub fn cut(&self) {
        // Cancelling the current generation, rather than replacing it, is what
        // reaches the copies that are already running: they are selecting on
        // this very token.
        self.link().cancel();
    }

    /// Accept and forward again.
    pub fn heal(&self) {
        // A cancelled token stays cancelled, so healing means a new generation.
        // Connections killed by the previous one stay dead, which is right: a
        // healed network does not resurrect a reset TCP connection either.
        //
        // A link that is not cut keeps its generation. Replacing it would
        // orphan the connections already selecting on it, and a later `cut`
        // would then leave them forwarding: an isolated broker that still
        // replicates.
        let mut link = self.state.link.lock().expect("relay link lock");
        if link.is_cancelled() {
            *link = self.state.shutdown.child_token();
        }
    }

    /// Stop the relay and await its tasks.
    pub async fn shutdown(mut self) {
        // One cancel covers both the accept loop and every in-flight copy,
        // because the link tokens are children of this one.
        self.state.shutdown.cancel();
        if let Some(accept) = self.accept.take() {
            accept.await.expect("relay accept loop panicked");
        }
        // The accept loop has returned, so nothing can spawn onto the tracker
        // any more and `wait` is guaranteed to finish. Closing before waiting is
        // required: `wait` returns only once the tracker is both closed and
        // empty. The listener is dropped with the accept-loop task, so the port
        // is unbound by the time this returns.
        self.state.conns.close();
        self.state.conns.wait().await;
    }

    fn link(&self) -> CancellationToken { self.state.link.lock().expect("relay link lock").clone() }
}

impl Drop for Relay {
    fn drop(&mut self) {
        // A test that fails an assertion unwinds without reaching `shutdown`.
        // Cancelling here keeps the accept loop, and the port it holds, from
        // outliving the relay for the rest of the test binary's run. Nothing is
        // awaited, because a `drop` cannot block; the tasks see the cancellation
        // themselves. Cancelling twice is a no-op, so the `shutdown` path stays
        // correct.
        self.state.shutdown.cancel();
    }
}

/// Relays for one node's controller and client listeners, cut and healed
/// together.
///
/// A stretch-cluster test partitions a *site*, not a single link: a site that
/// kept its data plane while losing its controller plane is not a failure any
/// deployment produces. This is a thin wrapper over two [`Relay`]s.
pub struct SiteLink {
    controller: Relay,
    client: Relay,
}

impl SiteLink {
    /// Bind both relays: one forwarding to the node's controller listener, one
    /// to its client listener.
    pub async fn start(controller_upstream: SocketAddr, client_upstream: SocketAddr) -> Self {
        Self {
            controller: Relay::start(controller_upstream).await,
            client: Relay::start(client_upstream).await,
        }
    }

    /// The address peers should use for this node's controller listener.
    #[must_use]
    pub fn controller_addr(&self) -> SocketAddr { self.controller.addr() }

    /// The address peers and clients should use for this node's data listener.
    #[must_use]
    pub fn client_addr(&self) -> SocketAddr { self.client.addr() }

    /// Partition the whole site away.
    pub fn cut(&self) {
        self.controller.cut();
        self.client.cut();
    }

    /// Restore the whole site.
    pub fn heal(&self) {
        self.controller.heal();
        self.client.heal();
    }

    /// Stop both relays and await their tasks.
    pub async fn shutdown(self) {
        self.controller.shutdown().await;
        self.client.shutdown().await;
    }
}

/// Accept forever, forwarding each connection, until the relay shuts down.
async fn accept_loop(listener: TcpListener, state: Arc<State>) {
    loop {
        let inbound = tokio::select! {
            () = state.shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => stream,
                Err(error) => {
                    // A single failed accept — ECONNABORTED when a peer resets
                    // between the handshake and the accept — must not kill the
                    // relay for the rest of the test. Yield so that an error
                    // that does repeat cannot become a hot spin.
                    tracing::debug!(%error, "relay accept failed");
                    tokio::task::yield_now().await;
                    continue;
                }
            },
        };
        let link = state.link.lock().expect("relay link lock").clone();
        if link.is_cancelled() {
            // Cut: accept and drop, so the dialer fails fast rather than
            // hanging. See the module header.
            drop(inbound);
            continue;
        }
        state.conns.spawn(forward(inbound, state.upstream, link));
    }
}

/// Copy one accepted connection to and from a fresh connection to `upstream`,
/// until either end closes or `link` is cancelled.
async fn forward(mut inbound: TcpStream, upstream: SocketAddr, link: CancellationToken) {
    let mut outbound = tokio::select! {
        // A cut while the upstream connect is still in flight aborts it too,
        // rather than leaving a socket that outlives the cut that killed it.
        () = link.cancelled() => return,
        connected = TcpStream::connect(upstream) => match connected {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(%error, %upstream, "relay upstream connect failed");
                return;
            }
        },
    };
    tokio::select! {
        // Returning here drops both sockets, which is what tears an established
        // connection down mid-copy. The token is a child of the relay's
        // shutdown token, so this branch also fires on shutdown.
        () = link.cancelled() => {}
        copied = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {
            if let Err(error) = copied {
                tracing::debug!(%error, "relay copy ended");
            }
        }
    }
}
