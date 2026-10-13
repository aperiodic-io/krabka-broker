//! The in-memory network the simulated engines talk over: a registry of live
//! engines keyed by node id, and the [`PeerSender`] that routes an encoded
//! KIP-595 body into the target engine's inbound queue.
//!
//! Registering and removing an engine is how the acceptances model a node
//! booting, crashing, and coming back; partitioning and healing one is how they
//! model a node that keeps running while the network cuts it off. The whole
//! notion of reachability in this simulation lives here.

use {
    bytes::Bytes,
    krabka_raft::{
        RaftError,
        kraft::{
            KraftController, NodeId, PeerSender,
            transport::{Inbound, api_key, wire},
        },
    },
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
    },
    tokio::sync::oneshot,
};

/// Shared registry of in-process engines, keyed by node id.
///
/// Each engine holds a clone of one of these, through [`SimNet`], so that its
/// outbound peer sends can reach the others. A drop from the registry removes a
/// node, which models a leader kill or a restart. Later sends to that node then
/// fail as unreachable, which mirrors a crash.
#[derive(Default)]
struct Registry {
    nodes: HashMap<NodeId, KraftController>,
    partitioned: HashSet<NodeId>,
}

/// An in-memory [`PeerSender`]. It routes the encoded request body to the target
/// engine's [`KraftController::deliver`] and awaits the oneshot reply.
#[derive(Clone)]
pub(crate) struct SimNet {
    registry: Arc<Mutex<Registry>>,
    /// The node whose outbound sends this handle carries, when it was made by
    /// [`SimNet::as_peer`]. The registry handle the test itself holds has none,
    /// and never sends.
    me: Option<NodeId>,
}

impl SimNet {
    pub(crate) fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
            me: None,
        }
    }

    /// The [`PeerSender`] one engine hands its outbound sends to.
    ///
    /// The sender identity is what lets a partition block both directions: an
    /// anonymous handle could only ever refuse deliveries INTO a partitioned
    /// node, leaving the isolated side still able to push `BeginQuorumEpoch` at
    /// a majority that has moved on.
    pub(crate) fn as_peer(&self, me: NodeId) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            me: Some(me),
        }
    }

    pub(crate) fn register(&self, id: NodeId, ctrl: KraftController) {
        self.registry.lock().unwrap().nodes.insert(id, ctrl);
    }

    pub(crate) fn remove(&self, id: NodeId) { self.registry.lock().unwrap().nodes.remove(&id); }

    pub(crate) fn get(&self, id: NodeId) -> Option<KraftController> {
        self.registry.lock().unwrap().nodes.get(&id).cloned()
    }

    /// Cut `id` off the network in BOTH directions while leaving its engine
    /// running and readable through [`SimNet::get`].
    ///
    /// This is what separates a partition from the kill that [`SimNet::remove`]
    /// models. A killed leader stops answering because it is gone; a
    /// partitioned one keeps its whole state machine ticking and still believes
    /// what it believed a moment ago, which is exactly the case check-quorum
    /// exists for. Blocking both directions matters: a leader that could still
    /// push `BeginQuorumEpoch` one way would keep re-attaching the majority side
    /// to an epoch it can no longer serve.
    pub(crate) fn partition(&self, id: NodeId) {
        self.registry.lock().unwrap().partitioned.insert(id);
    }

    /// Put `id` back on the network.
    pub(crate) fn heal(&self, id: NodeId) { self.registry.lock().unwrap().partitioned.remove(&id); }

    fn reachable(&self, to: NodeId) -> Option<KraftController> {
        let registry = self.registry.lock().unwrap();
        if registry.partitioned.contains(&to)
            || self.me.is_some_and(|me| registry.partitioned.contains(&me))
        {
            return None;
        }
        registry.nodes.get(&to).cloned()
    }
}

#[async_trait::async_trait]
impl PeerSender for SimNet {
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        // Look up the target engine. A removed/crashed node is unreachable, and
        // so is either end of a partition.
        let target = self.reachable(peer).ok_or(RaftError::NotLeader {
            current_leader: None,
        })?;
        let (reply, rx) = oneshot::channel();
        let inbound = match api_key {
            api_key::VOTE => Inbound::Vote {
                req: body,
                version: wire::VOTE_VERSION,
                reply,
            },
            api_key::BEGIN_QUORUM_EPOCH => Inbound::BeginQuorumEpoch {
                req: body,
                version: wire::QUORUM_EPOCH_VERSION,
                reply,
            },
            api_key::END_QUORUM_EPOCH => Inbound::EndQuorumEpoch {
                req: body,
                version: wire::QUORUM_EPOCH_VERSION,
                reply,
            },
            api_key::FETCH => Inbound::Fetch {
                req: body,
                version: wire::FETCH_VERSION,
                reply,
            },
            api_key::FETCH_SNAPSHOT => Inbound::FetchSnapshot {
                req: body,
                version: wire::FETCH_SNAPSHOT_VERSION,
                reply,
            },
            other => panic!("sim: unexpected api_key {other}"),
        };
        // Deliver to the target loop (non-blocking enqueue) and await its reply.
        // The loop processes inbound concurrently with our caller's loop, so this
        // never deadlocks even when engines RPC each other reciprocally.
        target
            .deliver(inbound)
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}
