//! The `/healthz` and `/readyz` HTTP probes an orchestrator polls.
//!
//! A TCP probe on the data-plane port succeeds as soon as the listener binds,
//! which is before log-dir recovery has finished and before the node has
//! caught up on `__cluster_metadata`. A rolling restart driven by that probe
//! routes clients to a broker that answers `Metadata` from a stale image, and
//! then restarts the next pod while this one is still unusable. These two
//! routes separate the two questions Kubernetes asks.
//!
//! # Why the conditions are split the way they are
//!
//! `/healthz` is the liveness probe, and the kubelet *kills* the container
//! when it fails. The only thing it can honestly assert is that the process
//! is up and its runtime still runs this handler. A broker that replays a
//! large log dir, or that fetches a million metadata records, is healthy but
//! not yet usable. A liveness failure there restarts the node in the middle of
//! its recovery and discards the recovery it has done, again and again, on the
//! nodes that need the most time.
//!
//! `/readyz` is the readiness probe, and failing it only takes the pod out of
//! the Service endpoints. Every condition that makes a broker *unusable but
//! recoverable* belongs here:
//!
//! - **log-dir recovery**: partitions are still being scanned and their
//!   writers spawned, so a `Fetch` would miss data this node holds;
//! - **listeners bound**: nothing answers on the data plane yet;
//! - **metadata lag**: the node's `__cluster_metadata` offset trails the
//!   quorum's committed offset, so `Metadata` responses would name stale
//!   leaders and send clients to the wrong broker.
//!
//! # Wiring
//!
//! [`HealthState`] is created by whoever owns the process, handed to
//! [`serve`] so the routes answer immediately, and handed to
//! [`Broker::start_with_health`](crate::Broker::start_with_health) so each
//! startup phase marks its condition as it completes. The state is an `Arc`
//! inside, so the clone the broker holds and the clone the router holds are
//! the same state.
//!
//! `wasm32-wasip1` has no `bind` and no HTTP server stack, so the router and
//! `serve` are native-only. The state and its readiness rules are on every
//! target.

#[cfg(not(target_family = "wasm"))]
use std::net::SocketAddr;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};

#[cfg(not(target_family = "wasm"))]
use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
#[cfg(not(target_family = "wasm"))]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// The two `__cluster_metadata` offsets the readiness probe compares.
///
/// It is a trait rather than a direct
/// [`MetadataSource`](crate::metadata_source::MetadataSource) reference so
/// that the readiness rule can be driven from a test with no quorum behind
/// it. [`metadata_progress`] adapts the real source onto it.
pub trait MetadataProgress: Send + Sync {
    /// Highest `__cluster_metadata` offset this node has applied, or `-1`
    /// before the first record.
    fn node_metadata_offset(&self) -> i64;
    /// Highest `__cluster_metadata` offset the quorum has committed, as this
    /// node last heard it, or `-1` before it has heard anything.
    fn quorum_committed_offset(&self) -> i64;
}

/// Adapts the broker's live metadata authority onto [`MetadataProgress`].
#[must_use]
pub fn metadata_progress(
    source: Arc<dyn crate::metadata_source::MetadataSource>,
) -> Arc<dyn MetadataProgress> {
    struct SourceProgress(Arc<dyn crate::metadata_source::MetadataSource>);
    impl MetadataProgress for SourceProgress {
        fn node_metadata_offset(&self) -> i64 {
            self.0.current_metadata_offset()
        }
        fn quorum_committed_offset(&self) -> i64 {
            self.0.quorum_committed_offset()
        }
    }
    Arc::new(SourceProgress(source))
}

/// The one readiness condition that is not met, in the order the probe checks
/// them. `Display` is the body of the 503, so it names the condition rather
/// than saying only that something is wrong.
///
/// `condition` is the stable machine-readable name of the condition, the first
/// token of the 503 body. An operator greps for this; the prose after it is
/// for a human reading `kubectl describe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", as_str = condition)]
pub enum NotReady {
    /// Controlled shutdown is draining leadership.
    ShuttingDown,
    /// The log dirs have not finished recovering.
    LogDirRecovery,
    /// The data-plane listeners are not accepting yet.
    ListenersBound,
    /// The node has not reached the metadata quorum, so it cannot know
    /// whether it is caught up.
    MetadataQuorumUnreached,
    /// The node's metadata offset trails the quorum's committed offset by
    /// more than the configured bound.
    MetadataLag {
        /// Records this node is behind the quorum's committed offset.
        lag: u64,
        /// The bound this lag exceeded.
        max_lag: u64,
    },
}

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not ready: {}: ", self.condition())?;
        match *self {
            Self::ShuttingDown => f.write_str("controlled shutdown is draining leadership"),
            Self::LogDirRecovery => f.write_str("log directory recovery has not completed"),
            Self::ListenersBound => f.write_str("the data-plane listeners are not bound"),
            Self::MetadataQuorumUnreached => {
                f.write_str("this node has not reached the metadata quorum")
            }
            Self::MetadataLag { lag, max_lag } => write!(
                f,
                "the __cluster_metadata offset trails the quorum's committed \
                 offset by {lag} records, over the bound of {max_lag}"
            ),
        }
    }
}

/// The conditions the `/readyz` route reports on, shared between the HTTP
/// routes and the broker startup phases that satisfy them. Cloning shares the
/// state.
#[derive(Clone)]
pub struct HealthState {
    inner: Arc<Inner>,
}

struct Inner {
    max_metadata_lag: u64,
    shutting_down: AtomicBool,
    log_dir_recovery_complete: AtomicBool,
    listeners_bound: AtomicBool,
    progress: OnceLock<Arc<dyn MetadataProgress>>,
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState")
            .field("readiness", &self.readiness())
            .finish()
    }
}

impl HealthState {
    /// A state with no condition met yet: `/readyz` answers 503 until the
    /// broker marks each one.
    ///
    /// `max_metadata_lag` is how many `__cluster_metadata` records this node
    /// may trail the quorum's committed offset by and still report ready. The
    /// broker's own default is
    /// [`DEFAULT_READINESS_MAX_METADATA_LAG`](crate::config::DEFAULT_READINESS_MAX_METADATA_LAG).
    #[must_use]
    pub fn new(max_metadata_lag: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                max_metadata_lag,
                shutting_down: AtomicBool::new(false),
                log_dir_recovery_complete: AtomicBool::new(false),
                listeners_bound: AtomicBool::new(false),
                progress: OnceLock::new(),
            }),
        }
    }

    /// Called when controlled shutdown begins draining leadership.
    pub fn mark_shutting_down(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
    }

    /// Called once the log dirs have been scanned and every recovered
    /// partition has its writer.
    pub fn mark_log_dir_recovery_complete(&self) {
        self.inner
            .log_dir_recovery_complete
            .store(true, Ordering::Release);
    }

    /// Called once every listener of this node is bound and accepting: each
    /// data-plane listener of a node with the broker role, and the controller
    /// listener of a node with the controller role, which the metadata phase
    /// binds first. A controller-only node opens no data-plane listener.
    pub fn mark_listeners_bound(&self) {
        self.inner.listeners_bound.store(true, Ordering::Release);
    }

    /// Installs the metadata authority the lag check reads. Called once, when
    /// the broker has reached the quorum. Later calls are ignored: the first
    /// authority this node joined is the one its readiness is measured
    /// against.
    pub fn install_metadata_progress(&self, progress: Arc<dyn MetadataProgress>) {
        let _ = self.inner.progress.set(progress);
    }

    /// Returns the Kafka `BrokerState` ordinal (1=STARTING, 2=RECOVERY,
    /// 3=RUNNING, 6=`PENDING_CONTROLLED_SHUTDOWN`, 7=`SHUTTING_DOWN`).
    #[must_use]
    pub fn broker_state(&self) -> i64 {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            6 // PENDING_CONTROLLED_SHUTDOWN
        } else if !self.inner.log_dir_recovery_complete.load(Ordering::Acquire) {
            2 // RECOVERY
        } else if !self.inner.listeners_bound.load(Ordering::Acquire) {
            1 // STARTING
        } else {
            3 // RUNNING
        }
    }

    /// The readiness verdict: `Ok(())`, or the first condition that fails.
    ///
    /// # Errors
    ///
    /// Returns the [`NotReady`] condition blocking readiness.
    pub fn readiness(&self) -> Result<(), NotReady> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(NotReady::ShuttingDown);
        }
        if !self.inner.log_dir_recovery_complete.load(Ordering::Acquire) {
            return Err(NotReady::LogDirRecovery);
        }
        if !self.inner.listeners_bound.load(Ordering::Acquire) {
            return Err(NotReady::ListenersBound);
        }
        let Some(progress) = self.inner.progress.get() else {
            return Err(NotReady::MetadataQuorumUnreached);
        };
        let node = progress.node_metadata_offset();
        let quorum = progress.quorum_committed_offset();
        // A node ahead of the offset it last heard from the quorum is not
        // behind: `saturating_sub` on the signed difference makes "ahead"
        // read as zero lag rather than wrapping into an enormous one.
        let lag = u64::try_from(quorum.saturating_sub(node)).unwrap_or(0);
        if lag > self.inner.max_metadata_lag {
            return Err(NotReady::MetadataLag {
                lag,
                max_lag: self.inner.max_metadata_lag,
            });
        }
        Ok(())
    }
}

/// Builds the router: `/healthz` for liveness and `/readyz` for readiness.
#[cfg(not(target_family = "wasm"))]
pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

/// Binds and serves the probes until `shutdown` fires, returning the bound
/// address so a caller that asked for port 0 can find the port the OS picked.
///
/// # Errors
///
/// Returns the bind error when `addr` cannot be bound.
#[cfg(not(target_family = "wasm"))]
pub async fn serve(
    addr: SocketAddr,
    state: HealthState,
    shutdown: CancellationToken,
) -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "health server listening");
    let app = router(state);
    crate::http_server::spawn(listener, app, shutdown, |error| {
        tracing::warn!(error = %error, "health server error");
    });
    Ok(bound)
}

/// Liveness. It answers 200 for as long as the process can run this handler,
/// and deliberately consults none of the readiness conditions: see the module
/// documentation for why a recovering broker must not be killed.
#[cfg(not(target_family = "wasm"))]
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

/// Readiness. 200 once every condition holds; otherwise 503 whose body names
/// the first condition that does not.
#[cfg(not(target_family = "wasm"))]
async fn readyz(State(state): State<HealthState>) -> impl IntoResponse {
    match state.readiness() {
        Ok(()) => (StatusCode::OK, "ready\n".to_string()),
        Err(not_ready) => (StatusCode::SERVICE_UNAVAILABLE, format!("{not_ready}\n")),
    }
}
