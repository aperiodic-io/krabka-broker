//! The lifecycle half of [`BrokerHandle`]: the bound-address accessors, the
//! raft membership calls, and the shutdown paths, plus the partition-writer
//! draining they share. The handle's test-only inspection helpers live in the
//! sibling modules declared below, so this file stays the production surface.

#[cfg(test)]
use crate::broker::Broker;
use {
    crate::{
        broker::BrokerHandle,
        clean_shutdown::write,
        error::BrokerError,
        future_log::{abort_moves, shutdown_moves},
        partition_registry::PartitionRegistry,
        time_util::now_ms,
    },
    krabka_ids::PartitionIndex,
    std::net::SocketAddr,
    tokio::task::JoinHandle,
};

#[cfg(any(test, feature = "test-helpers"))]
mod cluster;
#[cfg(any(test, feature = "test-helpers"))]
mod coordinator_topics;
#[cfg(any(test, feature = "test-helpers"))]
mod diskless;
#[cfg(any(test, feature = "test-helpers"))]
mod groups;
#[cfg(any(test, feature = "test-helpers"))]
mod log_waiters;
#[cfg(any(test, feature = "test-helpers"))]
mod partition;
#[cfg(any(test, feature = "test-helpers"))]
mod share_state;
#[cfg(any(test, feature = "test-helpers"))]
mod transactions;

/// Polls `condition` every 25ms until it holds, for at most
/// [`TEST_AWAITER_TIMEOUT`][crate::broker::TEST_AWAITER_TIMEOUT], and answers
/// whether it held. The test-only awaiters whose state has no
/// change-notification channel assert on it with their own panic message.
#[cfg(any(test, feature = "test-helpers"))]
async fn await_until<F, Fut>(mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    use crate::broker::TEST_AWAITER_TIMEOUT;

    tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
        while !condition().await {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

fn take_partition_writer_tasks(partitions: &PartitionRegistry) -> Vec<JoinHandle<()>> {
    partitions
        .arcs()
        .into_iter()
        .filter_map(|partition| partition.take_writer_handle())
        .collect()
}

async fn shutdown_partition_writers(partitions: &PartitionRegistry) {
    let tasks = take_partition_writer_tasks(partitions);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

/// Take a producer-state snapshot at the end of every partition's log, as
/// Kafka's `UnifiedLog.close` does on a clean shutdown, so the next start
/// restores producer state without replaying the active segment. The writers
/// are stopped first, so no append races the snapshot.
fn snapshot_producer_state(partitions: &PartitionRegistry) {
    for partition in partitions.arcs() {
        let Ok(mut log) = partition.log.lock() else {
            continue;
        };
        if let Err(error) = log.take_producer_snapshot() {
            tracing::warn!(
                topic = %partition.topic,
                partition = partition.index.0,
                %error,
                "producer-state snapshot at shutdown failed; the next start replays the log"
            );
        }
    }
}

fn abort_partition_writers(partitions: &PartitionRegistry) {
    for task in take_partition_writer_tasks(partitions) {
        task.abort();
    }
}

impl BrokerHandle {
    /// Test-only: the shared broker state behind this handle, for the unit
    /// tests that drive a subsystem taking `&Arc<Broker>` directly rather than
    /// over a socket. `BrokerControllerAdminRouter::bind` is one.
    #[cfg(test)]
    pub(crate) fn broker_for_test(&self) -> &std::sync::Arc<Broker> { &self.broker }

    /// The bound address of the listener that serves this node's Kafka
    /// requests. This is useful when `BrokerConfig.listen_addr` used port 0
    /// and the OS picked the port.
    ///
    /// On a node with the broker role this is the data-plane listener of
    /// [`Self::data_plane_addr`]. A node without the broker role opens no
    /// data-plane listener, as Kafka's controller-only node opens only the
    /// listeners that `controller.listener.names` names. There this is the
    /// controller listener of [`Self::controller_addr`], which serves the
    /// controller APIs that an admin client reaches with
    /// `--bootstrap-controller`.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr.unwrap_or_else(|| self.controller_addr())
    }

    /// The bound address of the data-plane listener that clients and the
    /// other brokers reach: the inter-broker listener, or the first listener
    /// when none has that name. `None` on a node without the broker role,
    /// which opens no data-plane listener.
    #[must_use]
    pub fn data_plane_addr(&self) -> Option<SocketAddr> { self.listen_addr }

    /// The actual bound address of the Prometheus `/metrics`
    /// HTTP server, if one is configured. Tests pass `127.0.0.1:0` in
    /// `BrokerConfig::metrics_listen_addr` and read the OS-assigned
    /// port back through this accessor.
    #[must_use]
    pub fn metrics_addr(&self) -> Option<SocketAddr> { self.broker.metrics_bound_addr }

    /// The actual `SocketAddr` this broker's controller listener bound to
    /// (resolves the OS-assigned port when `controller_listen_addr` used port
    /// 0). KIP-853 dynamic-voters tests read this to point joiners at the
    /// bootstrap broker's real controller endpoint.
    #[must_use]
    pub fn controller_addr(&self) -> SocketAddr { self.broker.controller.controller_bound_addr() }

    /// Current Raft leader id as observed by this broker's controller.
    /// Returns `None` before the first leader is elected. Trivial
    /// passthrough to [`krabka_raft::ControllerHandle::watch_leader`].
    #[must_use]
    pub fn controller_leader_id(&self) -> Option<krabka_raft::NodeId> {
        *self.broker.controller.watch_leader().borrow()
    }

    /// Number of brokers currently registered in this broker's
    /// `MetadataImage`. Used by replication integration tests to wait
    /// for all peers to come up before issuing `CreateTopics`.
    #[must_use]
    pub fn broker_count(&self) -> usize { self.broker.controller.current_image().brokers().count() }

    /// This broker's own registration endpoints, as stored in the
    /// quorum-replicated [`krabka_metadata::MetadataImage`]. Integration
    /// tests verify that the broker projected the per-listener endpoints
    /// from `BrokerConfig::effective_listeners()` onto the
    /// self-registration record. Returns the cloned endpoint list, or an
    /// empty list if the broker has not yet self-registered.
    #[must_use]
    pub fn self_registration_endpoints(&self) -> Vec<krabka_metadata::BrokerEndpoint> {
        let node_id = self.broker.config.node_id;
        self.broker
            .controller
            .current_image()
            .broker(node_id)
            .map(|b| b.endpoints.clone())
            .unwrap_or_default()
    }

    /// Manually mutate the controller voter set on this broker.
    /// `new_voters` is the complete desired set (not a delta), and it may
    /// differ from the current set by at most one voter. Callers must invoke
    /// this on the broker that's currently the controller leader, or the call
    /// returns [`BrokerError::Replication`] with the underlying
    /// `RaftError::NotLeader` rendered into the message. See
    /// [`krabka_raft::ControllerHandle::change_membership`] for full semantics.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<krabka_raft::NodeId>,
    ) -> Result<(), BrokerError> {
        self.broker
            .controller
            .change_membership(new_voters)
            .await
            .map_err(|e| BrokerError::Replication(format!("change_membership: {e}")))
    }

    /// Stage the identity of a non-voting observer at `addr`. The call records
    /// the node locally and returns at once; it starts no catch-up and changes
    /// no quorum membership. A later [`Self::change_membership`] that names
    /// `node_id` promotes the staged identity to a voter.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    pub async fn add_learner(
        &self,
        node_id: krabka_raft::NodeId,
        addr: std::net::SocketAddr,
    ) -> Result<(), BrokerError> {
        // KIP-853 membership keys on the full `Node` identity. This
        // `SocketAddr`-shaped convenience wrapper (used by integration tests)
        // synthesizes a single CONTROLLER endpoint and derives the directory
        // id from the node id, matching the `for_tests` convention.
        let node = krabka_raft::Node {
            directory_id: uuid::Uuid::from_u128(u128::from(node_id.0)),
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        };
        self.broker
            .controller
            .add_learner(node_id, node)
            .await
            .map_err(|e| BrokerError::Replication(format!("add_learner: {e}")))
    }

    /// Reports whether `(topic, partition)` is present in this broker's
    /// `MetadataImage`. Replication integration tests use this to wait for
    /// topic propagation.
    #[must_use]
    pub fn has_partition(&self, topic: &str, partition: i32) -> bool {
        self.broker
            .controller
            .current_image()
            .partition(topic, partition)
            .is_some()
    }

    /// Local `log_end_offset` for `(topic, partition)`, if this broker
    /// hosts the partition. Replication integration tests use this to
    /// assert that all followers caught up.
    #[must_use]
    pub fn local_log_end_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        // Unwrap `Offset` -> `i64` at this test-helper boundary: integration
        // tests compare the result against raw offset literals.
        Some(part.log_end_offset().0)
    }

    /// This broker's raft `node_id` (1-indexed broker id used in raft quorum
    /// and metadata records). Exposed so integration tests can build
    /// `IncrementalAlterConfigs` broker-resource requests that target this
    /// broker without a hard-coded node id.
    #[must_use]
    pub fn node_id(&self) -> u64 { self.broker.config.node_id.0 }

    /// Rebuild the TLS server config from the cert/key paths
    /// in `BrokerConfig::tls_config` *right now*, without the
    /// periodic mtime watcher. New TLS handshakes after this call see
    /// the rebuilt config. In-flight handshakes are unaffected.
    ///
    /// Operators call this from sidecars or hook scripts that just
    /// wrote new cert files into place and want the change to take
    /// effect before the next `tls_reload_interval` tick.
    ///
    /// # Errors
    ///
    /// - `BrokerError::Tls`: the new cert, key, or client-CA failed
    ///   to parse, or rustls rejected the assembled config. The
    ///   previous config stays in place, and the broker keeps serving
    ///   with the old cert.
    /// - `BrokerError::Startup`: no TLS config is configured.
    pub fn reload_tls(&self) -> Result<(), BrokerError> {
        let Some(dynamic) = self.broker.tls_dynamic.as_ref() else {
            return Err(BrokerError::Startup(
                "reload_tls: broker has no tls_config".into(),
            ));
        };
        let Some(tls_cfg) = self.broker.config.tls_config.as_ref() else {
            return Err(BrokerError::Startup(
                "reload_tls: broker has no tls_config".into(),
            ));
        };
        dynamic
            .reload_from(tls_cfg)
            .map_err(|e| BrokerError::Tls(e.to_string()))
    }

    /// Subscribe to the self-shutdown signal. Flips `true` when the broker
    /// decides to stop on its own. There are two such causes: all log dirs
    /// went offline (KIP-112), and the controller that this node hosts stopped
    /// itself over a fatal fault ([`Self::fatal_fault`] gives the reason). The
    /// embedding application should call [`Self::shutdown`] or
    /// `controlled_shutdown` when this fires, and exit non-zero for a fatal
    /// fault, as Kafka halts the process.
    #[must_use]
    pub fn should_shutdown_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self.broker.should_shutdown.subscribe()
    }

    /// The fatal fault that stopped this node, or `None` when there is none:
    /// a fault that stopped its controller, or the failure of its metadata
    /// log directory (KIP-858).
    ///
    /// Kafka's `ProcessTerminatingFaultHandler` halts the process with exit
    /// status 1 on such a fault, for example when a controller replays a
    /// `FeatureLevelRecord` above the range it supports. Kafka's
    /// `ReplicaManager.handleLogDirFailure` halts it too when the failed
    /// directory is the metadata log directory. A library cannot halt
    /// its host, so [`Self::should_shutdown_rx`] flips and this holds the
    /// message to exit with. The flag is already latched, so a
    /// `controlled_shutdown` after it skips the leadership drain, which a dead
    /// controller could not serve, and stops the broker at once. That stop
    /// leaves no clean-shutdown proof, as a halted Kafka process leaves none.
    #[must_use]
    pub fn fatal_fault(&self) -> Option<String> {
        self.broker
            .controller
            .watch_fatal()
            .borrow()
            .clone()
            .or_else(|| self.broker.log_dir_status.metadata_dir_fault())
    }

    /// A receiver of the fatal fault that stops this node's controller.
    ///
    /// [`Self::controlled_shutdown`] and [`Self::shutdown`] consume the handle,
    /// so a caller that must know whether the controller faulted while it was
    /// stopping the broker takes this first and reads it afterwards.
    #[must_use]
    pub fn fatal_fault_watch(&self) -> tokio::sync::watch::Receiver<Option<String>> {
        self.broker.controller.watch_fatal()
    }

    /// Request a graceful, controlled shutdown of this broker.
    ///
    /// Signals the heartbeat client to set `want_shut_down=true` on
    /// outbound `BrokerHeartbeat` requests. The controller leader
    /// reassigns leadership of every partition currently led by this
    /// broker. Once leadership is fully drained, the controller
    /// responds with `should_shut_down=true`. This call then invokes
    /// the regular [`shutdown`](Self::shutdown).
    ///
    /// A node without the broker role leads no partition and runs no
    /// heartbeat client, so no controller would ever answer the drain. It goes
    /// straight to [`shutdown`](Self::shutdown) and returns `Ok(())`, as
    /// Kafka's controller-only process stops without a controlled shutdown.
    ///
    /// This method always stops the broker before it returns. A clean drain
    /// goes through the regular [`shutdown`](Self::shutdown). A `timeout` goes
    /// through a hard shutdown fallback that returns `Err(ShutdownTimeout)`, so
    /// the caller knows the drain was incomplete. In both cases the broker
    /// stops, so the process can exit before a Kubernetes SIGKILL.
    ///
    /// # Errors
    ///
    /// - `BrokerError::ShutdownTimeout`: the controller did not
    ///   acknowledge `should_shut_down=true` within `timeout`. The broker was
    ///   hard-shut-down anyway.
    pub async fn controlled_shutdown(
        self,
        timeout: std::time::Duration,
    ) -> Result<(), BrokerError> {
        if !self.broker.config.is_broker() {
            self.shutdown().await;
            return Ok(());
        }
        let mut should_shutdown_rx = self.broker.should_shutdown.subscribe();
        // Latch the request flag. Idempotent — repeated sends to a
        // `watch::Sender` with the same value are harmless and the
        // heartbeat client reads `borrow_and_update()` each tick.
        let _ = self.broker.want_shutdown.send(true);
        // Wait for the heartbeat client to observe should_shut_down=true.
        let wait = async {
            // `subscribe()` returns the current value (`false`) without
            // marking it seen — so the first `changed()` only fires on
            // a true edge.
            loop {
                if *should_shutdown_rx.borrow() {
                    return;
                }
                if should_shutdown_rx.changed().await.is_err() {
                    return;
                }
            }
        };
        // `if`/`else` rather than `match { Ok(()) => .., Err(_) => .. }` to
        // satisfy `clippy::single_match_else`.
        if tokio::time::timeout(timeout, wait).await.is_ok() {
            self.shutdown().await;
            Ok(())
        } else {
            // Leadership did not fully drain in time (e.g. the controller
            // is itself unreachable). Still stop cleanly via the regular
            // hard shutdown so the process exits before the Kubernetes
            // SIGKILL — a partly-drained graceful stop still beats an
            // abrupt kill. The `ShutdownTimeout` return tells the caller
            // the drain was incomplete.
            tracing::warn!(
                ?timeout,
                "controlled shutdown drain timed out; falling back to hard shutdown"
            );
            self.shutdown().await;
            Err(BrokerError::ShutdownTimeout(timeout))
        }
    }

    /// Abort broker work without draining in-flight data-path tasks.
    ///
    /// This is intentionally exposed only as fault-injection support. Unlike
    /// [`shutdown`](Self::shutdown), it aborts retained tasks before cancelling
    /// controller participation, so tests can model process death rather than
    /// an orderly stop.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn crash_for_test(mut self) {
        self.broker.supervisor_shutdown.cancel();
        self.shutdown.cancel();
        self.broker.audit_log.close();

        // Stop the object writer first: an abrupt victim must not complete a
        // blocked flush while the rest of the broker is being torn down.
        if let Some(task) = self.diskless_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.topic_rlmm_task.take() {
            task.abort();
            let _ = task.await;
        }

        // Awaiting the cancelled listener tasks lets each accept loop abort
        // and join all active connection children. The requests themselves
        // are not drained, but this method does not return while a child can
        // still mutate the victim's data files.
        for task in self.listener_tasks.drain(..) {
            let _ = task.await;
        }

        // The supervisor's cancellation epilogue aborts and joins its private
        // ordinary and WAL follower tasks. Its run loop races every reconcile
        // after startup against this token, so awaiting it is bounded even if
        // a network-backed reconciliation was in flight.
        if let Some(task) = self.broker.supervisor_handle.lock().await.take() {
            let _ = task.await;
        }
        if let Some(task) = self.broker.disk_scanner_handle.lock().await.take() {
            let _ = task.await;
        }

        shutdown_moves(&self.broker.future_logs).await;
        shutdown_partition_writers(&self.broker.partitions).await;
        if let Some(task) = self.broker.audit_writer_handle.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        self.broker.controller.cancel().await;
    }

    /// Record the broker epoch this node is stopping on, so its next start can
    /// prove to the controller that the stop was graceful.
    ///
    /// Kafka writes the same proof from `LogManager.shutdown`, through
    /// `CleanShutdownFileHandler`, and reads it back as
    /// `BrokerRegistrationRequest.previousBrokerEpoch`, naming the epoch
    /// `BrokerLifecycleManager` registered at. A node that never registered
    /// has no epoch to name and writes nothing; so does a pure controller,
    /// which holds no replica and so no ELR membership.
    ///
    /// A stop that a fatal controller fault caused writes nothing either.
    /// Kafka handles that fault with `ProcessTerminatingFaultHandler`, which
    /// calls `Exit.halt(1)`: `BrokerServer.shutdown` never runs, so
    /// `LogManager.shutdown` never writes `.kafka_cleanshutdown`, and the
    /// restart registers with `previousBrokerEpoch` `-1`, which the controller
    /// counts as an unclean shutdown. Leaving a proof here would let this
    /// node's next registration read as a clean restart for ELR, which no
    /// Kafka broker's does after that fault.
    fn write_clean_shutdown_proof(&self) {
        let config = &self.broker.config;
        if !config.is_broker() || config.broker_epoch < 0 || self.fatal_fault().is_some() {
            return;
        }
        write(&config.log_dir, config.broker_epoch);
    }

    /// Cancel the listener and drain in-flight connections. The returned
    /// future completes when the listener task exits.
    pub async fn shutdown(mut self) {
        // Cancel the replicator supervisor BEFORE the controller drops:
        // in-flight replication tasks must observe a clean cancellation
        // rather than a torn-down metadata-watch channel.
        self.broker.supervisor_shutdown.cancel();
        if let Some(h) = self.broker.supervisor_handle.lock().await.take() {
            let _ = h.await;
        }
        // Drain the disk-usage scanner if it was spawned.
        // The scanner observes the same `supervisor_shutdown` cancellation
        // its sibling tasks do; awaiting the handle here ensures the
        // background tick is fully wound down before we tear the rest
        // of the broker apart.
        if let Some(h) = self.broker.disk_scanner_handle.lock().await.take() {
            let _ = h.await;
        }
        self.shutdown.cancel();
        if let Some(task) = self.topic_rlmm_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.diskless_task.take() {
            let _ = task.await;
        }
        for t in self.listener_tasks.drain(..) {
            let _ = t.await;
        }
        self.broker
            .audit_log
            .emit(krabka_audit::AuditEvent::Lifecycle {
                kind: krabka_audit::LifecycleKind::BrokerStopping,
                node_id: i64::from(self.broker.config.broker_id),
                time_ms: now_ms(),
            });
        self.broker.audit_log.close();
        if let Some(task) = self.broker.audit_writer_handle.lock().await.take() {
            let _ = task.await;
        }
        shutdown_moves(&self.broker.future_logs).await;
        shutdown_partition_writers(&self.broker.partitions).await;
        snapshot_producer_state(&self.broker.partitions);
        self.broker.client_metrics.shutdown().await;
        // Every log this broker holds is now closed, so the record set it
        // brings back on restart is the one the cluster believes it has. That
        // is the whole content of the clean-shutdown proof, and this is the
        // last moment it is true, so leave it here. A crash never reaches this
        // line, which is exactly why the absence of the file means unclean.
        self.write_clean_shutdown_proof();
        // Shut down the raft engine so this broker's controller stops
        // participating in elections after the broker is logically dead.
        // Without this, a killed broker's in-process raft engine keeps ticking
        // and re-elects itself, preventing the surviving nodes from detecting
        // the leader failure and electing a replacement.
        self.broker.controller.cancel().await;
    }
}

impl Drop for BrokerHandle {
    fn drop(&mut self) {
        self.broker.supervisor_shutdown.cancel();
        self.shutdown.cancel();
        self.broker.audit_log.close();
        if let Ok(mut handle) = self.broker.audit_writer_handle.try_lock()
            && let Some(task) = handle.take()
        {
            task.abort();
        }
        for task in self.listener_tasks.drain(..) {
            task.abort();
        }
        if let Some(task) = self.topic_rlmm_task.take() {
            task.abort();
        }
        if let Some(task) = self.diskless_task.take() {
            task.abort();
        }
        abort_moves(&self.broker.future_logs);
        abort_partition_writers(&self.broker.partitions);
    }
}

#[cfg(test)]
mod tests;
