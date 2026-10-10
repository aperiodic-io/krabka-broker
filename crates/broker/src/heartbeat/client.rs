//! Broker-side heartbeat client. It sends `BrokerHeartbeat` to the
//! controller leader at every configured `heartbeat_interval`, over one
//! connection that it keeps across ticks. It finds the current controller in
//! the metadata image, and it retries after transient errors.
//!
//! KIP-919 puts `BrokerHeartbeat` on the controller's CONTROLLER listener, so
//! the leader's address is the one the raft transport already dials it on --
//! its KIP-853 voter endpoint -- not a broker registration. A controller-only
//! node never registers as a broker, so resolving the leader through
//! `image.broker()` would strand every broker in a role-separated cluster: no
//! heartbeat would ever arrive, and the controller's liveness registry would
//! fence the whole cluster one `liveness_tick_interval` after boot.

use std::sync::Arc;

use krabka_client_core::ConnectionOptions;
use krabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use krabka_security::ListenerProtocol;
use krabka_units::{Time, convert::TimeExt as _, fmt::Human as _, millis, secs};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Where this broker's first unfencing stands: Kafka's
/// `BrokerLifecycleManager.initialUnfenceFuture`, which broker startup waits
/// on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InitialUnfence {
    /// No heartbeat answer has unfenced the broker yet.
    Pending,
    /// A heartbeat answer said the broker is unfenced.
    Unfenced,
    /// The controller refused the heartbeat with
    /// `CLUSTER_AUTHORIZATION_FAILED`. Nothing but an ACL change lets the
    /// broker unfence, so startup stops waiting for it.
    Refused,
}

pub(crate) struct Config {
    pub broker_id: i32,
    /// The broker epoch this process registered at, which every heartbeat
    /// names: Kafka's `BrokerLifecycleManager.brokerEpoch`. The local image
    /// can still hold the previous incarnation's registration for a while
    /// after a restart, and a heartbeat that named its epoch would be refused
    /// as `STALE_BROKER_EPOCH`.
    pub broker_epoch: i64,
    pub interval: Time,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub shutdown: CancellationToken,
    /// Shared outbound dialer that reaches the controller leader. It runs
    /// TLS / SASL when the controller listener needs them. If not, it uses
    /// plain TCP. This is the same dialer the raft transport uses to reach
    /// its peers, so a heartbeat travels the channel the quorum already
    /// authenticates on.
    pub outbound_client: Arc<crate::network::client::InterBrokerClient>,
    pub controller_listener_protocol: ListenerProtocol,
    /// SNI and SASL server name for the controller listener, matching the one
    /// the raft dialer presents.
    pub controller_server_name: String,
    /// The statically configured quorum, `controller_quorum_voters`. It backs
    /// the voter set as a source of the leader's address, for the window
    /// before the committed voter set names it. See
    /// [`crate::controller_endpoint::leader_endpoint`].
    pub controller_quorum_voters: Vec<(krabka_raft::NodeId, String)>,
    /// When `true`, the client stamps `want_shut_down=true` on outbound
    /// `BrokerHeartbeat` requests.
    /// [`crate::BrokerHandle::controlled_shutdown`] drives this flag.
    pub want_shutdown: tokio::sync::watch::Receiver<bool>,
    /// The client sets this to `true` when the controller responds with
    /// `should_shut_down=true`. The caller of `controlled_shutdown`
    /// awaits this flag.
    pub should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// The client moves this out of [`InitialUnfence::Pending`] on the first
    /// answer that unfences the broker or refuses it for authorization.
    pub unfenced: tokio::sync::watch::Sender<InitialUnfence>,
    /// Per-log-dir health registry. Each heartbeat reports the offline dirs
    /// to the controller as `offline_log_dirs` UUIDs (KIP-858).
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Stable per-log-dir UUIDs, to translate offline dir paths to ids.
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
    /// All configured log dirs. When every one of them is offline, the
    /// broker shuts itself down (KIP-112).
    pub all_log_dirs: Vec<std::path::PathBuf>,
    /// The broker cancels this when all dirs go offline. This stops
    /// replication and materialization against dead disks before teardown.
    pub supervisor_shutdown: tokio_util::sync::CancellationToken,
    /// KIP-1066: the log directories this broker holds cordoned. Heartbeats
    /// report their ids once the broker has caught up with the metadata.
    pub cordoned_log_dirs: crate::cordoned_log_dirs::CordonedLogDirs,
}

/// UUIDs of the currently-offline log dirs, for the heartbeat's `offline_log_dirs`.
fn offline_dir_uuids(
    status: &crate::log_dir_status::LogDirRegistry,
    ids: &crate::log_dir_id::LogDirIds,
) -> Vec<krabka_protocol::primitives::uuid::Uuid> {
    status
        .offline()
        .into_iter()
        .filter_map(|(path, _reason)| ids.id_for(&path))
        .map(|u| krabka_protocol::primitives::uuid::Uuid(*u.as_bytes()))
        .collect()
}

/// True when every configured log dir is offline. The broker then shuts itself
/// down.
fn all_dirs_offline(
    all_log_dirs: &[std::path::PathBuf],
    status: &crate::log_dir_status::LogDirRegistry,
) -> bool {
    !all_log_dirs.is_empty() && all_log_dirs.iter().all(|d| status.is_offline(d))
}

/// Returns `true` when every configured log dir is currently offline.
fn all_log_dirs_offline(cfg: &Config) -> bool {
    all_dirs_offline(&cfg.all_log_dirs, &cfg.log_dir_status)
}

fn heartbeat_rpc_timeout(interval: Time) -> Time {
    (interval * 2.0).max(millis(500)).min(secs(1))
}

fn heartbeat_connection_options(broker_id: i32, interval: Time) -> ConnectionOptions {
    let timeout = heartbeat_rpc_timeout(interval);
    ConnectionOptions {
        client_id: format!("krabka-broker-{broker_id}-heartbeat"),
        socket_connection_setup_timeout: timeout,
        socket_connection_setup_timeout_max: timeout,
        request_timeout: timeout,
        ..ConnectionOptions::default()
    }
}

/// The cordoned directory ids a heartbeat carries, or `None` when it carries
/// none.
///
/// Kafka's `BrokerLifecycleManager.sendBrokerHeartbeat` sets
/// `CordonedLogDirs` only once the broker has caught up with the metadata
/// (`initialCatchUpFuture`), because the dynamic config is up to date only
/// then, and only while `metadata.version` is at least `4.3-IV0`.
fn cordoned_dir_uuids(
    caught_up: bool,
    image: &krabka_metadata::MetadataImage,
    cordoned: &crate::cordoned_log_dirs::CordonedLogDirs,
    ids: &crate::log_dir_id::LogDirIds,
) -> Option<Vec<krabka_protocol::primitives::uuid::Uuid>> {
    let supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL
    });
    (caught_up && supported).then(|| {
        cordoned
            .cordoned()
            .iter()
            .filter_map(|dir| ids.id_for(dir))
            .map(|id| krabka_protocol::primitives::uuid::Uuid(*id.as_bytes()))
            .collect()
    })
}

/// The two directory lists a heartbeat reports.
#[derive(Debug, Default)]
struct HeartbeatLogDirs {
    /// KIP-858 directories that went offline.
    offline: Vec<krabka_protocol::primitives::uuid::Uuid>,
    /// KIP-1066 cordoned directories, `None` before the broker catches up.
    cordoned: Option<Vec<krabka_protocol::primitives::uuid::Uuid>>,
}

fn heartbeat_request(
    broker_id: i32,
    broker_epoch: i64,
    current_metadata_offset: i64,
    want_shut_down: bool,
    log_dirs: HeartbeatLogDirs,
) -> BrokerHeartbeatRequest {
    BrokerHeartbeatRequest {
        broker_id,
        broker_epoch,
        current_metadata_offset,
        want_shut_down,
        offline_log_dirs: log_dirs.offline,
        cordoned_log_dirs: log_dirs.cordoned,
        ..Default::default()
    }
}

/// Triggers the KIP-112 self-shutdown. It latches `should_shutdown` and
/// cancels the supervisor. Every early-exit path calls it, so the check is not
/// accidentally skipped when the controller is temporarily unreachable.
fn trigger_all_dirs_offline_shutdown(cfg: &mut Config, reason: &str) {
    tracing::error!(
        reason,
        "all log dirs offline — initiating broker self-shutdown (KIP-112)"
    );
    let _ = cfg.should_shutdown.send(true);
    cfg.supervisor_shutdown.cancel();
}

/// The controller leader, and the controller-listener endpoint the metadata
/// names for it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ControllerAddress {
    leader: krabka_raft::NodeId,
    host: String,
    port: u16,
}

/// The connection the heartbeats travel on, and the controller it reaches.
struct ControllerChannel {
    controller: ControllerAddress,
    connection: krabka_client_core::Connection,
}

/// Dials the controller listener of `controller`, with the TLS and SASL that
/// the listener needs, or gives up after `rpc_timeout`.
async fn dial(
    cfg: &Config,
    controller: &ControllerAddress,
    rpc_timeout: Time,
) -> Option<krabka_client_core::Connection> {
    let opts = heartbeat_connection_options(cfg.broker_id, cfg.interval);
    let dialled = tokio::time::timeout(
        rpc_timeout.to_std(),
        cfg.outbound_client.connect_as_connection(
            &controller.host,
            controller.port,
            cfg.controller_listener_protocol,
            &cfg.controller_server_name,
            opts,
        ),
    )
    .await;
    match dialled {
        Ok(Ok(connection)) => Some(connection),
        Ok(Err(error)) => {
            debug!(%error, "heartbeat: connect failed");
            None
        }
        Err(_) => {
            debug!(
                rpc_timeout = %rpc_timeout.human(),
                "heartbeat: connect timed out"
            );
            None
        }
    }
}

/// Sends a `BrokerHeartbeat` to the controller leader at every tick.
///
/// Every heartbeat travels on one connection, as Kafka's
/// `BrokerLifecycleManager` sends them through one
/// `NodeToControllerChannelManager`. Its `NodeToControllerRequestThread` keeps
/// one `NetworkClient` connection to the active controller. That connection
/// closes when it fails, when a heartbeat times out, and when the controller
/// answers `NOT_CONTROLLER`. A tick that finds no open connection to the
/// current leader dials one, so a new leader, or a new address for the same
/// leader, gets a new connection.
pub(crate) async fn run(mut cfg: Config) {
    // A node that never registered as a broker has no heartbeat to send, as a
    // Kafka controller-only node has no `BrokerLifecycleManager`.
    if cfg.broker_epoch < 0 {
        return;
    }
    let mut tick = tokio::time::interval(cfg.interval.to_std());
    let mut channel: Option<ControllerChannel> = None;
    // Kafka's `initialCatchUpFuture`: set by the first answer that says the
    // broker has caught up, and never cleared.
    let mut caught_up = false;
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // KIP-112 check: even if we cannot reach the controller, self-shutdown
        // must fire as long as every log dir is offline.
        if all_log_dirs_offline(&cfg) {
            trigger_all_dirs_offline_shutdown(&mut cfg, "detected before controller resolution");
            return;
        }
        // Resolve the current controller leader's CONTROLLER-listener address
        // from the metadata image, or skip this tick if it is not known yet.
        let leader_id = *cfg.controller.watch_leader().borrow();
        let Some(leader_id) = leader_id else {
            debug!("heartbeat: no controller leader yet");
            continue;
        };
        let image = cfg.controller.current_image();
        let Some((host, port)) = crate::controller_endpoint::leader_endpoint(
            &image,
            &cfg.controller_quorum_voters,
            leader_id,
        ) else {
            debug!(
                leader = leader_id.0,
                "heartbeat: controller leader has no known controller endpoint yet"
            );
            continue;
        };
        let controller = ControllerAddress {
            leader: leader_id,
            host,
            port,
        };
        let rpc_timeout = heartbeat_rpc_timeout(cfg.interval);
        let open = match channel.take() {
            Some(open) if open.controller == controller && !open.connection.is_closed() => open,
            stale => {
                if let Some(stale) = stale {
                    stale.connection.close();
                }
                let Some(connection) = dial(&cfg, &controller, rpc_timeout).await else {
                    continue;
                };
                ControllerChannel {
                    controller,
                    connection,
                }
            }
        };
        let want_shut_down = *cfg.want_shutdown.borrow_and_update();
        let log_dirs = HeartbeatLogDirs {
            offline: offline_dir_uuids(&cfg.log_dir_status, &cfg.log_dir_ids),
            cordoned: cordoned_dir_uuids(
                caught_up,
                &image,
                &cfg.cordoned_log_dirs,
                &cfg.log_dir_ids,
            ),
        };
        let resp = tokio::time::timeout(
            rpc_timeout.to_std(),
            open.connection.send(heartbeat_request(
                cfg.broker_id,
                cfg.broker_epoch,
                cfg.controller.current_metadata_offset(),
                want_shut_down,
                log_dirs,
            )),
        )
        .await;
        match resp {
            Ok(Ok(r)) => {
                if r.error_code == crate::codes::NOT_CONTROLLER {
                    open.connection.close();
                } else {
                    channel = Some(open);
                }
                if r.error_code == crate::codes::CLUSTER_AUTHORIZATION_FAILED {
                    cfg.unfenced.send_if_modified(|state| {
                        let pending = *state == InitialUnfence::Pending;
                        if pending {
                            *state = InitialUnfence::Refused;
                        }
                        pending
                    });
                }
                if r.error_code != crate::codes::NONE {
                    warn!(
                        error_code = r.error_code,
                        "heartbeat rejected by controller"
                    );
                    continue;
                }
                caught_up |= r.is_caught_up;
                if !r.is_fenced {
                    cfg.unfenced.send_replace(InitialUnfence::Unfenced);
                }
                if r.should_shut_down {
                    // Latch true; never flip back. The
                    // `BrokerHandle::controlled_shutdown` waiter is
                    // single-shot.
                    let _ = cfg.should_shutdown.send(true);
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, "heartbeat send failed");
                open.connection.close();
            }
            Err(_) => {
                warn!(
                    rpc_timeout = %rpc_timeout.human(),
                    "heartbeat send timed out"
                );
                open.connection.close();
            }
        }

        // KIP-112: re-check after the heartbeat round-trip. This covers the
        // window where dirs went offline *during* the connect/send. The
        // top-of-tick check already handles dirs that were offline before
        // leader resolution; this one handles the same-tick race.
        if all_log_dirs_offline(&cfg) {
            trigger_all_dirs_offline_shutdown(&mut cfg, "detected after heartbeat send");
            // Returning stops heartbeats; if shutdown drags, the controller's
            // session timeout fences this broker independently.
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Mutex,
        time::{Duration, Instant},
    };

    use assert2::{assert, check};
    use bytes::BytesMut;
    use krabka_client_core::{MockBroker, MockReply};
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    use krabka_protocol::{
        Encode as _,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            broker_heartbeat_request,
            broker_heartbeat_response::BrokerHeartbeatResponse,
        },
    };

    use super::*;
    use crate::test_support::FakeMetadataSource;

    // How a fake controller answers a heartbeat.
    #[derive(Debug, Clone, Copy)]
    enum Answer {
        // No error.
        Accept,
        // `NOT_CONTROLLER`, as a controller that lost the leadership answers.
        NotController,
        // No answer, so the heartbeat times out.
        Silent,
        // No answer, and the fake closes the connection.
        Close,
    }

    #[derive(Debug)]
    struct Seen {
        // Every connection starts with one `ApiVersions` exchange, so this
        // counts the connections the client dialled.
        connections: usize,
        heartbeats: usize,
        next_answer: Answer,
    }

    // A controller listener that answers `ApiVersions` and `BrokerHeartbeat`.
    struct FakeController {
        broker: MockBroker,
        seen: Arc<Mutex<Seen>>,
    }

    impl FakeController {
        async fn start() -> Self {
            let seen = Arc::new(Mutex::new(Seen {
                connections: 0,
                heartbeats: 0,
                next_answer: Answer::Accept,
            }));
            let handled = Arc::clone(&seen);
            let broker = MockBroker::start_with_replies(move |api_key, version, _, _| {
                let mut seen = handled.lock().unwrap();
                let mut body = BytesMut::new();
                if api_key == api_versions_request::API_KEY {
                    seen.connections += 1;
                    ApiVersionsResponse {
                        api_keys: vec![
                            ApiVersion {
                                api_key: api_versions_request::API_KEY,
                                min_version: 0,
                                max_version: api_versions_request::MAX_VERSION,
                                ..Default::default()
                            },
                            ApiVersion {
                                api_key: broker_heartbeat_request::API_KEY,
                                min_version: 0,
                                max_version: broker_heartbeat_request::MAX_VERSION,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }
                    .encode(&mut body, version)
                    .unwrap();
                    return MockReply::Respond(body.to_vec());
                }
                seen.heartbeats += 1;
                let error_code = match std::mem::replace(&mut seen.next_answer, Answer::Accept) {
                    Answer::Accept => crate::codes::NONE,
                    Answer::NotController => crate::codes::NOT_CONTROLLER,
                    Answer::Silent => return MockReply::Silent,
                    Answer::Close => return MockReply::Close,
                };
                // `BrokerHeartbeat` is flexible, so its response header ends
                // with an empty tagged-field section.
                body.extend_from_slice(&[0]);
                BrokerHeartbeatResponse {
                    error_code,
                    ..Default::default()
                }
                .encode(&mut body, version)
                .unwrap();
                MockReply::Respond(body.to_vec())
            })
            .await;
            Self { broker, seen }
        }

        fn connections(&self) -> usize {
            self.seen.lock().unwrap().connections
        }

        fn heartbeats(&self) -> usize {
            self.seen.lock().unwrap().heartbeats
        }

        fn answer_next(&self, answer: Answer) {
            self.seen.lock().unwrap().next_answer = answer;
        }

        // Waits until the fake has seen `more` heartbeats after the ones it
        // has seen already.
        async fn wait_for_heartbeats(&self, more: usize) {
            let target = self.heartbeats() + more;
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.heartbeats() < target {
                assert!(Instant::now() < deadline, "no heartbeat #{target} in 10s");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    // Kafka's `NodeToControllerRequestThread` keeps one connection to the
    // active controller. It closes it when it fails, when a request times out
    // and when the controller answers `NOT_CONTROLLER`, and the next request
    // goes to the controller the metadata names then.
    #[tokio::test]
    async fn heartbeats_share_one_connection_until_it_breaks_or_the_leader_moves() {
        let first = FakeController::start().await;
        let second = FakeController::start().await;
        let dials = || (first.connections(), second.connections());
        let source = Arc::new(
            FakeMetadataSource::builder()
                .records(&[MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        broker_epoch: 11,
                        host: "localhost".into(),
                        ..crate::test_support::broker_registration(krabka_raft::NodeId(7))
                    },
                )])
                .leader(Some(krabka_raft::NodeId(1)))
                .build(),
        );
        let no_dirs: Vec<std::path::PathBuf> = Vec::new();
        let (_want_shutdown, want_shutdown) = tokio::sync::watch::channel(false);
        let shutdown = CancellationToken::new();
        let heartbeats = tokio::spawn(run(Config {
            broker_id: 7,
            broker_epoch: 11,
            interval: millis(20),
            controller: Arc::clone(&source) as Arc<dyn crate::metadata_source::MetadataSource>,
            shutdown: shutdown.clone(),
            outbound_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
            controller_listener_protocol: ListenerProtocol::Plaintext,
            controller_server_name: "localhost".into(),
            controller_quorum_voters: vec![
                (krabka_raft::NodeId(1), first.broker.addr.to_string()),
                (krabka_raft::NodeId(2), second.broker.addr.to_string()),
            ],
            want_shutdown,
            should_shutdown: Arc::new(tokio::sync::watch::channel(false).0),
            unfenced: tokio::sync::watch::channel(InitialUnfence::Pending).0,
            log_dir_status: crate::log_dir_status::LogDirRegistry::probe(&no_dirs),
            log_dir_ids: crate::log_dir_id::LogDirIds::resolve(&no_dirs),
            all_log_dirs: no_dirs.clone(),
            supervisor_shutdown: CancellationToken::new(),
            cordoned_log_dirs: crate::cordoned_log_dirs::CordonedLogDirs::default(),
        }));

        // The dials each fake has taken after each step, compared at the end.
        first.wait_for_heartbeats(5).await;
        let mut dialled = vec![("five heartbeats", dials())];
        for (step, answer) in [
            ("a closed connection", Answer::Close),
            ("a heartbeat that timed out", Answer::Silent),
            ("NOT_CONTROLLER", Answer::NotController),
        ] {
            first.answer_next(answer);
            first.wait_for_heartbeats(3).await;
            dialled.push((step, dials()));
        }
        source.set_leader(Some(krabka_raft::NodeId(2)));
        second.wait_for_heartbeats(3).await;
        dialled.push(("a new leader", dials()));
        shutdown.cancel();
        heartbeats.await.unwrap();

        assert!(
            dialled
                == vec![
                    ("five heartbeats", (1, 0)),
                    ("a closed connection", (2, 0)),
                    ("a heartbeat that timed out", (3, 0)),
                    ("NOT_CONTROLLER", (4, 0)),
                    ("a new leader", (4, 1)),
                ]
        );
    }

    #[test]
    fn offline_dir_uuids_maps_offline_paths() {
        let (a, _b, paths) = crate::test_support::two_log_dirs();
        let ids = crate::log_dir_id::LogDirIds::resolve(&paths);
        let status = crate::log_dir_status::LogDirRegistry::probe(&paths);

        // Initially no dirs are offline.
        assert!(offline_dir_uuids(&status, &ids).is_empty());

        // Mark dir `a` as offline.
        status.mark_offline(a.path(), "test");
        let result = offline_dir_uuids(&status, &ids);
        assert!(result.len() == 1);
        let expected_id = ids.id_for(a.path()).unwrap();
        assert!(result[0].0 == *expected_id.as_bytes());
    }

    #[test]
    fn all_dirs_offline_true_only_when_every_dir_offline() {
        let (a, b, paths) = crate::test_support::two_log_dirs();
        let status = crate::log_dir_status::LogDirRegistry::probe(&paths);

        // Empty all_log_dirs: always false.
        assert!(!all_dirs_offline(&[], &status));

        // No dirs offline yet.
        assert!(!all_dirs_offline(&paths, &status));

        // Only `a` offline: still false.
        status.mark_offline(a.path(), "disk error");
        assert!(!all_dirs_offline(&paths, &status));

        // Both offline: true.
        status.mark_offline(b.path(), "disk error");
        assert!(all_dirs_offline(&paths, &status));
    }

    #[test]
    fn heartbeat_rpc_timeout_tracks_interval_with_bounds() {
        for (interval, want) in [
            (millis(50), millis(500)),
            (millis(500), secs(1)),
            (secs(5), secs(1)),
        ] {
            assert!(heartbeat_rpc_timeout(interval) == want, "{interval:?}");
        }
    }

    #[test]
    fn heartbeat_connection_options_use_bounded_rpc_timeout() {
        let opts = heartbeat_connection_options(9, millis(500));

        check!(opts.client_id == "krabka-broker-9-heartbeat");
        check!(opts.socket_connection_setup_timeout == secs(1));
        check!(opts.socket_connection_setup_timeout_max == secs(1));
        check!(opts.request_timeout == secs(1));
    }

    #[test]
    fn heartbeat_request_reports_registration_and_applied_metadata() {
        let offline = krabka_protocol::primitives::uuid::Uuid([7; 16]);
        let cordoned = krabka_protocol::primitives::uuid::Uuid([9; 16]);
        let req = heartbeat_request(
            3,
            41,
            47,
            true,
            HeartbeatLogDirs {
                offline: vec![offline],
                cordoned: Some(vec![cordoned]),
            },
        );

        assert!(
            req == BrokerHeartbeatRequest {
                broker_id: 3,
                broker_epoch: 41,
                current_metadata_offset: 47,
                want_fence: false,
                want_shut_down: true,
                offline_log_dirs: vec![offline],
                cordoned_log_dirs: Some(vec![cordoned]),
                ..Default::default()
            }
        );
    }

    /// Kafka's `BrokerLifecycleManager.sendBrokerHeartbeat`: the cordoned
    /// directory ids go out once the broker has caught up and only from
    /// `metadata.version` `4.3-IV0`, and an empty set is still sent.
    #[test]
    fn heartbeats_carry_cordoned_dirs_once_caught_up_on_a_supporting_version() {
        let (_a, b, paths) = crate::test_support::two_log_dirs();
        let ids = crate::log_dir_id::LogDirIds::resolve(&paths);
        let wire = |dir: &std::path::Path| {
            krabka_protocol::primitives::uuid::Uuid(*ids.id_for(dir).unwrap().as_bytes())
        };
        let image_at = |level: Option<i16>| {
            let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(
                    krabka_metadata::FeatureLevelRecord {
                        name: crate::features::METADATA_VERSION.into(),
                        level,
                    },
                ));
            }
            image
        };
        let one = crate::cordoned_log_dirs::CordonedLogDirs::new(
            paths.clone(),
            Some(b.path().display().to_string()),
        );
        let none = crate::cordoned_log_dirs::CordonedLogDirs::new(paths.clone(), None);
        let cases = [
            ("not caught up", false, Some(30), &one, None),
            (
                "caught up",
                true,
                Some(30),
                &one,
                Some(vec![wire(b.path())]),
            ),
            ("nothing cordoned", true, Some(30), &none, Some(vec![])),
            ("below 4.3-IV0", true, Some(29), &one, None),
            ("no metadata.version", true, None, &one, None),
        ];
        for (label, caught_up, level, cordoned, want) in cases {
            check!(
                cordoned_dir_uuids(caught_up, &image_at(level), cordoned, &ids) == want,
                "{label}"
            );
        }
    }
}
