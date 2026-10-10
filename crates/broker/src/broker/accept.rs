//! The per-listener accept loop and the socket tuning, connection-limit, and
//! KIP-612 accept-rate checks it applies to every inbound connection. It is a
//! module of its own because it is the broker's only network ingress point.

use std::sync::Arc;

use krabka_units::{ByteSize, Time, convert::TimeExt as _};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::broker::Broker;

/// KIP-612 quota key for accept-rate throttling; matches the
/// `connection_creation_rate` config name Kafka's `AlterClientQuotas` uses
/// for `ip` entities.
const CONNECTION_CREATION_RATE_QUOTA_KEY: &str = "connection_creation_rate";

fn connection_creation_delay(rate: f64, maximum: Time) -> Time {
    let delay_micros = crate::quota::positive_f64_to_u64((1.0_f64 / rate) * 1_000_000.0);
    Time::from_micros(i64::try_from(delay_micros).unwrap_or(i64::MAX)).min(maximum)
}

/// The throttle for a new connection from `peer_ip`, or `None` when the
/// connection may be served.
///
/// A connection is throttled when the peer's `ip` entity has a positive
/// `connection_creation_rate` and its token bucket is empty. The bucket spends
/// no token on a throttled connection, as Kafka's
/// `recordIpConnectionMaybeThrottle` un-records it.
fn connection_creation_throttle(broker: &Broker, peer_ip: std::net::IpAddr) -> Option<Time> {
    let image = broker.controller.current_image();
    let (entity_key, rate) = crate::quota::lookup_ip_quota_with_key(
        &image,
        broker.quota_buckets.ip_names(),
        peer_ip,
        CONNECTION_CREATION_RATE_QUOTA_KEY,
    )?;
    if rate <= 0.0 {
        return None;
    }
    let bucket =
        broker
            .quota_buckets
            .get_or_create(CONNECTION_CREATION_RATE_QUOTA_KEY, &entity_key, rate);
    (bucket.try_consume(1) == 0)
        .then(|| connection_creation_delay(rate, broker.config.connection_creation_throttle_max))
}

/// Hold a throttled socket for `delay`, then close it without reading a
/// request. Kafka's acceptor keeps the socket in `throttledSockets` until
/// the throttle ends, so the client does not reconnect at once.
async fn close_after(stream: tokio::net::TcpStream, delay: Time) {
    tokio::time::sleep(delay.to_std()).await;
    drop(stream);
}

async fn shutdown_connection_tasks(connections: &mut JoinSet<()>) {
    connections.shutdown().await;
}

pub(super) async fn accept_loop(
    broker: Arc<Broker>,
    listener: TcpListener,
    spec: crate::config::ListenerSpec,
    shutdown: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!(name = %spec.name, "listener shutting down");
                break;
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result
                    && error.is_panic()
                {
                    tracing::warn!(?error, name = %spec.name, "connection task panicked");
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, name = %spec.name, "accepted connection");
                        let peer_ip = peer.ip();
                        tune_accepted_socket(
                            &stream,
                            broker.config.socket_send_buffer,
                            broker.config.socket_receive_buffer,
                        );

                        // KIP-612 `connection_creation_rate`, checked before
                        // the connection takes a slot, as Kafka's
                        // `ConnectionQuotas.inc` records the ip rate before
                        // it counts the connection. A connection over the
                        // rate is never served: the broker holds the socket
                        // for the throttle time and then closes it, so the
                        // client backs off and reconnects later. The hold
                        // runs as a task of its own, so the accept loop keeps
                        // accepting other connections meanwhile, as Kafka's
                        // acceptor does with `throttledSockets`.
                        if let Some(delay) = connection_creation_throttle(&broker, peer_ip) {
                            broker.metrics.observe_quota_throttle(
                                crate::metrics::QuotaType::ConnectionCreation,
                                delay.secs_f64(),
                            );
                            tracing::debug!(
                                %peer,
                                name = %spec.name,
                                delay_ms = delay.millis_i64(),
                                "connection_creation_rate exceeded; closing connection after the throttle"
                            );
                            connections.spawn(close_after(stream, delay));
                            continue;
                        }

                        // `max.connections` / `max.connections.per.ip` caps.
                        // Reserve a slot before doing any work; on rejection
                        // close the socket immediately (Kafka silently drops
                        // connections past either ceiling). The returned guard
                        // is moved into the spawned task so both counters are
                        // released however the connection ends.
                        let conn_guard = match broker.connections.try_acquire(peer_ip) {
                            Ok(guard) => guard,
                            Err(limit) => {
                                let reason = match limit {
                                    krabka_raft::ConnectionLimit::Global => {
                                        crate::metrics::ConnectionCloseReason::MaxConnections
                                    }
                                    krabka_raft::ConnectionLimit::PerIp => {
                                        crate::metrics::ConnectionCloseReason::MaxConnectionsPerIp
                                    }
                                };
                                broker.metrics.record_connection_close(reason);
                                tracing::debug!(
                                    %peer,
                                    name = %spec.name,
                                    ?limit,
                                    "connection limit reached; closing connection"
                                );
                                drop(stream);
                                continue;
                            }
                        };

                        let b = broker.clone();
                        let s = spec.clone();
                        connections.spawn(async move {
                            // Hold the connection guard for the lifetime of the
                            // connection; dropping it releases the global +
                            // per-IP slots.
                            let _conn_guard = conn_guard;
                            // `Box::pin` the per-connection handler: the request
                            // dispatch state machine (68 API handlers, each now
                            // carrying a tracing span) is a legitimately large
                            // future that trips `clippy::large_futures` once held
                            // inline in this spawned task. Boxing moves it to the
                            // heap (one alloc per long-lived connection — free).
                            Box::pin(crate::network::dispatch::serve_connection_on_listener(
                                b, stream, s,
                            ))
                            .await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, name = %spec.name, "accept failed");
                    }
                }
            }
        }
    }
    shutdown_connection_tasks(&mut connections).await;
}

/// Tune an accepted broker connection before serving it.
///
/// - `TCP_NODELAY`: disable Nagle so that delayed ACKs do not stall the
///   request/response ping-pong by up to ~40 ms. Apache Kafka sets this on its
///   broker sockets. Without it, small-request latency and the header+records
///   write coalescing (once fetch uses `sendfile`) both get worse.
/// - `SO_SNDBUF`/`SO_RCVBUF`: apply the configured, independently tunable
///   buffers so large fetches and produces retain enough in-flight headroom.
///
/// All failures are non-fatal and logged at debug level. A connection that the
/// broker cannot tune still serves correctly, but less efficiently.
fn tune_accepted_socket(
    stream: &tokio::net::TcpStream,
    send_buffer: ByteSize,
    receive_buffer: ByteSize,
) {
    crate::network::client::tune_socket(
        stream,
        send_buffer,
        receive_buffer,
        [
            |e| tracing::debug!(error = %e, "TCP_NODELAY set failed on accepted socket"),
            |e| tracing::debug!(error = %e, "SO_SNDBUF set failed on accepted socket"),
            |e| tracing::debug!(error = %e, "SO_RCVBUF set failed on accepted socket"),
        ],
    );
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;
    use krabka_units::{kibibytes, millis};

    use super::*;

    #[tokio::test]
    async fn accepted_socket_tuning_sets_nodelay_and_large_buffers() {
        let (server, client) = crate::network::test_support::tcp_pair().await;
        crate::network::test_support::check_socket_tuning(&server, |socket| {
            tune_accepted_socket(socket, kibibytes(64), kibibytes(128));
        });
        drop(client);
    }

    #[test]
    fn connection_creation_delay_honors_nondefault_cap() {
        assert!(connection_creation_delay(0.1, millis(17)) == millis(17));
    }

    #[tokio::test]
    async fn shutdown_connection_tasks_aborts_and_awaits_every_task() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut connections = JoinSet::new();
        for _ in 0..2 {
            let task_started = Arc::clone(&started);
            let task_dropped = Arc::clone(&dropped);
            connections.spawn(async move {
                let _drop_counter = DropCounter(task_dropped);
                task_started.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            });
        }
        crate::test_support::wait_tasks_started(
            || started.load(Ordering::SeqCst) == 2,
            "connection tasks start",
        )
        .await;

        shutdown_connection_tasks(&mut connections).await;

        assert!(connections.is_empty());
        assert!(dropped.load(Ordering::SeqCst) == 2);
    }
}
