//! The follower's link to its WAL leader. It dials through the replicator's
//! reconnect loop, so the follower and the partition fetchers share one
//! doubling backoff and one cancellable sleep, and a shutdown token always
//! wins over a pending delay.

pub(super) use crate::replicator::connection::sleep_or_cancel;
use {
    super::Config,
    crate::replicator::connection::{LeaderDial, connect_leader_with_backoff},
    krabka_client_core::Connection,
};

pub(super) async fn connect_with_backoff(config: &Config) -> Result<Connection, String> {
    connect_leader_with_backoff(&LeaderDial {
        label: "diskless WAL follower",
        client: &config.connection.inter_broker_client,
        host: &config.leader_host,
        port: config.leader_port,
        protocol: config.connection.inter_broker_listener_protocol,
        server_name: &config.connection.inter_broker_server_name,
        client_id: &config.client_id,
        replication: &config.connection.replication,
        shutdown: &config.shutdown,
    })
    .await
}
