//! Outbound connections to a leader.
//!
//! The module holds the shared `ConnectionOptions` for every inter-broker call
//! a fetcher makes, the exponential reconnect schedule, and the retry loop that
//! dials the leader until it succeeds or the fetcher is cancelled.
//!
//! One connection serves every partition the fetcher follows on that leader,
//! so a leader restart costs one redial per fetcher rather than one per
//! partition -- the reconnect storm an operator used to see during a roll.

use {
    super::{
        FetcherConfig,
        leader_link::{LeaderLink, LinkSettings},
    },
    crate::{config::ReplicationRuntimeConfig, network::client::InterBrokerClient},
    krabka_client_core::{ClientDuplex, Connection, ConnectionOptions},
    krabka_security::ListenerProtocol,
    krabka_units::{
        Time,
        convert::{ByteSizeExt as _, TimeExt},
        fmt::Human as _,
    },
    tokio_util::sync::CancellationToken,
    tracing::warn,
};

pub(super) fn connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_string(),
        ..ConnectionOptions::default()
    }
}

/// One leader to dial, and the policy that governs redials.
///
/// Both the partition fetcher and the diskless WAL follower build one, so the
/// two share a reconnect schedule and a shutdown race.
pub(crate) struct LeaderDial<'a> {
    /// Names the caller in the retry warning.
    pub(crate) label: &'static str,
    pub(crate) client: &'a InterBrokerClient,
    pub(crate) host: &'a str,
    pub(crate) port: u16,
    pub(crate) protocol: ListenerProtocol,
    pub(crate) server_name: &'a str,
    pub(crate) client_id: &'a str,
    pub(crate) replication: &'a ReplicationRuntimeConfig,
    pub(crate) shutdown: &'a CancellationToken,
}

impl<'a> LeaderDial<'a> {
    fn fetcher(fetcher: &'a FetcherConfig) -> Self {
        Self {
            label: "replicator",
            client: &fetcher.connection.inter_broker_client,
            host: &fetcher.leader_host,
            port: fetcher.leader_port,
            protocol: fetcher.connection.inter_broker_listener_protocol,
            server_name: &fetcher.connection.inter_broker_server_name,
            client_id: &fetcher.client_id,
            replication: &fetcher.connection.replication,
            shutdown: &fetcher.shutdown,
        }
    }
}

/// A follower fetcher's link to its leader.
pub(super) type FollowerLink = LeaderLink<Box<dyn ClientDuplex>>;

/// Opens a [`FollowerLink`] against a fetcher's leader, with the retries of
/// [`connect_leader_with_backoff`].
pub(super) async fn connect_with_backoff(fetcher: &FetcherConfig) -> Result<FollowerLink, String> {
    let dial = LeaderDial::fetcher(fetcher);
    with_backoff(&dial, || open_link(&dial)).await
}

/// Dials `dial`'s leader, runs TLS and SASL as its listener asks, and
/// negotiates API versions on a [`LeaderLink`].
async fn open_link(dial: &LeaderDial<'_>) -> Result<FollowerLink, String> {
    let mut options = connection_options(dial.client_id);
    dial.client.apply_resource_policy(&mut options);
    let stream = dial
        .client
        .connect(
            dial.host,
            dial.port,
            dial.protocol,
            dial.server_name,
            &options,
        )
        .await
        .map_err(|error| error.to_string())?;
    let settings = LinkSettings {
        client_id: dial.client_id.to_owned(),
        frame_max: options.frame_max.size().bytes_usize(),
        request_timeout: options.request_timeout,
    };
    LeaderLink::open(stream, settings)
        .await
        .map_err(|error| error.to_string())
}

/// Opens a [`Connection`] against `dial`'s leader.
///
/// The function retries with exponential backoff, with a cap from the
/// configured reconnect policy. It returns `Err` only if a shutdown starts
/// during a dial or the wait.
///
/// The connection routes through the shared [`InterBrokerClient`], which runs
/// TLS and SASL when the inter-broker listener needs them. It falls back to
/// plain TCP for `ListenerProtocol::Plaintext`.
pub(crate) async fn connect_leader_with_backoff(
    dial: &LeaderDial<'_>,
) -> Result<Connection, String> {
    with_backoff(dial, || {
        dial.client.connect_as_connection(
            dial.host,
            dial.port,
            dial.protocol,
            dial.server_name,
            connection_options(dial.client_id),
        )
    })
    .await
}

/// Runs `attempt` until it succeeds, backing off between failures as the
/// reconnect policy says. Returns `Err` only if a shutdown starts during an
/// attempt or the wait.
async fn with_backoff<T, E: std::fmt::Display, F: Future<Output = Result<T, E>>>(
    dial: &LeaderDial<'_>,
    mut attempt: impl FnMut() -> F,
) -> Result<T, String> {
    let mut delay = reconnect_delay(dial.replication, None);
    loop {
        let result = tokio::select! {
            () = dial.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt() => r,
        };
        match result {
            Ok(connected) => return Ok(connected),
            Err(e) => {
                warn!(
                    host = %dial.host, port = dial.port, error = %e,
                    "{}: connect failed; retrying after {}", dial.label, delay.human()
                );
                sleep_or_cancel(dial.shutdown, delay).await?;
                delay = reconnect_delay(dial.replication, Some(delay));
            }
        }
    }
}

pub(crate) async fn sleep_or_cancel(
    shutdown: &CancellationToken,
    delay: Time,
) -> Result<(), String> {
    tokio::select! {
        () = shutdown.cancelled() => Err("cancelled".into()),
        () = tokio::time::sleep(delay.to_std()) => Ok(()),
    }
}

fn reconnect_delay(replication: &ReplicationRuntimeConfig, previous: Option<Time>) -> Time {
    previous.map_or(replication.reconnect_initial_delay, |delay| {
        // `Time` has no `Ord` — its `f64` storage is only `PartialOrd` — so the
        // cap is applied by comparison rather than `Ord::min`.
        let doubled = delay * 2.0;
        let cap = replication.reconnect_delay_cap;
        if doubled > cap { cap } else { doubled }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::replicator::test_support::{LEADER_ID, image_with_leader, test_config},
        assert2::assert,
        krabka_units::{millis, secs},
    };

    #[test]
    fn configured_reconnect_delay_doubles_until_cap() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.connection.replication.reconnect_initial_delay = millis(37);
        cfg.connection.replication.reconnect_delay_cap = millis(100);

        let first = reconnect_delay(&cfg.connection.replication, None);
        let second = reconnect_delay(&cfg.connection.replication, Some(first));
        let capped = reconnect_delay(&cfg.connection.replication, Some(second));

        assert!((first, second, capped) == (millis(37), millis(74), millis(100)));
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_completes_on_delay_or_cancellation() {
        let shutdown = CancellationToken::new();
        assert!(sleep_or_cancel(&shutdown, millis(10)).await.is_ok());

        shutdown.cancel();
        assert!(sleep_or_cancel(&shutdown, secs(1)).await.is_err());
    }
}
