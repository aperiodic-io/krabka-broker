//! The background task that re-fetches the JWKS and swaps it into the shared
//! [`JwksHandle`].
//!
//! The loop owns the refresh cadence, the on-demand refresh signal from a
//! validator, and the shared timestamps that validators read for cache-expiry
//! and rate-limit decisions.
//!
//! [`JwksHandle`]: krabka_security::JwksHandle

use {
    super::fetch_jwks,
    crate::time_util,
    krabka_security::JwksHandle,
    krabka_units::{Time, convert::TimeExt},
    krabka_verified::{
        JwksOnDemandDecision, jwks_on_demand_refresh_decision, jwks_publication_generations,
    },
    qubit_clock::Timer,
    std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicI64, AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

/// Names this cadence loop in the timer-failure logs that [`time_util::arm`]
/// and [`time_util::fired`] emit.
const TASK: &str = "JWKS refresher";

#[cfg(test)]
mod tests;

/// Periodically refreshes a [`JwksHandle`] from a JWKS endpoint.
///
/// The loop also serves on-demand refresh requests. A validator that met an
/// unknown-kid or bad-signature token starts one, and the loop receives it
/// through [`signal_rx`](Self::signal_rx).
///
/// [`min_on_demand_pause`](Self::min_on_demand_pause) rate-limits on-demand
/// refreshes, so a burst of verify failures cannot overload the `IdP`. A
/// successful refresh updates
/// [`last_successful_fetch_ms`](Self::last_successful_fetch_ms). The validator
/// reads that field to enforce hard cache expiry.
pub(crate) struct JwksRefresher {
    /// JWKS endpoint URL.
    pub endpoint: String,
    /// Shared key cell that the validator reads. This task stores into it.
    pub handle: JwksHandle,
    /// Re-fetch cadence (periodic).
    pub interval: Time,
    /// Timeout for each JWKS HTTP request.
    pub http_timeout: Time,
    /// Cancels the task on broker shutdown.
    pub shutdown: CancellationToken,
    /// Optional PEM path. When it is `Some`, this file builds the rustls
    /// `ClientConfig` that reqwest uses, and replaces the default
    /// webpki-roots trust store. When it is `None`, reqwest's webpki-roots
    /// default applies.
    ///
    /// The introspection client shares the bundle when the operator configures
    /// it. The operator's `tlsTrustedCertificates` arrives through
    /// `idp_tls_trust`, and JWKS, introspection, and userinfo HTTPS all use
    /// it.
    pub tls_trust: Option<PathBuf>,
    /// Receives a signal from a validator on a verify failure, which starts an
    /// on-demand refresh. `min_on_demand_pause` still applies. Capacity 1 with
    /// `try_send` on the producer side makes the signals coalesce.
    pub signal_rx: mpsc::Receiver<()>,
    /// Minimum pause between on-demand refreshes. The Strimzi default is 1
    /// second. This does not change the periodic refresh (`interval`).
    pub min_on_demand_pause: Time,
    /// Shared timestamp counter. The refresher updates it after each
    /// successful fetch, and validators read it for the cache-expiry check.
    /// It is an `Arc<AtomicI64>` shared with the paired `JwksHandle`.
    pub last_successful_fetch_ms: Arc<AtomicI64>,
    /// Even generations are stable. An odd generation fences validators while
    /// a successful refresh publishes its keys and timestamp.
    pub cache_generation: Arc<AtomicU64>,
    /// Holds the last on-demand-refresh epoch ms, for rate limiting. It is
    /// independent of the periodic refresh.
    pub last_on_demand_refresh_ms: Arc<AtomicI64>,
    /// When true, accept JWKS keys whatever their `use` field holds. Default
    /// false, which filters out `use=enc`. This value passes to
    /// [`Jwks::from_json`].
    ///
    /// [`Jwks::from_json`]: krabka_security::Jwks::from_json
    pub ignore_key_use: bool,
    /// Timer that drives the periodic refresh cadence. Production uses
    /// [`qubit_clock::StdTimer`], which is real time. Tests inject a timer
    /// taken from a [`qubit_clock::ManualMonotonicClock`], so the refresh
    /// interval fires on a controlled manual timeline instead of on
    /// wall-clock time.
    pub timer: Arc<dyn Timer>,
}

impl JwksRefresher {
    /// Runs until the caller cancels the task.
    ///
    /// The first periodic fetch happens immediately, because a zero-duration
    /// first deadline on the injected [`Timer`] reproduces the t=0 tick of
    /// `tokio::time::interval`. Keys are therefore available soon after
    /// startup. A failed fetch logs a warning and leaves the previous key set
    /// in place, so a short identity-provider outage never crashes the broker.
    ///
    /// On-demand refresh signals from validators race with the periodic tick
    /// in the same `select!`. The on-demand arm compares
    /// `last_on_demand_refresh_ms` against `min_on_demand_pause`, and drops
    /// the signal without a message when it is inside the window.
    ///
    /// The task also gives up when the timer refuses a deadline or fails one
    /// it had accepted. That joins the two start-up failures -- an unreadable
    /// TLS trust bundle and an unbuildable HTTP client -- as a reason this
    /// loop never starts, or stops early.
    pub(crate) async fn run(mut self) {
        let mut builder = reqwest::Client::builder().timeout(self.http_timeout.to_std());
        if let Some(path) = &self.tls_trust {
            match krabka_security::build_client_config_from_pem(path) {
                Ok(cfg) => {
                    // reqwest's use_preconfigured_tls takes the rustls
                    // ClientConfig by value; clone the inner config (cheap
                    // — it's a small struct of Arc fields).
                    builder = builder.use_preconfigured_tls((*cfg).clone());
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        path = %path.display(),
                        "failed to load OAUTHBEARER JWKS TLS trust bundle; refresher will not start",
                    );
                    return;
                }
            }
        }
        let client = match builder.build() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "failed to build JWKS HTTP client; OAUTHBEARER signed tokens will not validate");
                return;
            }
        };
        // Drive the periodic refresh cadence through the injected `Timer`
        // (production: real time; tests: a controlled manual timeline). A
        // zero-duration first deadline reproduces `tokio::time::interval`'s t=0
        // tick, so the first fetch fires immediately and keys are available
        // shortly after startup. Each subsequent deadline is re-armed to
        // `self.interval` only after the fetch completes, so a slow fetch never
        // triggers a catch-up burst — this preserves the never-burst intent of
        // the original `MissedTickBehavior::Skip` (re-arm-after-work aligns the
        // next tick to `Delay` rather than `Skip`, but neither ever bursts, and
        // a JWKS fetch is far shorter than the multi-minute refresh interval).
        // The timer is cloned into a local only to leave `self` free for the
        // `&mut self` work in the arms below; the tick future is `'static` and
        // borrows neither.
        //
        // Arming a deadline and completing one are both fallible. A refresher
        // that has lost its cadence has nothing left to drive it, and re-arming
        // a timer that keeps refusing would spin the task, so it gives up the
        // way the two start-up failures above do. `arm` and `fired` have
        // already logged which loop went away.
        let timer = Arc::clone(&self.timer);
        let Some(mut tick) = time_util::arm(&*timer, Duration::ZERO, TASK) else {
            return;
        };
        loop {
            tokio::select! {
                outcome = &mut tick => {
                    if !time_util::fired(outcome, TASK) {
                        return;
                    }
                    self.refresh_and_swap(&client).await;
                    let Some(next) = time_util::arm(&*timer, self.interval.to_std(), TASK) else {
                        return;
                    };
                    tick = next;
                }
                // On-demand refresh triggered by validator
                // signal. Subject to `min_on_demand_pause` rate-limit.
                // Signals coalesce via mpsc capacity 1 + `try_send`.
                Some(()) = self.signal_rx.recv() => {
                    let now_ms = time_util::now_ms();
                    let last = self.last_on_demand_refresh_ms.load(Ordering::Acquire);
                    let pause_ms = self.min_on_demand_pause.millis_i64();
                    if let JwksOnDemandDecision::Refresh { next_refresh_ms } =
                        jwks_on_demand_refresh_decision(now_ms, last, pause_ms)
                    {
                        self.last_on_demand_refresh_ms
                            .store(next_refresh_ms, Ordering::Release);
                        tracing::debug!(
                            endpoint = %self.endpoint,
                            now_ms,
                            last_refresh_ms = last,
                            "on-demand JWKS refresh triggered by validator signal",
                        );
                        self.refresh_and_swap(&client).await;
                    } else {
                        tracing::debug!(
                            endpoint = %self.endpoint,
                            now_ms,
                            last_refresh_ms = last,
                            pause_ms,
                            "on-demand JWKS refresh rate-limited; signal dropped",
                        );
                    }
                }
                () = self.shutdown.cancelled() => return,
            }
        }
    }

    /// This is a separate method so that both the periodic arm and the
    /// on-demand arm can call it. It updates `last_successful_fetch_ms` only
    /// on success. A failure leaves the timestamp unchanged, so the cache ages
    /// toward expiry and the validators then start to fail closed.
    async fn refresh_and_swap(&self, client: &reqwest::Client) {
        match fetch_jwks(client, &self.endpoint, self.ignore_key_use).await {
            Ok(jwks) => {
                let key_count = jwks.len();
                let generation = self.cache_generation.load(Ordering::Acquire);
                let Some((writing_generation, committed_generation)) =
                    jwks_publication_generations(generation)
                else {
                    tracing::error!(
                        "JWKS cache generation exhausted or writer in flight; fetched keys not installed"
                    );
                    return;
                };
                if self
                    .cache_generation
                    .compare_exchange(
                        generation,
                        writing_generation,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    tracing::error!(
                        "JWKS cache already has an in-flight writer; fetched keys not installed"
                    );
                    return;
                }
                self.handle.store(jwks);
                self.last_successful_fetch_ms
                    .store(time_util::now_ms(), Ordering::Release);
                self.cache_generation
                    .store(committed_generation, Ordering::Release);
                tracing::debug!(
                    endpoint = %self.endpoint,
                    keys = key_count,
                    generation = committed_generation,
                    "refreshed OAUTHBEARER JWKS",
                );
            }
            Err(e) => tracing::warn!(
                endpoint = %self.endpoint,
                error = %e,
                "failed to refresh OAUTHBEARER JWKS; keeping previous key set",
            ),
        }
    }
}
