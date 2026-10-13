//! The registry-backed record checker and its cache.
//!
//! The shape is the OPA authorizer's, for the same reasons: an LRU bounds what
//! a careless or hostile producer can make the broker hold, a TTL on each
//! entry is what makes a registry change observable, and failures are cached
//! as well as successes so that a produce storm against one bad id costs one
//! registry call rather than one per record.
//!
//! Not every failure is cached for the same length of time. "This id is not
//! registered" is an answer and keeps the full TTL; "the registry did not
//! answer" keeps [`UNAVAILABLE_TTL_MS`], because holding that one for minutes
//! would keep rejecting valid records after the registry recovered.
//!
//! It differs from the authorizer in one way that matters. `Authorizer` is a
//! synchronous trait, so `OpaAuthorizer` bridges to async with
//! `block_in_place`. This runs from `process_partition`, which is already
//! async, so a cache miss is a plain `.await` on one HTTP round trip and no
//! runtime worker is parked.

use {
    base64::{Engine as _, engine::general_purpose::STANDARD},
    krabka_schema_serde::registry::RegistryClient,
    krabka_units::{Time, convert::TimeExt as _},
    lru::LruCache,
    std::{
        num::NonZeroUsize,
        sync::{Arc, Mutex},
    },
};

mod cache;
mod check;
mod reject;
#[cfg(test)]
mod test_support;

use self::cache::Cached;
pub use self::reject::RejectReason;
/// Which field of a record a check reads, and so which subject it names.
pub use krabka_schema_serde::subject::Role;

/// How long a "the registry could not answer" result stays cached.
///
/// Much shorter than `expire_after`, and deliberately so. A 404 is the
/// registry *answering*, so it earns the full TTL: it turns a produce storm
/// against one bad id into a single registry call. A timeout or a 5xx is the
/// registry *failing* to answer, and remembering that for minutes would keep
/// rejecting valid records long after the registry came back — the opposite of
/// what `fail_open = false` is for, which is to reject only while no answer is
/// available. Caching it briefly rather than not at all is what keeps an
/// outage from becoming one registry call per record.
const UNAVAILABLE_TTL_MS: i64 = 2_000;

/// A [`SchemaValidator`] that could not be built from its configuration.
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidatorError {
    /// `maximum_cache_size` was zero, which would make every record a miss.
    #[error("schema_registry.maximum_cache_size must be greater than zero")]
    ZeroCache,
    /// The HTTP client could not be built from the configured timeout.
    #[error("schema registry HTTP client: {0}")]
    Http(String),
}

/// Registry-backed record validation with a bounded, expiring cache.
///
/// One instance per broker, held on [`crate::Broker`] as an `Option`. `None`
/// is "no `[schema_registry]` section", and then no topic can turn validation
/// on.
#[derive(derive_more::Debug)]
pub struct SchemaValidator {
    #[debug(skip)]
    client: RegistryClient,
    #[debug(skip)]
    cache: Mutex<LruCache<u32, Cached>>,
    expire_after: Time,
    #[debug(skip)]
    operation_timeout: std::time::Duration,
    /// **Security-sensitive.** `true` admits a record the broker could not
    /// validate because the registry was unreachable, which is fail-open: for
    /// the length of a registry outage, a validated topic accepts whatever it
    /// is sent. The default is `false`, which fails the produce instead. This
    /// is the same knob, with the same default and the same argument, as
    /// `allow_on_error` on [`crate::authorizer::opa::OpaAuthorizer`].
    ///
    /// An unknown id or a body that does not match its schema is a rejection
    /// under either setting. This governs only the case where the broker could
    /// not get an answer at all.
    fail_open: bool,
    /// Clock backing the cache TTL. Production uses
    /// [`qubit_clock::StdWallClock`]; tests inject a
    /// [`qubit_clock::ManualWallClock`] so an expiry is an assertion rather
    /// than a sleep.
    #[debug(skip)]
    clock: Arc<dyn qubit_clock::WallClock>,
}

impl SchemaValidator {
    /// Build a validator against the registry at `url`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidatorError::ZeroCache`] when `maximum_cache_size`
    /// is zero, and [`SchemaValidatorError::Http`] when the HTTP client cannot
    /// be built.
    pub fn new(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
    ) -> Result<Self, SchemaValidatorError> {
        Self::with_clock(
            url,
            fail_open,
            maximum_cache_size,
            expire_after,
            http_timeout,
            None,
            Arc::new(qubit_clock::StdWallClock::new()),
        )
    }

    /// Build a validator that authenticates to the registry with HTTP Basic.
    ///
    /// # Errors
    ///
    /// As [`SchemaValidator::new`].
    pub fn new_with_basic_auth(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        username: String,
        password: String,
    ) -> Result<Self, SchemaValidatorError> {
        Self::with_clock(
            url,
            fail_open,
            maximum_cache_size,
            expire_after,
            http_timeout,
            Some((username, password)),
            Arc::new(qubit_clock::StdWallClock::new()),
        )
    }

    /// [`SchemaValidator::new`] with the [`qubit_clock::WallClock`] injected,
    /// for tests that drive the cache TTL on a controlled timeline: a
    /// [`qubit_clock::ManualWallClock`], handed out by a
    /// [`qubit_clock::ManualMonotonicClock`], expires an entry without a
    /// sleep.
    ///
    /// # Errors
    ///
    /// As [`SchemaValidator::new`].
    pub fn with_clock(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        basic_auth: Option<(String, String)>,
        clock: Arc<dyn qubit_clock::WallClock>,
    ) -> Result<Self, SchemaValidatorError> {
        let capacity =
            NonZeroUsize::new(maximum_cache_size).ok_or(SchemaValidatorError::ZeroCache)?;
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some((username, password)) = basic_auth {
            let encoded = STANDARD.encode(format!("{username}:{password}"));
            let mut authorization = format!("Basic {encoded}")
                .parse::<reqwest::header::HeaderValue>()
                .map_err(|error| SchemaValidatorError::Http(format!("basic auth: {error}")))?;
            authorization.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, authorization);
        }
        let http = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .default_headers(headers)
            .build()
            .map_err(|e| SchemaValidatorError::Http(e.to_string()))?;
        Ok(Self {
            client: RegistryClient::with_http_client(url, http),
            cache: Mutex::new(LruCache::new(capacity)),
            expire_after,
            operation_timeout: http_timeout.to_std(),
            fail_open,
            clock,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{minutes, secs},
    };

    #[test]
    fn a_zero_sized_cache_is_a_configuration_error() {
        let got = SchemaValidator::new(
            "http://localhost:8081".into(),
            false,
            0,
            minutes(1),
            secs(5),
        );
        assert!(let Err(SchemaValidatorError::ZeroCache) = got);
    }
}
