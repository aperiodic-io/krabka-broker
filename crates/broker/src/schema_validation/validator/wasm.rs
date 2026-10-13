//! KFC-7 schema validation on `wasm32-wasip1`, where it is unavailable.
//!
//! The registry client needs an HTTP stack that this target does not have. The
//! types here keep the native signatures, so the produce path and the file
//! configuration build unchanged, but no [`SchemaValidator`] can be
//! constructed. A topic that turns validation on therefore fails closed, as it
//! does on a native broker with no `[schema_registry]` section.

use {
    crate::{metrics::BrokerMetrics, schema_validation::ValidationMode},
    krabka_units::Time,
};

mod reject;

pub use self::reject::RejectReason;

/// Which field of a record a check reads, and so which subject it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The record key.
    Key,
    /// The record value.
    Value,
}

/// A [`SchemaValidator`] that could not be built from its configuration.
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidatorError {
    /// This platform has no schema registry client.
    #[error("schema validation is unavailable on this platform: {reason}")]
    Unavailable {
        /// Why the platform has no client.
        reason: &'static str,
    },
}

/// Registry-backed record validation, which this platform cannot construct.
pub struct SchemaValidator {
    never: std::convert::Infallible,
}

impl std::fmt::Debug for SchemaValidator {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { match self.never {} }
}

impl SchemaValidator {
    /// Build a validator against the registry at `url`.
    ///
    /// # Errors
    ///
    /// Always returns [`SchemaValidatorError::Unavailable`].
    pub fn new(
        _url: String,
        _fail_open: bool,
        _maximum_cache_size: usize,
        _expire_after: Time,
        _http_timeout: Time,
    ) -> Result<Self, SchemaValidatorError> {
        Err(unavailable())
    }

    /// Build a validator that authenticates to the registry with HTTP Basic.
    ///
    /// # Errors
    ///
    /// Always returns [`SchemaValidatorError::Unavailable`].
    pub fn new_with_basic_auth(
        _url: String,
        _fail_open: bool,
        _maximum_cache_size: usize,
        _expire_after: Time,
        _http_timeout: Time,
        _username: String,
        _password: String,
    ) -> Result<Self, SchemaValidatorError> {
        Err(unavailable())
    }

    /// Check one record field. No value of this type exists, so no call
    /// reaches this method.
    ///
    /// # Errors
    ///
    /// Never returns.
    pub fn check(
        &self,
        _topic: &str,
        _role: Role,
        _mode: ValidationMode,
        _field: &[u8],
        _metrics: &BrokerMetrics,
    ) -> std::future::Ready<Result<(), RejectReason>> {
        match self.never {}
    }
}

fn unavailable() -> SchemaValidatorError {
    SchemaValidatorError::Unavailable {
        reason: "the schema registry client needs an HTTP stack that wasm32-wasip1 does not have",
    }
}
