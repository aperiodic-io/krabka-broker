//! KFC-7 broker-side schema validation.
//!
//! A topic can declare that its records carry schemas, and the broker then
//! refuses a produce whose records do not. The check runs on the produce path,
//! reads the schemas from a Confluent-compatible registry, and answers from a
//! bounded cache so that a registry round trip is not a per-record cost.
//!
//! The design is written up in `docs/KFCs/KFC-7-broker-side-schema-validation.md`.
//! Three pieces live here:
//!
//! - [`SchemaGate`], the topic's settings, resolved once per topic in the
//!   produce handler. A topic that has not turned validation on has no gate,
//!   and pays nothing.
//! - [`SchemaValidator`], the registry client and its cache. One per broker.
//! - [`RejectReason`], what came back when a record failed, which becomes both
//!   a metric label and the KIP-467 per-record message the producer sees.

// The registry client needs an HTTP stack that wasm32-wasip1 does not have,
// so that target builds the stand-in, which no configuration can construct.
#[cfg_attr(target_family = "wasm", path = "validator/wasm.rs")]
mod validator;

pub use validator::{RejectReason, Role, SchemaValidator, SchemaValidatorError};

/// What `schema.validation.mode` selects.
///
/// The two modes differ in how much of a record they read. `Id` decides from
/// the five-byte Confluent header alone. `Full` also decodes the body against
/// the schema the header names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationMode {
    /// Check the framing, that the schema id resolves, and that it is
    /// registered under this topic's subject. The body is never decoded.
    ///
    /// This is the default, and it is what Confluent Server does.
    #[default]
    Id,
    /// Everything [`ValidationMode::Id`] does, and then decode the body
    /// against the resolved schema.
    Full,
}

/// One topic's KFC-7 settings, resolved once per topic on the produce path.
///
/// The produce handler holds this as an `Option`, and `None` — the default —
/// means no validation code runs for that topic at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaGate {
    /// `schema.validation.key`.
    pub key: bool,
    /// `schema.validation.value`.
    pub value: bool,
    /// `schema.validation.mode`.
    pub mode: ValidationMode,
}

impl SchemaGate {
    /// Whether this gate asks for anything at all.
    ///
    /// `schema.validation.mode` alone does not turn validation on, so a topic
    /// that sets only the mode has no gate.
    #[must_use]
    pub fn is_active(self) -> bool { self.key || self.value }
}
