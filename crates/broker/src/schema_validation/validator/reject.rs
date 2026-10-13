//! Why a record was rejected, in the two forms the rest of the broker needs.
//!
//! [`RejectReason`] is the one value the validator returns on a failure, and
//! it carries both a low-cardinality metric label and the message the producer
//! reads. It lives in its own module because those two renderings are the
//! whole of its job, and neither the cache nor the record check has to know
//! how they are worded.

/// Why a record failed validation.
///
/// Each variant is both a metric label, through [`RejectReason::label`], and
/// the KIP-467 `batch_index_error_message` the producer reads, through
/// [`std::fmt::Display`]. The two are deliberately different: the label has to
/// be low-cardinality for Prometheus, and the message has to name the id and
/// the subject so that a person can act on it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", as_str = label)]
pub enum RejectReason {
    /// The field carries no Confluent frame.
    #[error("not a Confluent-framed payload: {0}")]
    Unframed(String),
    /// The frame is well formed but the registry does not know the id.
    #[error("schema id {0} is not registered")]
    UnknownId(u32),
    /// The id resolves, but it is not registered under this topic's subject.
    #[error("schema id {id} is not registered under subject {subject}")]
    WrongSubject { id: u32, subject: String },
    /// The id resolves and belongs here, but the body is not an instance of
    /// the schema. Only
    /// [`ValidationMode::Full`][crate::schema_validation::ValidationMode::Full]
    /// can produce this.
    #[error("body does not match schema id {id}: {detail}")]
    BodyMismatch { id: u32, detail: String },
    /// The registry could not provide an authoritative answer yet.
    #[error("schema registry unavailable: {0}")]
    RegistryUnavailable(String),
    /// The registry provided a permanent or malformed response.
    #[enum_str(name = "registry_unavailable")]
    #[error("schema registry unavailable: {detail}")]
    RegistryRejected {
        kind: krabka_verified::SchemaFailureKind,
        detail: String,
    },
}

impl RejectReason {
    /// Every value [`RejectReason::label`] can return.
    ///
    /// The `schema_validation_rejections` family keys on the topic paired with
    /// one of these, so evicting a deleted topic's series has to name each of
    /// them; there is no way to enumerate a `Family`'s live label sets.
    pub const LABELS: [&'static str; 5] = [
        "unframed",
        "unknown_id",
        "wrong_subject",
        "body_mismatch",
        "registry_unavailable",
    ];
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, std::collections::BTreeSet};

    #[test]
    fn every_reject_reason_has_a_label_and_a_message() {
        let cases = [
            (RejectReason::Unframed("bad magic".into()), "unframed"),
            (RejectReason::UnknownId(7), "unknown_id"),
            (
                RejectReason::WrongSubject {
                    id: 7,
                    subject: "orders-value".into(),
                },
                "wrong_subject",
            ),
            (
                RejectReason::BodyMismatch {
                    id: 7,
                    detail: "nope".into(),
                },
                "body_mismatch",
            ),
            (
                RejectReason::RegistryUnavailable("timeout".into()),
                "registry_unavailable",
            ),
            (
                RejectReason::RegistryRejected {
                    kind: krabka_verified::SchemaFailureKind::Malformed,
                    detail: "not json".into(),
                },
                "registry_unavailable",
            ),
        ];
        for (reason, label) in &cases {
            check!(reason.label() == *label);
            check!(!reason.to_string().is_empty(), "{label}");
            // A variant added to the enum without a case above stops this
            // arm compiling, which is what keeps `cases` -- and through it
            // `LABELS` -- honest. Without it a new variant returning a new
            // label would leave `LABELS` short and this test green.
            match reason {
                RejectReason::Unframed(_)
                | RejectReason::UnknownId(_)
                | RejectReason::WrongSubject { .. }
                | RejectReason::BodyMismatch { .. }
                | RejectReason::RegistryUnavailable(_)
                | RejectReason::RegistryRejected { .. } => {}
            }
        }
        // Metric eviction for a deleted topic names exactly `LABELS`, so a
        // label a variant returns but `LABELS` omits leaks a series, and one
        // `LABELS` carries but no variant returns evicts nothing.
        let returned: BTreeSet<&str> = cases.iter().map(|(reason, _)| reason.label()).collect();
        check!(returned == RejectReason::LABELS.into_iter().collect::<BTreeSet<_>>());
    }
}
