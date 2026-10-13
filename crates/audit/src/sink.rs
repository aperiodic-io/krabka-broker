//! The audit sink abstraction and an in-memory test sink.

use {
    crate::{
        chain::to_hex,
        event::{AuditEvent, AuditEventClass},
        ocsf::{ProductInfo, to_ocsf},
    },
    async_trait::async_trait,
    std::sync::Mutex,
};

/// Record-header key for the per-broker chain sequence number in decimal ASCII.
pub const HEADER_SEQ: &str = "seq";
/// Record-header key for the previous chain head, as lowercase hex of 32 bytes.
pub const HEADER_PREV_HASH: &str = "prev_hash";

/// A serialized, sink-ready audit record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub class: AuditEventClass,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl AuditRecord {
    /// Build the chained meta-record that declares fail-open audit loss.
    ///
    /// `generation` is the persisted loss-state generation the marker settles;
    /// the verifier requires it to advance strictly across a partition.
    #[must_use]
    pub fn records_lost(count: u64, generation: u64) -> Self {
        Self {
            class: AuditEventClass::RecordsLost,
            value: format!(r#"{{"records_lost":{count},"loss_generation":{generation}}}"#)
                .into_bytes(),
            headers: vec![(
                "event_class".to_string(),
                AuditEventClass::RecordsLost.as_header().as_bytes().to_vec(),
            )],
        }
    }

    /// Stamp the hash-chain headers onto this record.
    ///
    /// The `seq` header is decimal and the `prev_hash` header is lowercase
    /// hex. The writer calls this method, and the writer owns the chain.
    pub fn push_chain_headers(&mut self, seq: u64, prev_head: &[u8; 32]) {
        self.headers
            .push((HEADER_SEQ.to_string(), seq.to_string().into_bytes()));
        self.headers
            .push((HEADER_PREV_HASH.to_string(), to_hex(prev_head).into_bytes()));
    }

    /// Serialize an event to OCSF JSON and attach the SIEM-filterable headers.
    #[must_use]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?event.class(), bytes = tracing::field::Empty)
    )]
    pub fn from_event(event: &AuditEvent, product: &ProductInfo) -> Self {
        let class = event.class();
        let value = serde_json::to_vec(&to_ocsf(event, product)).unwrap_or_else(|_| b"{}".to_vec());
        tracing::Span::current().record("bytes", value.len());
        let mut headers = vec![(
            "event_class".to_string(),
            class.as_header().as_bytes().to_vec(),
        )];
        if let Some(PrincipalStatus { principal, status }) = principal_and_status(event) {
            headers.push(("principal".to_string(), principal.into_bytes()));
            headers.push(("status".to_string(), status.as_bytes().to_vec()));
        }
        AuditRecord {
            class,
            value,
            headers,
        }
    }
}

/// The principal name and outcome status an audit record carries as headers.
struct PrincipalStatus {
    principal: String,
    status: &'static str,
}

fn principal_and_status(event: &AuditEvent) -> Option<PrincipalStatus> {
    match event {
        AuditEvent::Authentication {
            principal, outcome, ..
        }
        | AuditEvent::AdminOperation {
            principal, outcome, ..
        }
        | AuditEvent::PrivilegedAction {
            principal, outcome, ..
        } => Some(PrincipalStatus {
            principal: principal.name.clone(),
            status: outcome.as_str(),
        }),
        AuditEvent::AuthorizationDenied { principal, .. } => Some(PrincipalStatus {
            principal: principal.name.clone(),
            status: "denied",
        }),
        AuditEvent::Lifecycle { .. } => None,
    }
}

/// Errors that a sink can report.
#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("audit sink: {0}")]
    Sink(String),
    #[error("audit key: {0}")]
    Key(String),
    #[error("audit spool I/O: {0}")]
    Io(String),
    #[error("audit unavailable: {0}")]
    Unavailable(String),
    /// The sink may have appended the record but could not prove durability.
    #[error("audit durability indeterminate: {0}")]
    Indeterminate(String),
    #[error("audit spool requires explicit recovery: {0}")]
    Poisoned(String),
    /// A file in the audit spool directory has no version header, or one this
    /// build does not read.
    #[error("audit spool file {file}: {}", describe_spool_version(*found))]
    UnsupportedSpoolFormat {
        /// The file name, such as `audit.spool`.
        file: String,
        /// The version the header holds, or `None` when the file has no
        /// header at all.
        found: Option<i16>,
    },
}

fn describe_spool_version(found: Option<i16>) -> String {
    match found {
        None => "has no version header; it predates krabka 1.0, so the audit spool \
                 directory must be emptied (the node reformatted) before this broker starts"
            .to_owned(),
        Some(version) => format!("has version {version}, which this build does not read"),
    }
}

/// Destination for serialized audit records.
#[async_trait]
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// Write one record. When `durable` is true, return only after the record
    /// has reached stable storage.
    async fn write(&self, record: AuditRecord, durable: bool) -> Result<(), AuditError>;
}

/// In-memory sink for tests.
#[derive(Debug, Default)]
pub struct MemorySink {
    records: Mutex<Vec<AuditRecord>>,
}

impl MemorySink {
    #[must_use]
    /// # Panics
    /// Panics if the mutex that holds the records is poisoned.
    pub fn records(&self) -> Vec<AuditRecord> {
        self.records
            .lock()
            .expect("audit memory sink poisoned")
            .clone()
    }
}

#[async_trait]
impl AuditSink for MemorySink {
    async fn write(&self, record: AuditRecord, _durable: bool) -> Result<(), AuditError> {
        self.records
            .lock()
            .expect("audit memory sink poisoned")
            .push(record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::test_support::authentication, assert2::check};

    #[test]
    fn io_error_displays() {
        let e = AuditError::Io("disk full".to_string());
        check!(format!("{e}").contains("disk full"));
    }

    #[test]
    fn records_lost_is_a_distinct_meta_record() {
        check!(
            AuditRecord::records_lost(3, 7)
                == AuditRecord {
                    class: AuditEventClass::RecordsLost,
                    value: br#"{"records_lost":3,"loss_generation":7}"#.to_vec(),
                    headers: vec![("event_class".to_string(), b"records_lost".to_vec())],
                }
        );
    }

    #[test]
    fn unavailable_error_displays() {
        check!(
            AuditError::Unavailable("spool full".into()).to_string()
                == "audit unavailable: spool full"
        );
    }

    fn hdr(r: &AuditRecord, k: &str) -> Option<String> {
        r.headers
            .iter()
            .find(|(hk, _)| hk == k)
            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
    }

    #[test]
    fn from_event_sets_principal_and_status_headers() {
        use crate::{
            event::{AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, LifecycleKind},
            ocsf::ProductInfo,
        };
        let product = ProductInfo {
            vendor_name: "v".into(),
            name: "n".into(),
            version: "0".into(),
        };
        let authn = authentication(1, None, 0);
        let r = AuditRecord::from_event(&authn, &product);
        check!(
            (hdr(&r, "principal"), hdr(&r, "status"))
                == (Some("alice".into()), Some("failure".into()))
        );
        let admin = AuditEvent::AdminOperation {
            outcome: AuditOutcome::Success,
            principal: AuditPrincipal {
                name: "bob".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.2".into(),
                port: 2,
            },
            operation: "CreateTopics".into(),
            resources: vec![],
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&admin, &product);
        check!(
            (hdr(&r, "principal"), hdr(&r, "status"))
                == (Some("bob".into()), Some("success".into()))
        );
        let deny = AuditEvent::AuthorizationDenied {
            principal: AuditPrincipal {
                name: "carol".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.3".into(),
                port: 3,
            },
            resource_type: "Topic".into(),
            resource_name: "t".into(),
            operation: "Write".into(),
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&deny, &product);
        check!(
            (hdr(&r, "principal"), hdr(&r, "status"))
                == (Some("carol".into()), Some("denied".into()))
        );
        let life = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&life, &product);
        check!((hdr(&r, "principal"), hdr(&r, "status")) == (None, None));
    }
}
