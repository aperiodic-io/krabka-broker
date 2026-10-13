//! The share-group dead-letter queue (KIP-1191).
//!
//! From `share.version` 2, a record that a share consumer rejects, or that
//! uses up the group's delivery count limit, can be written to a dead-letter
//! topic before the share partition archives it. The group opts in with
//! `errors.deadletterqueue.topic.name`. This module is what Kafka's
//! `ShareGroupDLQManager` and `ShareGroupDLQStateManager` do: it validates
//! the topic, creates it when the cluster allows, and produces one record for
//! each dead-lettered offset.
//!
//! The acquisition machine decides which records go ([`super::state`]), and
//! the leader manager runs the two phases around the write
//! ([`super::manager`]). This module owns the write:
//!
//! - `validate` checks the group's topic against the cluster's rules;
//! - `record` builds the dead-letter records;
//! - `source` reads the source records for a group that copies them;
//! - `coalesce` puts the rounds of every write for one destination into as few
//!   produce requests as `max.message.bytes` allows;
//! - `writer` sends the topic creation and the produce request, retries, and
//!   counts the `DeadLetterQueue*` meters.
//!
//! Whether a write succeeds does not decide the record's fate. Kafka archives
//! the record whatever [`DlqSink::write`] answers, and logs a failure.

use {crate::share_partition::state::DlqCause, async_trait::async_trait, krabka_log::Offset};

mod coalesce;
mod record;
mod source;
mod validate;
mod writer;

pub use self::writer::DlqWriter;

/// One run of records to dead-letter: Kafka's `ShareGroupDLQRecordParameter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRequest {
    pub group: String,
    /// The id of the source topic.
    pub topic_id: uuid::Uuid,
    pub source_partition: i32,
    /// The first source offset of the run.
    pub first: Offset,
    /// The last source offset of the run.
    pub last: Offset,
    pub delivery_count: i16,
    pub cause: DlqCause,
}

/// Why a dead-letter write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DlqError {
    /// The group's dead-letter topic is not usable as configured: Kafka's
    /// `ConfigException`.
    #[error("{0}")]
    Config(String),
    /// The topic could not be created, or the records could not be produced.
    #[error("{0}")]
    Write(String),
}

/// Where the leader manager sends a run of records: Kafka's
/// `ShareGroupDLQManager`.
#[async_trait]
pub trait DlqSink: Send + Sync {
    /// Writes the dead-letter records of `request`. It answers when the
    /// records are durable in the topic, or when the write has failed for
    /// good.
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError>;
}

#[cfg(test)]
pub mod test_support {
    use {super::*, std::sync::Mutex};

    /// A sink that records what it was asked to write, and fails on demand.
    #[derive(Default)]
    pub struct RecordingDlq {
        requests: Mutex<Vec<DlqRequest>>,
        fail_with: Mutex<Option<DlqError>>,
    }

    impl RecordingDlq {
        pub fn failing(error: DlqError) -> Self {
            Self {
                requests: Mutex::default(),
                fail_with: Mutex::new(Some(error)),
            }
        }

        pub fn requests(&self) -> Vec<DlqRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[async_trait]
    impl DlqSink for RecordingDlq {
        async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
            self.requests.lock().expect("requests lock").push(request);
            match self.fail_with.lock().expect("failure lock").clone() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
    }
}
