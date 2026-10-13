//! The clean-shutdown proof a broker leaves behind, and reads back on its next
//! start.
//!
//! A broker that stops gracefully writes its current broker epoch to
//! `{log_dir}/.kafka_cleanshutdown`. The next start reads that epoch and offers it
//! back at registration; the controller accepts the restart as clean only when
//! the epoch it still holds for that node is the one the file names.
//! A broker that died -- SIGKILL, a lost machine, a panic -- wrote no file, so
//! it can prove nothing and its restart is unclean.
//!
//! This is Kafka's `previousBrokerEpoch`.
//! `CleanShutdownFileHandler` in `kafka-storage-4.3.1.jar` writes
//! `.kafka_cleanshutdown` into each log dir on an orderly stop,
//! `LogManager.readBrokerEpochFromCleanShutdownFiles` reads it at startup, and
//! `ClusterControlManager.registerBroker` computes
//! `isCleanShutdown = storedBrokerEpoch == request.previousBrokerEpoch()`.
//! krabka writes the same file as Kafka: the name `.kafka_cleanshutdown` and
//! the bytes `{"version":0,"brokerEpoch":<epoch>}`, in the field order Jackson
//! gives `CleanShutdownFileHandler.Content`.
//!
//! ## Absent means unclean
//!
//! Every read failure -- no file, an unreadable file, a file that is not the
//! JSON above, a version this build does not know -- answers [`UNPROVEN`],
//! which no real broker epoch can equal, so it never compares clean. This is
//! the one place the on-disk contract does not refuse an unknown version:
//! Kafka's `CleanShutdownFileHandler.read` turns every read failure into
//! `OptionalLong.empty()`, which `LogManager` reports as `-1`, and an unclean
//! restart is the safe answer. It costs this node its ELR membership for one
//! restart (KIP-966) and nothing else, whereas a hard error would keep a node
//! from starting over a single-use file whose absence is already a valid
//! state. krabka is stricter than Kafka in one direction only: Kafka reads the
//! epoch of any version it can parse, and krabka reads only
//! [`FORMAT_VERSION`].
//!
//! Kafka takes the same default from two directions at once:
//! `previousBrokerEpoch` defaults to `-1` on the wire, and the controller
//! passes `cleanShutdownDetectionEnabled = false` for any `BrokerRegistration`
//! older than v3, which forces the comparison to `false` outright. A
//! controller that cannot prove clean assumes unclean.
//!
//! ## The proof is spent on use
//!
//! [`take`] deletes the file as it reads it. The proof covers exactly one
//! restart: a broker that starts, registers, and then dies has consumed it, so
//! the start after that one finds nothing and is unclean, which is the truth.
//! Kafka deletes the same file while `LogManager` loads the log dir.

use {
    krabka_metadata::{MetadataImage, NodeId},
    serde::{Deserialize, Serialize},
    std::{io::Write, path::Path},
};

/// The file name. Kafka's `CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME`
/// is `.kafka_cleanshutdown`; only krabka reads this file, so the name differs.
const FILE_NAME: &str = ".kafka_cleanshutdown";

/// The `version` of the clean-shutdown file this build writes and reads.
///
/// Part of the 1.x on-disk contract. It is Kafka's
/// `CleanShutdownFileHandler.CURRENT_VERSION`.
pub(crate) const FORMAT_VERSION: i32 = 0;

/// The epoch a broker offers when it holds no clean-shutdown proof.
///
/// Broker epochs are commit offsets, so they are never negative and this
/// sentinel can never be mistaken for one. It is also the value Kafka's
/// `BrokerRegistrationRequest.previousBrokerEpoch` defaults to.
pub(crate) const UNPROVEN: i64 = -1;

/// The JSON document, in the field order Kafka's Jackson writes.
///
/// Unknown fields are ignored, as `@JsonIgnoreProperties(ignoreUnknown =
/// true)` ignores them in Kafka. Both fields are required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Content {
    version: i32,
    #[serde(rename = "brokerEpoch")]
    broker_epoch: i64,
}

/// Why a clean-shutdown file is not a proof.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProofError {
    /// The file is not the JSON document Kafka writes.
    #[error("{FILE_NAME} is not a clean-shutdown document: {0}")]
    Malformed(String),
    /// The file names a version this build does not read.
    #[error("{FILE_NAME} has version {found}; this build reads only version {FORMAT_VERSION}")]
    UnknownVersion {
        /// The version the file holds.
        found: i32,
    },
}

/// The bytes of a clean-shutdown file that proves `broker_epoch`.
fn encode(broker_epoch: i64) -> String {
    serde_json::to_string(&Content {
        version: FORMAT_VERSION,
        broker_epoch,
    })
    .expect("a two-integer struct always serializes")
}

/// The broker epoch a clean-shutdown file proves.
///
/// # Errors
///
/// [`ProofError::Malformed`] for text that is not the document, and
/// [`ProofError::UnknownVersion`] for a version other than
/// [`FORMAT_VERSION`].
fn decode(text: &str) -> Result<i64, ProofError> {
    let content: Content =
        serde_json::from_str(text).map_err(|error| ProofError::Malformed(error.to_string()))?;
    if content.version != FORMAT_VERSION {
        return Err(ProofError::UnknownVersion {
            found: content.version,
        });
    }
    Ok(content.broker_epoch)
}

/// Read the clean-shutdown proof from `{log_dir}/.kafka_cleanshutdown` and delete
/// it, or return [`UNPROVEN`] when there is none to read.
pub(crate) fn take(log_dir: &Path) -> i64 {
    let path = log_dir.join(FILE_NAME);
    let epoch = match std::fs::read_to_string(&path) {
        Ok(text) => decode(&text).unwrap_or_else(|error| {
            tracing::warn!(path = %path.display(), %error, "clean-shutdown proof not readable; the restart is unclean");
            UNPROVEN
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => UNPROVEN,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "failed to read the clean-shutdown proof; the restart is unclean");
            UNPROVEN
        }
    };
    // Delete whatever was there, parsable or not: a proof this start could not
    // read is not one a later start should get to try again.
    if let Err(error) = std::fs::remove_file(&path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "failed to remove the clean-shutdown proof");
    }
    epoch
}

/// Write `broker_epoch` to `{log_dir}/.kafka_cleanshutdown` as this broker's
/// proof that it stopped on purpose, and sync it, as Kafka does.
///
/// A failure to write is logged and otherwise ignored: the broker is stopping
/// either way, and the consequence is that the next start is treated as
/// unclean, which is the safe direction.
pub(crate) fn write(log_dir: &Path, broker_epoch: i64) {
    let path = log_dir.join(FILE_NAME);
    let written = std::fs::File::create(&path).and_then(|mut file| {
        file.write_all(encode(broker_epoch).as_bytes())?;
        file.sync_all()
    });
    if let Err(error) = written {
        tracing::warn!(path = %path.display(), %error, "failed to write the clean-shutdown proof");
    }
}

/// Whether a broker rejoining as `node_id` can prove it stopped gracefully
/// last time, by offering back the very epoch the cluster still holds for it.
///
/// A node the image has no registration for cannot prove anything, so it is
/// unclean. Kafka reaches that through a `-2` sentinel for the held epoch,
/// which no `previousBrokerEpoch` can equal either.
pub(crate) fn restart_was_clean(
    image: &MetadataImage,
    node_id: NodeId,
    previous_broker_epoch: i64,
) -> bool {
    image
        .broker_epoch(node_id)
        .is_some_and(|held| held == previous_broker_epoch)
}

#[cfg(test)]
mod tests;
