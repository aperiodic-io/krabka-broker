//! Controller activation: the records that a new leader writes to a metadata
//! log that holds no `metadata.version`.
//!
//! Kafka's active controller writes them in its `CompleteActivationEvent`,
//! with `ActivationRecordsGenerator.recordsForEmptyLog`. They are the
//! bootstrap records, from the bootstrap checkpoint of a dynamic format or
//! else from the `bootstrap.checkpoint` of a static one, and the cluster-level
//! `min.insync.replicas` when the bootstrap records enable ELR. A broker never
//! writes them. Every replica, a broker-only observer too, applies them from
//! the log. A controller that cannot write them stops over a fatal fault, as
//! Kafka's `fatalFaultHandler` halts the process.

use {
    super::Engine,
    crate::error::RaftError,
    krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord,
        metadata_version::{ELR_VERSION_FEATURE, METADATA_VERSION_FEATURE},
    },
    tokio::sync::oneshot,
};

/// Kafka's `TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG`.
const MIN_IN_SYNC_REPLICAS_CONFIG: &str = "min.insync.replicas";

/// The `failureMessage` Kafka's `QuorumController` gives its fatal fault
/// handler when the activation records cannot be written.
const ACTIVATION_FAULT: &str = "exception while completing controller activation";

/// What a new leader of an empty metadata log writes: the inputs of Kafka's
/// `ActivationRecordsGenerator.recordsForEmptyLog`.
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub struct Activation {
    /// The bootstrap records. An empty list writes nothing.
    pub bootstrap_records: Vec<MetadataRecord>,
    /// This node's static `min.insync.replicas`: Kafka's
    /// `ConfigurationControlManager.getStaticallyConfiguredMinInsyncReplicas`.
    /// It becomes the cluster-level `min.insync.replicas` when the bootstrap
    /// records enable ELR. Default: `1`, Kafka's default.
    #[default(1)]
    pub default_min_insync_replicas: i32,
}

/// The level of the last `FeatureLevelRecord` for `name` in `records`, or
/// `None` when they hold none. The last record wins, as it does in Kafka's
/// `BootstrapMetadata` and on replay.
fn bootstrap_feature_level(records: &[MetadataRecord], name: &str) -> Option<i16> {
    records.iter().rev().find_map(|record| match record {
        MetadataRecord::V1FeatureLevel(feature) if feature.name == name => Some(feature.level),
        _ => None,
    })
}

/// The `metadata.version` level of the last `FeatureLevelRecord` for it in
/// `records`, or `None` when they hold none.
pub(super) fn bootstrap_metadata_version(records: &[MetadataRecord]) -> Option<i16> {
    bootstrap_feature_level(records, METADATA_VERSION_FEATURE)
}

/// The records of the activation of an empty log, in Kafka's order: the
/// bootstrap records, and then the cluster-level `min.insync.replicas` at the
/// static value when the bootstrap records finalize
/// `eligible.leader.replicas.version` above 0. They go to the log as one
/// batch, which commits whole, as Kafka writes them in one atomic batch or
/// metadata transaction.
pub(super) fn activation_records(activation: &Activation) -> Vec<MetadataRecord> {
    let mut records = activation.bootstrap_records.clone();
    if bootstrap_feature_level(&records, ELR_VERSION_FEATURE).is_some_and(|level| level > 0) {
        records.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: MIN_IN_SYNC_REPLICAS_CONFIG.to_owned(),
            config_value: Some(activation.default_min_insync_replicas.to_string()),
        }));
    }
    records
}

/// Refuse bootstrap metadata that holds records but does not finalize
/// `metadata.version`, as Kafka's `BootstrapMetadata` does.
///
/// The leader writes the bootstrap records to a log that holds no
/// `metadata.version`. Records that do not finalize one would leave the log
/// in that state, and the next leader would write them again.
///
/// # Errors
///
/// Returns [`RaftError::Startup`] when `records` is not empty and holds no
/// `FeatureLevelRecord` for `metadata.version`, with the message of Kafka's
/// `BootstrapMetadata.fromRecords`. It also returns it when the last such
/// record sets level 0, which removes the feature and which no
/// `MetadataVersion` has.
pub(super) fn check_bootstrap_records(
    records: &[MetadataRecord],
    source: &str,
) -> Result<(), RaftError> {
    if records.is_empty() {
        return Ok(());
    }
    match bootstrap_metadata_version(records) {
        Some(level) if level > 0 => Ok(()),
        Some(level) => Err(RaftError::Startup(format!(
            "No MetadataVersion with feature level {level} in the bootstrap metadata from \
             {source}"
        ))),
        None => Err(RaftError::Startup(format!(
            "No FeatureLevelRecord for {METADATA_VERSION_FEATURE} was found in the bootstrap \
             metadata from {source}"
        ))),
    }
}

impl Engine {
    /// Write the activation records when the log of this new leader holds no
    /// `metadata.version`: Kafka's `CompleteActivationEvent`.
    ///
    /// The leader calls this directly after it appends the `LeaderChange`
    /// batch of its epoch. So the records come before every other write of
    /// the epoch, as Kafka's activation event is the first event of the
    /// active controller. The log holds `metadata.version` when the committed
    /// image finalizes it. It also holds it when a batch that an earlier
    /// leader appended and did not commit finalizes it, because that batch
    /// commits with this epoch. Kafka makes the same decision after its
    /// controller has replayed the log up to the epoch start.
    ///
    /// The records go through [`Self::on_submit_change`], so the leader
    /// validates and encodes them as it does every other write. Nothing waits
    /// for them to commit. A leader that refuses them, or that cannot read
    /// its own log tail to decide, sets [`Engine::activation_fault`]. The
    /// engine then stops, and the controller stops over the fault, as Kafka's
    /// `QuorumController` gives the failure to its `fatalFaultHandler`.
    pub fn complete_activation(&mut self) {
        let Some(level) = bootstrap_metadata_version(&self.activation.bootstrap_records)
            .filter(|level| *level > 0)
        else {
            return;
        };
        match self.replay_earlier_epoch_tail(|_, _| {}) {
            Ok(image) if image.finalized_metadata_version().is_some() => return,
            Ok(_) => {}
            Err(error) => {
                self.activation_fault = Some(format!("{ACTIVATION_FAULT}: {error}"));
                return;
            }
        }
        let records = activation_records(&self.activation);
        tracing::info!(
            records = records.len(),
            metadata_version = level,
            "kraft: performing controller activation. The metadata log appears to be empty: \
             appending the bootstrap records"
        );
        let (reply, mut outcome) = oneshot::channel();
        self.on_submit_change(&records, reply);
        if let Ok(Err(error)) = outcome.try_recv() {
            self.activation_fault = Some(format!("{ACTIVATION_FAULT}: {error}"));
        }
    }
}
