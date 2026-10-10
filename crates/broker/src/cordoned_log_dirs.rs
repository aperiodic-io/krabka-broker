//! KIP-1066 cordoned log directories: the `cordoned.log.dirs` broker config,
//! the set of directories this broker holds cordoned, and the rules that read
//! it.
//!
//! A cordoned directory keeps serving the replicas it holds but takes no new
//! one. Kafka 4.3.1 applies the set in four places, and so does krabka:
//!
//! - New partition directories go to an uncordoned directory
//!   (`LogManager.nextLogDirs`). [`CordonedLogDirs::placement_dirs`] is that
//!   filter.
//! - `AlterReplicaLogDirs` refuses a cordoned destination with
//!   `INVALID_REPLICA_ASSIGNMENT` (`ReplicaManager.alterReplicaLogDirs`).
//! - `DescribeLogDirs` v5 reports `IsCordoned`, from `metadata.version`
//!   `4.3-IV0` on (`ReplicaManager.describeLogDirs`).
//! - The broker reports the directory ids in `BrokerHeartbeat`, and the
//!   controller keeps them on the registration. Automatic replica placement
//!   leaves out a broker whose directories are all cordoned
//!   (`BrokerRegistration.hasUncordonedDirs`); [`has_uncordoned_dirs`] is that
//!   test.
//!
//! The config is a Kafka `LIST` of entries of `log.dirs`, or `*` for all of
//! them. It is dynamic, per broker only
//! (`DynamicBrokerConfig.PER_BROKER_CONFIGS`), and the per-broker dynamic
//! value overrides the static one. An update Kafka cannot apply leaves the
//! previous set in force, as `DynamicBrokerConfig.updateBrokerConfig` does.
//!
//! Kafka 4.3.1 does not refuse a manual assignment, or a reassignment target,
//! that names a broker whose directories are all cordoned. That refusal is
//! KAFKA-20832, which is on Kafka trunk and the 4.3 branch after 4.3.1.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use krabka_metadata::{
    BrokerRegistrationChangeRecord, BrokerRegistrationRecord, MetadataImage, NodeId,
};

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG`.
pub(crate) const CORDONED_LOG_DIRS: &str = "cordoned.log.dirs";

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_ALL`: cordon every directory.
pub(crate) const CORDONED_LOG_DIRS_ALL: &str = "*";

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_DOC`.
pub(crate) const CORDONED_LOG_DIRS_DOC: &str = "A comma-separated list of the directories that \
                                                are cordoned. Entries in this list must be \
                                                entries in log.dirs or log.dir configuration. \
                                                This can also be set to * to cordon all log \
                                                directories.";

/// Parse a `LIST` value as Kafka's `ConfigDef.parseType` does: a blank value
/// is the empty list, and anything else splits on commas with the whitespace
/// around them removed. Kafka then drops duplicates (`ConfigDef.parseValue`).
fn parse_list(value: &str) -> Vec<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut entries: Vec<&str> = Vec::new();
    for entry in trimmed.split(',').map(str::trim) {
        if !entries.contains(&entry) {
            entries.push(entry);
        }
    }
    entries
}

/// Kafka's `ValidList.anyNonDuplicateValues(true, true)` on the value, which
/// `DynamicBrokerConfig.validateConfigTypes` applies before anything else.
///
/// # Errors
/// Returns Kafka's message when an entry is empty.
pub(crate) fn validate_value_type(value: &str) -> Result<(), String> {
    if parse_list(value).iter().any(|entry| entry.is_empty()) {
        return Err(format!(
            "Configuration '{CORDONED_LOG_DIRS}' values must not be empty."
        ));
    }
    Ok(())
}

/// The configured string of each log directory, which is what Kafka compares
/// a `cordoned.log.dirs` entry against (`AbstractKafkaConfig.logDirs`).
fn configured_names(log_dirs: &[PathBuf]) -> Vec<String> {
    log_dirs
        .iter()
        .map(|dir| dir.display().to_string())
        .collect()
}

/// Kafka's type check followed by `KafkaConfig.validateCordonedLogDirs`, the
/// check a broker applies to its own value at startup and to every dynamic
/// update of it. It returns the directories the value cordons.
///
/// # Errors
/// Returns Kafka's message: an empty entry, `*` beside another entry, or an
/// entry that names no directory in `log_dirs`. The last two carry the
/// `requirement failed: ` prefix of Scala's `require`, which is what
/// `kafka-configs` prints.
pub(crate) fn resolve(value: &str, log_dirs: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    validate_value_type(value)?;
    let entries = parse_list(value);
    if entries.contains(&CORDONED_LOG_DIRS_ALL) {
        if entries.len() != 1 {
            return Err(format!(
                "requirement failed: When {CORDONED_LOG_DIRS} is set to {CORDONED_LOG_DIRS_ALL}, \
                 it must not contain other values"
            ));
        }
        return Ok(log_dirs.to_vec());
    }
    let names = configured_names(log_dirs);
    let missing: Vec<&str> = entries
        .iter()
        .copied()
        .filter(|entry| !names.iter().any(|name| name == entry))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "requirement failed: All entries in {CORDONED_LOG_DIRS} must be present in log.dirs \
             or log.dir. Missing entries : {}",
            missing.join(", ")
        ));
    }
    Ok(log_dirs
        .iter()
        .zip(&names)
        .filter(|(_, name)| entries.contains(&name.as_str()))
        .map(|(dir, _)| dir.clone())
        .collect())
}

/// Kafka's `BrokerRegistration.hasUncordonedDirs`: whether automatic replica
/// placement may use the broker. A broker that has not reported its cordoned
/// directories yet, or that registered no directory ids, counts as usable.
pub(crate) fn has_uncordoned_dirs(registration: &BrokerRegistrationRecord) -> bool {
    let Some(cordoned) = &registration.cordoned_log_dirs else {
        return true;
    };
    registration.log_dirs.is_empty()
        || registration
            .log_dirs
            .iter()
            .any(|dir| !cordoned.contains(dir))
}

/// The registered brokers whose directories are all cordoned, which the
/// automatic placement of `CreateTopics` and `CreatePartitions` leaves out.
pub(crate) fn fully_cordoned_brokers(image: &MetadataImage) -> std::collections::HashSet<u64> {
    image
        .brokers()
        .filter(|registration| !has_uncordoned_dirs(registration))
        .map(|registration| registration.node_id.0)
        .collect()
}

/// The `BrokerRegistrationChangeRecord` that stores `reported` as the cordoned
/// directories of `node_id`, at its broker epoch, or `None` when the
/// registration already holds that set.
///
/// This is Kafka's `ReplicationControlManager.handleDirectoriesCordoned`. A
/// heartbeat with no cordoned directories (the broker has not caught up yet)
/// changes nothing. A registration that holds none yet always takes the
/// reported set, and otherwise the two are compared as sets
/// (`BrokerRegistration.cordonedDirChanged`).
pub(crate) fn registration_change(
    image: &MetadataImage,
    node_id: NodeId,
    reported: Option<&[uuid::Uuid]>,
) -> Option<krabka_metadata::MetadataRecord> {
    let reported = reported?;
    let current = image.broker(node_id)?;
    let changed = current.cordoned_log_dirs.as_ref().is_none_or(|stored| {
        stored.iter().collect::<BTreeSet<_>>() != reported.iter().collect::<BTreeSet<_>>()
    });
    changed.then(|| {
        krabka_metadata::MetadataRecord::V1BrokerRegistrationChange(
            BrokerRegistrationChangeRecord {
                cordoned_log_dirs: Some(reported.to_vec()),
                ..BrokerRegistrationChangeRecord::no_change(node_id, current.broker_epoch)
            },
        )
    })
}

/// The directories this broker holds cordoned, shared by every reader.
///
/// Kafka keeps the set on `LogManager` and `DynamicLogConfig.reconfigure`
/// replaces it. Here an image watcher replaces it with
/// [`CordonedLogDirs::apply_image`], and the placement, `AlterReplicaLogDirs`,
/// `DescribeLogDirs` and the heartbeat all read the same set.
#[derive(Clone, Debug, Default)]
pub(crate) struct CordonedLogDirs {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Every configured log directory, primary first.
    log_dirs: Vec<PathBuf>,
    /// The static `cordoned.log.dirs`, as the operator wrote it.
    static_value: Option<String>,
    /// The set in force.
    current: ArcSwap<Vec<PathBuf>>,
}

impl CordonedLogDirs {
    /// The set a broker starts with: its static `cordoned.log.dirs`. The
    /// broker has already refused a static value that does not resolve, so an
    /// unresolvable one here cordons nothing.
    #[must_use]
    pub(crate) fn new(log_dirs: Vec<PathBuf>, static_value: Option<String>) -> Self {
        let current = static_value
            .as_deref()
            .and_then(|value| resolve(value, &log_dirs).ok())
            .unwrap_or_default();
        Self {
            inner: Arc::new(Inner {
                log_dirs,
                static_value,
                current: ArcSwap::from_pointee(current),
            }),
        }
    }

    /// Whether `dir`, one of the configured log directories, is cordoned.
    #[must_use]
    pub(crate) fn is_cordoned(&self, dir: &Path) -> bool {
        self.inner
            .current
            .load()
            .iter()
            .any(|cordoned| cordoned == dir)
    }

    /// The cordoned directories, in configuration order.
    #[must_use]
    pub(crate) fn cordoned(&self) -> Vec<PathBuf> {
        self.inner.current.load().as_ref().clone()
    }

    /// The directories of `candidates` a new partition may go to: the
    /// uncordoned ones, or the first candidate when every one is cordoned.
    ///
    /// Kafka's `LogManager.nextLogDirs` falls back the same way, because the
    /// controller may have assigned the replica to this broker just before
    /// its last directory was cordoned.
    #[must_use]
    pub(crate) fn placement_dirs(&self, candidates: &[PathBuf]) -> Vec<PathBuf> {
        let open: Vec<PathBuf> = candidates
            .iter()
            .filter(|dir| !self.is_cordoned(dir))
            .cloned()
            .collect();
        if open.is_empty() {
            candidates.first().cloned().into_iter().collect()
        } else {
            open
        }
    }

    /// Replace the set with the one `image` gives this broker: its per-broker
    /// dynamic `cordoned.log.dirs` when it holds one, and its static value
    /// otherwise.
    ///
    /// A dynamic value that does not resolve leaves the set as it was, as a
    /// Kafka broker keeps its previous configuration when
    /// `DynamicBrokerConfig.updateBrokerConfig` cannot apply an update.
    pub(crate) fn apply_image(&self, image: &MetadataImage, node_id: NodeId) {
        let dynamic = image
            .broker_config(node_id)
            .and_then(|configs| configs.get(CORDONED_LOG_DIRS));
        let value = dynamic
            .map(String::as_str)
            .or(self.inner.static_value.as_deref());
        let next = match value {
            None => Vec::new(),
            Some(value) => match resolve(value, &self.inner.log_dirs) {
                Ok(next) => next,
                Err(reason) => {
                    tracing::error!(
                        %reason,
                        value,
                        "{CORDONED_LOG_DIRS} could not be applied; keeping the previous value"
                    );
                    return;
                }
            },
        };
        if *self.inner.current.load().as_ref() != next {
            tracing::info!(cordoned = ?next, "{CORDONED_LOG_DIRS} updated");
            self.inner.current.store(Arc::new(next));
        }
    }
}

/// Keep `cordoned` in step with the metadata image until `shutdown`.
pub(crate) fn spawn_watcher(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
    cordoned: CordonedLogDirs,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(crate::metadata_source::watch_image_loop(
        controller.watch_image(),
        "cordoned log dirs",
        shutdown,
        move |image| cordoned.apply_image(image, node_id),
    ));
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{BrokerConfigRecord, MetadataRecord};

    use super::*;

    /// A label, the registered directory ids, the cordoned ids, and whether
    /// placement may use the broker.
    type UsableCase<'a> = (&'a str, &'a [u128], Option<&'a [u128]>, bool);

    /// A label, the stored cordoned ids, the reported set, and whether a
    /// registration change is written.
    type HeartbeatCase<'a> = (&'a str, Option<&'a [u128]>, Option<Vec<uuid::Uuid>>, bool);

    fn dirs() -> Vec<PathBuf> {
        vec![PathBuf::from("/d1"), PathBuf::from("/d2")]
    }

    /// Each message is the one `kafka-configs --alter --entity-name 1
    /// --add-config cordoned.log.dirs=...` printed against `apache/kafka:4.3.1`
    /// with `log.dirs=/tmp/d1,/tmp/d2`.
    #[test]
    fn values_resolve_as_kafka_validates_them() {
        let d1 = PathBuf::from("/d1");
        let d2 = PathBuf::from("/d2");
        let cases: [(&str, Result<Vec<PathBuf>, &str>); 10] = [
            ("", Ok(vec![])),
            ("  ", Ok(vec![])),
            ("/d1", Ok(vec![d1.clone()])),
            ("/d2 , /d1", Ok(vec![d1.clone(), d2.clone()])),
            ("/d1,/d1", Ok(vec![d1])),
            ("*", Ok(vec![PathBuf::from("/d1"), d2])),
            (
                "/d1,",
                Err("Configuration 'cordoned.log.dirs' values must not be empty."),
            ),
            (
                "*,/d1",
                Err(
                    "requirement failed: When cordoned.log.dirs is set to *, it must not \
                     contain other values",
                ),
            ),
            (
                "/nope",
                Err(
                    "requirement failed: All entries in cordoned.log.dirs must be present in \
                     log.dirs or log.dir. Missing entries : /nope",
                ),
            ),
            (
                "/x,/d1,/y",
                Err(
                    "requirement failed: All entries in cordoned.log.dirs must be present in \
                     log.dirs or log.dir. Missing entries : /x, /y",
                ),
            ),
        ];
        for (value, want) in cases {
            check!(
                resolve(value, &dirs()) == want.map_err(str::to_owned),
                "{value:?}"
            );
        }
    }

    fn registration(
        log_dirs: &[u128],
        cordoned: Option<&[u128]>,
    ) -> krabka_metadata::BrokerRegistrationRecord {
        krabka_metadata::BrokerRegistrationRecord {
            cordoned_log_dirs: cordoned
                .map(|ids| ids.iter().copied().map(uuid::Uuid::from_u128).collect()),
            broker_epoch: 7,
            incarnation_id: uuid::Uuid::from_u128(1),
            log_dirs: log_dirs
                .iter()
                .copied()
                .map(uuid::Uuid::from_u128)
                .collect(),
            ..crate::test_support::broker_registration(krabka_raft::NodeId(1))
        }
    }

    /// Kafka's `BrokerRegistration.hasUncordonedDirs`.
    #[test]
    fn a_broker_is_usable_unless_every_directory_is_cordoned() {
        let cases: [UsableCase<'_>; 5] = [
            ("not reported yet", &[1, 2], None, true),
            ("nothing cordoned", &[1, 2], Some(&[]), true),
            ("one of two cordoned", &[1, 2], Some(&[1]), true),
            ("every directory cordoned", &[1, 2], Some(&[2, 1]), false),
            ("no directory ids registered", &[], Some(&[1]), true),
        ];
        for (label, log_dirs, cordoned, want) in cases {
            check!(
                has_uncordoned_dirs(&registration(log_dirs, cordoned)) == want,
                "{label}"
            );
        }
    }

    /// Kafka's `handleDirectoriesCordoned`: a record only when the heartbeat
    /// carries a set and it differs from the stored one as a set.
    #[test]
    fn a_heartbeat_changes_the_registration_only_when_its_set_differs() {
        let one = uuid::Uuid::from_u128(1);
        let two = uuid::Uuid::from_u128(2);
        let cases: [HeartbeatCase<'_>; 5] = [
            ("the broker has not caught up", None, None, false),
            ("the first report", None, Some(vec![]), true),
            (
                "an unchanged set",
                Some(&[1, 2]),
                Some(vec![two, one]),
                false,
            ),
            ("a new directory", Some(&[1]), Some(vec![one, two]), true),
            ("uncordon everything", Some(&[1]), Some(vec![]), true),
        ];
        for (label, stored, reported, writes) in cases {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            let current = registration(&[1, 2], stored);
            image.apply(&MetadataRecord::V1BrokerRegistration(current));
            let want = writes.then(|| {
                MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
                    node_id: NodeId(1),
                    broker_epoch: 7,
                    fenced: krabka_metadata::FencingChange::None,
                    in_controlled_shutdown: false,
                    log_dirs: vec![],
                    cordoned_log_dirs: reported.clone(),
                })
            });
            check!(
                registration_change(&image, NodeId(1), reported.as_deref()) == want,
                "{label}"
            );
        }
    }

    fn image_with(value: Option<&str>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(value) = value {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: CORDONED_LOG_DIRS.into(),
                config_value: Some(value.into()),
            }));
        }
        image
    }

    /// The dynamic value wins over the static one, and one that does not
    /// resolve keeps the set that was in force.
    #[test]
    fn the_set_follows_the_dynamic_value_and_keeps_the_last_good_one() {
        let cordoned = CordonedLogDirs::new(dirs(), Some("/d2".into()));
        check!(cordoned.cordoned() == vec![PathBuf::from("/d2")]);

        let steps: [(Option<&str>, Vec<PathBuf>); 5] = [
            (Some("/d1"), vec![PathBuf::from("/d1")]),
            (Some("/nope"), vec![PathBuf::from("/d1")]),
            (Some("*"), dirs()),
            (Some(""), vec![]),
            (None, vec![PathBuf::from("/d2")]),
        ];
        for (value, want) in steps {
            cordoned.apply_image(&image_with(value), NodeId(1));
            check!(cordoned.cordoned() == want, "{value:?}");
        }
    }

    /// Kafka's `LogManager.nextLogDirs`: uncordoned directories only, or the
    /// first one when all are cordoned.
    #[test]
    fn placement_skips_cordoned_directories_until_none_is_left() {
        let cordoned = CordonedLogDirs::new(dirs(), Some("/d1".into()));
        assert!(cordoned.placement_dirs(&dirs()) == vec![PathBuf::from("/d2")]);
        check!(cordoned.is_cordoned(Path::new("/d1")));
        check!(!cordoned.is_cordoned(Path::new("/d2")));

        cordoned.apply_image(&image_with(Some("*")), NodeId(1));
        assert!(cordoned.placement_dirs(&dirs()) == vec![PathBuf::from("/d1")]);
        assert!(cordoned.placement_dirs(&[]) == Vec::<PathBuf>::new());
    }
}
