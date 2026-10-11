//! KIP-858 log-dir assignment reporting: the change detection that finds which
//! `(topic, partition)` dir UUIDs moved since the last successful report, and
//! the reporter that sends `AssignReplicasToDirs` to the controller.

use std::{collections::HashSet, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
use tracing::warn;

use super::{ReplicatorSupervisor, TopicPartition};
use crate::{
    assign_dirs::{UNKNOWN_BROKER_EPOCH, build_request, send_assignments},
    controller_endpoint::ControllerDialer,
    log_dir_id::LogDirIds,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
};

/// Compute the dir-assignment reports that changed since last reported.
///
/// Returns `(wire_assignments, tracker_updates)`:
/// - `wire_assignments`: `(topic_id, partition, dir_uuid)` for `build_request`.
/// - `tracker_updates`: `(topic_name, partition, dir_uuid)` to write into
///   `reported_dirs` on a successful send.
///
/// This function is pure. It reads each partition's current owning dir exactly
/// once and does not load again after the change-check. That removes both the
/// TOCTOU race and the O(n²) `Vec::contains` scan of a double-iteration
/// approach.
type WireDirAssignment = (uuid::Uuid, i32, uuid::Uuid);
type ReportedDirUpdate = (String, i32, uuid::Uuid);
type ChangedAssignments = (Vec<WireDirAssignment>, Vec<ReportedDirUpdate>);

pub(crate) fn collect_changed_assignments(
    local_set: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    log_dir_ids: &LogDirIds,
    image: &MetadataImage,
    reported_dirs: &dashmap::DashMap<TopicPartition, uuid::Uuid>,
) -> ChangedAssignments {
    let mut wire = Vec::new();
    let mut updates = Vec::new();
    for (topic, partition) in local_set {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        let dir = part.log_dir.load();
        let Some(dir_uuid) = log_dir_ids.id_for(&dir) else {
            continue;
        };
        let Some(topic_rec) = image.topic(topic) else {
            continue;
        };
        let key = (topic.clone(), *partition);
        if reported_dirs.get(&key).map(|e| *e.value()) == Some(dir_uuid) {
            continue; // unchanged since last report
        }
        wire.push((topic_rec.topic_id, *partition, dir_uuid));
        updates.push((topic.clone(), *partition, dir_uuid));
    }
    (wire, updates)
}

#[async_trait::async_trait]
pub(super) trait AssignDirsReporter: Send + Sync {
    async fn send(
        &self,
        controller: &Arc<dyn MetadataSource>,
        client_id: &str,
        req: AssignReplicasToDirsRequest,
    ) -> Result<(), String>;
}

/// Sends the report over the controller leader's CONTROLLER listener, the
/// endpoint KIP-919 puts api key 73 on, through the same authenticated dialer
/// the raft transport reaches its peers with.
pub(super) struct NetworkAssignDirsReporter {
    pub(super) dialer: ControllerDialer,
}

#[async_trait::async_trait]
impl AssignDirsReporter for NetworkAssignDirsReporter {
    async fn send(
        &self,
        controller: &Arc<dyn MetadataSource>,
        client_id: &str,
        req: AssignReplicasToDirsRequest,
    ) -> Result<(), String> {
        send_assignments(controller, &self.dialer, client_id, req).await
    }
}

impl ReplicatorSupervisor {
    /// Collect changed `(topic_id, partition, dir_uuid)` assignments from
    /// `local_set` and send `AssignReplicasToDirs` to the controller leader
    /// when at least one assignment has changed since the last successful send.
    pub(super) async fn report_dir_assignments(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        let (wire, updates) = collect_changed_assignments(
            local_set,
            &self.partitions,
            &self.log_dir_ids,
            image,
            &self.reported_dirs,
        );
        if wire.is_empty() {
            return;
        }
        let broker_epoch = image
            .broker_epoch(krabka_raft::NodeId(
                u64::try_from(self.broker_id).unwrap_or(u64::MAX),
            ))
            .unwrap_or(UNKNOWN_BROKER_EPOCH);
        let req = build_request(self.broker_id, broker_epoch, &wire);
        match self
            .assign_dirs_reporter
            .send(&self.controller, &self.client_id, req)
            .await
        {
            Ok(()) => {
                for (topic, partition, dir_uuid) in updates {
                    self.reported_dirs.insert((topic, partition), dir_uuid);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "assign_replicas_to_dirs report failed; will retry next reconcile"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use assert2::assert;
    use krabka_raft::NodeId;
    use uuid::Uuid;

    use super::*;
    use crate::{
        replicator_supervisor::test_support::{
            materialized_partition, single_partition_image, static_source, supervisor_fixture,
        },
        test_support::plaintext_controller_dialer,
    };

    /// A reporter that dials a plaintext controller listener and knows no
    /// statically configured quorum.
    fn plaintext_reporter() -> NetworkAssignDirsReporter {
        NetworkAssignDirsReporter {
            dialer: plaintext_controller_dialer(),
        }
    }

    #[tokio::test]
    async fn network_reporter_send_propagates_controller_resolution_errors() {
        // The real network reporter must surface send_assignments' error
        // (here: no controller leader elected), not swallow it into Ok(()).
        let source: Arc<dyn MetadataSource> =
            Arc::new(static_source(MetadataImage::new(Uuid::nil())));
        let err = plaintext_reporter()
            .send(
                &source,
                "test",
                krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest::default(),
            )
            .await
            .expect_err("no controller leader must fail");
        assert!(err == "no controller leader");
    }

    #[tokio::test]
    async fn report_dir_assignments_sends_and_records_successful_updates() {
        let topic_id = Uuid::new_v4();
        let img = single_partition_image("t", topic_id, NodeId(2));
        let (supervisor, partitions, reporter, _dir) = supervisor_fixture(img.clone());
        supervisor
            .materialize_local_partition(&img, "t", 0)
            .unwrap();
        let mut local_set = HashSet::new();
        local_set.insert(("t".to_string(), 0));

        supervisor.report_dir_assignments(&local_set, &img).await;

        assert!(reporter.calls.load(Ordering::SeqCst) == 1);
        assert!(supervisor.reported_dirs.contains_key(&("t".to_string(), 0)));

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("materialized");
        let dir = part.log_dir.load();
        let expected = supervisor.log_dir_ids.id_for(&dir).expect("dir id");
        assert!(
            supervisor
                .reported_dirs
                .get(&("t".to_string(), 0))
                .map(|e| *e)
                == Some(expected)
        );
    }

    #[tokio::test]
    async fn collect_changed_assignments_reports_new_then_skips_unchanged() {
        use uuid::Uuid;

        // Build image with a single topic+partition.
        let topic_id = Uuid::new_v4();
        let img = single_partition_image("t", topic_id, krabka_raft::NodeId(1));

        // Materialize the partition under a temp dir.
        let (dir, partitions) = materialized_partition();

        // Resolve LogDirIds over the same temp dir.
        let log_dir_ids = LogDirIds::resolve(&[dir.path().to_path_buf()]);

        // Confirm the partition's log_dir equals the temp dir (the parent of
        // the placed partition sub-dir).
        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("part present");
        let loaded_dir = part.log_dir.load();
        assert!(**loaded_dir == dir.path().to_path_buf());

        let dir_uuid = log_dir_ids.id_for(dir.path()).expect("dir uuid resolvable");

        let mut local_set = HashSet::new();
        local_set.insert(("t".to_string(), 0));
        let reported_dirs: dashmap::DashMap<(String, i32), uuid::Uuid> = dashmap::DashMap::new();

        // First call: nothing reported yet → one wire entry + one update entry.
        let (wire, updates) = collect_changed_assignments(
            &local_set,
            &partitions,
            &log_dir_ids,
            &img,
            &reported_dirs,
        );
        assert!(wire == vec![(topic_id, 0, dir_uuid)]);
        assert!(updates == vec![("t".to_string(), 0, dir_uuid)]);

        // Simulate a successful send: insert the tracker update.
        for (topic, partition, uuid) in updates {
            reported_dirs.insert((topic, partition), uuid);
        }

        // Second call: already reported → both vecs empty.
        let (wire2, updates2) = collect_changed_assignments(
            &local_set,
            &partitions,
            &log_dir_ids,
            &img,
            &reported_dirs,
        );
        assert!(wire2.is_empty());
        assert!(updates2.is_empty());
    }
}
