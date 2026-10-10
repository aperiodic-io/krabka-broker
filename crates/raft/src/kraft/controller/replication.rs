//! The follower side of replication: the rules that classify a fetched batch
//! or a snapshot hint, the apply of a leader's Fetch response, and the
//! reassembly and install of a KIP-630 snapshot fetched from the leader.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord};
use krabka_verified::{
    SnapshotInstallDecision,
    raft::{
        FetchContent, FetchFence, FetchResponseFacts, FetchResponseMutation,
        fetch_response_mutation,
    },
    snapshot_install_decision,
};

use super::{
    Engine, KraftControlState, Role,
    checkpoint::{retain_recent_checkpoints, write_checkpoint},
    offsets::fetch_offset_has_records,
    records::{decode_batches, encode_batches},
};
use crate::{
    error::RaftError,
    kraft::{
        event::Event,
        snapshot_fetch::{SnapshotFetchState, SnapshotFetchStep},
        transport::wire,
        types::{Epoch, NodeId},
    },
};

pub fn should_serve_fetch_records(
    has_snapshot: bool,
    has_divergence: bool,
    is_leader: bool,
) -> bool {
    matches!(
        (has_snapshot, has_divergence, is_leader),
        (false, false, true)
    )
}

pub fn fetch_epoch_for_request(
    installed_snapshot_epoch: Option<Epoch>,
    log_start: Offset,
    log_end: Offset,
    last_epoch: Epoch,
) -> Epoch {
    match installed_snapshot_epoch {
        Some(epoch) if log_end.cmp(&log_start).is_eq() => epoch,
        _ => last_epoch,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchBatchDisposition {
    AlreadyPresent,
    Append,
    Gap,
}

pub fn classify_fetch_batch(at: Offset, log_end: Offset) -> FetchBatchDisposition {
    match at.cmp(&log_end) {
        std::cmp::Ordering::Less => FetchBatchDisposition::AlreadyPresent,
        std::cmp::Ordering::Equal => FetchBatchDisposition::Append,
        std::cmp::Ordering::Greater => FetchBatchDisposition::Gap,
    }
}

pub fn should_start_snapshot_fetch(
    snapshot_id: (i64, i32),
    log_end: Offset,
    active_snapshot_id: Option<(i64, i32)>,
) -> bool {
    snapshot_id.0.cmp(&log_end.0).is_gt()
        && !matches!(active_snapshot_id, Some(id) if id == snapshot_id)
}

pub fn snapshot_fetch_response_invalid(error_code: i16, from: NodeId, leader_id: NodeId) -> bool {
    !matches!(
        (error_code.cmp(&0), from.cmp(&leader_id)),
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal)
    )
}

impl Engine {
    /// (Leader side) serialize every log batch at/after `fetch_offset` up to our
    /// log end into a length-prefixed run of `RecordBatch::encode` blobs for the
    /// fetching follower. `KRaft` replicates up to the leader's log end (not just
    /// the HWM — the HWM is carried separately in the response and gates apply on
    /// the follower); this is what moves real record bytes so multi-voter
    /// `submit_change` waiters can commit once a majority has fetched.
    pub fn serve_fetch_records(&self, fetch_offset: Offset) -> bytes::Bytes {
        let log_end = self.log.log_end_offset();
        if !fetch_offset_has_records(fetch_offset, log_end) {
            return bytes::Bytes::new();
        }
        let batches = match self
            .log
            .read_decoded(fetch_offset, self.metadata_raft_fetch_max.size())
        {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(?e, "kraft: serve_fetch read failed");
                return bytes::Bytes::new();
            }
        };
        encode_batches(&batches)
    }

    /// (Follower side) apply the leader's Fetch response: truncate on a
    /// divergence hint, append the carried batches at their leader-assigned
    /// offsets, advance our HWM to `min(leader_hwm, own log_end)`, apply the
    /// newly-committed records to the image, then feed the core
    /// `ReceiveFetchResponse` (which re-arms the fetch timer / re-fetches).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, from = from.0, log_end = self.log.log_end_offset().0)
    )]
    pub fn on_fetch_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::Fetch(wire::FetchAnswer {
            error_code,
            leader,
            diverging,
            snapshot_id,
            hwm,
            log_start_offset: _,
            records,
        })) = wire::PeerResponse::decode_fetch(body)
        else {
            return;
        };
        let leader_epoch = leader.epoch;

        // KIP-595: a response from a newer epoch names the leader of that
        // epoch, and a voter or an observer follows it, whatever the error.
        // Kafka's `KafkaRaftClient.maybeHandleCommonResponse` does the same.
        // The fence below accepts only a response that matches this node's
        // own leader and epoch, so without this step a node whose leader lost
        // the epoch keeps fetching from it, rejects every answer, and stops
        // replicating. A voter learns the new leader from `BeginQuorumEpoch`
        // too, but an observer learns it only here. A newer epoch that names
        // no leader is left to that epoch's Vote or `BeginQuorumEpoch`;
        // Kafka moves to Unattached in it instead.
        if leader_epoch > self.core.quorum_state().leader_epoch {
            if let Some(leader_id) = leader.leader_id {
                self.remember_leader_endpoint(&leader);
                self.on_event(Event::ReceiveBeginQuorumEpoch {
                    leader_id,
                    leader_epoch,
                });
            }
            return;
        }

        // Decode is side-effect free. Fence the response against both the
        // live role and durable leader view before remembering its peer or
        // performing any response-derived mutation.
        let role_leader = match self.core.role() {
            Role::Follower { leader_id, .. }
            | Role::Observer {
                leader_id: Some(leader_id),
                ..
            } => Some(leader_id.0),
            _ => None,
        };
        let discovering = matches!(
            self.core.role(),
            Role::Observer {
                leader_id: None,
                ..
            }
        );
        let state = self.core.quorum_state();
        let mutation = fetch_response_mutation(
            FetchFence {
                discovering,
                role_leader,
                current_leader: state.leader_id.map(|id| id.0),
                current_epoch: state.leader_epoch,
            },
            FetchResponseFacts {
                from: from.0,
                leader: leader.leader_id.map(|id| id.0),
                epoch: leader_epoch,
                error_none: error_code == 0,
            },
            FetchContent {
                has_snapshot: snapshot_id.is_some(),
                has_divergence: diverging.is_some(),
                has_records: !records.is_empty(),
            },
        );

        // Record the leader's watermark as soon as the response has cleared the
        // leader fence, before any branch below can return: it is the quorum's
        // committed offset, and the readiness probe needs the gap between it and
        // where this node has got to. The `Snapshot` arm returns without
        // touching the log, and a node far enough behind to need a snapshot is
        // the one whose lag matters most -- reading its own clamped watermark
        // there would report the worst laggard as caught up. A discovery
        // response has not cleared that fence, so its watermark waits for the
        // refetch.
        if !matches!(
            mutation,
            FetchResponseMutation::Reject | FetchResponseMutation::Discover
        ) {
            self.leader_reported_hwm = self.leader_reported_hwm.max(hwm);
        }

        match mutation {
            FetchResponseMutation::Reject => return,
            FetchResponseMutation::Discover => {
                // A leaderless observer uses a discovery response only to
                // attach to the advertised leader, as Kafka's
                // `maybeHandleCommonResponse` does, and refetches under the
                // leader fence before it applies any content. It reaches that
                // leader through the leader's own `NodeEndpoints` entry, or
                // its voter-set listener, never through the responder's
                // address: the responder may be a follower redirecting it.
                if let Some(leader_id) = leader.leader_id {
                    self.remember_leader_endpoint(&leader);
                    self.on_event(Event::ReceiveBeginQuorumEpoch {
                        leader_id,
                        leader_epoch,
                    });
                }
                return;
            }
            FetchResponseMutation::Snapshot => {
                // The leader signalled our fetch offset is below its pruned
                // log-start. Start or continue a snapshot transfer and do not
                // perform another mutation from this response.
                let Some(id) = snapshot_id else { return };
                let active_id = self.snapshot_fetch.as_ref().map(|s| s.snapshot_id);
                if should_start_snapshot_fetch(id, self.log.log_end_offset(), active_id) {
                    self.snapshot_fetch = Some(SnapshotFetchState::with_max(
                        id,
                        from,
                        self.metadata_snapshot_fetch_max,
                    ));
                    self.send_fetch_snapshot(from, id, 0);
                }
            }
            FetchResponseMutation::Truncate => {
                // Diverged: truncate where Kafka's `truncateToEndOffset`
                // does, which also reads our own end of the hinted epoch. The
                // next fetch starts at the truncation point.
                let Some(point) = diverging else { return };
                let truncate_to = point.follower_truncation_offset(&self.log);
                if let Err(e) = self.log.truncate_to(Offset(truncate_to)) {
                    tracing::error!(?e, "kraft: follower truncate failed");
                } else {
                    self.restore_control_state_after_truncation(self.log.log_end_offset().0);
                }
            }
            FetchResponseMutation::Append => {
                // Append the carried batches at their leader-assigned offsets.
                match decode_batches(&records) {
                    Ok(batches) => {
                        for mut batch in batches {
                            let at = Offset(batch.base_offset);
                            let log_end = self.log.log_end_offset();
                            match classify_fetch_batch(at, log_end) {
                                FetchBatchDisposition::AlreadyPresent => continue,
                                FetchBatchDisposition::Append => {}
                                FetchBatchDisposition::Gap => break,
                            }
                            if let Err(e) = self.log.append_at(&mut batch, at) {
                                tracing::error!(?e, at = at.0, "kraft: follower append_at failed");
                                break;
                            }
                            if let Err(e) = self.apply_control_batch(&batch) {
                                tracing::error!(
                                    ?e,
                                    at = at.0,
                                    "kraft: invalid fetched control batch"
                                );
                                break;
                            }
                            self.installed_snapshot_epoch = None;
                        }
                    }
                    Err(e) => tracing::error!(?e, "kraft: follower decode batches failed"),
                }
                let target = Offset(hwm).min(self.log.log_end_offset());
                self.advance_and_apply(target);
            }
            FetchResponseMutation::HighWatermark => {
                // The leader may have committed records that were already
                // replicated locally.
                let target = Offset(hwm).min(self.log.log_end_offset());
                self.advance_and_apply(target);
            }
        }

        // Feed the core so it re-arms its fetch timer / issues the next fetch.
        // The leader fence held, so the responder is the leader.
        self.on_event(Event::ReceiveFetchResponse {
            leader_id: from,
            leader_epoch,
            diverging,
        });
    }

    /// Address the leader a Fetch response names by the endpoint it
    /// announced in `NodeEndpoints`, as Kafka's `transitionToFollower` takes
    /// the response's leader endpoints. Without one, the transport keeps
    /// using the leader's voter-set listener.
    fn remember_leader_endpoint(&self, leader: &wire::QuorumLeader) {
        if let (Some(leader_id), Some((host, port))) = (leader.leader_id, &leader.endpoint) {
            self.peers
                .remember_leader_endpoint(leader_id, format!("{host}:{port}"));
        }
    }

    /// (Follower side) handle a `FetchSnapshot` response chunk: reassemble via
    /// the [`SnapshotFetchState`], requesting the next range until complete, then
    /// install the assembled snapshot and resume normal fetching. Any error /
    /// abort falls back to a plain Fetch against the same peer.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, from = from.0))]
    pub fn on_fetch_snapshot_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::FetchSnapshot {
            snapshot_id,
            size,
            position,
            bytes,
            error_code,
        }) = wire::PeerResponse::decode_fetch_snapshot(body)
        else {
            return;
        };
        let Some(state) = self.snapshot_fetch.as_mut() else {
            return;
        };
        if snapshot_fetch_response_invalid(error_code, from, state.leader_id) {
            self.snapshot_fetch = None;
            self.send_fetch(from);
            return;
        }
        match state.on_chunk(snapshot_id, size, position, &bytes) {
            SnapshotFetchStep::Continue { next_position } => {
                self.send_fetch_snapshot(from, snapshot_id, next_position);
            }
            SnapshotFetchStep::Restart => {
                self.snapshot_fetch = None;
                self.send_fetch(from);
            }
            SnapshotFetchStep::Complete(assembled) => {
                let id = state.snapshot_id;
                self.snapshot_fetch = None;
                if let Err(e) = self.install_fetched_snapshot(id, &assembled) {
                    tracing::error!(?e, "kraft: snapshot install failed; will re-fetch");
                }
                self.send_fetch(from);
            }
        }
    }

    /// Validate, persist, and install a fetched snapshot: rebuild the image from
    /// its records, write the checkpoint, install it into the log (resetting the
    /// log-start/end to `end_offset`), publish the new image, and arm the
    /// post-install fetch epoch (see `send_fetch`).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, end_offset = id.0, snapshot_epoch = id.1, bytes = bytes.len()),
        err
    )]
    pub fn install_fetched_snapshot(
        &mut self,
        id: (i64, i32),
        bytes: &[u8],
    ) -> Result<(), RaftError> {
        // `end_offset` is the snapshot id's raw offset (wire / checkpoint-filename
        // boundary); wrap into the log offset domain where it addresses the log.
        let (end_offset, epoch) = id;
        let end_offset_pos = Offset(end_offset);
        let install = snapshot_install_decision(
            self.downgrade_snapshot_pending.is_some(),
            end_offset,
            epoch,
            self.log.log_end_offset().0,
        );
        if install == SnapshotInstallDecision::Reject {
            return Err(RaftError::ChangeRejected(
                "invalid snapshot identity or mandatory metadata downgrade snapshot pending".into(),
            ));
        }
        // Validate the bytes decode before mutating any durable state.
        let contents = crate::snapshot::SnapshotReader::read(bytes)?;
        if install == SnapshotInstallDecision::Stale {
            return Ok(()); // stale; we already advanced past this snapshot
        }
        let cluster_id = self.image.cluster_id();
        let mut new_image = MetadataImage::from_records(cluster_id, &contents.metadata_records);
        if let Some(control) = &contents.control_state {
            new_image.apply(&MetadataRecord::V1KRaftVersion(
                krabka_metadata::KRaftVersionRecord {
                    kraft_version: control.kraft_version,
                },
            ));
            new_image.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: control.voters.clone(),
            }));
        }
        write_checkpoint(&self.data_dir, end_offset, epoch, bytes)?;
        self.image = new_image;
        if let Some(control) = contents.control_state {
            self.controls = KraftControlState::new(control.voters.clone(), control.kraft_version);
            self.core.set_kraft_version(control.kraft_version);
            let actions = self.apply_voter_set(control.voters.clone());
            self.peers.update_voters(&control.voters);
            self.execute(actions);
        }
        self.log.install_snapshot(end_offset_pos)?;
        self.last_snapshot_end_offset = end_offset_pos;
        // The records below the boundary arrive only as this artifact, so its
        // header is the only source for the create-time of the last one a
        // checkpoint rewritten at the same boundary would contain.
        self.last_snapshot_timestamp_ms = contents.last_contained_log_timestamp;
        self.last_applied_record_timestamp_ms = contents.last_contained_log_timestamp;
        self.installed_snapshot_epoch = Some(
            u32::try_from(epoch).expect("snapshot install admission requires a nonnegative epoch"),
        );
        let _ = self.image_tx.send(Arc::new(self.image.clone()));
        retain_recent_checkpoints(&self.data_dir);
        Ok(())
    }
}
