//! `SharePersister`, the group-coordinator's client onto the share-state
//! persister (KIP-932). It bridges the share-group membership coordinator to
//! the durable [`ShareCoordinator`]:
//!
//! - When a share group joins a topic, the coordinator calls
//!   [`SharePersister::initialize`] for each newly-assigned `(topic, partition)`.
//! - When `DeleteGroups` deletes an empty share group, the coordinator calls
//!   [`SharePersister::delete`] for each initialized `(topic, partition)`.
//!   It also calls it for each initialized partition of a topic that the
//!   metadata image no longer holds.
//!
//! Routing mirrors [`crate::handlers::find_coordinator`]:
//! [`ShareCoordinator::state_partition_for`] maps the share key
//! `(group, topic_id, partition)` to a `__share_group_state` partition. If
//! this broker leads that state partition, the call dispatches into the local
//! [`ShareCoordinator`] directly. If it does not, the client resolves the
//! partition leader from the metadata image and sends the typed
//! `InitializeShareGroupState` or `DeleteShareGroupState` RPC over the
//! inter-broker client. That is the same shape as the `WriteTxnMarkers` dial
//! in [`crate::txn::handlers::end_txn`].
//!
//! This module returns errors to the caller. The lifecycle hook treats them as
//! best-effort and retries on the next heartbeat. It never fails the
//! heartbeat.

use std::{sync::Arc, time::Duration};

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::NodeId;
use krabka_protocol::owned::{
    delete_share_group_state_request::{
        DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePartitionData,
    },
    delete_share_group_state_response::DeleteShareGroupStateResponse,
    initialize_share_group_state_request::{
        InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPartitionData,
    },
    initialize_share_group_state_response::InitializeShareGroupStateResponse,
    read_share_group_state_request::{
        PartitionData as ReadPartitionData, ReadShareGroupStateRequest, ReadStateData,
    },
    read_share_group_state_summary_request::{
        PartitionData as ReadSummaryPartitionData, ReadShareGroupStateSummaryRequest,
        ReadStateSummaryData,
    },
    write_share_group_state_request::{
        PartitionData as WritePartitionData, StateBatch as ProtoStateBatch,
        WriteShareGroupStateRequest, WriteStateData,
    },
    write_share_group_state_response::WriteShareGroupStateResponse,
};
use krabka_security::ListenerProtocol;

use crate::{
    auto_topic_creation::AutoTopicCreation,
    broker::registered_listener_endpoint,
    codes::{COORDINATOR_NOT_AVAILABLE, UNKNOWN_TOPIC_OR_PARTITION},
    error::BrokerError,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    share_coordinator::{
        bootstrap,
        coordinator::{
            LoadStatus, ShareCoordinator, ShareStateSummary, ShareWrite, UNINITIALIZED_START_OFFSET,
        },
        persistence::StateBatch,
        state::SharePartitionState,
    },
};

/// The persister RPCs share a singleton topic/partition envelope.
macro_rules! one_partition_request {
    ($request:ident, $topic:ident, $partition_type:ident; $group:expr, $topic_id:expr; $($fields:tt)*) => {
        $request {
            group_id: ($group).to_string(),
            topics: vec![$topic {
                topic_id: krabka_protocol::primitives::uuid::Uuid(*($topic_id).as_bytes()),
                partitions: vec![$partition_type { $($fields)* ..Default::default() }],
                ..Default::default()
            }],
            ..Default::default()
        }
    };
}

/// Group-coordinator-side client for the share-state persister. `Broker::start`
/// constructs it after both the [`ShareCoordinator`] and the
/// `GroupCoordinator` exist, and hands it to the `GroupCoordinator` so its
/// per-group share actors can drive Initialize and Delete lifecycle calls.
pub(crate) struct SharePersister {
    node_id: NodeId,
    share_coordinator: Arc<ShareCoordinator>,
    controller: Arc<dyn MetadataSource>,
    /// Creates `__share_group_state` when the first call needs it.
    auto_topic_creation: Arc<AutoTopicCreation>,
    inter_broker_client: Arc<InterBrokerClient>,
    inter_broker_listener_protocol: ListenerProtocol,
    inter_broker_listener_name: String,
}

impl std::fmt::Debug for SharePersister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePersister")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl SharePersister {
    pub(crate) fn new(
        node_id: NodeId,
        share_coordinator: Arc<ShareCoordinator>,
        controller: Arc<dyn MetadataSource>,
        auto_topic_creation: Arc<AutoTopicCreation>,
        inter_broker_client: Arc<InterBrokerClient>,
        inter_broker_listener_protocol: ListenerProtocol,
        inter_broker_listener_name: String,
    ) -> Self {
        Self {
            node_id,
            share_coordinator,
            controller,
            auto_topic_creation,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_listener_name,
        }
    }

    /// Initialize the share state for `(group, topic_id, partition)` at
    /// `state_epoch` and `start_offset`. The call is local when this broker
    /// leads the target `__share_group_state` partition. If it does not, the
    /// client routes the call to the leader over RPC.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Share`] if the local coordinator fences or
    /// rejects the call, or [`BrokerError`] from the inter-broker dial or send
    /// on the remote path. The caller logs and retries. It must not fail a
    /// heartbeat.
    // cargo-mutants: the `topics` payload is only sent on the follower->leader
    // remote path (`send_to_leader` dials a real inter-broker socket); only the
    // live-broker integration suite exercises it, not in-file unit tests.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn initialize(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: i32,
        start_offset: Offset,
    ) -> Result<(), BrokerError> {
        // Lazily create `__share_group_state` and refresh local leadership —
        // the lifecycle hook is the first thing to touch the topic when no
        // client has issued FindCoordinator(SHARE) yet. Mirrors how the txn
        // handlers refresh before dispatching.
        let (state_partition, local) = self.route(group, topic_id, partition).await?;
        if local {
            return self
                .share_coordinator
                .initialize(
                    &self.controller.current_image(),
                    group,
                    topic_id,
                    partition,
                    state_epoch,
                    start_offset,
                )
                .await
                .map_err(|error| {
                    BrokerError::Share(format!(
                        "InitializeShareGroupState {group}:{topic_id}:{partition} failed (code {}): {}",
                        error.code(),
                        error.row_message("initialize"),
                    ))
                });
        }

        let req = one_partition_request!(InitializeShareGroupStateRequest, InitializeStateData, InitPartitionData; group, topic_id;
            partition,
            state_epoch,
            start_offset: start_offset.0,
        );
        self.send_to_leader(state_partition, req, "InitializeShareGroupState", partition)
            .await
    }

    /// Delete the share state for `(group, topic_id, partition)`. The call is
    /// local when this broker leads the target `__share_group_state`
    /// partition. If it does not, the client routes the call to the leader
    /// over RPC.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`].
    pub(crate) async fn delete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), BrokerError> {
        let (state_partition, local) = self.route(group, topic_id, partition).await?;
        let result = if local {
            self.share_coordinator
                .delete(&self.controller.current_image(), group, topic_id, partition)
                .await
                .map_err(|error| {
                    refused(
                        "DeleteShareGroupState",
                        partition,
                        error.code(),
                        &error.row_message("delete"),
                    )
                })
        } else {
            let req = one_partition_request!(DeleteShareGroupStateRequest, DeleteStateData, DeletePartitionData; group, topic_id;
                partition,
            );
            self.send_to_leader(state_partition, req, "DeleteShareGroupState", partition)
                .await
        };
        // The coordinator refuses a topic that the image no longer holds, and
        // tombstones the state of that topic itself when it sees the
        // deletion (Kafka's `ShareCoordinatorService.handleTopicsDeletion`).
        // The group has nothing left to delete for it.
        match result {
            Err(BrokerError::SharePartitionState {
                code: UNKNOWN_TOPIC_OR_PARTITION,
                ..
            }) if self
                .controller
                .current_image()
                .topic_by_id(&topic_id)
                .is_none() =>
            {
                Ok(())
            }
            other => other,
        }
    }

    /// Refresh leadership before choosing the local coordinator or its remote leader.
    async fn route(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(PartitionIndex, bool), BrokerError> {
        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        self.ensure_topic_and_refresh(state_partition).await?;
        let local = self.share_coordinator.is_leader(state_partition).await;
        Ok((state_partition, local))
    }

    /// Ask for `__share_group_state` if it does not exist, and refresh this
    /// broker's view of which of its partitions it leads. The broker creates
    /// the topic in the background, with its configured partition count and
    /// replication factor ([`AutoTopicCreation`]), as Kafka's persister gets
    /// it created through `FindCoordinator(SHARE)`. The leadership refresh
    /// picks up every partition that the replicator supervisor has already
    /// materialized locally. When this broker leads `state_partition`, the
    /// method waits, up to the same deadline, until the load of that
    /// partition ends. A load that is still running at the deadline is not an
    /// error: the local coordinator then answers
    /// `COORDINATOR_LOAD_IN_PROGRESS`, and the caller retries.
    ///
    /// # Errors
    ///
    /// Every failure is `COORDINATOR_NOT_AVAILABLE`: no share coordinator can
    /// serve the call yet, which is what Kafka's persister reports when it
    /// cannot find the coordinator.
    async fn ensure_topic_and_refresh(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<(), BrokerError> {
        self.find_state_partition(state_partition)
            .await
            .map_err(|error| BrokerError::SharePartitionState {
                code: COORDINATOR_NOT_AVAILABLE,
                message: error.to_string(),
            })
    }

    async fn find_state_partition(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<(), BrokerError> {
        if self
            .controller
            .current_image()
            .topic(bootstrap::TOPIC)
            .is_none()
        {
            self.auto_topic_creation.request(bootstrap::TOPIC);
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let image = self.controller.current_image();
            drop(
                self.share_coordinator
                    .refresh_leader_partitions(&image)
                    .await,
            );
            let status = self.share_coordinator.load_status(state_partition).await;
            match image.partition(bootstrap::TOPIC, state_partition.get()) {
                Some(metadata) if metadata.leader != self.node_id => return Ok(()),
                Some(_) if status == Some(LoadStatus::Active) => return Ok(()),
                Some(_) if status.is_some() && tokio::time::Instant::now() >= deadline => {
                    return Ok(());
                }
                _ if tokio::time::Instant::now() >= deadline => {
                    return Err(BrokerError::Share(format!(
                        "timed out waiting for {}-{state_partition} to materialize locally",
                        bootstrap::TOPIC
                    )));
                }
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    /// Read the durable share state for `(group, topic_id, partition)`, as
    /// the share-partition leader at `leader_epoch`. The call is local when
    /// this broker leads the target `__share_group_state` partition. If it
    /// does not, the client routes the call to the leader over RPC and decodes
    /// the typed response.
    ///
    /// This is Kafka's `ReadShareGroupState`: the coordinator persists a new
    /// `leader_epoch`, which fences a share-partition leader with an older
    /// one, and it refuses a key that the group coordinator has not
    /// initialized. A `start_offset` of `UNINITIALIZED_START_OFFSET` is state:
    /// the group coordinator registered the partition and did not decide where
    /// the group starts.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::SharePartitionState`] when the coordinator
    /// answers a non-zero error code (among them `INVALID_REQUEST` for a key
    /// with no state), or [`BrokerError`] from the connect or send on the
    /// remote path.
    // Consumed by `SharePartitionLeaderManager::get_or_load`, which the
    // ShareFetch/ShareAcknowledge handlers drive.
    pub(crate) async fn read_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        leader_epoch: i32,
    ) -> Result<SharePartitionState, BrokerError> {
        let (state_partition, local) = self.route(group, topic_id, partition).await?;
        if local {
            return self
                .share_coordinator
                .read(
                    &self.controller.current_image(),
                    group,
                    topic_id,
                    partition,
                    leader_epoch,
                )
                .await
                .map_err(|error| {
                    refused(
                        "ReadShareGroupState",
                        partition,
                        error.code(),
                        &error.row_message("read"),
                    )
                });
        }

        let req = one_partition_request!(ReadShareGroupStateRequest, ReadStateData, ReadPartitionData; group, topic_id;
            partition,
            leader_epoch,
        );
        let resp = self.send_to_leader_resp(state_partition, req).await?;
        let pr = checked_partition(
            resp.results.into_iter().flat_map(|topic| topic.partitions),
            "ReadShareGroupState",
            partition,
            |row| (row.partition, row.error_code, row.error_message.as_deref()),
        )?;

        // The read response carries no leader epoch and no delivery complete
        // count.
        Ok(SharePartitionState {
            state_epoch: pr.state_epoch,
            leader_epoch,
            start_offset: Offset(pr.start_offset),
            delivery_complete_count: 0,
            state_batches: pr.state_batches.into_iter().map(StateBatch::from).collect(),
            ..SharePartitionState::default()
        })
    }

    /// Read the share state summary for `(group, topic_id, partition)`, with
    /// no side effect. `Ok(None)` means the key has no state.
    ///
    /// This is Kafka's `ReadShareGroupStateSummary`, which the admin offset
    /// RPCs use. Unlike [`SharePersister::read_state`] it does not change the
    /// stored leader epoch, and a key with no state is not an error.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::read_state`].
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, BrokerError> {
        let (state_partition, local) = self.route(group, topic_id, partition).await?;
        if local {
            return self
                .share_coordinator
                .read_summary(group, topic_id, partition)
                .await
                .map_err(|code| {
                    refused(
                        "ReadShareGroupStateSummary",
                        partition,
                        code,
                        "no error message",
                    )
                });
        }

        let req = one_partition_request!(ReadShareGroupStateSummaryRequest, ReadStateSummaryData, ReadSummaryPartitionData; group, topic_id;
            partition,
        );
        let resp = self.send_to_leader_resp(state_partition, req).await?;
        let pr = checked_partition(
            resp.results.into_iter().flat_map(|topic| topic.partitions),
            "ReadShareGroupStateSummary",
            partition,
            |row| (row.partition, row.error_code, row.error_message.as_deref()),
        )?;

        // The summary of a key with no state is the default row: epochs 0 and
        // start offset -1. An initialized key always has a state epoch above
        // 0, because the group coordinator stamps the group epoch.
        if pr.state_epoch == 0 && pr.start_offset == UNINITIALIZED_START_OFFSET {
            return Ok(None);
        }
        Ok(Some((
            pr.state_epoch,
            pr.leader_epoch,
            Offset(pr.start_offset),
            pr.delivery_complete_count,
        )))
    }

    /// Persist a `WriteShareGroupState` delta for `(group, topic_id,
    /// partition)`. The call is local when this broker leads the target
    /// `__share_group_state` partition. If it does not, the client routes the
    /// call to the leader over RPC.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`].
    // cargo-mutants: the `topics` payload is only sent on the follower->leader
    // remote path (`send_to_leader` dials a real inter-broker socket); only the
    // live-broker integration suite exercises it, not in-file unit tests.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn write_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        epochs: (i32, i32),
        progress: (Offset, i32),
        batches: Vec<StateBatch>,
    ) -> Result<(), BrokerError> {
        let (state_epoch, leader_epoch) = epochs;
        let (start_offset, delivery_complete_count) = progress;
        let (state_partition, local) = self.route(group, topic_id, partition).await?;
        if local {
            let request = ShareWrite {
                state_epoch,
                leader_epoch,
                start_offset,
                delivery_complete_count,
                batches,
            };
            return self
                .share_coordinator
                .write(
                    &self.controller.current_image(),
                    group,
                    topic_id,
                    partition,
                    request,
                )
                .await
                .map_err(|error| {
                    refused(
                        "WriteShareGroupState",
                        partition,
                        error.code(),
                        &error.row_message("write"),
                    )
                });
        }

        let req = one_partition_request!(WriteShareGroupStateRequest, WriteStateData, WritePartitionData; group, topic_id;
            partition,
            state_epoch,
            leader_epoch,
            start_offset: start_offset.0,
            delivery_complete_count,
            state_batches: batches
                .into_iter()
                .map(ProtoStateBatch::from)
                .collect(),
        );
        self.send_to_leader(state_partition, req, "WriteShareGroupState", partition)
            .await
    }

    /// Resolve the leader of `__share_group_state`-`state_partition` from the
    /// metadata image and open an inter-broker connection to it. Mirrors the
    /// endpoint resolution in `txn::handlers::end_txn`.
    async fn connect_to_leader(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<krabka_client_core::Connection, BrokerError> {
        let image = self.controller.current_image();
        let pr = image
            .partition(bootstrap::TOPIC, state_partition.get())
            .ok_or_else(|| BrokerError::SharePartitionState {
                code: COORDINATOR_NOT_AVAILABLE,
                message: format!(
                    "{}-{state_partition} not present in metadata image",
                    bootstrap::TOPIC
                ),
            })?;
        let leader = pr.leader;
        let broker_info = image
            .broker(leader)
            .ok_or_else(|| BrokerError::SharePartitionState {
                code: COORDINATOR_NOT_AVAILABLE,
                message: format!("share-state leader node {leader} not in metadata image"),
            })?;
        let (host, port) =
            registered_listener_endpoint(broker_info, &self.inter_broker_listener_name);
        let host = host.to_owned();

        let opts = krabka_client_core::ConnectionOptions {
            client_id: format!("krabka-broker-share-{}", self.node_id),
            ..krabka_client_core::ConnectionOptions::default()
        };
        self.inter_broker_client
            .connect_as_connection(
                &host,
                port,
                self.inter_broker_listener_protocol,
                "localhost",
                opts,
            )
            .await
            .map_err(|e| BrokerError::Share(format!("share-state connect to {host}:{port}: {e}")))
    }

    /// Send `req` to the `state_partition` leader and turn the leader's answer
    /// for `partition` into a `Result`. `what` names the RPC for the error
    /// message.
    ///
    /// A transport-level `Ok` says only that the bytes made the round trip.
    /// The leader reports whether it accepted the call in the per-partition
    /// `error_code`, and a rejection there has to reach the caller: the
    /// lifecycle hook records a partition as initialized on `Ok(())` and never
    /// looks at it again, so an unread `NOT_COORDINATOR` -- which is what a
    /// state-partition leader answers while its view of its own leadership is
    /// still catching up -- leaves the group permanently believing in share
    /// state that was never written.
    async fn send_to_leader<R>(
        &self,
        state_partition: PartitionIndex,
        req: R,
        what: &str,
        partition: i32,
    ) -> Result<(), BrokerError>
    where
        R: krabka_client_core::ProtocolRequest,
        R::Response: PartitionResults,
    {
        let resp = self.send_to_leader_resp(state_partition, req).await?;
        check_partition_result(resp, what, partition)
    }

    /// Send `req` to the `state_partition` leader and return the typed
    /// response.
    async fn send_to_leader_resp<R>(
        &self,
        state_partition: PartitionIndex,
        req: R,
    ) -> Result<R::Response, BrokerError>
    where
        R: krabka_client_core::ProtocolRequest,
    {
        let conn = self.connect_to_leader(state_partition).await?;
        let resp = conn
            .send(req)
            .await
            .map_err(|e| BrokerError::Share(format!("share-state RPC: {e}")))?;
        conn.close();
        Ok(resp)
    }
}

/// The three share-state write RPCs — `InitializeShareGroupState`,
/// `WriteShareGroupState` and `DeleteShareGroupState` — answer with the same
/// shape: one result per topic, each holding one `PartitionResult` per
/// partition. This trait is the one thing [`SharePersister::send_to_leader`]
/// needs from all three, so the remote path can read the leader's verdict
/// without a copy of the send per RPC.
trait PartitionResults {
    /// `(partition, error_code, error_message)` for every partition the leader
    /// answered for.
    fn partition_results(self) -> Vec<(i32, i16, Option<String>)>;
}

macro_rules! impl_partition_results {
    ($($response:ty),+ $(,)?) => {
        $(impl PartitionResults for $response {
            fn partition_results(self) -> Vec<(i32, i16, Option<String>)> {
                self.results
                    .into_iter()
                    .flat_map(|topic| topic.partitions)
                    .map(|p| (p.partition, p.error_code, p.error_message))
                    .collect()
            }
        })+
    };
}

impl_partition_results!(
    InitializeShareGroupStateResponse,
    WriteShareGroupStateResponse,
    DeleteShareGroupStateResponse,
);

/// The error for a share-state call on `partition` that the coordinator
/// refused with `error_code`. The share-partition leader maps the code as
/// Kafka's `SharePartition.fetchPersisterError` does.
fn refused(what: &str, partition: i32, error_code: i16, message: &str) -> BrokerError {
    BrokerError::SharePartitionState {
        code: error_code,
        message: format!("{what} for partition {partition} refused by the leader: {message}"),
    }
}

type PartitionStatus<'a> = (i32, i16, Option<&'a str>);

/// Finds the requested row and checks the leader's verdict without changing its payload.
fn checked_partition<T>(
    rows: impl IntoIterator<Item = T>,
    what: &str,
    partition: i32,
    status: impl Fn(&T) -> PartitionStatus<'_>,
) -> Result<T, BrokerError> {
    let row = rows
        .into_iter()
        .find(|row| status(row).0 == partition)
        .ok_or_else(|| {
            BrokerError::Share(format!(
                "{what} for partition {partition}: leader answered for no such partition"
            ))
        })?;
    let (_, code, message) = status(&row);
    if code != 0 {
        return Err(refused(
            what,
            partition,
            code,
            message.unwrap_or("no error message"),
        ));
    }
    Ok(row)
}

/// `Ok(())` only when the leader answered for `partition` with error code 0.
///
/// A non-zero code is the leader refusing the call — it is still loading, it
/// is no longer the coordinator, the state epoch is fenced. A missing entry is
/// no better: the caller asked about exactly one partition and got no verdict
/// for it, so there is no evidence the write landed. Both become an error, and
/// the lifecycle hook retries on the next heartbeat.
fn check_partition_result<R: PartitionResults>(
    resp: R,
    what: &str,
    partition: i32,
) -> Result<(), BrokerError> {
    checked_partition(resp.partition_results(), what, partition, |row| {
        (row.0, row.1, row.2.as_deref())
    })
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use krabka_protocol::{
        owned::initialize_share_group_state_response::{InitializeStateResult, PartitionResult},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::codes::{
        COORDINATOR_LOAD_IN_PROGRESS, FENCED_LEADER_EPOCH, FENCED_STATE_EPOCH, INVALID_REQUEST,
    };

    fn response(partitions: &[(i32, i16)]) -> InitializeShareGroupStateResponse {
        InitializeShareGroupStateResponse {
            results: vec![InitializeStateResult {
                topic_id: ProtoUuid([7; 16]),
                partitions: partitions
                    .iter()
                    .map(|&(partition, error_code)| PartitionResult {
                        partition,
                        error_code,
                        error_message: None,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn leader_accepting_the_partition_is_success() {
        assert2::assert!(check_partition_result(response(&[(0, 0)]), "Initialize", 0).is_ok());
    }

    /// The bug this guards: the group-coordinator lifecycle hook records a
    /// partition as initialized whenever the persister answers `Ok(())` and
    /// never revisits it. A refusal that reads as success therefore strands
    /// the share group without the state it believes it has, permanently.
    #[test]
    fn leader_refusing_the_partition_is_an_error() {
        // 16 is NOT_COORDINATOR: the state-partition leader has not caught up
        // with its own leadership yet. It is transient, and the caller only
        // retries if it hears about it.
        let error = check_partition_result(response(&[(0, 16)]), "Initialize", 0)
            .expect_err("a refused partition must not read as success");
        assert2::assert!(error.to_string().contains("16"));
    }

    /// A refused `Read`, `Write` or `ReadSummary` keeps the coordinator code,
    /// so the share-partition leader can map it as Kafka's
    /// `fetchPersisterError` does.
    #[test]
    fn a_refused_share_state_call_keeps_the_coordinator_code() {
        let cases = [
            ("ReadShareGroupState", INVALID_REQUEST),
            ("ReadShareGroupState", FENCED_LEADER_EPOCH),
            ("WriteShareGroupState", FENCED_STATE_EPOCH),
            ("ReadShareGroupStateSummary", COORDINATOR_LOAD_IN_PROGRESS),
        ];
        let actual: Vec<_> = cases
            .iter()
            .map(|&(what, code)| match refused(what, 3, code, "detail") {
                BrokerError::SharePartitionState { code, message } => Some((code, message)),
                _ => None,
            })
            .collect();
        let expected: Vec<_> = cases
            .iter()
            .map(|&(what, code)| {
                Some((
                    code,
                    format!("{what} for partition 3 refused by the leader: detail"),
                ))
            })
            .collect();
        assert2::assert!(actual == expected);
    }

    #[test]
    fn a_leader_silent_about_the_partition_is_an_error() {
        assert2::assert!(check_partition_result(response(&[(1, 0)]), "Initialize", 0).is_err());
        assert2::assert!(check_partition_result(response(&[]), "Initialize", 0).is_err());
    }
}
