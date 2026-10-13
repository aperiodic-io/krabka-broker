//! Live consumer state for one [`MetadataEventLog::subscribe`] call.
//!
//! A subscription owns one cancellable manual-`Fetch` task per assigned
//! partition. Each task drives its own dedicated
//! [`krabka_client_core::Connection`], because the broker is serial per
//! connection and a long-`max_wait_ms` fetch on a shared socket would
//! head-of-line-block every other RPC. All tasks emit
//! [`MetadataEventRecord`]s into one shared queue, and the
//! [`AssignmentHandle`] this module implements adds and removes partitions
//! while the subscription is live.
//!
//! [`MetadataEventLog::subscribe`]: crate::log::MetadataEventLog::subscribe

use {
    crate::{
        kafka_log::{METADATA_TOPIC_SCOPE, config::MetadataEventQueueCapacity, partition_leader},
        log::{AssignmentHandle, MetadataEventRecord, PartitionStart},
    },
    krabka_client_core::{
        Client, ClientFrameMax, ConnectionDispatchQueueCapacity, FetchMinBytes, IsolatedFetch,
    },
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    krabka_units::prelude::{ByteSize, Time, TimeExt as _},
    std::{
        collections::HashMap,
        future::Future,
        sync::{Arc, Mutex as StdMutex},
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{instrument, warn},
};

/// Per-subscription live consumer: one cancellable fetch task per assigned
/// partition, and every task emits into the shared `tx`.
pub(super) struct ConsumerState {
    pub(super) bootstrap: String,
    pub(super) client_id: String,
    pub(super) security: Option<krabka_client_core::security::ClientSecurity>,
    pub(super) topic: String,
    pub(super) topic_id: WireUuid,
    pub(super) tx: mpsc::Sender<MetadataEventRecord>,
    pub(super) fetch_max_wait: Time,
    pub(super) fetch_max_bytes: ByteSize,
    pub(super) fetch_retry_backoff: Time,
    pub(super) dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub(super) frame_max: ClientFrameMax,
    /// partition -> cancel token for its fetch task.
    pub(super) tasks: StdMutex<HashMap<i32, CancellationToken>>,
}

impl ConsumerState {
    pub(super) fn spawn_partition(self: &Arc<Self>, start: PartitionStart) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        if tasks.contains_key(&start.partition) {
            return; // already assigned
        }
        let cancel = CancellationToken::new();
        tasks.insert(start.partition, cancel.clone());
        tokio::spawn(partition_fetch_loop(
            Arc::clone(self),
            start.partition,
            start.start_offset,
            cancel,
        ));
    }

    fn cancel_partition(&self, partition: i32) {
        if let Some(tok) = self
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .remove(&partition)
        {
            tok.cancel();
        }
    }

    pub(super) fn cancel_all(&self) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        for (_, tok) in tasks.drain() {
            tok.cancel();
        }
    }
}

pub(super) struct KafkaAssignmentHandle {
    pub(super) state: Arc<ConsumerState>,
}

impl AssignmentHandle for KafkaAssignmentHandle {
    fn add(&self, start: PartitionStart) { self.state.spawn_partition(start); }

    fn remove(&self, partition: i32) { self.state.cancel_partition(partition); }

    fn assigned(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .state
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

pub(super) fn metadata_event_channel(
    capacity: MetadataEventQueueCapacity,
) -> (
    mpsc::Sender<MetadataEventRecord>,
    mpsc::Receiver<MetadataEventRecord>,
) {
    mpsc::channel(capacity.capacity())
}

async fn connect_with_retry<T, E, F, Fut>(
    cancel: &CancellationToken,
    backoff: std::time::Duration,
    mut connect: F,
) -> Option<T>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    loop {
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            result = connect() => result,
        };
        match result {
            Ok(client) => return Some(client),
            Err(error) => warn!(%error, "metadata consumer: connect failed; retrying"),
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(backoff) => {}
        }
    }
}

/// Manual single-partition fetch loop over a dedicated connection.
///
/// A dedicated connection per partition keeps the metadata consumer off any
/// parkable or shared stream. The broker is serial per-connection, so a
/// long-`max_wait_ms` fetch must not head-of-line-block other RPCs.
// cargo-mutants: live-broker fetch loop over a real connection, not unit-testable
#[cfg_attr(test, mutants::skip)]
#[instrument(level = "debug", skip_all, fields(partition, start_offset))]
async fn partition_fetch_loop(
    state: Arc<ConsumerState>,
    partition: i32,
    start_offset: i64,
    cancel: CancellationToken,
) {
    let Some(client) = connect_with_retry(&cancel, state.fetch_retry_backoff.to_std(), || {
        Client::builder()
            .bootstrap(state.bootstrap.clone())
            .client_id(state.client_id.clone())
            .dispatch_queue_capacity(state.dispatch_queue_capacity.get())
            .frame_max(state.frame_max.size())
            .maybe_security(state.security.clone())
            .metadata_scope(METADATA_TOPIC_SCOPE)
            .build()
    })
    .await
    else {
        return;
    };
    client.metadata_topics().set([state.topic.as_str()]);

    let mut next_offset = start_offset.max(0);
    let mut leader = None;
    loop {
        if leader.is_none() {
            match client.refresh_metadata().await {
                Ok(metadata) => {
                    leader = partition_leader(&metadata, &state.topic, partition);
                    if leader.is_none() {
                        warn!(
                            partition,
                            "metadata consumer: partition leader is unavailable"
                        );
                        tokio::time::sleep(state.fetch_retry_backoff.to_std()).await;
                        continue;
                    }
                }
                Err(error) => {
                    warn!(%error, partition, "metadata consumer: metadata refresh failed");
                    tokio::time::sleep(state.fetch_retry_backoff.to_std()).await;
                    continue;
                }
            }
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return;
            }
            res = client.fetch_partition_with_isolation_on(leader.expect("leader resolved above"), IsolatedFetch {
                topic: &state.topic,
                topic_id: state.topic_id,
                partition,
                fetch_offset: next_offset,
                max_wait: state.fetch_max_wait,
                max: krabka_client_core::DEFAULT_FETCH_RESPONSE_MAX,
                partition_max: state.fetch_max_bytes,
                fetch_min: FetchMinBytes::default(),
                isolation_level: 0,
            }) => {
                match res {
                    Ok(records) => {
                        for r in records {
                            // Re-check cancellation before every send: a
                            // remove() (for reassignment) that fires
                            // after fetch_partition resolved must not flush
                            // the rest of an already-fetched batch, or a
                            // task spawned on re-add from a new start_offset
                            // would double-deliver these same records.
                            if cancel.is_cancelled() {
                                return;
                            }
                            if r.offset < next_offset {
                                continue; // defensive: never go backwards
                            }
                            let tombstone = r.value.is_none();
                            let record = MetadataEventRecord {
                                partition,
                                offset: r.offset,
                                key: r.key,
                                payload: r.value.unwrap_or_default(),
                                tombstone,
                            };
                            next_offset = r.offset + 1;
                            let sent = tokio::select! {
                                biased;
                                () = cancel.cancelled() => return,
                                result = state.tx.send(record) => result,
                            };
                            if sent.is_err() {
                                return; // stream dropped
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, partition, "metadata consumer: fetch failed; retrying");
                        leader = None;
                        tokio::time::sleep(state.fetch_retry_backoff.to_std()).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::kafka_log::config::METADATA_TOPIC,
        assert2::{assert, check},
        krabka_units::prelude::{mebibytes, millis},
    };

    #[tokio::test]
    async fn initial_connection_retries_without_reassignment() {
        let cancel = CancellationToken::new();
        let mut attempts = 0;
        let client = connect_with_retry(&cancel, std::time::Duration::ZERO, || {
            attempts += 1;
            std::future::ready(if attempts == 1 {
                Err("temporary connection failure")
            } else {
                Ok(42)
            })
        })
        .await;
        assert!(client == Some(42));
        assert!(attempts == 2);
    }

    #[tokio::test]
    async fn initial_connection_and_retry_delay_are_cancellable() {
        let cancel = CancellationToken::new();
        let pending = connect_with_retry::<(), &str, _, _>(
            &cancel,
            std::time::Duration::from_secs(3600),
            std::future::pending,
        );
        tokio::pin!(pending);
        tokio::select! {
            biased;
            result = &mut pending => panic!("pending connection completed: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        cancel.cancel();
        assert!(pending.await.is_none());

        let cancel = CancellationToken::new();
        let started = tokio::sync::Notify::new();
        let retrying = connect_with_retry::<(), _, _, _>(
            &cancel,
            std::time::Duration::from_secs(3600),
            || {
                started.notify_one();
                std::future::ready(Err("temporary connection failure"))
            },
        );
        tokio::pin!(retrying);
        tokio::select! {
            biased;
            result = &mut retrying => panic!("retry ended: {result:?}"),
            () = started.notified() => {}
        }
        cancel.cancel();
        assert!(retrying.await.is_none());
    }

    #[test]
    fn metadata_event_channel_uses_configured_capacity() {
        let (tx, _rx) = metadata_event_channel(MetadataEventQueueCapacity::new(2048).unwrap());
        check!(tx.max_capacity() == 2048);
    }

    #[tokio::test]
    async fn assignment_handle_tracks_spawned_partitions() {
        let (tx, _rx) = mpsc::channel::<MetadataEventRecord>(1);
        let state = Arc::new(ConsumerState {
            bootstrap: "invalid-bootstrap".into(),
            client_id: "test-consumer".into(),
            security: None,
            topic: METADATA_TOPIC.into(),
            topic_id: WireUuid::ZERO,
            tx,
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
            tasks: StdMutex::new(HashMap::new()),
        });
        check!(state.fetch_max_wait == millis(750));
        check!(state.fetch_max_bytes == mebibytes(2));
        check!(state.fetch_retry_backoff == millis(300));
        let handle = KafkaAssignmentHandle {
            state: Arc::clone(&state),
        };

        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 7,
        });
        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 9,
        });
        handle.add(PartitionStart {
            partition: 0,
            start_offset: 0,
        });
        handle.remove(2);

        assert!(handle.assigned() == vec![0]);
        state.cancel_all();
    }

    #[test]
    fn partition_fetches_route_to_the_metadata_leader() {
        use krabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        };

        let metadata = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some(METADATA_TOPIC.into()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 2,
                    leader_id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        check!(partition_leader(&metadata, METADATA_TOPIC, 2) == Some(9));
        check!(partition_leader(&metadata, METADATA_TOPIC, 1) == None);
    }
}
