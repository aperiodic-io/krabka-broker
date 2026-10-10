//! The produce requests of one destination, coalesced: Kafka's
//! `ShareGroupDLQStateManager.SendThread` and `coalesceProduceRequests`.
//!
//! Every range that is dead-lettered has rounds of records to produce, and
//! each round is a batch for one partition of the dead-letter topic. A round
//! waits in the queue of the broker that leads its partition. While a request
//! is in flight to a broker, the rounds that arrive for it pile up, and when
//! the request completes they go out together: one produce request holds the
//! rounds of every range and every partition that the broker leads, however
//! many ranges they came from. The rounds for one partition become one batch,
//! because a produce request takes one batch for a partition.
//!
//! A request holds as many rounds as the topic's `max.message.bytes` lets one
//! batch hold. A round that would take the batch of its partition past the limit
//! waits for the next request, and the first round for a partition is always
//! taken, so a round that is over the limit by itself still goes out, and the
//! broker reports the limit. Each round is answered with the response to the
//! request that carried it, and the round reads its own partition's row, so a
//! range learns of its own records and no other's.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use async_trait::async_trait;
use krabka_metadata::NodeId;
use krabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{HEADER_LEN, Record, RecordBatch},
};
use tokio::sync::oneshot;

/// The `timeout.ms` of the produce request: Kafka's
/// `ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT`.
const PRODUCE_TIMEOUT_MS: i32 = 30_000;

/// What the response of a coalesced request comes back as, for each round in
/// it: the whole response, or why the request did not get one.
pub(super) type Answer = Result<Arc<ProduceResponse>, String>;

/// Where a coalesced produce request goes.
#[async_trait]
pub(super) trait ProduceTransport: Send + Sync + 'static {
    /// Sends `request` to `node`, and answers when the response is in, or
    /// says why there is none.
    async fn send(&self, node: NodeId, request: ProduceRequest) -> Result<ProduceResponse, String>;
}

/// One round of a range: a batch of dead-letter records for one partition of
/// the dead-letter topic.
#[derive(Clone)]
pub(super) struct Produce {
    pub(super) topic: String,
    pub(super) topic_id: uuid::Uuid,
    pub(super) partition: i32,
    /// The topic's `max.message.bytes`.
    pub(super) max_message_bytes: i32,
    /// The records of the round, numbered from zero, all stamped with the
    /// batch's timestamp: what `record::build_round` builds.
    pub(super) batch: RecordBatch,
    /// Runs when a request takes the round, before its answer, and later than
    /// the call if the round waits for the request in flight.
    pub(super) admitted: Admitted,
}

/// What runs when a request takes a round: Kafka's `recordProduceMetric`,
/// which `coalesceProduceRequests` calls for a handler when its data is
/// admitted into the outgoing request, and not for a handler it defers.
pub(super) type Admitted = Arc<dyn Fn() + Send + Sync>;

/// A round that waits for a request, and where its answer goes.
struct Pending {
    produce: Produce,
    done: oneshot::Sender<Answer>,
}

/// The rounds that wait for one broker, and whether a request to it is in
/// flight.
#[derive(Default)]
struct Queue {
    waiting: Vec<Pending>,
    sending: bool,
}

struct Inner<T> {
    transport: T,
    queues: Mutex<HashMap<NodeId, Queue>>,
}

/// The queues of the brokers that dead-letter rounds go to, and the task that
/// empties each one.
pub(super) struct Coalescer<T> {
    inner: Arc<Inner<T>>,
}

impl<T: ProduceTransport> Coalescer<T> {
    pub(super) fn new(transport: T) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                queues: Mutex::default(),
            }),
        }
    }

    /// Produces `produce` to `node`, in a request that may carry the rounds of
    /// other ranges too, and answers with that request's response.
    pub(super) async fn produce(&self, node: NodeId, produce: Produce) -> Answer {
        let (done, answer) = oneshot::channel();
        let start = {
            let mut queues = self.inner.queues();
            let queue = queues.entry(node).or_default();
            queue.waiting.push(Pending { produce, done });
            !std::mem::replace(&mut queue.sending, true)
        };
        if start {
            tokio::spawn(Arc::clone(&self.inner).drain(node));
        }
        answer
            .await
            .map_err(|_| "the dead-letter sender stopped".to_owned())?
    }
}

impl<T> Inner<T> {
    /// The queues, locked. A panic under the lock leaves them as
    /// [`AbandonedDrain`] can repair, so a poisoned lock is still usable.
    fn queues(&self) -> MutexGuard<'_, HashMap<NodeId, Queue>> {
        self.queues.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Ends the drain of a node that stops before its queue is empty: a panic in
/// [`pack`] or the transport, or the runtime dropping the task.
///
/// Without it `sending` would stay set, no drain would run for the node again,
/// and every writer to it would wait for an answer that never comes, holding
/// its permit, with its run left `ARCHIVING`. It clears `sending` and drops the
/// rounds that wait, which answers each of their writers with an error, so each
/// retries and starts a drain of its own. The rounds of the request that was
/// being sent are dropped by the unwinding, and are answered the same way.
struct AbandonedDrain<'a, T> {
    inner: &'a Inner<T>,
    node: NodeId,
    /// Set by the drain when it ends by finding the queue empty, which
    /// `take` has already recorded.
    finished: bool,
}

impl<T> Drop for AbandonedDrain<'_, T> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(queue) = self.inner.queues().get_mut(&self.node) {
            queue.waiting.clear();
            queue.sending = false;
        }
    }
}

impl<T: ProduceTransport> Inner<T> {
    /// Sends the waiting rounds of `node`, one request after another, until
    /// none wait. The request in flight is the only one to the node, so the
    /// rounds that arrive during it are what the next request carries.
    async fn drain(self: Arc<Self>, node: NodeId) {
        let mut guard = AbandonedDrain {
            inner: &self,
            node,
            finished: false,
        };
        loop {
            // The writers that are ready to send get to join the request.
            tokio::task::yield_now().await;
            let Some(Packed { request, answers }) = self.take(node) else {
                guard.finished = true;
                return;
            };
            let response = self.transport.send(node, request).await.map(Arc::new);
            for done in answers {
                // A writer that gave up has dropped its end; the records it
                // sent are in the request regardless.
                let _ = done.send(response.clone());
            }
        }
    }

    /// The next request of `node`, or `None` when nothing waits, which ends
    /// the drain. A round that did not fit stays for the request after it.
    fn take(&self, node: NodeId) -> Option<Packed> {
        let mut queues = self.queues();
        let queue = queues.entry(node).or_default();
        let (packed, deferred) = pack(std::mem::take(&mut queue.waiting));
        queue.waiting = deferred;
        if packed.answers.is_empty() {
            queue.sending = false;
            return None;
        }
        Some(packed)
    }
}

/// A produce request, and where the answer for each round in it goes.
struct Packed {
    request: ProduceRequest,
    answers: Vec<oneshot::Sender<Answer>>,
}

/// The rounds of one partition that a request carries, as one batch.
struct PartitionBatch {
    topic: String,
    topic_id: uuid::Uuid,
    partition: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    /// The size of the batch as it goes out: the batch header and the records.
    size: i64,
    records: Vec<Record>,
}

impl PartitionBatch {
    fn start(produce: Produce) -> Self {
        let Produce {
            topic,
            topic_id,
            partition,
            batch,
            ..
        } = produce;
        let mut records = batch.records;
        let size = renumber(&mut records, 0, 0);
        Self {
            topic,
            topic_id,
            partition,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            size: i64::try_from(HEADER_LEN)
                .unwrap_or(i64::MAX)
                .saturating_add(size),
            records,
        }
    }

    /// Adds the records of `produce` after those of the batch, unless that takes
    /// the batch past the topic's `max.message.bytes`, in which case it gives
    /// the round back, untouched.
    ///
    /// The batch is counted as it will go out, with the records renumbered and
    /// their timestamps counted from the batch's own base, so the check is on
    /// the size that the broker sees rather than on the sum of the rounds.
    fn merge(&mut self, mut produce: Produce) -> Option<Produce> {
        // The rounds are taken oldest first (see `pack`), so this is never
        // before the base of the batch.
        let timestamp_delta = produce.batch.base_timestamp - self.base_timestamp;
        let first_delta = i32::try_from(self.records.len()).unwrap_or(i32::MAX);
        let added = renumber(&mut produce.batch.records, first_delta, timestamp_delta);
        if self.size.saturating_add(added) > i64::from(produce.max_message_bytes) {
            renumber(&mut produce.batch.records, 0, 0);
            return Some(produce);
        }
        self.size = self.size.saturating_add(added);
        self.max_timestamp = self.max_timestamp.max(produce.batch.max_timestamp);
        self.records.append(&mut produce.batch.records);
        None
    }

    /// The batch as a row of the request.
    fn into_wire(self) -> (String, WireUuid, PartitionProduceData) {
        let count = i32::try_from(self.records.len()).unwrap_or(i32::MAX);
        let batch = RecordBatch {
            last_offset_delta: count - 1,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            records: self.records,
            ..Default::default()
        };
        let data = PartitionProduceData {
            index: self.partition,
            records: Some(batch.into()),
            ..Default::default()
        };
        (self.topic, WireUuid(*self.topic_id.as_bytes()), data)
    }
}

/// Numbers `records` from `first_delta`, gives each of them `timestamp_delta`,
/// and returns their size on the wire, which the numbers change.
fn renumber(records: &mut [Record], first_delta: i32, timestamp_delta: i64) -> i64 {
    let mut size = 0_i64;
    for (delta, record) in (first_delta..).zip(records) {
        record.offset_delta = delta;
        record.timestamp_delta = timestamp_delta;
        size = size.saturating_add(i64::try_from(record.encoded_len()).unwrap_or(i64::MAX));
    }
    size
}

/// Builds one request from the `waiting` rounds of a node: Kafka's
/// `coalesceProduceRequests`. It returns the request and the rounds that did
/// not fit, which wait for the next.
///
/// The oldest rounds are taken first, so that a round that was retried, and
/// so has an older timestamp than the rounds beside it, does not put a
/// negative timestamp delta in a record. The order is otherwise the order the
/// rounds arrived in, and so is the order of the partitions in the request.
fn pack(mut waiting: Vec<Pending>) -> (Packed, Vec<Pending>) {
    waiting.sort_by_key(|pending| pending.produce.batch.base_timestamp);
    let mut partitions: Vec<PartitionBatch> = Vec::new();
    let mut answers = Vec::with_capacity(waiting.len());
    let mut deferred = Vec::new();
    for Pending { produce, done } in waiting {
        let admitted = Arc::clone(&produce.admitted);
        let existing = partitions.iter_mut().find(|batch| {
            batch.topic_id == produce.topic_id && batch.partition == produce.partition
        });
        let refused = if let Some(batch) = existing {
            batch.merge(produce)
        } else {
            partitions.push(PartitionBatch::start(produce));
            None
        };
        match refused {
            None => {
                admitted();
                answers.push(done);
            }
            Some(produce) => deferred.push(Pending { produce, done }),
        }
    }
    let mut topic_data: Vec<TopicProduceData> = Vec::new();
    for partition in partitions {
        let (name, topic_id, data) = partition.into_wire();
        match topic_data
            .iter_mut()
            .find(|topic| topic.topic_id == topic_id)
        {
            Some(topic) => topic.partition_data.push(data),
            None => topic_data.push(TopicProduceData {
                name,
                topic_id,
                partition_data: vec![data],
                ..Default::default()
            }),
        }
    }
    let request = ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: PRODUCE_TIMEOUT_MS,
        topic_data,
        ..Default::default()
    };
    (Packed { request, answers }, deferred)
}

/// A broker that the tests send requests to.
#[cfg(test)]
pub(super) mod test_support {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use krabka_metadata::NodeId;
    use krabka_protocol::owned::{
        produce_request::ProduceRequest, produce_response::ProduceResponse,
    };
    use tokio::sync::Semaphore;

    use super::ProduceTransport;

    type Script = dyn Fn(usize, &ProduceRequest) -> Result<ProduceResponse, String> + Send + Sync;

    /// A transport that keeps the requests it is sent, holds each in flight
    /// until it is released, and answers from a script: the closure gets the
    /// index of the request and the request.
    #[derive(Clone)]
    pub(in crate::share_partition::dlq) struct FakeBroker {
        sent: Arc<Mutex<Vec<(NodeId, ProduceRequest)>>>,
        gate: Arc<Semaphore>,
        script: Arc<Script>,
    }

    impl FakeBroker {
        /// A broker that answers each request as `script` says, at once.
        pub(in crate::share_partition::dlq) fn answering(
            script: impl Fn(usize, &ProduceRequest) -> Result<ProduceResponse, String>
            + Send
            + Sync
            + 'static,
        ) -> Self {
            Self {
                sent: Arc::default(),
                gate: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
                script: Arc::new(script),
            }
        }

        /// A broker whose requests stay in flight until [`Self::release`].
        pub(in crate::share_partition::dlq) fn held() -> Self {
            Self::held_answering(|_, _| Ok(ProduceResponse::default()))
        }

        /// A broker that holds each request until [`Self::release`], and then
        /// answers as `script` says.
        pub(in crate::share_partition::dlq) fn held_answering(
            script: impl Fn(usize, &ProduceRequest) -> Result<ProduceResponse, String>
            + Send
            + Sync
            + 'static,
        ) -> Self {
            Self {
                gate: Arc::new(Semaphore::new(0)),
                ..Self::answering(script)
            }
        }

        /// Lets every request, now and later, answer.
        pub(in crate::share_partition::dlq) fn release(&self) {
            self.gate.add_permits(1_000);
        }

        pub(in crate::share_partition::dlq) fn sent(&self) -> Vec<(NodeId, ProduceRequest)> {
            self.sent.lock().expect("sent lock").clone()
        }
    }

    #[async_trait]
    impl ProduceTransport for FakeBroker {
        async fn send(
            &self,
            node: NodeId,
            request: ProduceRequest,
        ) -> Result<ProduceResponse, String> {
            let index = {
                let mut sent = self.sent.lock().expect("sent lock");
                sent.push((node, request.clone()));
                sent.len() - 1
            };
            self.gate.acquire().await.expect("gate is open").forget();
            (self.script)(index, &request)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use futures_util::future::join_all;

    use super::{test_support::FakeBroker, *};

    /// A `max.message.bytes` that leaves room for any round of these tests.
    const ROOMY: i32 = 1_048_588;

    /// A round of records with `values` for partition `partition` of the topic
    /// `dlq.<id>`, numbered from zero, all stamped `timestamp`: what
    /// `record::build_round` builds.
    use krabka_ids::PartitionIndex;

    use crate::test_support::UnixMillis;

    #[derive(Clone, Copy)]
    struct FixtureTopicId(u8);

    #[derive(Clone, Copy)]
    struct MessageByteLimit(i32);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct RoundSetup<'a> {
        #[default(FixtureTopicId(1))]
        id: FixtureTopicId,
        #[default(PartitionIndex(0))]
        partition: PartitionIndex,
        #[default(UnixMillis(1_000))]
        timestamp: UnixMillis,
        #[default(&["a0"])]
        values: &'a [&'a str],
        #[default(MessageByteLimit(ROOMY))]
        limit: MessageByteLimit,
    }

    fn round(setup: RoundSetup<'_>) -> Produce {
        let RoundSetup {
            id,
            partition,
            timestamp,
            values,
            limit,
        } = setup;
        let id = id.0;
        let records: Vec<(i64, &str)> = values.iter().map(|value| (0, *value)).collect();
        Produce {
            topic: format!("dlq.{id}"),
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            partition: partition.0,
            max_message_bytes: limit.0,
            batch: batch_of(timestamp.0, timestamp.0, &records),
            admitted: Arc::new(|| {}),
        }
    }

    /// `produce`, with a count of the requests that took it as it stands.
    fn counted(produce: Produce) -> (Produce, Arc<std::sync::atomic::AtomicUsize>) {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let admitted = Arc::clone(&count);
        let produce = Produce {
            admitted: Arc::new(move || {
                admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
            ..produce
        };
        (produce, count)
    }

    /// A batch of records, each with a timestamp delta and a value.
    fn batch_of(base_timestamp: i64, max_timestamp: i64, records: &[(i64, &str)]) -> RecordBatch {
        RecordBatch {
            last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
            base_timestamp,
            max_timestamp,
            records: records
                .iter()
                .enumerate()
                .map(|(delta, (timestamp_delta, value))| Record {
                    offset_delta: i32::try_from(delta).unwrap(),
                    timestamp_delta: *timestamp_delta,
                    value: Some(Bytes::copy_from_slice(value.as_bytes())),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn partition_data(index: i32, batch: RecordBatch) -> PartitionProduceData {
        PartitionProduceData {
            index,
            records: Some(batch.into()),
            ..Default::default()
        }
    }

    /// The request that carries `partitions` of the topic `dlq.<id>`.
    fn request(id: u8, partitions: Vec<PartitionProduceData>) -> ProduceRequest {
        ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 30_000,
            topic_data: vec![TopicProduceData {
                name: format!("dlq.{id}"),
                topic_id: WireUuid([id; 16]),
                partition_data: partitions,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// What the healthy broker answers every round with.
    fn reached() -> Arc<ProduceResponse> {
        Arc::new(ProduceResponse::default())
    }

    fn healthy() -> (FakeBroker, Coalescer<FakeBroker>) {
        let broker = FakeBroker::answering(|_, _| Ok(ProduceResponse::default()));
        let coalescer = Coalescer::new(broker.clone());
        (broker, coalescer)
    }

    /// Releases the transport and joins these rounds in their original order.
    async fn release_and_join(
        broker: &FakeBroker,
        [first, second, third]: [tokio::task::JoinHandle<Answer>; 3],
    ) -> [Answer; 3] {
        broker.release();
        [
            first.await.unwrap(),
            second.await.unwrap(),
            third.await.unwrap(),
        ]
    }

    fn spawn_round(
        coalescer: &Arc<Coalescer<FakeBroker>>,
        partition: i32,
        value: &'static str,
    ) -> tokio::task::JoinHandle<Answer> {
        let coalescer = Arc::clone(coalescer);
        tokio::spawn(async move {
            coalescer
                .produce(
                    NodeId(1),
                    round(RoundSetup {
                        partition: PartitionIndex(partition),
                        values: &[value],
                        ..Default::default()
                    }),
                )
                .await
        })
    }

    /// Lets every task that can run, run.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    /// The rounds of every range and every partition that a broker leads go in
    /// one request, with the rounds for one partition as one batch, and a
    /// broker of its own gets a request of its own. Every round is answered
    /// with the response. A round that comes after the queue has emptied
    /// starts a request again.
    #[tokio::test(start_paused = true)]
    async fn the_rounds_for_one_broker_go_in_one_request() {
        let (broker, coalescer) = healthy();

        let answers = join_all([
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    values: &["a0", "a1"],
                    ..Default::default()
                }),
            ),
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    partition: PartitionIndex(1),
                    values: &["b0"],
                    ..Default::default()
                }),
            ),
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    values: &["c0"],
                    ..Default::default()
                }),
            ),
            coalescer.produce(
                NodeId(2),
                round(RoundSetup {
                    values: &["d0"],
                    ..Default::default()
                }),
            ),
        ])
        .await;
        let later = coalescer
            .produce(
                NodeId(1),
                round(RoundSetup {
                    values: &["e0"],
                    ..Default::default()
                }),
            )
            .await;

        // Which broker's queue empties first is the scheduler's choice, so the
        // requests are read by broker, each broker's in the order sent.
        let mut sent = broker.sent();
        sent.sort_by_key(|(node, _)| node.0);

        assert!(answers == vec![Ok(reached()); 4]);
        assert!(later == Ok(reached()));
        assert!(
            sent == vec![
                (
                    NodeId(1),
                    request(
                        1,
                        vec![
                            partition_data(
                                0,
                                batch_of(1_000, 1_000, &[(0, "a0"), (0, "a1"), (0, "c0")])
                            ),
                            partition_data(1, batch_of(1_000, 1_000, &[(0, "b0")])),
                        ]
                    ),
                ),
                (
                    NodeId(1),
                    request(
                        1,
                        vec![partition_data(0, batch_of(1_000, 1_000, &[(0, "e0")]))]
                    ),
                ),
                (
                    NodeId(2),
                    request(
                        1,
                        vec![partition_data(0, batch_of(1_000, 1_000, &[(0, "d0")]))]
                    ),
                ),
            ]
        );
    }

    /// Only one request is in flight to a broker. The rounds that arrive while
    /// it is wait, and go out together when it answers, as Kafka's `inFlight`
    /// set holds them.
    #[tokio::test(start_paused = true)]
    async fn rounds_that_arrive_during_a_request_share_the_next() {
        let broker = FakeBroker::held();
        let coalescer = Arc::new(Coalescer::new(broker.clone()));
        let send = |partition, value| spawn_round(&coalescer, partition, value);

        let first = send(0, "a0");
        settle().await;
        let second = send(1, "b0");
        let third = send(2, "c0");
        settle().await;
        let in_flight = broker.sent().len();
        let answers = release_and_join(&broker, [first, second, third]).await;

        assert!(in_flight == 1);
        assert!(answers.to_vec() == vec![Ok(reached()); 3]);
        assert!(
            broker.sent()
                == vec![
                    (
                        NodeId(1),
                        request(
                            1,
                            vec![partition_data(0, batch_of(1_000, 1_000, &[(0, "a0")]))]
                        ),
                    ),
                    (
                        NodeId(1),
                        request(
                            1,
                            vec![
                                partition_data(1, batch_of(1_000, 1_000, &[(0, "b0")])),
                                partition_data(2, batch_of(1_000, 1_000, &[(0, "c0")])),
                            ]
                        ),
                    ),
                ]
        );
    }

    /// A batch holds as many rounds as `max.message.bytes` lets it: the size
    /// that counts is that of the batch as it goes out, with one batch header
    /// and the records renumbered, so the limit is met exactly, and a round
    /// over it waits, untouched, for the next request. The first round for a
    /// partition is always taken, so a limit that no round fits still lets
    /// each one out.
    #[tokio::test(start_paused = true)]
    async fn a_batch_holds_the_rounds_that_the_topic_limit_lets_it() {
        let together = batch_of(1_000, 1_000, &[(0, "a0"), (0, "b0")]);
        let a = || partition_data(0, batch_of(1_000, 1_000, &[(0, "a0")]));
        let b = || partition_data(0, batch_of(1_000, 1_000, &[(0, "b0")]));
        let merged = request(1, vec![partition_data(0, together.clone())]);
        let split = vec![request(1, vec![a()]), request(1, vec![b()])];
        let fit = i32::try_from(together.encoded_len()).unwrap();
        let cases = [(fit, vec![merged]), (fit - 1, split.clone()), (1, split)];

        for (limit, expected) in cases {
            let (broker, coalescer) = healthy();

            let answers = join_all([
                coalescer.produce(
                    NodeId(1),
                    round(RoundSetup {
                        limit: MessageByteLimit(limit),
                        ..Default::default()
                    }),
                ),
                coalescer.produce(
                    NodeId(1),
                    round(RoundSetup {
                        values: &["b0"],
                        limit: MessageByteLimit(limit),
                        ..Default::default()
                    }),
                ),
            ])
            .await;

            assert!(answers == vec![Ok(reached()); 2], "limit {limit}");
            assert!(
                broker
                    .sent()
                    .into_iter()
                    .map(|(_, request)| request)
                    .collect::<Vec<_>>()
                    == expected,
                "limit {limit}"
            );
        }
    }

    /// A round that was retried is older than the rounds beside it. The batch
    /// counts its timestamps from the oldest, so no record has a negative
    /// timestamp delta, and each record keeps the time its round was built.
    #[tokio::test(start_paused = true)]
    async fn the_oldest_round_sets_the_base_timestamp_of_the_batch() {
        let (broker, coalescer) = healthy();

        join_all([
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    timestamp: UnixMillis(2_000),
                    values: &["new"],
                    ..Default::default()
                }),
            ),
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    values: &["old"],
                    ..Default::default()
                }),
            ),
        ])
        .await;

        assert!(
            broker.sent()
                == vec![(
                    NodeId(1),
                    request(
                        1,
                        vec![partition_data(
                            0,
                            batch_of(1_000, 2_000, &[(0, "old"), (1_000, "new")])
                        )]
                    ),
                )]
        );
    }

    /// A request that gets no response fails every round in it with the
    /// reason, and does not wedge the queue: the next round is sent.
    #[tokio::test(start_paused = true)]
    async fn a_request_that_fails_answers_each_round_and_the_queue_goes_on() {
        let broker = FakeBroker::answering(|index, _| match index {
            0 => Err("connection refused".to_owned()),
            _ => Ok(ProduceResponse::default()),
        });
        let coalescer = Coalescer::new(broker.clone());

        let failed = join_all([
            coalescer.produce(NodeId(1), round(RoundSetup::default())),
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    id: FixtureTopicId(2),
                    values: &["b0"],
                    ..Default::default()
                }),
            ),
        ])
        .await;
        let next = coalescer
            .produce(NodeId(1), round(RoundSetup::default()))
            .await;

        assert!(failed == vec![Err("connection refused".to_owned()); 2]);
        assert!(next == Ok(reached()));
        assert!(broker.sent().len() == 2);
    }

    /// A request that panics does not wedge the queue of its broker. The
    /// rounds in it, and the rounds that wait behind it, are answered with an
    /// error, so that their writers retry, and the round after them is sent.
    /// Without that, a writer would wait for an answer for ever and its run
    /// would stay `ARCHIVING`.
    #[tokio::test(start_paused = true)]
    async fn a_request_that_panics_answers_each_round_and_the_queue_goes_on() {
        let broker = FakeBroker::held_answering(|index, _| match index {
            0 => panic!("the transport broke"),
            _ => Ok(ProduceResponse::default()),
        });
        let coalescer = Arc::new(Coalescer::new(broker.clone()));
        let send = |partition, value| spawn_round(&coalescer, partition, value);
        let limit = std::time::Duration::from_secs(60);

        let in_flight = send(0, "a0");
        settle().await;
        let waiting = send(1, "b0");
        settle().await;
        broker.release();
        let stopped = tokio::time::timeout(limit, async {
            [in_flight.await.unwrap(), waiting.await.unwrap()]
        })
        .await;
        let next = tokio::time::timeout(
            limit,
            coalescer.produce(
                NodeId(1),
                round(RoundSetup {
                    partition: PartitionIndex(2),
                    values: &["c0"],
                    ..Default::default()
                }),
            ),
        )
        .await;

        assert!(stopped == Ok(std::array::from_fn(|_| Err(STOPPED.to_owned()))));
        assert!(next == Ok(Ok(reached())));
        assert!(broker.sent().len() == 2);
    }

    /// What a round is answered with when its sender is gone.
    const STOPPED: &str = "the dead-letter sender stopped";

    /// A round counts when a request takes it, as Kafka's
    /// `coalesceProduceRequests` counts a handler: while that request is in
    /// flight, not while the round waits for the request in flight before it,
    /// and not while the batch is too full for it and it waits for the request
    /// after.
    #[tokio::test(start_paused = true)]
    async fn a_round_counts_when_a_request_takes_it() {
        // A limit that one record of a batch fits under, and two do not.
        let two = batch_of(1_000, 1_000, &[(0, "b0"), (0, "c0")]);
        let limit = i32::try_from(two.encoded_len()).unwrap() - 1;
        let rounds = ["a0", "b0", "c0"].map(|value| {
            counted(round(RoundSetup {
                values: &[value],
                limit: MessageByteLimit(limit),
                ..Default::default()
            }))
        });
        let counts: Vec<_> = rounds.iter().map(|(_, count)| Arc::clone(count)).collect();
        let read = |counts: &[Arc<std::sync::atomic::AtomicUsize>]| -> Vec<usize> {
            counts
                .iter()
                .map(|count| count.load(std::sync::atomic::Ordering::SeqCst))
                .collect()
        };
        // What the rounds count at the moment each request is answered.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::held_answering({
            let (counts, seen) = (counts.clone(), Arc::clone(&seen));
            move |_, _| {
                seen.lock().expect("seen lock").push(read(&counts));
                Ok(ProduceResponse::default())
            }
        });
        let coalescer = Arc::new(Coalescer::new(broker.clone()));
        let send = |produce: Produce| {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move { coalescer.produce(NodeId(1), produce).await })
        };
        let [(first, _), (second, _), (third, _)] = rounds;

        let first = send(first);
        settle().await;
        let in_flight = read(&counts);
        let (second, third) = (send(second), send(third));
        settle().await;
        let waiting = read(&counts);
        let answers = release_and_join(&broker, [first, second, third]).await;

        assert!(in_flight == vec![1, 0, 0]);
        assert!(waiting == vec![1, 0, 0]);
        assert!(answers.to_vec() == vec![Ok(reached()); 3]);
        assert!(
            *seen.lock().expect("seen lock") == vec![vec![1, 0, 0], vec![1, 1, 0], vec![1, 1, 1]]
        );
    }
}
