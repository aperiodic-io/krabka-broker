//! Bounded offset-range reads for
//! [`MetadataEventLog::visit_range`](crate::log::MetadataEventLog::visit_range).
//!
//! A range read fetches one page at a time from the partition leader and hands
//! each record to the caller before it fetches the next page, so it holds one
//! page at most. It steps over compacted offsets by the batch progress each
//! `Fetch` reports, not by the records it returns. On a transient failure,
//! such as a leader change or a dropped connection, it looks the leader up
//! again and resumes at the offset it had reached. The read has no deadline of
//! its own, and the caller bounds it.
//!
//! [`RangeFetcher`] is the seam between that loop and the broker: the Kafka log
//! implements it over its broker pool, and the tests here script it.

use {
    crate::{
        error::MetadataLogError,
        log::{MetadataEventRecord, RangeVisitor},
    },
    krabka_client_core::{ClientError, FetchPartitionResult, FetchedRecord},
    std::time::Duration,
    tracing::warn,
};

/// The leader lookup and the `Fetch` that a range read makes.
pub(super) trait RangeFetcher: Sync {
    /// A connection to a partition leader.
    type Leader: Send + Sync;

    /// Look up the current leader of `partition` and connect to it.
    async fn leader(&self, partition: i32) -> Result<Self::Leader, RangeReadFailure>;

    /// Fetch one page of `partition` from `leader`, from `offset`.
    async fn fetch(
        &self,
        leader: &Self::Leader,
        partition: i32,
        offset: i64,
    ) -> Result<FetchPartitionResult, ClientError>;
}

/// A failed leader lookup or `Fetch` of a range read.
#[derive(Debug)]
pub(super) struct RangeReadFailure {
    /// Whether a fresh leader lookup and another `Fetch` can succeed.
    retriable: bool,
    error: MetadataLogError,
}

impl RangeReadFailure {
    /// A failure that another leader lookup can clear.
    pub(super) fn retriable(message: String) -> Self {
        Self {
            retriable: true,
            error: MetadataLogError::Other(message),
        }
    }

    /// A client failure, retriable when [`range_fetch_retriable`] says so.
    pub(super) fn client(context: &str, error: &ClientError) -> Self {
        Self {
            retriable: range_fetch_retriable(error),
            error: MetadataLogError::Other(format!("{context}: {error}")),
        }
    }
}

/// Whether a range read can look the leader up again and repeat a request
/// that failed with `error`.
///
/// The partition codes are the ones Kafka's consumer answers with a metadata
/// refresh or a plain retry in `FetchCollector.handleInitializeErrors`, plus
/// the leader, timeout and network codes a request can fail with. A
/// connection that failed, closed or timed out can be made again.
/// Authentication, authorization, version and decoding failures cannot clear
/// by themselves, so the read stops on them.
fn range_fetch_retriable(error: &ClientError) -> bool {
    match error {
        // UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
        // LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT,
        // REPLICA_NOT_AVAILABLE, NETWORK_EXCEPTION, KAFKA_STORAGE_ERROR,
        // FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH, OFFSET_NOT_AVAILABLE,
        // UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID.
        ClientError::Server { error_code } => matches!(
            *error_code,
            -1 | 3 | 5 | 6 | 7 | 9 | 13 | 56 | 74 | 75 | 78 | 100 | 103
        ),
        ClientError::Connect { .. }
        | ClientError::Tls { .. }
        | ClientError::Sasl { .. }
        | ClientError::Disconnected
        | ClientError::Timeout(_)
        | ClientError::Io(_) => true,
        _ => false,
    }
}

/// Where a range read goes after one `Fetch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeStep {
    /// Fetch again from this offset.
    Continue(i64),
    /// The read reached the end of its range.
    Done,
    /// The fetch returned no batch past its offset, so the read cannot
    /// advance.
    Stalled,
}

/// The step after a `Fetch` at `current` that reported `next_offset`.
///
/// `next_offset` is one past the last offset of every batch the fetch decoded,
/// filtered or not, so a compacted hole cannot stall the read the way a cursor
/// derived from the returned records would.
fn next_range_step(current: i64, end: i64, next_offset: Option<i64>) -> RangeStep {
    match next_offset {
        Some(next) if next >= end => RangeStep::Done,
        Some(next) if next > current => RangeStep::Continue(next),
        _ => RangeStep::Stalled,
    }
}

/// The records of a page fetched at `offset` that lie in `[offset, end)`.
///
/// A fetch starts at the batch that holds `offset`, so a page can carry
/// records before it, and its last batch can run past `end`.
fn page_records(
    partition: i32,
    offset: i64,
    end: i64,
    records: Vec<FetchedRecord>,
) -> impl Iterator<Item = MetadataEventRecord> {
    records
        .into_iter()
        .filter(move |record| offset <= record.offset && record.offset < end)
        .map(move |record| MetadataEventRecord {
            partition,
            offset: record.offset,
            tombstone: record.value.is_none(),
            key: record.key,
            payload: record.value.unwrap_or_default(),
        })
}

/// What a range read needs to know about the topic it reads.
pub(super) struct RangeTopic<'a> {
    pub(super) name: &'a str,
    pub(super) partition_count: i32,
    /// How long to wait before a retry.
    pub(super) retry_backoff: Duration,
}

/// Hand every record of `partition` in `[start, end)` to `visit`, one fetched
/// page at a time.
pub(super) async fn visit_range_pages<F: RangeFetcher>(
    fetcher: &F,
    topic: &RangeTopic<'_>,
    partition: i32,
    start: i64,
    end: i64,
    visit: &mut RangeVisitor<'_>,
) -> Result<(), MetadataLogError> {
    if partition < 0 || partition >= topic.partition_count {
        return Err(MetadataLogError::PartitionOutOfRange {
            partition,
            count: topic.partition_count,
        });
    }
    if start >= end {
        return Ok(());
    }
    let mut leader = None;
    let mut offset = start;
    loop {
        let page = match fetch_page(fetcher, topic.name, &mut leader, partition, offset).await {
            Ok(page) => page,
            Err(failure) if failure.retriable => {
                warn!(
                    topic = topic.name,
                    partition,
                    offset,
                    error = %failure.error,
                    "range read: looking the leader up again"
                );
                tokio::time::sleep(topic.retry_backoff).await;
                continue;
            }
            Err(failure) => return Err(failure.error),
        };
        let step = next_range_step(offset, end, page.next_offset);
        page_records(partition, offset, end, page.records).try_for_each(&mut *visit)?;
        match step {
            RangeStep::Continue(next) => offset = next,
            RangeStep::Done => return Ok(()),
            RangeStep::Stalled => {
                return Err(MetadataLogError::Other(format!(
                    "{} partition {partition} Fetch at {offset} returned no batch before {end}",
                    topic.name
                )));
            }
        }
    }
}

/// One page from the cached leader, or from a freshly looked-up one. A
/// failure drops the cached leader, so the next attempt looks it up again.
async fn fetch_page<F: RangeFetcher>(
    fetcher: &F,
    topic: &str,
    leader: &mut Option<F::Leader>,
    partition: i32,
    offset: i64,
) -> Result<FetchPartitionResult, RangeReadFailure> {
    let connection = match leader.take() {
        Some(connection) => connection,
        None => fetcher.leader(partition).await?,
    };
    let page = fetcher
        .fetch(&connection, partition, offset)
        .await
        .map_err(|error| {
            RangeReadFailure::client(
                &format!("{topic} partition {partition} Fetch at {offset} failed"),
                &error,
            )
        })?;
    *leader = Some(connection);
    Ok(page)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::Bytes,
        krabka_units::prelude::secs,
        std::{collections::VecDeque, sync::Mutex},
    };

    const TOPIC: RangeTopic<'static> = RangeTopic {
        name: "__diskless_wal_index",
        partition_count: 2,
        retry_backoff: Duration::from_millis(200),
    };

    #[test]
    fn range_steps_follow_batch_progress_to_the_end() {
        let cases = [
            // A batch that ends inside the range continues past it, even
            // across a compacted hole wider than one offset.
            (4, 10, Some(5), RangeStep::Continue(5)),
            (4, 10, Some(8), RangeStep::Continue(8)),
            // Reaching or passing the end finishes the read.
            (4, 10, Some(10), RangeStep::Done),
            (4, 10, Some(12), RangeStep::Done),
            // No batch, or no progress, cannot finish the range.
            (4, 10, None, RangeStep::Stalled),
            (4, 10, Some(4), RangeStep::Stalled),
            (4, 10, Some(3), RangeStep::Stalled),
        ];
        for (current, end, next, expected) in cases {
            check!(
                next_range_step(current, end, next) == expected,
                "current {current}, end {end}, next {next:?}"
            );
        }
    }

    #[test]
    fn retriable_fetch_failures_are_leader_moves_and_broken_connections() {
        let address = "127.0.0.1:9092".parse().unwrap();
        let cases = [
            (ClientError::Server { error_code: 6 }, true), // NOT_LEADER_OR_FOLLOWER
            (ClientError::Server { error_code: 74 }, true), // FENCED_LEADER_EPOCH
            (ClientError::Server { error_code: 75 }, true), // UNKNOWN_LEADER_EPOCH
            (ClientError::Server { error_code: 5 }, true), // LEADER_NOT_AVAILABLE
            (ClientError::Server { error_code: 9 }, true), // REPLICA_NOT_AVAILABLE
            (ClientError::Server { error_code: 56 }, true), // KAFKA_STORAGE_ERROR
            (ClientError::Server { error_code: 78 }, true), // OFFSET_NOT_AVAILABLE
            (ClientError::Server { error_code: 100 }, true), // UNKNOWN_TOPIC_ID
            (ClientError::Server { error_code: -1 }, true), // UNKNOWN_SERVER_ERROR
            (ClientError::Disconnected, true),
            (ClientError::Timeout(secs(30)), true),
            (
                ClientError::Io(std::io::ErrorKind::ConnectionReset.into()),
                true,
            ),
            (
                ClientError::Connect {
                    addr: address,
                    source: std::io::ErrorKind::ConnectionRefused.into(),
                },
                true,
            ),
            (ClientError::Server { error_code: 1 }, false), // OFFSET_OUT_OF_RANGE
            (ClientError::Server { error_code: 2 }, false), // CORRUPT_MESSAGE
            (ClientError::Server { error_code: 29 }, false), // TOPIC_AUTHORIZATION_FAILED
            (ClientError::Server { error_code: 35 }, false), // UNSUPPORTED_VERSION
            (ClientError::InvalidConfig("bad".into()), false),
        ];
        for (error, expected) in cases {
            check!(range_fetch_retriable(&error) == expected, "{error:?}");
        }
    }

    fn fetched(offset: i64, value: Option<&'static [u8]>) -> FetchedRecord {
        FetchedRecord {
            offset,
            key: Some(Bytes::from_static(b"key")),
            value: value.map(Bytes::from_static),
            timestamp: 0,
            headers: Vec::new(),
        }
    }

    fn record(offset: i64, value: Option<&'static [u8]>) -> MetadataEventRecord {
        MetadataEventRecord {
            partition: 1,
            offset,
            key: Some(Bytes::from_static(b"key")),
            payload: value.map_or_else(Bytes::new, Bytes::from_static),
            tombstone: value.is_none(),
        }
    }

    #[test]
    fn page_records_keep_the_fetch_offset_up_to_the_range_end() {
        let page = vec![
            fetched(3, Some(b"before")),
            fetched(4, Some(b"first")),
            fetched(6, None),
            fetched(7, Some(b"past")),
        ];

        let records: Vec<_> = page_records(1, 4, 7, page).collect();

        assert!(records == vec![record(4, Some(b"first")), record(6, None)]);
    }

    /// One call a range read made through [`ScriptedFetcher`].
    #[derive(Debug, PartialEq, Eq)]
    enum Call {
        Leader,
        Fetch { leader: i32, offset: i64 },
    }

    /// A [`RangeFetcher`] that answers from scripts and records its calls.
    /// A leader is its broker id.
    struct ScriptedFetcher {
        leaders: Mutex<VecDeque<Result<i32, RangeReadFailure>>>,
        pages: Mutex<VecDeque<Result<FetchPartitionResult, ClientError>>>,
        calls: Mutex<Vec<Call>>,
    }

    impl ScriptedFetcher {
        fn new(
            leaders: impl IntoIterator<Item = Result<i32, RangeReadFailure>>,
            pages: impl IntoIterator<Item = Result<FetchPartitionResult, ClientError>>,
        ) -> Self {
            Self {
                leaders: Mutex::new(leaders.into_iter().collect()),
                pages: Mutex::new(pages.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Call> { std::mem::take(&mut *self.calls.lock().unwrap()) }
    }

    impl RangeFetcher for ScriptedFetcher {
        type Leader = i32;

        fn leader(&self, _partition: i32) -> impl Future<Output = Result<i32, RangeReadFailure>> {
            self.calls.lock().unwrap().push(Call::Leader);
            std::future::ready(self.leaders.lock().unwrap().pop_front().unwrap())
        }

        fn fetch(
            &self,
            leader: &i32,
            _partition: i32,
            offset: i64,
        ) -> impl Future<Output = Result<FetchPartitionResult, ClientError>> {
            self.calls.lock().unwrap().push(Call::Fetch {
                leader: *leader,
                offset,
            });
            std::future::ready(self.pages.lock().unwrap().pop_front().unwrap())
        }
    }

    fn page(offsets: std::ops::Range<i64>) -> FetchPartitionResult {
        FetchPartitionResult {
            next_offset: Some(offsets.end),
            records: offsets.map(|offset| fetched(offset, Some(b"v"))).collect(),
        }
    }

    /// Every record the read hands over, or the error it stops with.
    async fn read(
        fetcher: &ScriptedFetcher,
        partition: i32,
        start: i64,
        end: i64,
    ) -> Result<Vec<MetadataEventRecord>, MetadataLogError> {
        let mut records = Vec::new();
        visit_range_pages(fetcher, &TOPIC, partition, start, end, &mut |record| {
            records.push(record);
            Ok(())
        })
        .await?;
        Ok(records)
    }

    #[tokio::test(start_paused = true)]
    async fn a_retriable_fetch_failure_looks_the_leader_up_again_and_resumes() {
        let fetcher = ScriptedFetcher::new([Ok(1), Ok(2)], [
            Ok(page(0..2)),
            Err(ClientError::Server { error_code: 6 }),
            Ok(page(2..4)),
        ]);

        let records = read(&fetcher, 1, 0, 4).await.unwrap();

        assert!(
            records
                == (0..4)
                    .map(|offset| record(offset, Some(b"v")))
                    .collect::<Vec<_>>()
        );
        assert!(
            fetcher.calls()
                == vec![
                    Call::Leader,
                    Call::Fetch {
                        leader: 1,
                        offset: 0
                    },
                    Call::Fetch {
                        leader: 1,
                        offset: 2
                    },
                    Call::Leader,
                    Call::Fetch {
                        leader: 2,
                        offset: 2
                    },
                ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_partition_without_a_leader_is_looked_up_again() {
        let fetcher = ScriptedFetcher::new(
            [
                Err(RangeReadFailure::retriable("no leader yet".into())),
                Ok(3),
            ],
            [Ok(page(0..2))],
        );

        let records = read(&fetcher, 1, 0, 2).await.unwrap();

        assert!(records == vec![record(0, Some(b"v")), record(1, Some(b"v"))]);
        assert!(
            fetcher.calls()
                == vec![Call::Leader, Call::Leader, Call::Fetch {
                    leader: 3,
                    offset: 0
                },]
        );
    }

    #[tokio::test]
    async fn a_fatal_failure_stops_the_read_with_its_cause() {
        let fetcher = ScriptedFetcher::new(
            [
                Ok(1),
                Err(RangeReadFailure::client(
                    "Metadata failed",
                    &ClientError::Server { error_code: 29 },
                )),
            ],
            [Ok(page(0..2)), Err(ClientError::Server { error_code: 2 })],
        );

        let fetch_error = read(&fetcher, 1, 0, 4).await.unwrap_err();
        let lookup_error = read(&fetcher, 1, 0, 4).await.unwrap_err();

        check!(
            fetch_error.to_string()
                == "metadata log error: __diskless_wal_index partition 1 Fetch at 2 failed: \
                    protocol error from server: 2"
        );
        check!(
            lookup_error.to_string()
                == "metadata log error: Metadata failed: protocol error from server: 29"
        );
    }

    #[tokio::test]
    async fn a_fetch_that_makes_no_progress_stalls_the_read() {
        let fetcher = ScriptedFetcher::new([Ok(1)], [Ok(FetchPartitionResult {
            records: Vec::new(),
            next_offset: None,
        })]);

        let error = read(&fetcher, 1, 4, 10).await.unwrap_err();

        assert!(
            error.to_string()
                == "metadata log error: __diskless_wal_index partition 1 Fetch at 4 returned no \
                    batch before 10"
        );
    }

    #[tokio::test]
    async fn a_visitor_error_stops_the_read_before_the_next_record() {
        let fetcher = ScriptedFetcher::new([Ok(1)], [Ok(page(0..3))]);
        let mut seen = Vec::new();

        let error = visit_range_pages(&fetcher, &TOPIC, 1, 0, 6, &mut |record| {
            seen.push(record.offset);
            Err(MetadataLogError::Closed)
        })
        .await
        .unwrap_err();

        check!(matches!(error, MetadataLogError::Closed));
        check!(seen == vec![0]);
        check!(
            fetcher.calls()
                == vec![Call::Leader, Call::Fetch {
                    leader: 1,
                    offset: 0
                }]
        );
    }

    #[tokio::test]
    async fn an_empty_or_out_of_range_read_sends_nothing() {
        let fetcher = ScriptedFetcher::new([], []);

        check!(read(&fetcher, 1, 5, 5).await.unwrap().is_empty());
        for partition in [-1, 2] {
            check!(matches!(
                read(&fetcher, partition, 0, 1).await,
                Err(MetadataLogError::PartitionOutOfRange { partition: got, count: 2 })
                    if got == partition
            ));
        }
        check!(fetcher.calls().is_empty());
    }
}
