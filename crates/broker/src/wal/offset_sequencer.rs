//! Controller-backed offset assignment for diskless WAL partitions.

use {
    crate::{error::BrokerError, metadata_source::MetadataSource},
    async_trait::async_trait,
    krabka_ids::{Offset, PartitionIndex},
    krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord},
    krabka_raft::RaftError,
    std::sync::Arc,
};

#[async_trait]
pub(crate) trait OffsetSequencer: Send + Sync {
    /// Reserve `count` offsets of `partition` of `topic`, and return the first
    /// of them.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::NotLeader`] in [`BrokerError::Raft`] when the
    /// controller quorum refused the reservation before it reserved anything
    /// because it has no active leader: see [`controller_unavailable`]. Every
    /// other failure is a [`BrokerError::Replication`].
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError>;
}

/// The error of a reservation that the controller quorum refused because it
/// has no active leader, or `None` for every other failure of `error`.
///
/// The quorum has no active leader while it elects one, and the new leader
/// is not active until it has committed its own epoch. It then refuses with
/// [`RaftError::NotLeader`], [`RaftError::LeaderUnknown`] or
/// [`RaftError::UncommittedTail`], before it reserves an offset. Kafka's
/// controller answers `NOT_CONTROLLER` in this window. All three become
/// [`RaftError::NotLeader`], which the produce path answers with
/// `NOT_LEADER_OR_FOLLOWER`.
fn controller_unavailable(error: &RaftError) -> Option<BrokerError> {
    let current_leader = match error {
        RaftError::NotLeader { current_leader } => *current_leader,
        RaftError::LeaderUnknown | RaftError::UncommittedTail => None,
        _ => return None,
    };
    Some(BrokerError::Raft(RaftError::NotLeader { current_leader }))
}

pub(crate) struct ControllerSequencer {
    metadata: Arc<dyn MetadataSource>,
}

impl ControllerSequencer {
    #[must_use]
    pub(crate) fn new(metadata: Arc<dyn MetadataSource>) -> Self { Self { metadata } }
}

#[async_trait]
impl OffsetSequencer for ControllerSequencer {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError> {
        let request_epoch = controller_epoch_coordinate(self.metadata.current_controller_epoch());
        let result = self
            .metadata
            .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: topic.to_owned(),
                    partition: partition.0,
                    count: i64::from(count),
                },
            )])
            .await
            .map_err(|error| {
                controller_unavailable(&error).unwrap_or_else(|| {
                    BrokerError::Replication(format!("offset sequencer: {error}"))
                })
            })?;

        let [reservation] = result.offset_reservations.as_slice() else {
            return Err(BrokerError::Replication(format!(
                "offset sequencer: expected one reservation, got {}",
                result.offset_reservations.len()
            )));
        };
        let request_matches = reservation.topic == topic
            && reservation.partition == partition.0
            && reservation.count == i64::from(count);
        let observed_epoch = controller_epoch_coordinate(self.metadata.current_controller_epoch());
        let response_epoch = i64::try_from(reservation.leader_epoch).unwrap_or(-2);
        let Some(base_offset) = krabka_verified::wal_reservation_response(
            request_matches,
            request_epoch,
            observed_epoch,
            response_epoch,
            reservation.base_offset,
            reservation.count,
        ) else {
            let detail = if request_matches {
                "reservation epoch or range is stale or invalid"
            } else {
                "reservation does not match request"
            };
            return Err(BrokerError::Replication(format!(
                "offset sequencer: {detail}"
            )));
        };
        Ok(Offset(base_offset))
    }
}

fn controller_epoch_coordinate(epoch: Option<u64>) -> i64 {
    match epoch {
        None => -1,
        Some(epoch) => i64::try_from(epoch).unwrap_or(-2),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::FakeMetadataSource,
        krabka_raft::{NodeId, OffsetReservation, SubmitChangeResult},
        std::sync::Arc,
    };

    fn reservation(
        topic: &str,
        partition: i32,
        base_offset: i64,
        count: i64,
        leader_epoch: u64,
    ) -> OffsetReservation {
        OffsetReservation {
            topic: topic.to_string(),
            partition,
            base_offset,
            count,
            leader_epoch,
        }
    }

    /// The batches the sequencer must have submitted after one `assign` of
    /// three records on `topic-0`. Every case below asks for that same
    /// advance, so every case must submit exactly this -- whatever the
    /// controller then answers.
    fn expected_submissions() -> Vec<Vec<MetadataRecord>> {
        vec![vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count: 3,
            },
        )]]
    }

    async fn assign_topic_zero(source: Arc<dyn MetadataSource>) -> Result<Offset, BrokerError> {
        ControllerSequencer::new(source)
            .assign("topic", PartitionIndex(0), 3)
            .await
    }

    /// A controller in term 7 that answers every `submit_change` with
    /// `result`.
    fn source_answering(result: SubmitChangeResult) -> Arc<FakeMetadataSource> {
        Arc::new(
            FakeMetadataSource::builder()
                .term(7)
                .on_submit(move |_| Ok(result.clone()))
                .build(),
        )
    }

    #[tokio::test]
    async fn controller_sequencer_uses_returned_reservation_base() {
        let source = source_answering(SubmitChangeResult {
            offset_reservations: vec![reservation("topic", 0, 11, 3, 7)],
        });
        let base = assign_topic_zero(Arc::clone(&source) as Arc<dyn MetadataSource>)
            .await
            .unwrap();

        assert2::assert!((base) == (Offset(11)));
        // One batch, carrying exactly the advance the caller asked for.
        assert2::assert!(source.submitted() == expected_submissions());
    }

    #[tokio::test]
    async fn controller_sequencer_rejects_stale_malformed_and_failed_responses() {
        let cases = [
            SubmitChangeResult::default(),
            SubmitChangeResult {
                offset_reservations: vec![
                    reservation("topic", 0, 11, 3, 7),
                    reservation("topic", 0, 14, 3, 7),
                ],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("wrong", 0, 11, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 1, 11, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, 11, 4, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, 11, 3, 6)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, -1, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, i64::MAX, 3, 7)],
            },
        ];

        for result in cases {
            let source = source_answering(result);
            assert2::assert!(
                assign_topic_zero(Arc::clone(&source) as Arc<dyn MetadataSource>)
                    .await
                    .is_err()
            );
            // The write went out as asked; only the answer was unusable.
            assert2::assert!(source.submitted() == expected_submissions());
        }

        let failing = Arc::new(
            FakeMetadataSource::builder()
                .term(7)
                .on_submit(|_| Err(RaftError::Shutdown))
                .build(),
        );
        assert2::assert!(
            assign_topic_zero(Arc::clone(&failing) as Arc<dyn MetadataSource>)
                .await
                .is_err()
        );
        assert2::assert!(failing.submitted() == expected_submissions());
    }

    /// A controller quorum without an active leader refuses a reservation
    /// before it reserves anything: it has no leader, its leader moved, it
    /// elects one, or its new leader has not committed its epoch. Each of
    /// those is a `NotLeader`, with the leader when the refusal names one.
    /// Every other failure stays a failure of the reservation.
    #[tokio::test]
    async fn only_a_controller_without_an_active_leader_answers_not_leader() {
        /// Builds the refusal of the controller for one case.
        type Refusal = fn() -> RaftError;
        /// What `assign` returns for a refusal.
        #[derive(Debug, PartialEq)]
        enum Answer {
            /// A `NotLeader` naming the leader, when the refusal names one.
            NotLeader(Option<NodeId>),
            /// Any other error.
            Other,
        }
        /// What the case is, the refusal of the controller, and what `assign`
        /// answers.
        struct RefusalCase {
            what: &'static str,
            refusal: Refusal,
            want: Answer,
        }
        let cases: [RefusalCase; 6] = [
            RefusalCase {
                what: "no leader is known",
                refusal: || RaftError::NotLeader {
                    current_leader: None,
                },
                want: Answer::NotLeader(None),
            },
            RefusalCase {
                what: "the leader moved",
                refusal: || RaftError::NotLeader {
                    current_leader: Some(NodeId(2)),
                },
                want: Answer::NotLeader(Some(NodeId(2))),
            },
            RefusalCase {
                what: "an election is in progress",
                refusal: || RaftError::LeaderUnknown,
                want: Answer::NotLeader(None),
            },
            RefusalCase {
                what: "the new leader has not committed its epoch",
                refusal: || RaftError::UncommittedTail,
                want: Answer::NotLeader(None),
            },
            RefusalCase {
                what: "the reservation chain is invalid",
                refusal: || {
                    RaftError::ChangeRejected("pending offset reservation chain is invalid".into())
                },
                want: Answer::Other,
            },
            RefusalCase {
                what: "the controller stopped",
                refusal: || RaftError::Shutdown,
                want: Answer::Other,
            },
        ];
        for RefusalCase {
            what,
            refusal,
            want,
        } in cases
        {
            let source = Arc::new(
                FakeMetadataSource::builder()
                    .term(7)
                    .on_submit(move |_| Err(refusal()))
                    .build(),
            );
            let sequencer =
                ControllerSequencer::new(Arc::clone(&source) as Arc<dyn MetadataSource>);

            let error = sequencer
                .assign("topic", PartitionIndex(0), 3)
                .await
                .expect_err("the controller refused the reservation");
            let got = match error {
                BrokerError::Raft(RaftError::NotLeader { current_leader }) => {
                    Answer::NotLeader(current_leader)
                }
                _ => Answer::Other,
            };

            assert2::check!(got == want, "{what}");
            assert2::check!(source.submitted() == expected_submissions(), "{what}");
        }
    }

    #[tokio::test]
    async fn broker_only_sequencer_accepts_a_committed_response_epoch() {
        // A broker-only observer owns no term state, so it fences the write
        // against no epoch at all and must still take the controller's answer.
        let source = Arc::new(
            FakeMetadataSource::builder()
                .without_controller_epoch()
                .on_submit(|_| {
                    Ok(SubmitChangeResult {
                        offset_reservations: vec![reservation("topic", 0, 11, 3, 7)],
                    })
                })
                .build(),
        );
        assert2::assert!(
            assign_topic_zero(Arc::clone(&source) as Arc<dyn MetadataSource>)
                .await
                .unwrap()
                == Offset(11)
        );
        assert2::assert!(source.submitted() == expected_submissions());
    }
}
