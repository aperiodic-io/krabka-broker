//! The commit wait of an `acks=-1` append that this broker made as the
//! partition's leader.
//!
//! Kafka completes such an append in `DelayedProduce` once the high watermark
//! covers it. The append fails when the broker stops leading the partition
//! first: `ReplicaManager.makeFollowers` completes every delayed produce of the
//! partition with `NOT_LEADER_OR_FOLLOWER`. The high watermark alone does not
//! prove a commit. A former leader that follows the new leader takes the high
//! watermark of the new leader, and that watermark can pass the offset of the
//! append over records that replaced it.
//!
//! The group coordinator's `__consumer_offsets` writes and the transaction
//! markers both wait here. Each caller says what its term is.

use std::{sync::Arc, time::Instant};

use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::watch;

use super::Partition;

/// Why an append did not commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Uncommitted {
    /// The term of the append ended before the high watermark covered it.
    TermEnded,
    /// The deadline passed before the high watermark covered the append.
    TimedOut,
}

impl Partition {
    /// Wait until the high watermark reaches `end_offset`, the offset after
    /// the appended records, while the term of the append holds.
    ///
    /// `holds` tells whether the term holds. It gets this partition and the
    /// newest image of `images`, or `None` when the caller follows no image.
    /// The wait calls it at the start, again when the high watermark or the
    /// installed leadership of the partition changes, again when a new image
    /// arrives, and a last time after the high watermark reached
    /// `end_offset`. An installed leadership change always wakes the wait,
    /// because it fires `hw_advance_notify`.
    ///
    /// # Errors
    ///
    /// Returns [`Uncommitted::TermEnded`] when `holds` returns `false` or the
    /// image source closes before the append commits. Returns
    /// [`Uncommitted::TimedOut`] when `deadline` passes first.
    pub(crate) async fn await_committed_while(
        &self,
        end_offset: Offset,
        deadline: Instant,
        mut images: Option<&mut watch::Receiver<Arc<MetadataImage>>>,
        holds: impl Fn(&Self, Option<&MetadataImage>) -> bool,
    ) -> Result<(), Uncommitted> {
        let newest = |images: Option<&mut watch::Receiver<Arc<MetadataImage>>>| {
            images.map(|images| Arc::clone(&images.borrow_and_update()))
        };
        let committed = self.await_hw_at_least(end_offset, deadline);
        tokio::pin!(committed);
        loop {
            // Register for the next change before the check, so a change
            // between the check and the wait still wakes the wait.
            let partition_changed = self.hw_advance_notify.notified();
            tokio::pin!(partition_changed);
            partition_changed.as_mut().enable();
            if !holds(self, newest(images.as_deref_mut()).as_deref()) {
                return Err(Uncommitted::TermEnded);
            }
            let image_changed =
                crate::metadata_source::wait_for_image_change(images.as_deref_mut());
            tokio::select! {
                reached = &mut committed => {
                    reached.map_err(|_| Uncommitted::TimedOut)?;
                    return if holds(self, newest(images.as_deref_mut()).as_deref()) {
                        Ok(())
                    } else {
                        Err(Uncommitted::TermEnded)
                    };
                }
                () = &mut partition_changed => {}
                open = image_changed => {
                    if !open {
                        return Err(Uncommitted::TermEnded);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::Ordering,
        time::{Duration, Instant},
    };

    use assert2::assert;
    use krabka_log::Offset;

    use super::Uncommitted;
    use crate::partition::Partition;

    /// Whether `partition` still has the leader and leader epoch it had when
    /// the case appended: node 1 at epoch 0.
    fn led_by_one_at_zero(partition: &Partition) -> bool {
        partition.current_leader.load(Ordering::Acquire) == 1
            && partition.current_leader_epoch.load(Ordering::Acquire) == 0
    }

    /// A wait that follows no image ends with the installed leadership of the
    /// partition: an installed change wakes it although the high watermark
    /// does not move.
    #[tokio::test]
    async fn a_wait_ends_when_the_installed_leadership_changes() {
        struct Case {
            what: &'static str,
            /// The high watermark when the wait starts.
            hw_now: i64,
            /// The leader and epoch the partition installs during the wait.
            moves_to: Option<(u64, i32)>,
            /// The high watermark that followers bring during the wait,
            /// after any move.
            hw_later: Option<i64>,
            timeout: Duration,
            expected: Result<(), Uncommitted>,
        }
        let long = Duration::from_secs(30);
        let cases = [
            Case {
                what: "already committed",
                hw_now: 2,
                moves_to: None,
                hw_later: None,
                timeout: long,
                expected: Ok(()),
            },
            Case {
                what: "committed when the followers catch up",
                hw_now: 0,
                moves_to: None,
                hw_later: Some(2),
                timeout: long,
                expected: Ok(()),
            },
            Case {
                what: "another broker takes the partition",
                hw_now: 0,
                moves_to: Some((2, 1)),
                hw_later: None,
                timeout: long,
                expected: Err(Uncommitted::TermEnded),
            },
            Case {
                what: "the same broker leads again at a newer epoch",
                hw_now: 0,
                moves_to: Some((1, 1)),
                hw_later: None,
                timeout: long,
                expected: Err(Uncommitted::TermEnded),
            },
            Case {
                what: "the high watermark passes the append after the move",
                hw_now: 0,
                moves_to: Some((2, 1)),
                hw_later: Some(2),
                timeout: long,
                expected: Err(Uncommitted::TermEnded),
            },
            Case {
                what: "the followers never catch up",
                hw_now: 0,
                moves_to: None,
                hw_later: None,
                timeout: Duration::from_millis(100),
                expected: Err(Uncommitted::TimedOut),
            },
        ];
        for case in cases {
            let (partition, _dir, hw_notify) = crate::partition::test_support::commit_partition(
                crate::partition::test_support::CommitPartitionSetup {
                    high_watermark: Offset(case.hw_now),
                    leader:
                        crate::partition::test_support::CommitFixtureLeader::BrokerOneAtEpochZero,
                },
            )
            .await;

            let changes = crate::coordinator::test_support::schedule_commit_changes!(case, partition;
                captures {} leader(leader, epoch) {
                    partition.install_leader_change(leader, epoch).await;
                } notify {
                    hw_notify.notify_waiters();
                }
            );
            let result = partition
                .await_committed_while(
                    Offset(2),
                    Instant::now() + case.timeout,
                    None,
                    |partition, _| led_by_one_at_zero(partition),
                )
                .await;
            changes.await.expect("the changes land");

            assert!(result == case.expected, "{}", case.what);
        }
    }
}
