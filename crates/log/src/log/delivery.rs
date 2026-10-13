//! The deliver-at-time watermark: the first offset whose activation time
//! has not passed yet.
//!
//! A fetch caps its limit offset at the watermark, so a scheduled batch is
//! never served before its activation time while the declared clock bound
//! holds. Nothing here is persisted: restart recomputes using the current
//! clock and uncertainty bound, which may differ from a previously released
//! in-memory prefix.

use {
    super::Log,
    crate::{
        config::DeliveryPolicy,
        delivery::{self, DeliveryAdvance},
    },
    krabka_ids::Offset,
    krabka_units::prelude::TimeExt as _,
};

impl Log {
    /// Recompute the deliver-at-time watermark for the clock reading
    /// `now_ms`, and report when the next scheduled batch comes due.
    ///
    /// The watermark is the first offset that is not visible yet. A fetch
    /// caps its limit offset at it, so a batch whose activation time has not
    /// passed is never served. The value stays inside
    /// `[log_start_offset(), log_end_offset()]`, and it only moves forward
    /// while the records under it stay: a truncation carries it back down with
    /// the records it cut away, because the offsets they held may be filled
    /// again by a batch that is not due.
    ///
    /// A topic that does not schedule delivery answers `log_end_offset()`
    /// before any I/O: this sits on the fetch hot path and an ordinary topic
    /// must pay nothing for it. A scheduled topic walks batch headers forward
    /// from the cached watermark, and skips whole segments whose own maximum
    /// timestamp proves everything in them is active. A repeat call with no
    /// change of time and no new records answers from the cache.
    /// Increasing clock uncertainty or moving the clock backward does not
    /// retract the published prefix; current settings govern records at and
    /// above the cached watermark.
    ///
    /// Nothing here is persisted. The schedule is in the record timestamps,
    /// so after a restart or a leader change the walk starts again from the
    /// log start using the current clock and bound. If uncertainty increased
    /// or the clock moved backward, a rebuilt watermark can be below the
    /// prefix this instance had already released.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn advance_delivery_watermark(&mut self, now_ms: i64) -> DeliveryAdvance {
        let (scheduled, uncertainty_ms) = self.delivery_settings();
        if !scheduled {
            return DeliveryAdvance {
                watermark: self.log_end_offset(),
                next_deadline_ms: None,
            };
        }

        // The walk reads records, so it starts at the local floor: on a
        // tiered topic the offsets below it are in the remote tier and the
        // segments that held them are gone.
        let start = self.local_log_start_offset();
        let end = self.log_end_offset();
        let cursor = self.bounded_watermark();

        if cursor == self.delivery_watermark {
            match self.delivery_pending_ms {
                // The batch that stopped the last walk is still waiting, and
                // no walk can get past it, so the watermark cannot have moved.
                Some(activation_ms)
                    if !delivery::batch_is_deliverable(
                        DeliveryPolicy::Scheduled,
                        uncertainty_ms,
                        activation_ms,
                        now_ms,
                    ) =>
                {
                    return DeliveryAdvance {
                        watermark: cursor,
                        next_deadline_ms: Some(delivery::visible_at_ms(
                            activation_ms,
                            uncertainty_ms,
                        )),
                    };
                }
                // The last walk reached the log end and nothing was appended.
                None if cursor == end => {
                    return DeliveryAdvance {
                        watermark: cursor,
                        next_deadline_ms: None,
                    };
                }
                _ => {}
            }
        }

        let (candidate, pending_ms) = self.walk_activation(cursor, end, uncertainty_ms, now_ms);
        let watermark = Offset(krabka_verified::delivery_watermark_advance(
            start.0,
            cursor.0,
            candidate.0,
            end.0,
        ));
        let pending_ms = if watermark == candidate {
            pending_ms
        } else {
            None
        };
        self.delivery_watermark = watermark;
        self.delivery_pending_ms = pending_ms;
        DeliveryAdvance {
            watermark,
            next_deadline_ms: pending_ms
                .map(|activation_ms| delivery::visible_at_ms(activation_ms, uncertainty_ms)),
        }
    }

    /// Deliver-at-time watermark as the last advance left it.
    ///
    /// This method reads the cached value and walks nothing, so a reader that
    /// must not do I/O can use it. It answers `log_end_offset()` on a topic
    /// that does not schedule delivery, where every durable record is visible.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn delivery_watermark(&self) -> Offset {
        if self.config.read().unwrap().delivery_policy != DeliveryPolicy::Scheduled {
            return self.log_end_offset();
        }
        self.bounded_watermark()
    }

    /// Inclusive, batch-aligned offset ranges inside `[start, end]` that are
    /// not visible yet.
    ///
    /// A share consumer may skip a waiting record and come back to it later,
    /// so it needs every gap in the window and not only the leading prefix
    /// that [`Log::advance_delivery_watermark`] reports. Adjacent ranges are
    /// merged. Each range covers whole batches even where the window cuts
    /// through one, because the share reader fetches with [`Log::read_raw`]
    /// and that is batch-granular.
    ///
    /// The result is empty on a topic that does not schedule delivery, and
    /// empty when the window holds no records.
    /// Records below the published delivery watermark stay visible even if
    /// clock uncertainty subsequently increases or the clock moves backward.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn pending_activation_ranges(
        &self,
        start: Offset,
        end: Offset,
        now_ms: i64,
    ) -> Vec<(Offset, Offset)> {
        let (scheduled, uncertainty_ms) = self.delivery_settings();
        if !scheduled {
            return Vec::new();
        }
        let low = start.max(self.bounded_watermark());
        let high = end.min(self.log_end_offset() - 1);
        if low > high {
            return Vec::new();
        }
        let mut ranges: Vec<(Offset, Offset)> = Vec::new();
        let mut cursor = low;
        for segment in self.segments.iter().chain(self.active.as_ref()) {
            if cursor > high {
                break;
            }
            if segment.last_offset() < cursor {
                continue;
            }
            if let Err(error) = segment.pending_activation_ranges_into(
                cursor,
                high,
                uncertainty_ms,
                now_ms,
                &mut ranges,
            ) {
                tracing::warn!(
                    %error,
                    base_offset = segment.base_offset().0,
                    "activation scan failed; treating the rest of the window as pending",
                );
                // What cannot be read must not be served.
                ranges.push((cursor, high));
                break;
            }
            cursor = (segment.last_offset() + 1).max(cursor);
        }
        delivery::coalesce_ranges(ranges)
    }

    /// Whether this topic schedules delivery, and its clock bound in
    /// milliseconds.
    fn delivery_settings(&self) -> (bool, i64) {
        let config = self.config.read().unwrap();
        (
            config.delivery_policy == DeliveryPolicy::Scheduled,
            config.delivery_clock_uncertainty.millis_i64_trunc(),
        )
    }

    /// The cached watermark held inside the offsets the log still has.
    ///
    /// A truncation lowers the log end below the watermark, and a raised log
    /// start moves past it, so the cached value is bounded on each read rather
    /// than reset at every mutation site.
    fn bounded_watermark(&self) -> Offset {
        self.delivery_watermark
            .max(self.local_log_start_offset())
            .min(self.log_end_offset())
    }

    /// Walk segments from `from` and stop at the first batch that is not
    /// active. Returns the new watermark and that batch's activation time.
    ///
    /// A failed scan stops the walk and keeps the watermark where it reached,
    /// because a watermark that is too low delays delivery while one that is
    /// too high breaks the promise never to deliver early.
    fn walk_activation(
        &self,
        from: Offset,
        end: Offset,
        uncertainty_ms: i64,
        now_ms: i64,
    ) -> (Offset, Option<i64>) {
        let mut cursor = from;
        for segment in self.segments.iter().chain(self.active.as_ref()) {
            if cursor >= end {
                break;
            }
            if segment.last_offset() < cursor {
                continue;
            }
            match segment.scan_activation(cursor, uncertainty_ms, now_ms) {
                Ok(scan) => {
                    cursor = scan.active_end.max(cursor).min(end);
                    if let Some(activation_ms) = scan.pending_at {
                        return (cursor, Some(activation_ms));
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        base_offset = segment.base_offset().0,
                        "activation scan failed; delivery watermark holds",
                    );
                    return (cursor, None);
                }
            }
        }
        (cursor, None)
    }

    /// Forget what the last activation walk learned at and above
    /// `changed_from`.
    ///
    /// Every path that removes or rewrites records calls this, so the next
    /// advance walks instead of trusting a deadline for a batch that is no
    /// longer there.
    ///
    /// The watermark comes down with the deadline, and that half is what keeps
    /// the promise never to deliver early. A truncation cuts a suffix away, and
    /// [`Log::bounded_watermark`] then masks the stale value against the lower
    /// log end. But the field still holds it, so an append that pushes the log
    /// end back above it unmasks it: the next walk resumes at the stale offset
    /// and steps straight over the records that took the truncated ones' place,
    /// declaring them visible without ever reading their activation times.
    pub(super) fn invalidate_delivery_schedule(&mut self, changed_from: Offset) {
        self.delivery_pending_ms = None;
        self.delivery_watermark = self.delivery_watermark.min(changed_from);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{LogConfig, log::test_support::configured_test_log},
        assert2::assert,
        krabka_protocol::records::{Record, RecordBatch},
        krabka_units::millis,
    };

    #[test]
    fn larger_uncertainty_is_prospective_for_the_released_prefix() {
        let (dir, mut log) = configured_test_log(LogConfig {
            delivery_policy: DeliveryPolicy::Scheduled,
            delivery_clock_uncertainty: millis(0),
            ..LogConfig::default()
        });
        for time in [100, 200] {
            log.append(&mut RecordBatch {
                base_timestamp: time,
                max_timestamp: time,
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .unwrap();
        }
        assert!(log.advance_delivery_watermark(150).watermark == Offset(1));
        let config = LogConfig {
            delivery_clock_uncertainty: millis(100),
            ..log.config_snapshot()
        };
        log.set_config(config.clone());
        assert!(log.advance_delivery_watermark(150).watermark == Offset(1));
        assert!(log.advance_delivery_watermark(50).watermark == Offset(1));
        assert!(
            log.pending_activation_ranges(Offset(0), Offset(1), 150)
                == vec![(Offset(1), Offset(1))]
        );
        log.sync().unwrap();
        drop(log);
        let mut reopened = Log::open(dir.path(), config).unwrap();
        assert!(reopened.advance_delivery_watermark(150).watermark == Offset(0));
        assert!(reopened.advance_delivery_watermark(300).watermark == Offset(2));
    }
}
