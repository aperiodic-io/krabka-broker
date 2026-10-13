//! The injection protocol of the barrier coordinator.
//!
//! The module holds the entry points that start an injection, the run that
//! allocates the epoch and fans the markers out, and the publication of the
//! cut that closes it. It is the one place that consumes an epoch, so it is
//! separate from the group edits that only define what an epoch covers.

use {
    super::{BarrierCoordinator, clamp_timeout},
    crate::{
        barrier::{
            error::BarrierError,
            injection::{MarkerFanout, freeze_targets},
            marker::BarrierMarker,
            metrics::InjectionReport,
            persistence::{
                CutStatus, CutValue, GroupValue, InjectionStartValue, RecordKey, encode_cut,
                encode_group, encode_injection_start,
            },
            state::{
                GroupEntry, PendingInjection, TargetPartition, build_cut, expand_targets,
                expired_cut_epochs, is_due, next_epoch, schedule_next,
            },
        },
        time_util::now_ms,
    },
    krabka_log::Offset,
    krabka_units::{Time, convert::TimeExt as _},
    std::collections::BTreeMap,
    tracing::{info, warn},
};

#[cfg(test)]
mod tests;

/// What one finished injection returns to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InjectionOutcome {
    pub(crate) epoch: i64,
    pub(crate) cut: CutValue,
}

impl BarrierCoordinator {
    /// Run one injection for `group`.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, [`BarrierError::InjectionInProgress`] when the group entry is
    /// busy, [`BarrierError::CoordinatorEpochChanged`] when this broker lost
    /// the state partition during the fan-out, and [`BarrierError::Persist`]
    /// when an append fails.
    /// `timeout` bounds how long the fan-out retries the partitions that carry
    /// no marker yet. `None` uses the configured default, and a value above it
    /// is clamped to it, so a caller cannot hold the group's lock for longer
    /// than the operator allows.
    ///
    /// The bound shortens the fan-out deadline rather than dropping the
    /// injection. Abandoning it would leave the epoch's injection-start record
    /// with no cut record, which is the state a crashed coordinator leaves
    /// behind, so a caller's impatience must not manufacture one.
    pub(crate) async fn trigger_injection(
        &self,
        group: &str,
        timeout: Option<Time>,
    ) -> Result<InjectionOutcome, BarrierError> {
        self.require_coordinator(group).await?;
        let handle = self.live_entry(group)?;
        let mut entry = handle
            .try_lock()
            .map_err(|_| BarrierError::InjectionInProgress {
                group: group.to_owned(),
            })?;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }
        self.inject(group, &mut entry, self.effective_timeout(timeout))
            .await
    }

    /// The fan-out deadline for one injection.
    fn effective_timeout(&self, requested: Option<Time>) -> Time {
        clamp_timeout(requested, self.config.injection_timeout)
    }

    /// Inject into every group whose interval elapsed at `now_ms`.
    ///
    /// The scheduler calls this. A group that another caller holds keeps its
    /// due time, so the next tick picks it up again.
    pub(crate) async fn run_due_injections(&self, now_ms: i64) -> Vec<String> {
        let candidates: Vec<String> = self
            .groups
            .iter()
            .filter_map(|e| {
                let entry = e.value().try_lock().ok()?;
                (entry.is_defined() && is_due(&entry, now_ms)).then(|| e.key().clone())
            })
            .collect();

        let mut injected = Vec::new();
        for group in candidates {
            match self.trigger_injection(&group, None).await {
                Ok(outcome) => {
                    info!(group, epoch = outcome.epoch, "scheduled barrier injection");
                    injected.push(group);
                }
                Err(error) => {
                    warn!(group, %error, "scheduled barrier injection failed");
                }
            }
        }
        injected
    }

    /// Run the injection protocol under the group's mutex.
    async fn inject(
        &self,
        group: &str,
        entry: &mut GroupEntry,
        timeout: Time,
    ) -> Result<InjectionOutcome, BarrierError> {
        let image = self.controller.current_image();
        let coordinator_epoch =
            self.coordinator_epoch(group, &image)
                .ok_or_else(|| BarrierError::NotCoordinator {
                    group: group.to_owned(),
                })?;

        let epoch = next_epoch(entry.last_epoch()).ok_or_else(|| BarrierError::EpochExhausted {
            group: group.to_owned(),
        })?;
        let triggered_at = now_ms();
        let targets = freeze_targets(&entry.definition.topics, &image);
        let start = InjectionStartValue {
            coordinator_epoch,
            triggered_at,
            targets,
        };

        // The injection-start record lands before the first marker, so a crash
        // here cannot let another coordinator reuse this epoch.
        self.append_records(group, vec![(
            RecordKey::injection_start(group, epoch),
            Some(encode_injection_start(&start)?.into()),
        )])
        .await?;
        entry.pending = Some(PendingInjection {
            epoch,
            start: start.clone(),
        });
        self.metrics.injection_started(group, epoch);

        let marker = BarrierMarker {
            group: group.to_owned(),
            epoch,
            triggered_at,
        };
        let placed = self
            .fan_out(&marker, expand_targets(&start.targets), timeout)
            .await;

        // A coordinator that lost the state partition during the fan-out must
        // not write the cut. The new coordinator finalises the epoch from the
        // injection-start record.
        let current = self
            .coordinator_epoch(group, &self.controller.current_image())
            .ok_or_else(|| BarrierError::NotCoordinator {
                group: group.to_owned(),
            })?;
        if current != coordinator_epoch {
            return Err(BarrierError::CoordinatorEpochChanged {
                group: group.to_owned(),
                expected: coordinator_epoch,
                current,
            });
        }

        let completed_at = now_ms();
        let cut = build_cut(triggered_at, completed_at, &start.targets, &placed);
        let report = InjectionReport {
            epoch,
            status: cut.status,
            marked: placed.len(),
            missing: cut.missing.len(),
            elapsed: Time::from_millis(completed_at.saturating_sub(triggered_at)),
        };
        self.publish_cut(group, entry, epoch, cut.clone()).await?;
        self.metrics.injection_completed(group, report);
        if cut.status == CutStatus::Partial {
            warn!(
                group,
                epoch,
                missing = cut.missing.len(),
                "published a partial barrier cut"
            );
        }
        Ok(InjectionOutcome { epoch, cut })
    }

    /// Write the markers of one epoch and collect their offsets.
    async fn fan_out(
        &self,
        marker: &BarrierMarker,
        targets: Vec<TargetPartition>,
        timeout: Time,
    ) -> BTreeMap<TargetPartition, Offset> {
        MarkerFanout {
            node_id: self.node_id,
            partitions: &self.partitions,
            controller: &self.controller,
            remote: self.remote.as_ref(),
            metrics: self.metrics.as_ref(),
            config: &self.config,
        }
        .run(marker, targets, timeout)
        .await
    }

    /// Publish one cut, and retire the epoch that leaves the retention window.
    ///
    /// The cut record, the rewritten group record, and the tombstones go into
    /// one append, so a reader never sees a cut without the group record that
    /// counts it. The coordinator tombstones the expired epoch instead of a
    /// log trim, because the group definitions live in the same prefix and a
    /// trim would delete them.
    pub(super) async fn publish_cut(
        &self,
        group: &str,
        entry: &mut GroupEntry,
        epoch: i64,
        cut: CutValue,
    ) -> Result<(), BarrierError> {
        let definition = GroupValue {
            last_epoch: epoch,
            ..entry.definition.clone()
        };
        let held: Vec<i64> = entry.cuts.keys().copied().collect();
        let expired = expired_cut_epochs(epoch, entry.definition.retained_cuts, &held);
        let mut records = vec![
            (RecordKey::cut(group, epoch), Some(encode_cut(&cut)?.into())),
            (
                RecordKey::group(group),
                Some(encode_group(&definition)?.into()),
            ),
            // The cut supersedes the injection-start record of its own epoch.
            (RecordKey::injection_start(group, epoch), None),
        ];
        for epoch in &expired {
            records.push((RecordKey::cut(group, *epoch), None));
        }
        self.append_records(group, records).await?;

        entry.definition = definition;
        entry.cuts.insert(epoch, cut);
        entry.pending = None;
        for epoch in &expired {
            entry.cuts.remove(epoch);
        }
        schedule_next(entry, now_ms());
        Ok(())
    }
}
