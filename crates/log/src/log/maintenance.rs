//! Flush thresholds, per-segment roll jitter, and deferred file reclamation.

use {
    super::Log,
    crate::{error::LogError, retention},
    krabka_ids::Offset,
    krabka_units::{Time, prelude::TimeExt as _},
    std::{
        collections::hash_map::RandomState,
        hash::BuildHasher,
        time::{Duration, Instant, SystemTime},
    },
};

impl Log {
    /// Flush dirty data and reclaim retired files whose deadlines have passed.
    ///
    /// # Errors
    /// Returns the first flush or file deletion failure.
    ///
    /// # Panics
    /// Panics when the configuration lock is poisoned.
    pub fn maintain(&mut self, now: SystemTime) -> Result<(), LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(Instant::now())?;
        self.reap_deleted_files(now)
    }

    /// Time until the next flush or retired-file deadline, or `None` when idle.
    ///
    /// # Panics
    /// Panics when the configuration lock is poisoned.
    #[must_use]
    pub fn maintenance_delay(&self, now: SystemTime) -> Option<std::time::Duration> {
        self.maintenance_delay_at(now, Instant::now())
    }

    fn maintenance_delay_at(&self, now: SystemTime, elapsed: Instant) -> Option<Duration> {
        let config = self.config.read().unwrap();
        let flush = if self.unflushed_messages == 0 {
            None
        } else if config
            .flush_messages
            .is_some_and(|limit| self.unflushed_messages >= limit)
        {
            Some(Duration::ZERO)
        } else {
            config.flush_interval.map(|interval| {
                Duration::from_millis(u64::try_from(interval.millis_i64_trunc()).unwrap_or(0))
                    .saturating_sub(elapsed.saturating_duration_since(self.last_flush))
            })
        };
        flush
            .into_iter()
            .chain(
                self.pending_deletes
                    .iter()
                    .map(|(deadline, _)| deadline.duration_since(now).unwrap_or_default()),
            )
            .min()
    }

    pub(super) fn jittered_roll_interval(&mut self, interval: Time) -> Time {
        let bound = self
            .config
            .read()
            .unwrap()
            .segment_jitter
            .millis_i64_trunc()
            .min(interval.millis_i64_trunc());
        if bound <= 0 {
            return interval;
        }
        let base = self
            .active
            .as_ref()
            .expect("an open log has an active segment")
            .base_offset();
        let sample = match self.roll_jitter {
            Some((previous, sample)) if previous == base => sample,
            _ => {
                let sample = RandomState::new().hash_one(base);
                self.roll_jitter = Some((base, sample));
                sample
            }
        };
        let bound = u64::try_from(bound).expect("the jitter bound is positive");
        let jitter = i64::try_from(sample % bound).expect("jitter is smaller than an i64 bound");
        Time::from_millis(interval.millis_i64_trunc().saturating_sub(jitter))
    }

    pub(super) fn flush_after_append(&mut self, delta: i32, force: bool) -> Result<(), LogError> {
        self.unflushed_messages = self
            .unflushed_messages
            .saturating_add(u64::try_from(delta).unwrap_or(0) + 1);
        let threshold = self.config.read().unwrap().flush_messages;
        if force {
            // Durable sidecars must not precede records in pending rollover jobs.
            self.rollover_flusher.finish()?;
            self.active_segment_flush()?;
            self.unflushed_messages = 0;
            self.last_flush = Instant::now();
        } else if threshold.is_some_and(|limit| self.unflushed_messages >= limit) {
            self.sync()?;
        } else {
            self.flush_if_due(Instant::now())?;
        }
        Ok(())
    }

    pub(super) fn flush_if_due(&mut self, now: Instant) -> Result<(), LogError> {
        let due = {
            let config = self.config.read().unwrap();
            self.unflushed_messages > 0
                && (config
                    .flush_messages
                    .is_some_and(|limit| self.unflushed_messages >= limit)
                    || config.flush_interval.is_some_and(|interval| {
                        now.saturating_duration_since(self.last_flush).as_millis()
                            >= u128::try_from(interval.millis_i64_trunc()).unwrap_or(0)
                    }))
        };
        if due {
            self.sync()?;
            self.last_flush = now;
        }
        Ok(())
    }

    pub(super) fn retire_segment(&mut self, base: Offset, now: SystemTime) -> Result<(), LogError> {
        if self.config.read().unwrap().file_delete_delay <= Time::ZERO {
            return retention::delete_segment_files(&*self.io, &self.dir, base);
        }
        self.retire_files(base, now, &[
            "log",
            "index",
            "timeindex",
            "txnindex",
            "stampindex",
            "snapshot",
        ])
    }

    pub(super) fn retire_files(
        &mut self,
        base: Offset,
        now: SystemTime,
        extensions: &[&str],
    ) -> Result<(), LogError> {
        let delay = self.config.read().unwrap().file_delete_delay;
        let delay =
            std::time::Duration::from_millis(u64::try_from(delay.millis_i64_trunc()).unwrap_or(0));
        let deadline = now
            .checked_add(delay)
            .ok_or_else(|| LogError::InvalidArgument("file deletion deadline overflow".into()))?;
        let mut paths = Vec::new();
        let result = retention::retire_files(&*self.io, &self.dir, base, extensions, &mut paths);
        if !paths.is_empty() {
            self.pending_deletes.push((deadline, paths));
        }
        result
    }

    pub(super) fn reap_deleted_files(&mut self, now: SystemTime) -> Result<(), LogError> {
        for (deadline, paths) in &mut self.pending_deletes {
            if *deadline > now {
                continue;
            }
            while let Some(path) = paths.last() {
                retention::remove_optional(&*self.io, path)?;
                paths.pop();
            }
        }
        self.pending_deletes.retain(|(_, paths)| !paths.is_empty());
        Ok(())
    }
}

#[cfg(test)]
mod tests;
