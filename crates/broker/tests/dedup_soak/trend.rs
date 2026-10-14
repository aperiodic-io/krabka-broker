//! Whether a series of one value per cycle climbs over the run.
//!
//! [`drift::judge`](crate::drift::judge) fits a slope to a series sampled on a
//! clock, whatever phase of a cycle each sample landed in. This module judges
//! the other kind of series: one value per cycle, each taken at the same point
//! of its cycle -- right after the idle drain, when every index is empty and
//! the producers are stopped, or over the load phase, which is the same
//! workload every cycle. Values at equivalent points need no fit across a
//! sawtooth; they need a comparison that one odd cycle cannot swing.
//!
//! The rule: leave out the warm-up cycles, split the rest into thirds, and
//! compare the median of the last third with the median of the first. A
//! median ignores a single outlier on either side -- the cycle right after a
//! broker restart, a cycle that ran while another process held the CPU -- so
//! the comparison moves only when most of the late cycles sit above most of
//! the early ones. That is what a leak looks like at equivalent points: every
//! cycle leaves a little behind, and the floor of the sawtooth creeps up.
//!
//! The tolerance is a fraction of the first third's median with an absolute
//! floor under it, one-sided: a series that falls is level, because memory
//! that an allocator gave back is not a leak.

use std::fmt;

/// The fewest judged cycles a comparison of thirds means anything for.
const MIN_CYCLES: usize = 3;

/// How far the last third's median may sit above the first third's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerance {
    /// The allowed rise, as a fraction of the first third's median.
    pub fraction: f64,
    /// A rise that is always allowed, in the series' unit.
    pub floor: f64,
}

/// What [`judge_cycles`] concluded.
#[derive(Debug, Clone, PartialEq)]
pub enum CycleVerdict {
    /// The last third sits no higher than the tolerance allows.
    Level(Thirds),
    /// The last third sits higher than the tolerance allows.
    Rising(Thirds),
    /// Too few cycles were judged to compare thirds.
    TooFewCycles { series: String, cycles: usize },
}

/// The two medians a verdict compared.
#[derive(Debug, Clone, PartialEq)]
pub struct Thirds {
    pub series: String,
    pub unit: &'static str,
    /// The cycles in each third.
    pub per_third: usize,
    pub first: f64,
    pub last: f64,
    pub allowed: f64,
}

impl CycleVerdict {
    /// Whether this verdict fails the soak.
    pub fn is_failure(&self) -> bool { !matches!(self, Self::Level(_)) }
}

impl fmt::Display for CycleVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (word, thirds) = match self {
            Self::Level(thirds) => ("level", thirds),
            Self::Rising(thirds) => ("RISING", thirds),
            Self::TooFewCycles { series, cycles } => {
                return write!(
                    f,
                    "{series}: {cycles} judged cycles, fewer than the {MIN_CYCLES} a comparison of \
                     thirds needs"
                );
            }
        };
        let Thirds {
            series,
            unit,
            per_third,
            first,
            last,
            allowed,
        } = thirds;
        write!(
            f,
            "{series}: {word} -- median of the first {per_third} cycles {first:.0} {unit}, of the \
             last {per_third} {last:.0} {unit}, a rise of {:+.0} against an allowance of \
             {allowed:.0}",
            last - first
        )
    }
}

/// The median of `values`, which must not be empty.
fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[middle]
    } else {
        f64::midpoint(sorted[middle - 1], sorted[middle])
    }
}

/// Judge one per-cycle series, warm-up already left out.
pub fn judge_cycles(
    series: &str,
    unit: &'static str,
    values: &[f64],
    tolerance: Tolerance,
) -> CycleVerdict {
    if values.len() < MIN_CYCLES {
        return CycleVerdict::TooFewCycles {
            series: series.to_owned(),
            cycles: values.len(),
        };
    }
    let per_third = values.len() / 3;
    let first = median(&values[..per_third]);
    let last = median(&values[values.len() - per_third..]);
    let thirds = Thirds {
        series: series.to_owned(),
        unit,
        per_third,
        first,
        last,
        allowed: (tolerance.fraction * first).max(tolerance.floor),
    };
    if last - first > thirds.allowed {
        CycleVerdict::Rising(thirds)
    } else {
        CycleVerdict::Level(thirds)
    }
}

mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    const TOLERANCE: Tolerance = Tolerance {
        fraction: 0.05,
        floor: 10.0,
    };

    #[yare::parameterized(
        flat = { &[500.0; 12], false },
        jitter = { &[500.0, 520.0, 490.0, 510.0, 495.0, 515.0, 505.0, 488.0, 512.0], false },
        // One cycle after a restart sits far above the rest; the median of
        // its third does not move.
        one_late_spike = { &[500.0, 500.0, 500.0, 500.0, 500.0, 500.0, 500.0, 900.0, 500.0], false },
        // The first cycles sat high and the allocator gave memory back.
        falling = { &[700.0, 680.0, 650.0, 600.0, 560.0, 530.0, 510.0, 505.0, 500.0], false },
        // A rise inside the floor on a series near zero.
        near_zero = { &[1.0, 2.0, 1.0, 3.0, 2.0, 4.0, 5.0, 6.0, 8.0], false },
        // Every cycle leaves 4% of the baseline behind.
        creeping = { &[500.0, 520.0, 540.0, 560.0, 580.0, 600.0, 620.0, 640.0, 660.0], true },
        // The floor of a sawtooth that climbs, with an early outlier.
        stepping_up = { &[500.0, 900.0, 500.0, 550.0, 550.0, 550.0, 600.0, 600.0, 600.0], true },
    )]
    fn a_series_rises_only_when_its_late_cycles_sit_above_its_early_ones(
        values: &[f64],
        rising: bool,
    ) {
        let verdict = judge_cycles("rss", "KiB", values, TOLERANCE);
        check!(verdict.is_failure() == rising, "{verdict}");
        check!(matches!(verdict, CycleVerdict::Rising(_)) == rising);
    }

    #[test]
    fn the_verdict_names_both_medians_and_the_allowance() {
        let verdict = judge_cycles(
            "post-drain rss",
            "KiB",
            &[100.0, 100.0, 100.0, 200.0, 200.0, 200.0],
            TOLERANCE,
        );
        assert!(
            verdict
                == CycleVerdict::Rising(Thirds {
                    series: "post-drain rss".into(),
                    unit: "KiB",
                    per_third: 2,
                    first: 100.0,
                    last: 200.0,
                    allowed: 10.0,
                })
        );
        assert!(
            verdict.to_string()
                == "post-drain rss: RISING -- median of the first 2 cycles 100 KiB, of the last 2 \
                    200 KiB, a rise of +100 against an allowance of 10"
        );
    }

    #[test]
    fn too_few_cycles_is_not_a_pass() {
        let verdict = judge_cycles("rss", "KiB", &[1.0, 1.0], TOLERANCE);
        assert!(
            verdict
                == CycleVerdict::TooFewCycles {
                    series: "rss".into(),
                    cycles: 2,
                }
        );
        assert!(verdict.is_failure());
    }

    #[test]
    fn the_median_of_an_even_count_is_the_mean_of_the_middle_two() {
        let near = |a: f64, b: f64| (a - b).abs() < 1.0e-9;
        assert!(near(median(&[4.0, 1.0, 3.0, 2.0]), 2.5));
        assert!(near(median(&[3.0, 1.0, 2.0]), 2.0));
    }
}
