//! The judgement of a run: what the converged log must hold, given every
//! batch a leader acknowledged.
//!
//! A batch acknowledged at base offset `b` with `acks=all` names an exact
//! offset for each of its records: the `i`-th is at `b + i`, or that offset is
//! a gap because the record was dropped as a duplicate of an earlier one. So
//! for every acknowledged record with key `k`:
//!
//! * `k` is in the log, exactly once, since the window outlasts the run;
//! * it is at `b + i`, or it is at an earlier offset and `b + i` holds
//!   nothing;
//! * if `k` was already acknowledged before the batch was first sent, the
//!   record is a deliberate repeat and must be the dropped one.
//!
//! And every key in the log is one some producer sent.

use std::collections::{BTreeMap, BTreeSet};

/// One batch as a producer built it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub keys: Vec<String>,
    /// For each record, whether its key had already been acknowledged when
    /// the batch was built. Such a record must be dropped.
    pub repeats: Vec<bool>,
}

/// A batch that a leader acknowledged at `base_offset`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acked {
    pub sent: Sent,
    pub base_offset: i64,
}

/// Every way `log`, as `(offset, key)` pairs, contradicts the acknowledged
/// batches. `sent` is every key any producer sent, acknowledged or not.
pub fn violations(log: &[(i64, String)], acked: &[Acked], sent: &BTreeSet<String>) -> Vec<String> {
    let mut found = Vec::new();
    let mut offsets_of: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    for (offset, key) in log {
        offsets_of.entry(key.as_str()).or_default().push(*offset);
    }
    let key_at: BTreeMap<i64, &str> = log
        .iter()
        .map(|(offset, key)| (*offset, key.as_str()))
        .collect();
    for (key, offsets) in &offsets_of {
        if offsets.len() > 1 {
            found.push(format!(
                "{key} is in the log {} times, at {offsets:?}",
                offsets.len()
            ));
        }
        if !sent.contains(*key) {
            found.push(format!("{key} is in the log, but no producer sent it"));
        }
    }
    for batch in acked {
        let records = batch.sent.keys.iter().zip(&batch.sent.repeats);
        for (own, (key, repeat)) in (batch.base_offset..).zip(records) {
            let Some(&first) = offsets_of
                .get(key.as_str())
                .and_then(|offsets| offsets.first())
            else {
                found.push(format!(
                    "{key} was acknowledged at {own} and is not in the log"
                ));
                continue;
            };
            if first > own {
                found.push(format!(
                    "{key} was acknowledged at {own}, but its copy is at the later offset {first}"
                ));
            }
            if first < own
                && let Some(other) = key_at.get(&own)
            {
                found.push(format!(
                    "{key} was acknowledged at {own} as a duplicate of {first}, but {own} holds \
                     {other}"
                ));
            }
            if *repeat && first == own {
                found.push(format!(
                    "{key} had already been acknowledged, and its repeat at {own} was kept"
                ));
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use {
        super::{Acked, Sent, violations},
        crate::harness::at,
        assert2::assert,
        std::collections::BTreeSet,
    };

    fn batch(base_offset: i64, records: &[(&str, bool)]) -> Acked {
        Acked {
            sent: Sent {
                keys: records.iter().map(|(key, _)| (*key).to_owned()).collect(),
                repeats: records.iter().map(|(_, repeat)| *repeat).collect(),
            },
            base_offset,
        }
    }

    /// A converged log, and what [`violations`] must find in it.
    struct Case {
        name: &'static str,
        log: &'static [(i64, &'static str)],
        expected: &'static [&'static str],
    }

    #[test]
    fn a_history_is_judged_by_the_offsets_its_acknowledgements_name() {
        let sent: BTreeSet<String> = ["a", "b", "c", "x"].map(str::to_owned).into();
        let acked = [
            batch(0, &[("a", false), ("b", false)]),
            // A repeat of `a`, dropped, and a retry of `c` whose first
            // attempt was committed at 2 although it timed out.
            batch(3, &[("a", true), ("c", false)]),
        ];
        let cases = [
            Case {
                name: "consistent",
                log: &[(0, "a"), (1, "b"), (2, "c")],
                expected: &[],
            },
            Case {
                name: "an acknowledged key is lost",
                log: &[(0, "a"), (2, "c")],
                expected: &["b was acknowledged at 1 and is not in the log"],
            },
            Case {
                name: "a key is twice in the log",
                log: &[(0, "a"), (1, "b"), (2, "c"), (4, "c")],
                expected: &[
                    "c is in the log 2 times, at [2, 4]",
                    "c was acknowledged at 4 as a duplicate of 2, but 4 holds c",
                ],
            },
            Case {
                name: "a repeat is kept and its original lost",
                log: &[(1, "b"), (2, "c"), (3, "a")],
                expected: &[
                    "a was acknowledged at 0, but its copy is at the later offset 3",
                    "a had already been acknowledged, and its repeat at 3 was kept",
                ],
            },
            Case {
                name: "a dropped record's offset holds another key",
                log: &[(0, "a"), (1, "b"), (2, "c"), (3, "x")],
                expected: &["a was acknowledged at 3 as a duplicate of 0, but 3 holds x"],
            },
            Case {
                name: "a key nobody sent",
                log: &[(0, "a"), (1, "b"), (2, "c"), (5, "y")],
                expected: &["y is in the log, but no producer sent it"],
            },
        ];
        for case in cases {
            assert!(
                violations(&at(case.log), &acked, &sent) == case.expected,
                "{}",
                case.name
            );
        }
    }
}
