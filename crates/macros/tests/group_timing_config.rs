//! The generated timing methods preserve borrowing, bounds and unrelated fields.

use {
    assert2::assert,
    std::{borrow::Cow, collections::BTreeMap, time::Duration},
};

mod coordinator {
    pub(crate) mod unified {
        pub(crate) mod config {
            use std::{collections::BTreeMap, time::Duration};

            pub(crate) const MIN_ASSIGNMENT_INTERVAL: Duration = Duration::ZERO;
            pub(crate) const MAX_ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(15);

            pub(crate) fn group_millis(
                overrides: Option<&BTreeMap<String, String>>,
                key: &str,
                min: Duration,
                max: Duration,
            ) -> Option<Duration> {
                overrides
                    .and_then(|values| values.get(key))
                    .and_then(|text| text.trim().parse::<i32>().ok())
                    .map(|millis| Duration::from_millis(u64::try_from(millis).unwrap_or(0)))
                    .map(|duration| duration.clamp(min, max))
            }
        }
    }
}

const SESSION: &str = "consumer.session.timeout.ms";
const HEARTBEAT: &str = "consumer.heartbeat.interval.ms";
const ASSIGNMENT: &str = "consumer.assignment.interval.ms";

#[krabka_macros::membership_defaults]
#[derive(
    Debug, Clone, PartialEq, krabka_macros::FieldDefaults, krabka_macros::GroupTimingConfig,
)]
#[group_timing(session = SESSION, heartbeat = HEARTBEAT, assignment = ASSIGNMENT)]
struct Config {
    session_timeout: Duration,
    heartbeat_interval: Duration,
    #[default(Duration::from_secs(1))]
    assignment_interval: Duration,
    min_session_timeout: Duration,
    max_session_timeout: Duration,
    min_heartbeat_interval: Duration,
    max_heartbeat_interval: Duration,
    #[default(77)]
    unrelated: u32,
}

fn overrides(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|&(key, value)| (key.into(), value.into()))
        .collect()
}

#[test]
fn absent_invalid_and_other_protocol_timings_borrow_the_original() {
    let config = Config::default();
    let ignored = overrides(&[
        ("share.session.timeout.ms", "50000"),
        (HEARTBEAT, "invalid"),
    ]);
    for values in [None, Some(&BTreeMap::new()), Some(&ignored)] {
        let Cow::Borrowed(actual) = config.for_group(values) else {
            panic!("no valid timing overrides must borrow the broker defaults")
        };
        assert!(std::ptr::eq(actual, &raw const config));
    }
}

#[test]
fn applies_each_timing_and_keeps_unrelated_configuration() {
    let config = Config::default();
    for (entries, expected) in [
        (vec![(SESSION, "45000")], [45, 5, 1]),
        (vec![(SESSION, "50000")], [50, 5, 1]),
        (vec![(HEARTBEAT, "7000")], [45, 7, 1]),
        (vec![(ASSIGNMENT, "0")], [45, 5, 0]),
        (
            vec![(SESSION, "0"), (HEARTBEAT, "20000"), (ASSIGNMENT, "20000")],
            [45, 15, 15],
        ),
    ] {
        let values = overrides(&entries);
        let Cow::Owned(actual) = config.for_group(Some(&values)) else {
            panic!("a valid override must create a configuration")
        };
        let [session, heartbeat, assignment] = expected;
        assert!(
            actual
                == Config {
                    session_timeout: Duration::from_secs(session),
                    heartbeat_interval: Duration::from_secs(heartbeat),
                    assignment_interval: Duration::from_secs(assignment),
                    ..config.clone()
                }
        );
    }
    assert!(config == Config::default());
    assert!(
        Config::assigning_at_once()
            == Config {
                assignment_interval: Duration::ZERO,
                ..config
            }
    );
}
