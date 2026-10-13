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

struct Override<'a> {
    key: &'a str,
    value: &'a str,
}

fn overrides(entries: &[Override<'_>]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|entry| (entry.key.into(), entry.value.into()))
        .collect()
}

#[test]
fn absent_invalid_and_other_protocol_timings_borrow_the_original() {
    let config = Config::default();
    let ignored = overrides(&[
        Override {
            key: "share.session.timeout.ms",
            value: "50000",
        },
        Override {
            key: HEARTBEAT,
            value: "invalid",
        },
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
    struct Case<'a> {
        entries: Vec<Override<'a>>,
        session: u64,
        heartbeat: u64,
        assignment: u64,
    }
    let config = Config::default();
    for case in [
        Case {
            entries: vec![Override {
                key: SESSION,
                value: "45000",
            }],
            session: 45,
            heartbeat: 5,
            assignment: 1,
        },
        Case {
            entries: vec![Override {
                key: SESSION,
                value: "50000",
            }],
            session: 50,
            heartbeat: 5,
            assignment: 1,
        },
        Case {
            entries: vec![Override {
                key: HEARTBEAT,
                value: "7000",
            }],
            session: 45,
            heartbeat: 7,
            assignment: 1,
        },
        Case {
            entries: vec![Override {
                key: ASSIGNMENT,
                value: "0",
            }],
            session: 45,
            heartbeat: 5,
            assignment: 0,
        },
        Case {
            entries: vec![
                Override {
                    key: SESSION,
                    value: "0",
                },
                Override {
                    key: HEARTBEAT,
                    value: "20000",
                },
                Override {
                    key: ASSIGNMENT,
                    value: "20000",
                },
            ],
            session: 45,
            heartbeat: 15,
            assignment: 15,
        },
    ] {
        let values = overrides(&case.entries);
        let Cow::Owned(actual) = config.for_group(Some(&values)) else {
            panic!("a valid override must create a configuration")
        };
        assert!(
            actual
                == Config {
                    session_timeout: Duration::from_secs(case.session),
                    heartbeat_interval: Duration::from_secs(case.heartbeat),
                    assignment_interval: Duration::from_secs(case.assignment),
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
