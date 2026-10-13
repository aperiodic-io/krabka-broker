//! WORM diagnostic precedence and native observation evaluation.

use {assert2::assert, std::cell::RefCell};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WormObjectSetDecision {
    Empty,
    DuplicateKey,
    CoordinateMismatch,
    MissingObject,
    CountMismatch,
    SizeMismatch,
    DigestMismatch,
    Admit,
}

#[derive(Clone, Copy)]
struct Identity {
    unique_keys: bool,
    coordinates_match: bool,
}

#[derive(Clone, Copy)]
struct Availability {
    all_present: bool,
    sizes_match: bool,
}

#[derive(Clone, Copy)]
struct Digests {
    require_digests: bool,
    digests_match: bool,
}

struct Facts {
    object_count: u64,
    listed_count: u64,
    identity: Identity,
    availability: Availability,
    digests: Digests,
}

krabka_macros::worm_object_set! {
    Runtime;
    {
        #[must_use]
        fn decide(facts: Facts) -> WormObjectSetDecision
    }
    (facts);
    count [facts.object_count] listed [facts.listed_count]
}

krabka_macros::worm_object_set! {
    Model;
    {
        /// The cfg excludes the proof declaration before pearlite expands.
        #[cfg(any())]
        #[cfg_attr(any(), proof_attribute)]
        fn proof(facts: Facts) -> WormObjectSetDecision
    }
    (facts);
    count [facts.object_count@] listed [facts.listed_count@]
}

#[test]
fn every_fact_combination_returns_the_first_refusal() {
    use WormObjectSetDecision::{
        Admit, CoordinateMismatch, CountMismatch, DigestMismatch, DuplicateKey, Empty,
        MissingObject, SizeMismatch,
    };

    for object_count in 0..=2 {
        for listed_count in 0..=2 {
            for bits in 0_u8..64 {
                let unique_keys = bits & 1 != 0;
                let coordinates_match = bits & 2 != 0;
                let all_present = bits & 4 != 0;
                let sizes_match = bits & 8 != 0;
                let require_digests = bits & 16 != 0;
                let digests_match = bits & 32 != 0;
                let expected = [
                    RefusalCase {
                        refused: object_count == 0,
                        decision: Empty,
                    },
                    RefusalCase {
                        refused: !unique_keys,
                        decision: DuplicateKey,
                    },
                    RefusalCase {
                        refused: !coordinates_match,
                        decision: CoordinateMismatch,
                    },
                    RefusalCase {
                        refused: !all_present,
                        decision: MissingObject,
                    },
                    RefusalCase {
                        refused: object_count != listed_count,
                        decision: CountMismatch,
                    },
                    RefusalCase {
                        refused: !sizes_match,
                        decision: SizeMismatch,
                    },
                    RefusalCase {
                        refused: require_digests && !digests_match,
                        decision: DigestMismatch,
                    },
                ]
                .into_iter()
                .find_map(|case| case.refused.then_some(case.decision))
                .unwrap_or(Admit);
                let facts = Facts {
                    object_count,
                    listed_count,
                    identity: Identity {
                        unique_keys,
                        coordinates_match,
                    },
                    availability: Availability {
                        all_present,
                        sizes_match,
                    },
                    digests: Digests {
                        require_digests,
                        digests_match,
                    },
                };
                assert!(
                    decide(facts) == expected,
                    "count={object_count} listed={listed_count} flags={bits}"
                );
            }
        }
    }
}

struct Observed<'a> {
    identity: Identity,
    availability: Availability,
    digests: Digests,
    counts: &'a [u64],
    listed: u64,
    events: &'a RefCell<Vec<&'static str>>,
}

impl Observed<'_> {
    fn count(&self) -> u64 {
        let index = self
            .events
            .borrow()
            .iter()
            .filter(|&&event| event == "count")
            .count();
        self.events.borrow_mut().push("count");
        self.counts[index]
    }

    fn listed(&self) -> u64 {
        self.events.borrow_mut().push("listed");
        self.listed
    }
}

macro_rules! observe_count {
    ($facts:ident) => {
        $facts.count()
    };
}

krabka_macros::worm_object_set! {
    Runtime;
    {
        fn observed(facts: Observed<'_>) -> WormObjectSetDecision
    }
    (facts);
    count [observe_count!(facts)] listed [{ facts.listed() }]
}

#[test]
fn observations_keep_native_macro_scope_short_circuiting_and_repeated_reads() {
    use WormObjectSetDecision::{
        Admit, CoordinateMismatch, CountMismatch, DuplicateKey, Empty, MissingObject,
    };

    for ObservationCase {
        counts,
        unique_keys,
        coordinates_match,
        all_present,
        listed,
        expected,
        expected_events,
    } in [
        ObservationCase {
            counts: &[0][..],
            unique_keys: false,
            coordinates_match: false,
            all_present: false,
            listed: 1,
            expected: Empty,
            expected_events: &["count"][..],
        },
        ObservationCase {
            counts: &[1][..],
            unique_keys: false,
            coordinates_match: false,
            all_present: false,
            listed: 1,
            expected: DuplicateKey,
            expected_events: &["count"],
        },
        ObservationCase {
            counts: &[1][..],
            unique_keys: true,
            coordinates_match: false,
            all_present: false,
            listed: 1,
            expected: CoordinateMismatch,
            expected_events: &["count"],
        },
        ObservationCase {
            counts: &[1][..],
            unique_keys: true,
            coordinates_match: true,
            all_present: false,
            listed: 1,
            expected: MissingObject,
            expected_events: &["count"],
        },
        ObservationCase {
            counts: &[1, 2][..],
            unique_keys: true,
            coordinates_match: true,
            all_present: true,
            listed: 1,
            expected: CountMismatch,
            expected_events: &["count", "count", "listed"],
        },
        ObservationCase {
            counts: &[1, 2][..],
            unique_keys: true,
            coordinates_match: true,
            all_present: true,
            listed: 2,
            expected: Admit,
            expected_events: &["count", "count", "listed"],
        },
    ] {
        let events = RefCell::new(Vec::new());
        let facts = Observed {
            identity: Identity {
                unique_keys,
                coordinates_match,
            },
            availability: Availability {
                all_present,
                sizes_match: true,
            },
            digests: Digests {
                require_digests: false,
                digests_match: false,
            },
            counts,
            listed,
            events: &events,
        };
        assert!(observed(facts) == expected);
        assert!(events.borrow().as_slice() == expected_events);
    }
}

struct ObservationCase {
    counts: &'static [u64],
    unique_keys: bool,
    coordinates_match: bool,
    all_present: bool,
    listed: u64,
    expected: WormObjectSetDecision,
    expected_events: &'static [&'static str],
}

struct RefusalCase {
    refused: bool,
    decision: WormObjectSetDecision,
}
