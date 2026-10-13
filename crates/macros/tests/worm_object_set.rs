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
                    (object_count == 0, Empty),
                    (!unique_keys, DuplicateKey),
                    (!coordinates_match, CoordinateMismatch),
                    (!all_present, MissingObject),
                    (object_count != listed_count, CountMismatch),
                    (!sizes_match, SizeMismatch),
                    (require_digests && !digests_match, DigestMismatch),
                ]
                .into_iter()
                .find_map(|(refused, decision)| refused.then_some(decision))
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

    for (counts, unique_keys, coordinates_match, all_present, listed, expected, expected_events) in [
        (&[0][..], false, false, false, 1, Empty, &["count"][..]),
        (&[1][..], false, false, false, 1, DuplicateKey, &["count"]),
        (&[1][..], true, false, false, 1, CoordinateMismatch, &[
            "count",
        ]),
        (&[1][..], true, true, false, 1, MissingObject, &["count"]),
        (&[1, 2][..], true, true, true, 1, CountMismatch, &[
            "count", "count", "listed",
        ]),
        (&[1, 2][..], true, true, true, 2, Admit, &[
            "count", "count", "listed",
        ]),
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
