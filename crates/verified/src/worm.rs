//! Admission decisions for signed WORM manifests and their object sets.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

model_types! {
    @derives (derive(DeepModel))
        (derive(Debug, Clone, Copy, PartialEq, Eq));
    /// Why a manifest signature is not an accepted attestation.
    pub enum WormSignatureDecision {
        Unsigned,
        Untrusted,
        Invalid,
        Admit,
    }
}

/// Requires a present signature, an externally trusted key, and verification
/// against the canonical manifest signing bytes.
#[ensures(!present ==> result == WormSignatureDecision::Unsigned)]
#[ensures(present && !trusted ==> result == WormSignatureDecision::Untrusted)]
#[ensures(present && trusted && !canonical_valid
    ==> result == WormSignatureDecision::Invalid)]
#[ensures(present && trusted && canonical_valid
    ==> result == WormSignatureDecision::Admit)]
#[must_use]
pub fn worm_signature_decision(
    present: bool,
    trusted: bool,
    canonical_valid: bool,
) -> WormSignatureDecision {
    if !present {
        WormSignatureDecision::Unsigned
    } else if !trusted {
        WormSignatureDecision::Untrusted
    } else if !canonical_valid {
        WormSignatureDecision::Invalid
    } else {
        WormSignatureDecision::Admit
    }
}

model_types! {
    @derives (derive(DeepModel))
        (derive(Debug, Clone, Copy, PartialEq, Eq));
    /// Why the objects named by one manifest are not an exact archive set.
    pub enum WormObjectSetDecision {
        Empty,
        DuplicateKey,
        CoordinateMismatch,
        MissingObject,
        CountMismatch,
        SizeMismatch,
        DigestMismatch,
        Admit,
    }

    /// Host observations about the objects belonging to one signed segment.
    pub struct WormObjectIdentityFacts {
        pub unique_keys: bool,
        pub coordinates_match: bool,
    }

    pub struct WormObjectAvailabilityFacts {
        pub all_present: bool,
        pub sizes_match: bool,
    }

    pub struct WormDigestFacts {
        pub require_digests: bool,
        pub digests_match: bool,
    }

    pub struct WormObjectSetFacts {
        pub object_count: u64,
        pub listed_count: u64,
        pub identity: WormObjectIdentityFacts,
        pub availability: WormObjectAvailabilityFacts,
        pub digests: WormDigestFacts,
    }
}

macro_rules! worm_rule_body {
    (model; {$($head:tt)*} {$($body:tt)*}) => { $($head)* { pearlite! { $($body)* } } };
    (runtime; {$($head:tt)*} {$($body:tt)*}) => { $($head)* { $($body)* } };
}

macro_rules! worm_object_set_kernel {
    ($mode:ident; {$($head:tt)*} ($facts:ident);
        count [$($count:tt)*] listed [$($listed:tt)*]) => {
        worm_rule_body! { $mode;
            { $($head)* }
            {
                if $($count)* == 0 {
                    WormObjectSetDecision::Empty
                } else if !$facts.identity.unique_keys {
                    WormObjectSetDecision::DuplicateKey
                } else if !$facts.identity.coordinates_match {
                    WormObjectSetDecision::CoordinateMismatch
                } else if !$facts.availability.all_present {
                    WormObjectSetDecision::MissingObject
                } else if $($count)* != $($listed)* {
                    WormObjectSetDecision::CountMismatch
                } else if !$facts.availability.sizes_match {
                    WormObjectSetDecision::SizeMismatch
                } else if $facts.digests.require_digests && !$facts.digests.digests_match {
                    WormObjectSetDecision::DigestMismatch
                } else {
                    WormObjectSetDecision::Admit
                }
            }
        }
    };
}

worm_object_set_kernel! {
    model;
{
    /// The object-set rule, as the first failed check in diagnostic order: an
    /// empty set, a repeated key, a key at the wrong coordinates, an object the
    /// store does not hold, a listing with another object count, an object of
    /// another size, and, when digests are required, a digest mismatch. A set
    /// that passes every check is `Admit`.
    // cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
    #[cfg(creusot)]
    #[cfg_attr(test, mutants::skip)]
    #[logic]
    fn worm_object_set_model(facts: WormObjectSetFacts) -> WormObjectSetDecision
}
(facts);
    count [facts.object_count@] listed [facts.listed_count@]
}

worm_object_set_kernel! {
    runtime;
{
    /// Requires a nonempty, one-to-one object set with exact coordinates, sizes,
    /// and, when requested, digests; see `worm_object_set_model`.
    #[ensures(result == worm_object_set_model(facts))]
    #[must_use]
    pub fn worm_object_set_decision(facts: WormObjectSetFacts) -> WormObjectSetDecision
}
(facts);
    count [facts.object_count] listed [facts.listed_count]
}

#[cfg(test)]
mod tests {
    use {
        super::{
            WormDigestFacts, WormObjectAvailabilityFacts, WormObjectIdentityFacts,
            WormObjectSetDecision, WormObjectSetFacts, WormSignatureDecision,
            worm_object_set_decision, worm_signature_decision,
        },
        assert2::assert,
    };

    #[test]
    fn signatures_require_every_attestation_fact() {
        assert!(worm_signature_decision(false, true, true) == WormSignatureDecision::Unsigned);
        assert!(worm_signature_decision(true, false, true) == WormSignatureDecision::Untrusted);
        assert!(worm_signature_decision(true, true, false) == WormSignatureDecision::Invalid);
        assert!(worm_signature_decision(true, true, true) == WormSignatureDecision::Admit);
    }

    #[test]
    fn object_sets_fail_closed_in_diagnostic_order() {
        use WormObjectSetDecision::{
            Admit, CoordinateMismatch, CountMismatch, DigestMismatch, DuplicateKey, Empty,
            MissingObject, SizeMismatch,
        };

        let exact = WormObjectSetFacts {
            object_count: 1,
            listed_count: 1,
            identity: WormObjectIdentityFacts {
                unique_keys: true,
                coordinates_match: true,
            },
            availability: WormObjectAvailabilityFacts {
                all_present: true,
                sizes_match: true,
            },
            digests: WormDigestFacts {
                require_digests: true,
                digests_match: true,
            },
        };
        // Every refusal also fails the checks after it, so each row shows the
        // earlier check outranking the later ones.
        let broken = WormObjectSetFacts {
            object_count: 1,
            listed_count: 2,
            identity: WormObjectIdentityFacts {
                unique_keys: false,
                coordinates_match: false,
            },
            availability: WormObjectAvailabilityFacts {
                all_present: false,
                sizes_match: false,
            },
            digests: WormDigestFacts {
                require_digests: true,
                digests_match: false,
            },
        };
        for (what, facts, expected) in [
            (
                "an empty set",
                WormObjectSetFacts {
                    object_count: 0,
                    listed_count: 0,
                    ..broken
                },
                Empty,
            ),
            ("a repeated key", broken, DuplicateKey),
            (
                "a key at the wrong coordinates",
                WormObjectSetFacts {
                    identity: WormObjectIdentityFacts {
                        unique_keys: true,
                        coordinates_match: false,
                    },
                    ..broken
                },
                CoordinateMismatch,
            ),
            (
                "a missing object",
                WormObjectSetFacts {
                    identity: exact.identity,
                    ..broken
                },
                MissingObject,
            ),
            (
                "another object count",
                WormObjectSetFacts {
                    identity: exact.identity,
                    availability: WormObjectAvailabilityFacts {
                        all_present: true,
                        sizes_match: false,
                    },
                    ..broken
                },
                CountMismatch,
            ),
            (
                "another object size",
                WormObjectSetFacts {
                    identity: exact.identity,
                    availability: WormObjectAvailabilityFacts {
                        all_present: true,
                        sizes_match: false,
                    },
                    listed_count: 1,
                    ..broken
                },
                SizeMismatch,
            ),
            (
                "a required digest mismatch",
                WormObjectSetFacts {
                    digests: broken.digests,
                    ..exact
                },
                DigestMismatch,
            ),
            (
                "a digest mismatch nobody required",
                WormObjectSetFacts {
                    digests: WormDigestFacts {
                        require_digests: false,
                        digests_match: false,
                    },
                    ..exact
                },
                Admit,
            ),
            ("an exact set", exact, Admit),
        ] {
            assert!(worm_object_set_decision(facts) == expected, "{what}");
        }
    }
}
