//! Admission decisions for signed WORM manifests and their object sets.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

/// Why a manifest signature is not an accepted attestation.
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub enum WormSignatureDecision {
    Unsigned,
    Untrusted,
    Invalid,
    Admit,
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

/// Why the objects named by one manifest are not an exact archive set.
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
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
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub struct WormObjectIdentityFacts {
    pub unique_keys: bool,
    pub coordinates_match: bool,
}

#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub struct WormObjectAvailabilityFacts {
    pub all_present: bool,
    pub sizes_match: bool,
}

#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub struct WormDigestFacts {
    pub require_digests: bool,
    pub digests_match: bool,
}

#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub struct WormObjectSetFacts {
    pub object_count: u64,
    pub listed_count: u64,
    pub identity: WormObjectIdentityFacts,
    pub availability: WormObjectAvailabilityFacts,
    pub digests: WormDigestFacts,
}

krabka_macros::worm_object_set! {
    Model;
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

krabka_macros::worm_object_set! {
    Runtime;
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

    fn exact_object_set() -> WormObjectSetFacts {
        WormObjectSetFacts {
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
        }
    }

    fn broken_object_set() -> WormObjectSetFacts {
        // Every refusal also fails the checks after it, so each row shows the
        // earlier check outranking the later ones.
        WormObjectSetFacts {
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
        }
    }

    #[yare::parameterized(
        empty = {
            "an empty set",
            WormObjectSetFacts { object_count: 0, listed_count: 0, ..broken_object_set() },
            WormObjectSetDecision::Empty
        },
        duplicate_key = {
            "a repeated key",
            broken_object_set(),
            WormObjectSetDecision::DuplicateKey,
        },
        coordinate_mismatch = {
            "a key at the wrong coordinates",
            WormObjectSetFacts {
                identity: WormObjectIdentityFacts {
                    unique_keys: true,
                    coordinates_match: false,
                },
                ..broken_object_set()
            },
            WormObjectSetDecision::CoordinateMismatch,
        },
        missing_object = {
            "a missing object",
            WormObjectSetFacts {
                identity: WormObjectIdentityFacts {
                    unique_keys: true,
                    coordinates_match: true,
                },
                ..broken_object_set()
            },
            WormObjectSetDecision::MissingObject,
        },
        count_mismatch = {
            "another object count",
            WormObjectSetFacts {
                identity: WormObjectIdentityFacts {
                    unique_keys: true,
                    coordinates_match: true,
                },
                availability: WormObjectAvailabilityFacts {
                    all_present: true,
                    sizes_match: false,
                },
                ..broken_object_set()
            },
            WormObjectSetDecision::CountMismatch,
        },
        size_mismatch = {
            "another object size",
            WormObjectSetFacts {
                identity: WormObjectIdentityFacts {
                    unique_keys: true,
                    coordinates_match: true,
                },
                availability: WormObjectAvailabilityFacts {
                    all_present: true,
                    sizes_match: false,
                },
                listed_count: 1,
                ..broken_object_set()
            },
            WormObjectSetDecision::SizeMismatch,
        },
        required_digest_mismatch = {
            "a required digest mismatch",
            WormObjectSetFacts {
                digests: WormDigestFacts {
                    require_digests: true,
                    digests_match: false,
                },
                ..exact_object_set()
            },
            WormObjectSetDecision::DigestMismatch,
        },
        optional_digest_mismatch = {
            "a digest mismatch nobody required",
            WormObjectSetFacts {
                digests: WormDigestFacts {
                    require_digests: false,
                    digests_match: false,
                },
                ..exact_object_set()
            },
            WormObjectSetDecision::Admit,
        },
        exact = { "an exact set", exact_object_set(), WormObjectSetDecision::Admit },
    )]
    fn object_sets_fail_closed_in_diagnostic_order(
        what: &'static str,
        facts: WormObjectSetFacts,
        expected: WormObjectSetDecision,
    ) {
        assert2::assert!(worm_object_set_decision(facts) == expected, "{what}");
    }
}
