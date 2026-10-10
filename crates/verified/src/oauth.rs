//! OAuth session-lifetime and reauthentication admission.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, logic};

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Whether token validation supplied an absolute expiry.
    pub enum OAuthExpiryPresence {
        Missing,
        Present,
    }

    /// Whether the broker applies a maximum session lifetime.
    pub enum OAuthSessionCap {
        Disabled,
        Enabled,
    }

    /// Authentication phase being admitted.
    pub enum OAuthAuthenticationKind {
        Initial,
        Reauthentication,
    }

    /// Relationship between the prior and validated principals.
    pub enum OAuthPrincipalMatch {
        Matches,
        Differs,
    }

    /// Inputs that bind a validated token to one broker session.
    pub struct OAuthSessionFacts {
        pub expiry: OAuthExpiryPresence,
        pub token_expires_at_ms: i64,
        pub now_ms: i64,
        pub cap: OAuthSessionCap,
        pub cap_ms: i64,
        pub authentication: OAuthAuthenticationKind,
        pub principal: OAuthPrincipalMatch,
    }

    /// Session state selected after token validation.
    pub enum OAuthSessionDecision {
        Reject,
        Admit {
            session_lifetime_ms: i64,
            effective_expires_at_ms: i64,
        },
    }
}

open_logic! {
/// Session admissibility at the clock when token validation completes.
pub(crate) fn oauth_session_admissible_at(facts: OAuthSessionFacts, now: Int) -> bool {
    pearlite! {
        facts.expiry == OAuthExpiryPresence::Present
            && facts.token_expires_at_ms@ > now
            && facts.token_expires_at_ms@ - now <= i64::MAX@
            && (facts.cap == OAuthSessionCap::Disabled || facts.cap_ms@ > 0)
            && (facts.authentication == OAuthAuthenticationKind::Initial
                || facts.principal == OAuthPrincipalMatch::Matches)
    }
}
}

/// Whether a validated token may bind a session: it carries an expiry
/// strictly in the future, its remaining lifetime fits an `i64`, an enabled
/// cap is positive, and a reauthentication keeps the prior principal
/// (KIP-368).
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub fn oauth_session_admissible(facts: OAuthSessionFacts) -> bool {
    pearlite! { oauth_session_admissible_at(facts, facts.now_ms@) }
}

/// The session lifetime: the token's remaining lifetime, bounded above by an
/// enabled cap.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub fn oauth_session_lifetime(facts: OAuthSessionFacts) -> Int {
    pearlite! {
        let token_lifetime = facts.token_expires_at_ms@ - facts.now_ms@;
        if facts.cap == OAuthSessionCap::Enabled && facts.cap_ms@ < token_lifetime {
            facts.cap_ms@
        } else {
            token_lifetime
        }
    }
}

/// Admit exactly the admissible sessions, with the smaller of the token
/// lifetime and the enabled cap, and the matching absolute expiry.
#[ensures(match result {
    OAuthSessionDecision::Reject => !oauth_session_admissible(facts),
    OAuthSessionDecision::Admit {
        session_lifetime_ms,
        effective_expires_at_ms,
    } => oauth_session_admissible(facts)
        && session_lifetime_ms@ == oauth_session_lifetime(facts)
        && effective_expires_at_ms@ == facts.now_ms@ + session_lifetime_ms@,
})]
#[must_use]
pub fn oauth_session_admission(facts: OAuthSessionFacts) -> OAuthSessionDecision {
    if let OAuthExpiryPresence::Missing = facts.expiry {
        return OAuthSessionDecision::Reject;
    }
    if facts.token_expires_at_ms <= facts.now_ms {
        return OAuthSessionDecision::Reject;
    }
    if let (OAuthAuthenticationKind::Reauthentication, OAuthPrincipalMatch::Differs) =
        (facts.authentication, facts.principal)
    {
        return OAuthSessionDecision::Reject;
    }
    if let OAuthSessionCap::Enabled = facts.cap
        && facts.cap_ms <= 0
    {
        return OAuthSessionDecision::Reject;
    }
    let Some(token_lifetime_ms) = facts.token_expires_at_ms.checked_sub(facts.now_ms) else {
        return OAuthSessionDecision::Reject;
    };
    let session_lifetime_ms = match facts.cap {
        OAuthSessionCap::Enabled => token_lifetime_ms.min(facts.cap_ms),
        OAuthSessionCap::Disabled => token_lifetime_ms,
    };
    let Some(effective_expires_at_ms) = facts.now_ms.checked_add(session_lifetime_ms) else {
        return OAuthSessionDecision::Reject;
    };
    OAuthSessionDecision::Admit {
        session_lifetime_ms,
        effective_expires_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OAuthAuthenticationKind, OAuthExpiryPresence, OAuthPrincipalMatch, OAuthSessionCap,
        OAuthSessionDecision, OAuthSessionFacts, oauth_session_admission,
    };

    fn facts() -> OAuthSessionFacts {
        OAuthSessionFacts {
            expiry: OAuthExpiryPresence::Present,
            token_expires_at_ms: 1_100,
            now_ms: 1_000,
            cap: OAuthSessionCap::Disabled,
            cap_ms: 0,
            authentication: OAuthAuthenticationKind::Initial,
            principal: OAuthPrincipalMatch::Matches,
        }
    }

    #[test]
    fn session_expiry_is_exact_and_capped() {
        let admit = |session_lifetime_ms, effective_expires_at_ms| OAuthSessionDecision::Admit {
            session_lifetime_ms,
            effective_expires_at_ms,
        };
        for (facts, expected) in [
            (facts(), admit(100, 1_100)),
            (
                OAuthSessionFacts {
                    cap: OAuthSessionCap::Enabled,
                    cap_ms: 40,
                    ..facts()
                },
                admit(40, 1_040),
            ),
            // A cap longer than the token keeps the token's own expiry.
            (
                OAuthSessionFacts {
                    cap: OAuthSessionCap::Enabled,
                    cap_ms: 500,
                    ..facts()
                },
                admit(100, 1_100),
            ),
            // KIP-368: reauthentication as the same principal is admitted.
            (
                OAuthSessionFacts {
                    authentication: OAuthAuthenticationKind::Reauthentication,
                    ..facts()
                },
                admit(100, 1_100),
            ),
            // The largest representable lifetime.
            (
                OAuthSessionFacts {
                    token_expires_at_ms: i64::MAX,
                    now_ms: 0,
                    ..facts()
                },
                admit(i64::MAX, i64::MAX),
            ),
        ] {
            assert2::check!(oauth_session_admission(facts) == expected);
        }
    }

    #[test]
    fn invalid_lifetime_or_principal_fails_closed() {
        for rejected in [
            OAuthSessionFacts {
                expiry: OAuthExpiryPresence::Missing,
                ..facts()
            },
            OAuthSessionFacts {
                token_expires_at_ms: 1_000,
                ..facts()
            },
            OAuthSessionFacts {
                token_expires_at_ms: i64::MAX,
                now_ms: -1,
                ..facts()
            },
            OAuthSessionFacts {
                cap: OAuthSessionCap::Enabled,
                cap_ms: 0,
                ..facts()
            },
            OAuthSessionFacts {
                authentication: OAuthAuthenticationKind::Reauthentication,
                principal: OAuthPrincipalMatch::Differs,
                ..facts()
            },
        ] {
            assert2::check!(oauth_session_admission(rejected) == OAuthSessionDecision::Reject);
        }
    }
}

mod completion;
pub use completion::oauth_validation_admission;
