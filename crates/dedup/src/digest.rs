use {
    std::hash::{BuildHasherDefault, Hash, Hasher},
    twox_hash::XxHash3_128,
};

// Any fixed value works. It stays fixed so that tests can pin digests and
// every process digests an identity the same way.
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// A fixed-width digest of one dedup identity.
///
/// The index stores this in place of the identity bytes, so each entry costs the same whatever the size of the Kafka key or header value. The digest is xxh3-128 with a fixed seed. At the 10^7-entry upper limit, the probability of a collision is near 10^-25, far below the window-edge error the feature already accepts.
///
/// xxh3 does not resist a chosen-input attacker. This is acceptable: a producer that wants to suppress another producer's identity can write that identity first, because dedup is first-wins on any identity it gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdentityDigest {
    /// The high 64 bits of the digest.
    pub hi: u64,
    /// The low 64 bits of the digest. The index hashes this half only.
    pub lo: u64,
}

impl IdentityDigest {
    /// Digests one identity.
    #[must_use]
    pub fn of(identity: &[u8]) -> Self {
        let wide = XxHash3_128::oneshot_with_seed(SEED, identity);
        Self {
            hi: u64::try_from(wide >> 64).unwrap_or(u64::MAX),
            lo: u64::try_from(wide & u128::from(u64::MAX)).unwrap_or(u64::MAX),
        }
    }
}

// The digest is already avalanched, so the map takes one half as it is and
// does not mix it again.
impl Hash for IdentityDigest {
    fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(self.lo); }
}

/// A [`Hasher`] that passes an [`IdentityDigest`] through without mixing it again.
#[derive(Clone, Copy, Debug, Default)]
pub struct DigestHasher {
    state: u64,
}

impl Hasher for DigestHasher {
    fn finish(&self) -> u64 { self.state }

    // Only `write_u64` is reached through `IdentityDigest`. This fold keeps the
    // hasher correct, if weak, for any other input.
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state = self.state.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) { self.state = value; }
}

/// The [`std::hash::BuildHasher`] of the dedup index.
pub type DigestBuildHasher = BuildHasherDefault<DigestHasher>;

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::hash::BuildHasher};

    // Prefixes are the case to pin: a digest that ignored length would let
    // "ab" and "abc" share an entry.
    #[test]
    fn distinct_identities_get_distinct_digests() {
        let ab = IdentityDigest::of(b"ab");
        let abc = IdentityDigest::of(b"abc");
        let abd = IdentityDigest::of(b"abd");
        let empty = IdentityDigest::of(b"");

        assert!(ab != abc);
        assert!(abc != abd);
        assert!(ab != empty);
        assert!(ab == IdentityDigest::of(b"ab"));
    }

    // Catches an accidental change of the seed or the algorithm, which would
    // make every test that names a digest meaningless.
    #[test]
    fn digest_is_pinned() {
        assert!(
            IdentityDigest::of(b"dedup-identity")
                == IdentityDigest {
                    hi: 0x9571_F597_7D6C_10CD,
                    lo: 0x1B99_4BB1_00BD_7EA2,
                }
        );
    }

    #[test]
    fn large_identities_digest_by_content() {
        let big_a = vec![b'a'; 200_000];
        let big_b = vec![b'b'; 200_000];
        assert!(IdentityDigest::of(&big_a) == IdentityDigest::of(&vec![b'a'; 200_000]));
        assert!(IdentityDigest::of(&big_a) != IdentityDigest::of(&big_b));
    }

    #[test]
    fn hasher_passes_the_low_half_through() {
        let digest = IdentityDigest {
            hi: 7,
            lo: 0xDEAD_BEEF,
        };
        assert!(DigestBuildHasher::default().hash_one(digest) == 0xDEAD_BEEF);
    }

    #[test]
    fn hasher_folds_other_input() {
        let mut hasher = DigestHasher::default();
        hasher.write(&[1, 2]);
        assert!(hasher.finish() == 0x0102);
    }
}
