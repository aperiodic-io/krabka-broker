//! Static SASL/PLAIN credentials with redacted debug output.

/// The broker's static SASL/PLAIN credential table: username → password.
///
/// A newtype rather than a bare `HashMap` so that `BrokerConfig`'s derived
/// `Debug` cannot print a password. The hand-written `Debug` below prints the
/// usernames alone.
#[derive(Clone, Default, PartialEq, Eq, derive_more::Deref, derive_more::DerefMut)]
pub struct PlainCredentials(std::collections::HashMap<String, String>);

impl PlainCredentials {
    /// The table as a plain map, for
    /// [`krabka_security::verify_plain`].
    #[must_use]
    pub fn as_map(&self) -> &std::collections::HashMap<String, String> { &self.0 }
}

/// Prints the usernames and no password, so `{config:?}` cannot leak the
/// credential table.
impl std::fmt::Debug for PlainCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl FromIterator<(String, String)> for PlainCredentials {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
