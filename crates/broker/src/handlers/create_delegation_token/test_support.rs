//! Super-user sets and act-as ACL fixtures for `CreateDelegationToken`.

use crate::authorizer::SimpleAclAuthorizer;

/// An ACL authorizer with no super users and no ACLs, for tests that do not
/// exercise the act-as path.
pub(super) fn empty_super_users() -> SimpleAclAuthorizer { super_users_with(&[]) }

/// An ACL authorizer whose super users are `names`, with no ACLs.
pub(super) fn super_users_with(names: &[&str]) -> SimpleAclAuthorizer {
    SimpleAclAuthorizer::new(names.iter().map(|s| (*s).to_string()).collect())
}

/// The `CreateTokens` check inputs over `authorizer`, from a fixed peer.
pub(super) fn token_acl(authorizer: &SimpleAclAuthorizer) -> super::TokenAcl<'_> {
    static PEER: std::sync::LazyLock<std::net::SocketAddr> =
        std::sync::LazyLock::new(|| "127.0.0.1:50000".parse().expect("static addr"));
    super::TokenAcl {
        authorizer,
        peer: &PEER,
    }
}
