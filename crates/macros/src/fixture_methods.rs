//! Missing remote reads and authorizer delegation on visible trait implementations.

use {
    crate::meta::{edit_native_body, no_arguments},
    moxy::{
        ast::{ItemImpl, ParseError},
        token::{Spanner, TokenStream, TokenTree},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

fn check_impl(item: &TokenStream, expected: &str) -> Result<(), ParseError> {
    let mut tokens = item.clone().into_inner();
    let Some(TokenTree::Group(body)) = tokens.last_mut() else {
        return Err(ParseError::new(
            item.span(),
            "expected one trait implementation",
        ));
    };
    if !body.delim.is_brace() {
        return Err(ParseError::new(
            body.span(),
            "expected an implementation body",
        ));
    }
    body.tokens = TokenStream::new();
    let header: ItemImpl = moxy::parse!({ TokenStream::from(tokens) })?;
    if header.unsafety.is_some()
        || !header.trait_ref.as_ref().is_some_and(|contract| {
            contract.polarity.is_none()
                && contract
                    .path
                    .last()
                    .is_some_and(|segment| segment.ident.text() == expected)
        })
    {
        return Err(ParseError::new(
            item.span(),
            format!("expected an impl of {expected}"),
        ));
    }
    Ok(())
}

fn authorizer_methods(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    no_arguments(meta.clone(), "delegate_authorizer")?;
    check_impl(item, "Authorizer")?;
    Ok(moxy::template! {
        fn is_configured(&self) -> bool { self.0.is_configured() }
        fn decision_ttl(&self) -> Option<::std::time::Duration> { self.0.decision_ttl() }
        fn authorize_by_resource_type(
            &self,
            source: &dyn ::krabka_authz::AclSource,
            principal: &::krabka_security::Principal,
            host: &::std::net::SocketAddr,
            resource_type: ::krabka_metadata::ResourceType,
            operation: ::krabka_metadata::AclOperation,
        ) -> ::krabka_authz::AuthorizationResult {
            self.0.authorize_by_resource_type(source, principal, host, resource_type, operation)
        }
    })
}

fn remote_methods(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    let delete_ok = if meta.is_empty() {
        false
    } else {
        let policy: moxy::token::Ident = moxy::parse!({ meta })?;
        if policy.text() != "delete_ok" {
            return Err(ParseError::new(
                policy.span(),
                "expected delete_ok or no delete policy",
            ));
        }
        true
    };
    check_impl(item, "RemoteStorageManager")?;
    Ok(moxy::template! {
        fn fetch_log_segment(
            &self,
            metadata: &::krabka_remote_storage::RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, ::krabka_remote_storage::RemoteStorageError> {
            Err(::krabka_remote_storage::RemoteStorageError::SegmentNotFound(metadata.remote_log_segment_id().clone()))
        }
        fn fetch_index(
            &self,
            metadata: &::krabka_remote_storage::RemoteLogSegmentMetadata,
            _index_type: ::krabka_remote_storage::IndexType,
        ) -> Result<Vec<u8>, ::krabka_remote_storage::RemoteStorageError> {
            Err(::krabka_remote_storage::RemoteStorageError::SegmentNotFound(metadata.remote_log_segment_id().clone()))
        }
        @if delete_ok {
            fn delete_log_segment_data(
                &self,
                _metadata: &::krabka_remote_storage::RemoteLogSegmentMetadata,
            ) -> Result<(), ::krabka_remote_storage::RemoteStorageError> { Ok(()) }
        }
    })
}

fn append_methods(item: NativeTokenStream, methods: TokenStream) -> NativeTokenStream {
    edit_native_body(item, |mut body| {
        body.extend(NativeTokenStream::from(methods));
        body
    })
}

pub(crate) fn authorizer(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let methods = authorizer_methods(&meta.into(), &item.clone().into())?;
    Ok(append_methods(item, methods))
}

pub(crate) fn remote(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let methods = remote_methods(&meta.into(), &item.clone().into())?;
    Ok(append_methods(item, methods))
}

#[cfg(test)]
mod tests {
    use {
        super::{authorizer_methods, remote_methods},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn metadata_delegation_retains_all_three_authorizer_contracts() {
        let output = authorizer_methods(
            &"".parse().unwrap(),
            &"impl<A: Authorizer> Authorizer for Wrapper<A> { caller!(@native); }"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let output = compact(&output);
        assert!(
            output.contains("self.0.is_configured()") && output.contains("self.0.decision_ttl()")
        );
        assert!(output.contains(
            "self.0.authorize_by_resource_type(source,principal,host,resource_type,operation)"
        ));
        assert!(
            authorizer_methods(
                &"option".parse().unwrap(),
                &"impl Authorizer for Wrapper {}".parse().unwrap()
            )
            .is_err()
        );
        assert!(
            authorizer_methods(&"".parse().unwrap(), &"impl Wrapper {}".parse().unwrap()).is_err()
        );
    }

    #[yare::parameterized(
        default = { "" },
        delete_ok = { "delete_ok" },
    )]
    fn missing_reads_keep_segment_identity_and_only_requested_delete_success(policy: &str) {
        let item = "impl RemoteStorageManager for Backend {}".parse().unwrap();
        let output = compact(&remote_methods(&policy.parse().unwrap(), &item).unwrap());
        assert2::assert!(
            output
                .matches("SegmentNotFound(metadata.remote_log_segment_id().clone())")
                .count()
                == 2
        );
        assert2::assert!(output.contains("fndelete_log_segment_data") == !policy.is_empty());
    }

    #[yare::parameterized(
        unsupported = { "allow" },
        extra_tokens = { "delete_ok, extra" },
    )]
    fn refuses_unsupported_remote_method_policies(policy: &str) {
        let item = "impl RemoteStorageManager for Backend {}".parse().unwrap();
        assert2::assert!(remote_methods(&policy.parse().unwrap(), &item).is_err());
    }

    #[test]
    fn refuses_an_unrelated_trait() {
        assert!(
            remote_methods(
                &"".parse().unwrap(),
                &"impl Other for Backend {}".parse().unwrap()
            )
            .is_err()
        );
    }
}
