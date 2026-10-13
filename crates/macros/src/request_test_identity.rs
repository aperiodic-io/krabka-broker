//! Visible identity bindings or hygienic context resources, without recursive fixture presets.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups},
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{List, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Span, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

enum ContextPolicy {
    Absent,
    DefaultBuilder,
    Builder,
    Client,
    ConnectedClient,
}

struct Identity {
    principal: Ident,
    peer: Ident,
    context: Option<Ident>,
    user: bool,
    policy: ContextPolicy,
}

fn binding_names(names: &[Ident], span: Span) -> Result<Identity, ParseError> {
    let mut identity = Identity {
        principal: Ident::new("__request_principal").with_span(Span::mixed_site()),
        peer: Ident::new("__request_peer").with_span(Span::mixed_site()),
        context: None,
        user: false,
        policy: ContextPolicy::Absent,
    };
    match names {
        [context] => identity.context = Some(context.clone()),
        [principal, peer, context @ ..] if context.len() <= 1 => {
            identity.principal = principal.clone();
            identity.peer = peer.clone();
            identity.context = context.first().cloned();
        }
        _ => {
            return Err(ParseError::new(
                span,
                "bind one context or two/three identity names",
            ));
        }
    }
    Ok(identity)
}

fn declaration(input: &TokenStream) -> Result<Identity, ParseError> {
    let parser = Parser::from_tokens(input);
    expect_name(
        &parser,
        "bind",
        "expected bind(context) or bind(principal, peer[, context])",
    )?;
    let group = grouped_value(&parser, "expected identity binding tuple")?;
    if !group.delim.is_paren() {
        return Err(ParseError::new(
            group.span(),
            "identity bindings need parentheses",
        ));
    }
    let names = List::<Ident, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
    let names: Vec<_> = names.into_iter().collect();
    let mut identity = binding_names(&names, group.span())?;
    let _: Token![;] = Parse::parse(&parser)?;
    let mut clauses = Vec::new();
    while !parser.is_empty() {
        let name = Ident::parse(&parser)?;
        if !matches!(
            name.text(),
            "identity" | "address" | "user" | "context" | "client" | "connection"
        ) {
            return Err(ParseError::new(
                name.span(),
                "expected identity/address, user, context/client or connection",
            ));
        }
        let group = grouped_value(&parser, "expected bracketed identity expression")?;
        if !group.delim.is_bracket() || group.tokens.is_empty() {
            return Err(ParseError::new(
                group.span(),
                "expected nonempty bracketed identity expression",
            ));
        }
        if name == "context" {
            let _: Path = moxy::parse!({ group.tokens.clone() })?;
        }
        let _: Token![;] = Parse::parse(&parser)?;
        clauses.push(name.text().to_owned());
    }
    let user = clauses.first().is_some_and(|name| name == "user");
    let required = if user {
        &["user"][..]
    } else {
        &["identity", "address"][..]
    };
    if !clauses
        .iter()
        .take(required.len())
        .map(String::as_str)
        .eq(required.iter().copied())
    {
        return Err(ParseError::new(
            input.span(),
            "begin with user, or identity then address",
        ));
    }
    let policy = &clauses[required.len()..];
    let default_builder = user && identity.context.is_some() && policy.is_empty();
    let valid_policy = if identity.context.is_none() {
        policy.is_empty()
    } else {
        default_builder
            || matches!(
                policy
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .as_slice(),
                ["context" | "client"] | ["client", "connection"]
            )
    };
    if !valid_policy {
        return Err(ParseError::new(
            input.span(),
            "a context needs a builder or client; connection is valid only with client",
        ));
    }
    let policy = if identity.context.is_none() {
        ContextPolicy::Absent
    } else if default_builder {
        ContextPolicy::DefaultBuilder
    } else if policy.first().is_some_and(|name| name == "context") {
        ContextPolicy::Builder
    } else if policy.len() == 2 {
        ContextPolicy::ConnectedClient
    } else {
        ContextPolicy::Client
    };
    identity.user = user;
    identity.policy = policy;
    Ok(identity)
}

fn template(identity: &Identity) -> TokenStream {
    let Identity {
        principal,
        peer,
        context,
        user,
        policy,
    } = identity;
    let default_builder = matches!(policy, ContextPolicy::DefaultBuilder);
    let builder = matches!(policy, ContextPolicy::Builder);
    let connection = matches!(policy, ContextPolicy::ConnectedClient);
    moxy::template! {
        @if *user {
            let {{ principal }} = crate::test_support::principal(__identity_value);
            let {{ peer }} = crate::test_support::peer();
        } @else {
            let {{ principal }} = __identity_value;
            let {{ peer }} = __address_value;
        }
        @if let Some(context) = context {
            @if default_builder {
                let {{ context }} = test_context(&{{ principal }}, &{{ peer }});
            } @else if builder {
                let {{ context }} = __context_value(&{{ principal }}, &{{ peer }});
            } @else if connection {
                let {{ context }} = crate::handlers::RequestContext::new(&{{ principal }}, &{{ peer }}, __context_value, __connection_value, false, "PLAINTEXT");
            } @else {
                let {{ context }} = crate::test_support::request_context(&{{ principal }}, &{{ peer }}, __context_value);
            }
        }
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let identity = declaration(&input.clone().into())?;
    let mut groups = native_groups(input).skip(1);
    let placeholders = if identity.user {
        &["__identity_value", "__context_value", "__connection_value"][..]
    } else {
        &[
            "__identity_value",
            "__address_value",
            "__context_value",
            "__connection_value",
        ][..]
    };
    let mut values = Vec::new();
    for name in placeholders {
        let Some(group) = groups.next() else { break };
        values.push(SpliceValue {
            name: (*name).to_owned(),
            group: Group::new(Delimiter::None, group.stream()),
        });
    }
    Ok(splice(template(&identity).into(), &values, &[]))
}

/// Canonical identity resources stay in the caller's scope, including visible bindings.
pub(crate) fn user_bindings(
    names: &[Ident],
    user: NativeTokenStream,
    client: Option<NativeTokenStream>,
) -> NativeTokenStream {
    let mut identity =
        binding_names(names, Span::call_site()).expect("validated identity binding count");
    let policy = if identity.context.is_none() {
        ContextPolicy::Absent
    } else if client.is_some() {
        ContextPolicy::Client
    } else {
        ContextPolicy::DefaultBuilder
    };
    identity.user = true;
    identity.policy = policy;
    let mut values = vec![SpliceValue {
        name: "__identity_value".to_owned(),
        group: Group::new(Delimiter::None, user),
    }];
    if let Some(client) = client {
        values.push(SpliceValue {
            name: "__context_value".to_owned(),
            group: Group::new(Delimiter::None, client),
        });
    }
    splice(template(&identity).into(), &values, &[])
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, template},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn exposes_identity_or_keeps_context_resources_hygienic_and_validates_policy() {
        for valid in [
            "bind(ctx); user [\"admin\"];",
            "bind(principal, peer); user [native_user()];",
            "bind(principal, peer, ctx); user [native_user()]; client [client];",
            "bind(ctx); identity [principal(user)]; address [peer()]; context [test_context];",
            "bind(p, peer); identity [principal(user)]; address [address::<1, 2>()];",
            "bind(p, peer, ctx); identity [principal(user)]; address [peer()]; client [client];",
            "bind(p, peer, ctx); identity [principal(user)]; address [peer()]; client [client]; connection [connection];",
        ] {
            let identity = declaration(&valid.parse().unwrap()).unwrap();
            assert!(compact(&template(&identity)).starts_with("let"));
        }
        for invalid in [
            "",
            "bind(); identity [p]; address [a];",
            "bind(ctx); identity [p]; address [a];",
            "bind(p, peer); address [a]; identity [p];",
            "bind(p, peer); identity []; address [a];",
            "bind(p, peer); identity [p]; address [a]; client [c];",
            "bind(ctx); identity [p]; address [a]; context [test_context]; connection [c];",
            "bind(ctx); identity [p]; address [a]; context [|p,a| p];",
        ] {
            assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
