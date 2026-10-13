//! Bind retained broker resources with explicit starter, readiness and optional caller policy.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups},
        request_test_identity::user_bindings,
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{List, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

struct Setup {
    names: Vec<Ident>,
    policy: Option<String>,
    ready: bool,
    leader: bool,
    user: UserPolicy,
}

enum UserPolicy {
    Absent,
    Default,
    Client,
}

fn declaration(input: &TokenStream) -> Result<Setup, ParseError> {
    let parser = Parser::from_tokens(input);
    expect_name(
        &parser,
        "bind",
        "expected broker bindings and optional context or principal/peer/context",
    )?;
    let group = grouped_value(&parser, "expected broker binding tuple")?;
    if !group.delim.is_paren() {
        return Err(ParseError::new(
            group.span(),
            "broker bindings need parentheses",
        ));
    }
    let names = List::<Ident, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
    if !matches!(names.len(), 3..=6) {
        return Err(ParseError::new(
            group.span(),
            "bind handle/directory/broker and optional context or principal/peer/context",
        ));
    }
    let _: Token![;] = Parse::parse(&parser)?;
    expect_name(&parser, "boot", "expected boot clause")?;
    let boot = grouped_value(&parser, "expected native starter expression")?;
    if !boot.delim.is_bracket() || boot.tokens.is_empty() {
        return Err(ParseError::new(
            boot.span(),
            "boot needs a nonempty bracketed expression",
        ));
    }
    let policy = if <Token![;]>::peek(parser.cursor()) {
        None
    } else {
        let policy = Ident::parse(&parser)?;
        if !matches!(policy.text(), "allow_all" | "deny_all" | "controller_peer") {
            return Err(ParseError::new(
                policy.span(),
                "starter policy must be allow_all, deny_all or controller_peer",
            ));
        }
        let _: Path = moxy::parse!({ boot.tokens.clone() })?;
        Some(policy.text().to_owned())
    };
    let _: Token![;] = Parse::parse(&parser)?;
    let mut ready = false;
    let mut leader = false;
    let mut user = UserPolicy::Absent;
    while !parser.is_empty() {
        let name = Ident::parse(&parser)?;
        let token = parser
            .advance()
            .ok_or_else(|| parser.error("expected readiness or user value"))?;
        if name == "ready" && !ready && !leader && matches!(user, UserPolicy::Absent) {
            if token.as_group().is_some_and(|group| group.delim.is_brace()) {
                ready = true;
            } else if token
                .as_ident()
                .is_some_and(|name| name == "controller_leader")
            {
                leader = true;
            } else {
                return Err(ParseError::new(
                    token.span(),
                    "ready needs native statements or controller_leader",
                ));
            }
        } else if name == "user" && matches!(user, UserPolicy::Absent) && names.len() > 3 {
            if !token
                .as_group()
                .is_some_and(|group| group.delim.is_bracket() && !group.tokens.is_empty())
            {
                return Err(ParseError::new(
                    token.span(),
                    "user needs a nonempty bracketed name expression",
                ));
            }
            user = UserPolicy::Default;
        } else if name == "client"
            && matches!(user, UserPolicy::Default)
            && matches!(names.len(), 4 | 6)
        {
            if !token
                .as_group()
                .is_some_and(|group| group.delim.is_bracket() && !group.tokens.is_empty())
            {
                return Err(ParseError::new(
                    token.span(),
                    "client needs a nonempty bracketed expression",
                ));
            }
            user = UserPolicy::Client;
        } else {
            return Err(ParseError::new(
                name.span(),
                "expected optional readiness, then user and optional context client",
            ));
        }
        let _: Token![;] = Parse::parse(&parser)?;
    }
    if (names.len() > 3) == matches!(user, UserPolicy::Absent) {
        return Err(ParseError::new(
            input.span(),
            "a context binding requires its user policy",
        ));
    }
    Ok(Setup {
        names: names.into_iter().collect(),
        policy,
        ready,
        leader,
        user,
    })
}

fn template(setup: &Setup) -> TokenStream {
    let handle = &setup.names[0];
    let directory = &setup.names[1];
    let broker = &setup.names[2];
    let deny = setup.policy.as_deref() == Some("deny_all");
    let controller_peer = setup.policy.as_deref() == Some("controller_peer");
    let policy = setup.policy.is_some();
    let leader = setup.leader;
    moxy::template! {
        let crate::handlers::test_support::StartedBroker { broker: {{ handle }}, dir: {{ directory }} } = @if policy {
            __broker_boot(@if deny { crate::handlers::test_support::deny_all_authorizer() } @else if controller_peer { crate::handlers::test_support::controller_peer_authorizer() } @else { crate::handlers::test_support::allow_all_authorizer() }).await
        } @else { __broker_boot.await };
        let {{ broker }} = {{ handle }}.broker_arc_for_test();
        @if leader { crate::test_support::wait_for_controller_leader(&{{ broker }}).await; }
        __broker_ready
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let setup = declaration(&input.clone().into())?;
    let mut groups = native_groups(input).skip(1);
    let boot = groups.next().expect("validated starter");
    let ready = if setup.ready {
        groups.next().expect("validated readiness").stream()
    } else {
        NativeTokenStream::new()
    };
    let boot_delimiter = if setup.policy.is_some() {
        Delimiter::None
    } else {
        Delimiter::Parenthesis
    };
    let mut output = splice(
        template(&setup).into(),
        &[
            SpliceValue {
                name: "__broker_boot".to_owned(),
                group: Group::new(boot_delimiter, boot.stream()),
            },
            SpliceValue {
                name: "__broker_ready".to_owned(),
                group: Group::new(Delimiter::None, ready),
            },
        ],
        &[],
    );
    if !matches!(setup.user, UserPolicy::Absent) {
        let user = groups.next().expect("validated user").stream();
        let client = matches!(setup.user, UserPolicy::Client)
            .then(|| groups.next().expect("validated client").stream());
        output.extend(user_bindings(&setup.names[3..], user, client));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, template},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn keeps_native_starters_and_combined_policy_order_explicit() {
        for valid in [
            "bind(handle, dir, broker); boot [start::<1, 2>(authorizer)];",
            "bind(handle, dir, broker); boot [{ start(config) }]; ready { let part = local_partition(&broker, dir.path()); };",
            "bind(handle, dir, broker, ctx); boot [start_broker] allow_all; ready controller_leader; user [\"admin\"];",
            "bind(handle,dir,broker); boot [start_broker] deny_all;",
            "bind(h,d,b,p,peer); boot [start_broker] controller_peer; user [\"alice\"];",
            "bind(h,d,b,p,peer,ctx); boot [start_broker] allow_all; user [\"admin\"]; client [native_client::<1, 2>()];",
        ] {
            let setup = declaration(&valid.parse().unwrap()).unwrap();
            assert!(compact(&template(&setup)).contains("broker_arc_for_test"));
        }
        for invalid in [
            "",
            "bindings(handle,dir,broker);",
            "bind(handle,dir); boot [start()];",
            "bind(handle,dir,broker); boot [];",
            "bind(handle,dir,broker); boot {};",
            "bind(handle,dir,broker); boot [start()]",
            "bind(handle,dir,broker); boot [start()]; ready [];",
            "bind(handle,dir,broker); boot [start()]; extra {};",
            "bind(h,d,b,c); boot [start()];",
            "bind(h,d,b); boot [start] unknown;",
            "bind(h,d,b); boot [start()]; user [\"admin\"];",
            "bind(h,d,b,p,peer); boot [start()]; user [\"admin\"]; client [\"bad\"];",
            "bind(h,d,b,ctx); boot [start()]; client [\"bad\"];",
            "bind(h,d,b,c); boot [start]; user [\"admin\"]; ready controller_leader;",
        ] {
            assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
