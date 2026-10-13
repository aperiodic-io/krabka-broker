//! Flat coordinator test schemas and caller-owned commit-wait operations.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups, nonempty_paths},
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{ast::ParseError, token::TokenStream},
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

pub(crate) fn heartbeat(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let responses = nonempty_paths(input, "expected heartbeat response paths")?;
    Ok(moxy::template! {
        @for response in responses {
            impl HeartbeatStatus for {{ response }} {
                fn status(self) -> HeartbeatReply {
                    HeartbeatReply { error_code: self.error_code, member_epoch: self.member_epoch }
                }
            }
        }
    })
}

fn key_boundary_template(input: &TokenStream) -> Result<TokenStream, ParseError> {
    use moxy::{
        ast::{Parse, Parser, Path, Token},
        token::Spanner,
    };
    let parser = Parser::from_tokens(input);
    let pending = Path::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    expect_name(&parser, "factory", "expected factory clause")?;
    let factory = grouped_value(&parser, "expected bracketed pending-record factory")?;
    if !factory.delim.is_bracket() || factory.tokens.is_empty() {
        return Err(ParseError::new(
            factory.span(),
            "expected nonempty bracketed factory",
        ));
    }
    <Token![;]>::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("unexpected key-boundary input"));
    }
    Ok(moxy::template! {
        #[test]
        fn a_key_string_over_32767_bytes_does_not_encode() {
            struct KeyField {
                name: &'static str,
                set: fn(&mut {{ pending }}, String),
            }
            struct LengthCase { length: usize, encodes: bool }
            let fields = [
                KeyField { name: "member id of a member record", set: |pending, value| pending.member_metadata[0].0 = value },
                KeyField { name: "member id of a target assignment", set: |pending, value| pending.target_per_member[0].0 = value },
                KeyField { name: "member id of a current assignment", set: |pending, value| pending.current_per_member[0].0 = value },
            ];
            let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
            let lengths = [LengthCase { length: limit, encodes: true }, LengthCase { length: limit + 1, encodes: false }];
            for field in fields {
                for case in &lengths {
                    let mut pending: {{ pending }} = {
                        let factory = __pending_factory;
                        factory()
                    };
                    (field.set)(&mut pending, "a".repeat(case.length));
                    let outcome = pending.into_batch("g", 0);
                    ::assert2::assert!(outcome.is_ok() == case.encodes, "{} of {} bytes", field.name, case.length);
                    ::assert2::assert!(case.encodes || matches!(outcome, Err(crate::error::BrokerError::Protocol(_))), "{} of {} bytes is a protocol error", field.name, case.length);
                }
            }
            for case in lengths {
                let outcome = {
                    let factory = __pending_factory;
                    factory().into_batch(&"g".repeat(case.length), 0)
                };
                ::assert2::assert!(outcome.is_ok() == case.encodes, "group id of {} bytes", case.length);
            }
        }
    })
}

pub(crate) fn key_boundaries(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let template = key_boundary_template(&input.clone().into())?;
    let factory = native_groups(input).next().expect("validated factory");
    let mut group = Group::new(Delimiter::None, factory.stream());
    group.set_span(factory.span());
    Ok(splice(
        template.into(),
        &[SpliceValue {
            name: "__pending_factory".to_owned(),
            group,
        }],
        &[],
    ))
}

fn commit_template(input: &TokenStream) -> Result<TokenStream, ParseError> {
    use moxy::{
        ast::{Parse, Parser, Token},
        token::Ident,
    };
    let parser = Parser::from_tokens(input);
    let case = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let partition = Ident::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    expect_name(&parser, "captures", "expected captures block")?;
    let captures = grouped_value(&parser, "expected captures block")?;
    expect_name(&parser, "leader", "expected leader bindings and block")?;
    let bindings = grouped_value(&parser, "expected leader bindings")?;
    let parameters = Parser::from_tokens(&bindings.tokens);
    let leader = Ident::parse(&parameters)?;
    <Token![,]>::parse(&parameters)?;
    let epoch = Ident::parse(&parameters)?;
    let action = grouped_value(&parser, "expected leader block")?;
    expect_name(&parser, "notify", "expected notify block")?;
    let notify = grouped_value(&parser, "expected notify block")?;
    if !captures.delim.is_brace()
        || !bindings.delim.is_paren()
        || !parameters.is_empty()
        || !action.delim.is_brace()
        || !notify.delim.is_brace()
        || !parser.is_empty()
    {
        return Err(parser.error("expected captures {}, leader(name, epoch) {}, notify {}"));
    }
    Ok(moxy::template! {
        {
            __commit_captures
            let {{ partition }} = ::std::sync::Arc::clone(&{{ partition }});
            let moves_to = {{ case }}.moves_to;
            let hw_later = {{ case }}.hw_later;
            ::tokio::spawn(async move {
                ::tokio::time::sleep(::std::time::Duration::from_millis(20)).await;
                if let Some(moved) = moves_to {
                    let {{ leader }} = moved.leader;
                    let {{ epoch }} = moved.epoch;
                    __commit_leader
                }
                if let Some(hw) = hw_later {
                    {{ partition }}.replica_state.lock().await.hw = ::krabka_log::Offset(hw);
                    __commit_notify
                }
            })
        }
    })
}

/// Land the commit-wait fixture's changes after its wait has started. Capture
/// statements and leader/notify actions stay explicit at each caller.
pub(crate) fn commit_changes(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let template = commit_template(&input.clone().into())?;
    let mut groups = native_groups(input);
    let captures = groups.next().expect("validated captures");
    let _parameters = groups.next().expect("validated bindings");
    let leader = groups.next().expect("validated leader operation");
    let notify = groups.next().expect("validated notify operation");
    let mut capture_group = Group::new(Delimiter::None, captures.stream());
    capture_group.set_span(captures.span());
    let values = [
        SpliceValue {
            name: "__commit_captures".to_owned(),
            group: capture_group,
        },
        SpliceValue {
            name: "__commit_leader".to_owned(),
            group: leader,
        },
        SpliceValue {
            name: "__commit_notify".to_owned(),
            group: notify,
        },
    ];
    Ok(splice(template.into(), &values, &[
        "moves_to", "hw_later", "moved", "hw",
    ]))
}

#[cfg(test)]
mod tests {
    use {
        super::{commit_template, heartbeat, key_boundary_template},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn heartbeat_schema_reads_both_fields_without_defaulting_either() {
        let output = compact(
            &heartbeat(&"Consumer, share::Reply, streams::Reply".parse().unwrap()).unwrap(),
        );
        assert!(output.matches("fnstatus(self)->HeartbeatReply").count() == 3);
        assert!(
            output
                .matches("error_code:self.error_code,member_epoch:self.member_epoch")
                .count()
                == 3
        );
        for input in ["", "Reply;", "&Reply"] {
            assert!(heartbeat(&input.parse().unwrap()).is_err());
        }
    }

    #[test]
    fn boundary_checks_cover_all_member_keys_and_both_group_lengths() {
        let output = compact(
            &key_boundary_template(
                &"records::Delta; factory [native!(|&row| row)];"
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        assert!(output.contains("pending.member_metadata[0].0=value"));
        assert!(output.contains("pending.target_per_member[0].0=value"));
        assert!(output.contains("pending.current_per_member[0].0=value"));
        assert!(output.contains("length:limit+1,encodes:false"));
        for input in [
            "",
            "Delta; create [factory];",
            "Delta; factory [];",
            "Delta; factory {};",
            "Delta; factory [factory]; extra",
        ] {
            assert!(key_boundary_template(&input.parse().unwrap()).is_err());
        }
    }

    #[test]
    fn commit_actions_follow_the_delay_and_keep_native_operations_opaque() {
        let valid = "case, part; captures { let capture = native!(@tokens); } leader(next, epoch) { native!(|&row| row); } notify { notify(); }";
        let output = compact(&commit_template(&valid.parse().unwrap()).unwrap());
        assert!(output.contains("letpart=::std::sync::Arc::clone(&part)"));
        assert!(output.find("sleep").unwrap() < output.find("__commit_leader").unwrap());
        assert!(output.find("__commit_leader").unwrap() < output.find("__commit_notify").unwrap());
        for input in [
            "",
            "case, part; captures [] leader(a,b) {} notify {}",
            "case, part; captures {} leader(a) {} notify {}",
            "case, part; captures {} leader(a,b) {} notify {} extra",
        ] {
            assert!(commit_template(&input.parse().unwrap()).is_err());
        }
    }
}
