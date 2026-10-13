//! Visible external member constructors and shared topology traversal.

use {
    crate::meta::{is_plain_sync, native_braced_bodies, parameter_ident, plain_path},
    moxy::{
        ast::{Attributes, FnParam, Parse, ParseError, Parser, Path, Signature, Token, Visibility},
        token::{Group, Ident, Spanner, ToTokenStream, TokenStream},
    },
    proc_macro::{Delimiter, Group as NativeGroup, TokenStream as NativeTokenStream, TokenTree},
};

struct MemberConstructor {
    attributes: Attributes,
    visibility: Visibility,
    signature: Signature,
    target: Path,
    tagged: bool,
}

fn members(input: &TokenStream) -> Result<Vec<MemberConstructor>, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut members = Vec::new();
    while !parser.is_empty() {
        let attributes = Attributes::parse(&parser)?;
        let visibility = Visibility::parse(&parser)?;
        let encoding = Ident::parse(&parser)?;
        let tagged = match encoding.text() {
            "plain" => false,
            "tagged" => true,
            _ => {
                return Err(ParseError::new(
                    encoding.span(),
                    "member encoding must be plain or tagged",
                ));
            }
        };
        let signature = Signature::parse(&parser)?;
        if !is_plain_sync(&signature) || signature.params.inner.inputs.len() != 1 {
            return Err(ParseError::new(
                signature.span(),
                "member constructor needs one ordinary member-id parameter",
            ));
        }
        parameter_ident(
            &signature.params.inner.inputs[0],
            "member id needs a local parameter name",
        )?;
        let FnParam::Typed(parameter) = &signature.params.inner.inputs[0] else {
            unreachable!("checked named parameter")
        };
        if !parameter.ty.as_reference().is_some_and(|reference| {
            reference.mutability.is_none()
                && reference
                    .elem
                    .as_path()
                    .is_some_and(|path| path.path.is_ident("str"))
        }) {
            return Err(ParseError::new(
                parameter.ty.span(),
                "member id must be an immutable str reference",
            ));
        }
        let output = signature.output.as_type().ok_or_else(|| {
            ParseError::new(
                signature.span(),
                "member constructor needs a concrete output type",
            )
        })?;
        let target = plain_path(
            &output.to_token_stream(),
            output.span(),
            "member constructor needs a concrete output type",
        )?;
        let extra = Group::parse(&parser)?;
        if !extra.delim.is_brace() {
            return Err(ParseError::new(
                extra.span(),
                "member fields need a native brace group",
            ));
        }
        if members.iter().any(|member: &MemberConstructor| {
            member.signature.ident.text() == signature.ident.text()
        }) {
            return Err(ParseError::new(
                signature.ident.span(),
                "member constructor names must be distinct",
            ));
        }
        members.push(MemberConstructor {
            attributes,
            visibility,
            signature,
            target,
            tagged,
        });
    }
    if members.is_empty() {
        return Err(parser.error("declare at least one member constructor"));
    }
    Ok(members)
}

fn identity_fields(member: &Ident) -> TokenStream {
    moxy::template! {
        member_id: {{ member }}.into(),
        instance_id: None,
        rack_id: None,
        member_epoch: 5,
        client_id: "client".into(),
        client_host: "/10.0.0.1".into(),
        subscribed_topic_names: vec!["orders".into(), "payments".into()],
        subscribed_topic_regex: None,
    }
}

pub(crate) fn member_identity(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let declarations = members(&input.clone().into())?;
    let mut extras = native_braced_bodies(input).into_iter();
    let mut output = NativeTokenStream::new();
    for declaration in declarations {
        let MemberConstructor {
            attributes,
            visibility,
            signature,
            target,
            tagged,
        } = declaration;
        let member = parameter_ident(&signature.params.inner.inputs[0], "member id needs a name")?;
        let extra = extras
            .next()
            .expect("one native field group per checked constructor");
        let needs_comma =
            extra.stream().into_iter().last().is_some_and(
                |last| !matches!(last, TokenTree::Punct(comma) if comma.as_char() == ','),
            );
        let mut fields: NativeTokenStream = identity_fields(member).into();
        fields.extend(extra.stream());
        fields.extend(NativeTokenStream::from(moxy::template! {
            @if tagged {
                @if needs_comma { , }
                unknown_tagged_fields: ::krabka_protocol::UnknownTaggedFields::default(),
            }
        }));
        let mut fields = NativeGroup::new(Delimiter::Brace, fields);
        fields.set_span(extra.span());
        let mut body: NativeTokenStream = moxy::template! { {{ target }} }.into();
        body.extend([TokenTree::Group(fields)]);
        output.extend(NativeTokenStream::from(
            moxy::template! { {{ attributes }} {{ visibility }} {{ signature }} },
        ));
        output.extend([TokenTree::Group(NativeGroup::new(Delimiter::Brace, body))]);
    }
    Ok(output)
}

pub(crate) fn required_topics(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let attributes = Attributes::parse(&parser)?;
    let visibility = Visibility::parse(&parser)?;
    let signature = Signature::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    if !parser.is_empty() || !is_plain_sync(&signature) || signature.params.inner.inputs.len() != 1
    {
        return Err(ParseError::new(
            signature.span(),
            "topic traversal needs one synchronous topology parameter",
        ));
    }
    let topology = parameter_ident(
        &signature.params.inner.inputs[0],
        "topology needs a local name",
    )?;
    let FnParam::Typed(parameter) = &signature.params.inner.inputs[0] else {
        unreachable!("checked named parameter")
    };
    let reference = parameter
        .ty
        .as_reference()
        .filter(|ty| ty.mutability.is_none())
        .ok_or_else(|| {
            ParseError::new(
                parameter.ty.span(),
                "topology must be an immutable schema reference",
            )
        })?;
    plain_path(
        &reference.elem.to_token_stream(),
        reference.elem.span(),
        "topology needs a concrete schema type",
    )?;
    let output = signature.output.as_type().ok_or_else(|| {
        ParseError::new(signature.span(), "topic traversal must return Vec<String>")
    })?;
    let expected: moxy::ast::Type = moxy::parse!("Vec<String>")?;
    if output.to_token_stream().to_string() != expected.to_token_stream().to_string() {
        return Err(ParseError::new(
            output.span(),
            "topic traversal must return Vec<String>",
        ));
    }
    Ok(moxy::template! {
        {{ attributes }} {{ visibility }} {{ signature }} {
            dedup_first_seen({{ topology }}.subtopologies.iter().flat_map(|subtopology| {
                subtopology.source_topics.iter().map(String::as_str)
                    .chain(subtopology.repartition_sink_topics.iter().map(String::as_str))
                    .chain(subtopology.repartition_source_topics.iter().map(|topic| topic.name.as_str()))
                    .chain(subtopology.state_changelog_topics.iter().map(|topic| topic.name.as_str()))
            }))
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{identity_fields, members, required_topics},
        crate::meta::compact,
        assert2::assert,
        moxy::token::Ident,
    };

    #[test]
    fn member_headers_and_identity_order_are_independent_of_native_extra_fields() {
        let declarations = members(&"plain fn view(member: &str) -> View {} tagged fn wire(id: &str) -> wire::Member { assignment: arbitrary!(@body), }".parse().unwrap()).unwrap();
        assert!(declarations.len() == 2 && !declarations[0].tagged && declarations[1].tagged);
        let fields = compact(&identity_fields(&Ident::new("member")));
        assert!(fields.find("member_id:").unwrap() < fields.find("client_id:").unwrap());
        assert!(
            fields.find("client_id:").unwrap() < fields.find("subscribed_topic_regex:").unwrap()
        );
        for input in [
            "",
            "other fn x(id: &str) -> X {}",
            "plain async fn x(id: &str) -> X {}",
            "plain fn x() -> X {}",
            "plain fn x(id: &str) {}",
            "plain fn x(id: &str) -> X()",
            "plain fn x(id: &str) -> X {} plain fn x(id: &str) -> X {}",
        ] {
            assert!(members(&input.parse().unwrap()).is_err(), "{input}");
        }
    }

    #[test]
    fn topic_traversal_keeps_all_four_sources_in_first_seen_order() {
        let input = "pub(super) fn required_topics(topology: &Topology) -> Vec<String>;"
            .parse()
            .unwrap();
        let output = compact(&required_topics(&input).unwrap());
        assert!(
            output.find("source_topics.iter()").unwrap()
                < output.find("repartition_sink_topics.iter()").unwrap()
        );
        assert!(
            output.find("repartition_source_topics.iter()").unwrap()
                < output.find("state_changelog_topics.iter()").unwrap()
        );
        for input in [
            "fn topics(t: Topology) -> Vec<String>;",
            "async fn topics(t: &Topology) -> Vec<String>;",
            "fn topics(t: &mut Topology) -> Vec<String>;",
            "fn topics(t: &Topology) -> String;",
            "fn topics(t: &Topology) -> Vec<String>; trailing",
        ] {
            assert!(required_topics(&input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
