//! Flat request declarations for coordinator record-string traversal.

use {
    crate::meta::edit_native_body,
    moxy::{
        ast::{Attributes, List, Parse, ParseError, Parser, Path, PathArguments, Token},
        token::{Delim, Group, Ident, Span, Spanner, ToTokenStream, TokenStream},
    },
    proc_macro::{Delimiter, TokenStream as NativeTokenStream, TokenTree},
};

enum Tail {
    None,
    Extra,
    OffsetMetadata(Ident),
}

struct Request {
    attrs: Attributes,
    ty: Path,
    receiver: Ident,
    required: List<Ident, Token![,]>,
    optional: List<Ident, Token![,]>,
    tail: Tail,
}

fn fields(parser: &Parser, expected: &str) -> Result<List<Ident, Token![,]>, ParseError> {
    let label = Ident::parse(parser)?;
    if label.text() != expected {
        return Err(ParseError::new(
            label.span(),
            format!("expected `{expected}(fields);`"),
        ));
    }
    let fields = List::parse_all(&parser.parse_group(Delim::Paren)?)?;
    <Token![;]>::parse(parser)?;
    Ok(fields)
}

fn requests(input: &TokenStream) -> Result<Vec<Request>, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut requests = Vec::new();
    while !parser.is_empty() {
        let attrs = Attributes::parse(&parser)?;
        let ty = Path::parse(&parser)?;
        if ty.is_empty()
            || ty
                .iter()
                .any(|segment| !matches!(segment.args, PathArguments::None))
        {
            return Err(ParseError::new(
                ty.span(),
                "expected a request type path without arguments",
            ));
        }
        if requests.iter().any(|request: &Request| {
            request.ty.to_token_stream().to_string() == ty.to_token_stream().to_string()
        }) {
            return Err(ParseError::new(
                ty.span(),
                "request type declared more than once",
            ));
        }
        let receiver_parser = parser.parse_group(Delim::Paren)?;
        let receiver = receiver_parser.parse_ident_any()?;
        if receiver.text() != "self" || !receiver_parser.is_empty() {
            return Err(receiver_parser.error("expected `(self)` receiver"));
        }
        let body = parser.parse_group(Delim::Brace)?;
        let required = fields(&body, "required")?;
        let optional = fields(&body, "optional")?;
        let tail = if body.is_empty() {
            Tail::None
        } else {
            let label = Ident::parse(&body)?;
            match label.text() {
                "extra" => {
                    let expression = Group::parse(&body)?;
                    if !expression.delim.is_brace() || expression.tokens.is_empty() {
                        return Err(ParseError::new(
                            expression.span(),
                            "extra needs a nonempty native expression block",
                        ));
                    }
                    Tail::Extra
                }
                "offset_metadata" => {
                    let topics = body.parse_group(Delim::Paren)?;
                    let field = Ident::parse(&topics)?;
                    if !topics.is_empty() {
                        return Err(topics.error("offset_metadata needs one topics field"));
                    }
                    <Token![;]>::parse(&body)?;
                    Tail::OffsetMetadata(field)
                }
                _ => {
                    return Err(ParseError::new(
                        label.span(),
                        "expected extra { iterator } or offset_metadata(topics);",
                    ));
                }
            }
        };
        if !body.is_empty() {
            return Err(body.error("unexpected tokens after record-string traversal"));
        }
        requests.push(Request {
            attrs,
            ty,
            receiver,
            required,
            optional,
            tail,
        });
    }
    if requests.is_empty() {
        return Err(parser.error("expected at least one request declaration"));
    }
    Ok(requests)
}

impl Request {
    fn shell(&self) -> TokenStream {
        let Self {
            attrs,
            ty,
            receiver,
            ..
        } = self;
        moxy::template! {
            {{ attrs }}
            impl RecordStrings for {{ ty }} {
                fn longest_record_string(&{{ receiver }}) -> usize {}
            }
        }
    }

    fn iterator(&self) -> TokenStream {
        let Self {
            receiver,
            required,
            optional,
            tail,
            ..
        } = self;
        moxy::template! {
            [@for field in required { {{ receiver }}.{{ field }}.as_str(), }].into_iter()
            @for field in optional { .chain({{ receiver }}.{{ field }}.as_deref()) }
            @if let Tail::OffsetMetadata(topics) = tail {
                .chain({{ receiver }}.{{ topics }}.iter().flat_map(|topic| {
                    ::std::iter::once(topic.name.as_str()).chain(
                        topic.partitions.iter().filter_map(|partition| partition.committed_metadata.as_deref()),
                    )
                }))
            }
        }
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let requests = requests(&input.clone().into())?;
    let native_bodies: Vec<_> = input
        .into_iter()
        .filter_map(|token| match token {
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => Some(group),
            _ => None,
        })
        .collect();
    if native_bodies.len() != requests.len() {
        return Err(ParseError::new(
            Span::call_site(),
            "each request needs its own native declaration body",
        ));
    }
    let mut output = NativeTokenStream::new();
    for (request, body) in requests.into_iter().zip(native_bodies) {
        let iterator = request.iterator();
        let body = if matches!(request.tail, Tail::Extra) {
            let Some(TokenTree::Group(extra)) = body.stream().into_iter().last() else {
                unreachable!("validated extra block")
            };
            let fields = Ident::new("__record_strings_fields").with_span(Span::mixed_site());
            let extra_name = Ident::new("__record_strings_extra").with_span(Span::mixed_site());
            // Build the required/optional iterator before evaluating the original
            // native extra block, then chain the two without exposing generated locals.
            let mut body: NativeTokenStream = moxy::template! {
                let {{ fields }} = {{ iterator }};
                let {{ extra_name }} =
            }
            .into();
            body.extend([TokenTree::Group(extra)]);
            body.extend(NativeTokenStream::from(moxy::template! {
                ;
                longest({{ fields }}.chain({{ extra_name }}))
            }));
            body
        } else {
            moxy::template! { longest({{ iterator }}) }.into()
        };
        output.extend(edit_native_body(request.shell().into(), |method| {
            edit_native_body(method, |_| body)
        }));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {super::requests, assert2::assert};

    #[test]
    fn refuses_invalid_rows_receivers_fields_and_traversal_policies() {
        for input in [
            "",
            "Request(self) { required(); }",
            "Request(req) { required(); optional(); }",
            "Request(self, other) { required(); optional(); }",
            "Request<T>(self) { required(); optional(); }",
            "Request(self) { required[a]; optional(); }",
            "Request(self) { required(a.b); optional(); }",
            "Request(self) { required(); optional(); unknown; }",
            "Request(self) { required(); optional(); extra {} }",
            "Request(self) { required(); optional(); extra [self.names.iter()] }",
            "Request(self) { required(); optional(); offset_metadata(topics, extra); }",
            "Request(self) { required(); optional(); offset_metadata(topics); extra { self.names.iter() } }",
            "Request(self) { required(); optional(); } Request(self) { required(); optional(); }",
        ] {
            assert!(requests(&input.parse().unwrap()).is_err(), "{input}");
        }
    }

    #[test]
    fn native_extra_is_opaque_and_offset_traversal_is_direct() {
        let input = "Request(self) { required(group_id); optional(instance_id); extra { local_macro!(@tokens self.names) } } Offset(self) { required(group_id); optional(); offset_metadata(topics); }".parse().unwrap();
        let requests = requests(&input).unwrap();
        assert!(requests.len() == 2);
        assert!(requests[0].required.len() == 1);
        assert!(requests[0].optional.len() == 1);
        let iterator = requests[1].iterator().to_string();
        assert!(iterator.contains("committed_metadata"));
        assert!(!iterator.contains("offset_record_strings"));
    }
}
