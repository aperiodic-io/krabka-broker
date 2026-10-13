//! Syntax and attribute helpers shared by the macros.

use moxy::{
    ast::{
        Attributes, Field, FnParam, ItemStruct, List, Parse, ParseError, Parser, Path,
        PathArguments, Pattern, Signature, Token, Type, Visibility,
    },
    token::{Group, Ident, Span, Spanner, TokenStream, TokenTree},
};

/// The common broker request-handler signature, with explicit wire schemas.
pub(crate) fn broker_handler_signature(request: &Path, response: &Path) -> TokenStream {
    moxy::template! {
        pub(crate) async fn handle(
            broker: &crate::broker::Broker,
            req: {{ request }},
            _version: i16,
            ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<{{ response }}, crate::error::BrokerError>
    }
}

#[cfg(test)]
pub(crate) struct RejectedStruct<'a> {
    pub(crate) label: &'a str,
    pub(crate) declaration: &'a str,
}

#[cfg(test)]
pub(crate) fn assert_rejected_structs(
    cases: &[RejectedStruct<'_>],
    expand: impl Fn(ItemStruct) -> Result<TokenStream, ParseError>,
) {
    for case in cases {
        let item: ItemStruct = moxy::parse!({ case.declaration }).expect(case.label);
        assert2::assert!(expand(item).is_err(), "{}", case.label);
    }
}

/// A safe, synchronous Rust signature without constness or generic parameters.
pub(crate) fn is_plain_sync(signature: &Signature) -> bool {
    signature.asyncness.is_none() && has_plain_qualifiers(signature)
}

/// An async signature without constness, unsafety, an ABI or generic parameters.
pub(crate) fn is_plain_async(signature: &Signature) -> bool {
    signature.asyncness.is_some() && has_plain_qualifiers(signature)
}

fn has_plain_qualifiers(signature: &Signature) -> bool {
    signature.constness.is_none()
        && signature.safety.is_none()
        && signature.abi.is_none()
        && signature.generics.params.is_empty()
}

/// Read a nonempty external-schema table with the caller's domain error.
pub(crate) fn nonempty_paths(
    input: &TokenStream,
    message: &str,
) -> Result<List<Path, Token![,]>, ParseError> {
    let parser = Parser::from_tokens(input);
    let paths: List<Path, Token![,]> = List::parse_all(&parser)?;
    if paths.is_empty() {
        return Err(parser.error(message));
    }
    Ok(paths)
}

pub(crate) fn parameter_ident<'a>(
    parameter: &'a FnParam,
    message: &str,
) -> Result<&'a Ident, ParseError> {
    if let FnParam::Typed(parameter) = parameter
        && let Pattern::Ident(name) = parameter.pat.as_ref()
        && name.ident.text() != "_"
        && name.by_ref.is_none()
        && name.subpat.is_none()
    {
        Ok(&name.ident)
    } else {
        Err(ParseError::new(parameter.span(), message))
    }
}

/// Validate a concrete async declaration and return its named parameter types.
pub(crate) fn named_async_parameter_types(
    signature: &Signature,
    names: &[&str],
    message: &str,
) -> Result<Vec<String>, ParseError> {
    let parameters = &signature.params.inner.inputs;
    if !is_plain_async(signature)
        || signature.generics.where_clause.is_some()
        || signature.params.inner.variadic.is_some()
        || parameters.len() != names.len()
    {
        return Err(ParseError::new(signature.span(), message));
    }
    parameters
        .iter()
        .zip(names)
        .map(|(parameter, name)| {
            let FnParam::Typed(parameter) = parameter else {
                return Err(ParseError::new(signature.span(), message));
            };
            if !parameter.attrs.is_empty()
                || compact(&moxy::template! { {{ parameter.pat }} }) != *name
            {
                return Err(ParseError::new(signature.span(), message));
            }
            Ok(compact(&moxy::template! { {{ parameter.ty }} }))
        })
        .collect()
}

/// Parse an ordinary function's header while keeping its operation body opaque.
pub(crate) fn function_signature(item: &TokenStream) -> Result<Signature, ParseError> {
    let parser = Parser::from_tokens(item);
    let _: Attributes = Parse::parse(&parser)?;
    let _: Visibility = Parse::parse(&parser)?;
    let signature = Signature::parse(&parser)?;
    finish_braced_body(&parser, "expected one function with a body")?;
    Ok(signature)
}

/// Parse a complete path without generic arguments for a concrete type or constant.
pub(crate) fn plain_path(
    tokens: &TokenStream,
    span: Span,
    message: &str,
) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(tokens);
    let path = Path::parse(&parser)?;
    if !parser.is_empty()
        || path.is_empty()
        || path
            .iter()
            .any(|segment| !matches!(segment.args, PathArguments::None))
    {
        return Err(ParseError::new(span, message));
    }
    Ok(path)
}

/// Consume the final native body without parsing its operations.
pub(crate) fn finish_braced_body(parser: &Parser, message: &str) -> Result<(), ParseError> {
    if !parser
        .advance()
        .is_some_and(|token| token.as_group().is_some_and(|group| group.delim.is_brace()))
        || !parser.is_empty()
    {
        return Err(parser.error(message));
    }
    Ok(())
}

/// Split a complete declaration's trailing metadata and operation body as native tokens.
pub(crate) struct BracketedFunction {
    pub(crate) header: proc_macro::TokenStream,
    pub(crate) metadata: proc_macro::TokenStream,
    pub(crate) body: proc_macro::Group,
}

pub(crate) fn bracketed_function(
    input: proc_macro::TokenStream,
    macro_name: &str,
) -> Result<BracketedFunction, ParseError> {
    use proc_macro::{Delimiter, TokenTree};

    let mut tokens: Vec<_> = input.into_iter().collect();
    let Some(TokenTree::Group(body)) = tokens.pop() else {
        return Err(ParseError::new(
            Span::call_site(),
            format!("{macro_name} needs a braced body"),
        ));
    };
    if body.delimiter() != Delimiter::Brace {
        return Err(ParseError::new(
            body.span().into(),
            format!("{macro_name} needs a braced body"),
        ));
    }
    let Some(TokenTree::Group(metadata)) = tokens.pop() else {
        return Err(ParseError::new(
            body.span().into(),
            format!("{macro_name} needs bracketed metadata before its body"),
        ));
    };
    if metadata.delimiter() != Delimiter::Bracket {
        return Err(ParseError::new(
            metadata.span().into(),
            format!("{macro_name} needs bracketed metadata before its body"),
        ));
    }
    Ok(BracketedFunction {
        header: tokens.into_iter().collect(),
        metadata: metadata.stream(),
        body,
    })
}

/// Wrap generated statements as one native brace expression.
pub(crate) fn native_expression(body: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro::TokenStream::from_iter([proc_macro::TokenTree::Group(proc_macro::Group::new(
        proc_macro::Delimiter::Brace,
        body,
    ))])
}

/// Collect top-level native declaration bodies without traversing caller operations.
pub(crate) fn native_braced_bodies(input: proc_macro::TokenStream) -> Vec<proc_macro::Group> {
    native_groups(input)
        .filter(|group| group.delimiter() == proc_macro::Delimiter::Brace)
        .collect()
}

/// Yield top-level native groups without looking inside a caller's expressions.
pub(crate) fn native_groups(
    input: proc_macro::TokenStream,
) -> impl Iterator<Item = proc_macro::Group> {
    input.into_iter().filter_map(|token| match token {
        proc_macro::TokenTree::Group(group) => Some(group),
        _ => None,
    })
}

/// Read a named declaration clause and retain its identifier span for errors.
pub(crate) fn expect_name(
    parser: &Parser,
    expected: &str,
    message: &str,
) -> Result<Ident, ParseError> {
    let name = Ident::parse(parser)?;
    if name != expected {
        return Err(ParseError::new(name.span(), message));
    }
    Ok(name)
}

/// Read an opaque clause group; each caller validates its delimiter and emptiness.
pub(crate) fn grouped_value(parser: &Parser, message: &str) -> Result<Group, ParseError> {
    let token = parser.advance().ok_or_else(|| parser.error(message))?;
    token
        .as_group()
        .cloned()
        .ok_or_else(|| ParseError::new(token.span(), message))
}

/// The named fields of a struct, or an error naming the derive.
pub(crate) fn named_fields<'a>(
    item: &'a ItemStruct,
    derive: &str,
) -> Result<&'a List<Field, Token![,]>, ParseError> {
    item.fields
        .as_named()
        .map(|named| &named.fields.inner)
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                format!("`{derive}` needs a struct with named fields"),
            )
        })
}

/// A field's name, or an error naming the derive.
pub(crate) fn field_ident<'a>(field: &'a Field, derive: &str) -> Result<&'a Ident, ParseError> {
    field
        .ident
        .as_ref()
        .ok_or_else(|| ParseError::new(field.span(), format!("`{derive}` needs named fields")))
}

/// The single field of a newtype, or an error naming the derive.
pub(crate) fn inner_type(item: &ItemStruct, derive: &str) -> Result<Type, ParseError> {
    item.fields
        .as_unnamed()
        .filter(|unnamed| unnamed.fields.len() == 1)
        .and_then(|unnamed| unnamed.fields.first())
        .map(|field| field.ty.clone())
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                format!("`{derive}` needs a tuple struct with exactly one field"),
            )
        })
}

/// Token text with every whitespace character removed.
pub(crate) fn compact(tokens: &(impl ToString + ?Sized)) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// The value of a `key = <expression>` argument, as the expression's tokens.
pub(crate) struct Tokens(pub(crate) TokenStream);

impl moxy::ast::FromMeta for Tokens {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::Expr { expr, .. } => Ok(Self(expr.clone())),
            _ => Err(ParseError::new(
                meta.span(),
                "expected `key = <expression>`",
            )),
        }
    }
}

/// Locate an item body while preserving attributes, visibility and generics.
pub(crate) fn item_body(
    item: TokenStream,
    macro_name: &str,
    is_kind: fn(&moxy::token::TokenTree) -> bool,
) -> Result<(Vec<moxy::token::TokenTree>, usize), ParseError> {
    let tokens: Vec<_> = item.into_iter().collect();
    let body = tokens
        .iter()
        .any(is_kind)
        .then(|| {
            tokens
                .iter()
                .rposition(|token| token.as_group().is_some_and(|group| group.delim.is_brace()))
        })
        .flatten();
    match body {
        Some(body) => Ok((tokens, body)),
        None => Err(ParseError::new(
            tokens.first().map_or_else(Span::call_site, Spanner::span),
            format!("`#[{macro_name}]` needs a braced item of the expected kind"),
        )),
    }
}

pub(crate) fn named_body(
    item: TokenStream,
    macro_name: &str,
) -> Result<(Vec<moxy::token::TokenTree>, usize), ParseError> {
    item_body(item, macro_name, moxy::token::TokenTree::is_keyword_struct)
}

/// Transform a named struct's brace-delimited body and preserve the item header.
pub(crate) fn edit_named_body(
    item: TokenStream,
    macro_name: &str,
    edit: impl FnOnce(TokenStream) -> Result<TokenStream, ParseError>,
) -> Result<TokenStream, ParseError> {
    let (mut tokens, body) = named_body(item, macro_name)?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    group.tokens = edit(group.tokens.clone())?;
    Ok(tokens.into())
}

/// Edit a validated item's final native body without rebuilding nested groups.
pub(crate) fn edit_native_body(
    item: proc_macro::TokenStream,
    edit: impl FnOnce(proc_macro::TokenStream) -> proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let mut tokens: Vec<_> = item.into_iter().collect();
    let Some(proc_macro::TokenTree::Group(body)) = tokens.pop() else {
        unreachable!("validated item body")
    };
    let mut replacement = proc_macro::Group::new(body.delimiter(), edit(body.stream()));
    replacement.set_span(body.span());
    tokens.push(proc_macro::TokenTree::Group(replacement));
    tokens.into_iter().collect()
}

/// Require a nonempty token argument without changing its tokens or error span.
pub(crate) fn required_tokens(
    tokens: TokenStream,
    message: &str,
) -> Result<TokenStream, ParseError> {
    if tokens.is_empty() {
        Err(ParseError::new(Span::call_site(), message))
    } else {
        Ok(tokens)
    }
}

/// Read one of the explicit modes accepted by a field-group attribute.
pub(crate) fn mode<const N: usize>(
    tokens: TokenStream,
    modes: [&'static str; N],
) -> Result<&'static str, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    if let [TokenTree::Ident(name)] = tokens.as_slice()
        && let Some(mode) = modes.into_iter().find(|mode| name == *mode)
    {
        return Ok(mode);
    }
    Err(ParseError::new(
        Span::call_site(),
        format!(
            "expected one of {}",
            modes.map(|mode| format!("`{mode}`")).join(", ")
        ),
    ))
}

/// Wrap generated members in the declaration's original generics and where clause.
pub(crate) fn impl_block(
    item: ItemStruct,
    trait_prefix: &TokenStream,
    members: &TokenStream,
) -> TokenStream {
    let ident = item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    moxy::template! {
        impl {{ impl_generics }} {{ trait_prefix }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            {{ members }}
        }
    }
}

/// Split nonempty top-level token arguments, preserving grouped expressions.
pub(crate) fn arguments(input: TokenStream, count: usize) -> Result<Vec<TokenStream>, ParseError> {
    let mut arguments = vec![TokenStream::new()];
    for token in input {
        if token.is_punct_comma() {
            arguments.push(TokenStream::new());
        } else {
            arguments.last_mut().expect("one argument").extend([token]);
        }
    }
    if arguments.len() != count || arguments.iter().any(TokenStream::is_empty) {
        return Err(ParseError::new(
            Span::call_site(),
            format!("expected {count} nonempty comma-separated arguments"),
        ));
    }
    Ok(arguments)
}

/// Reject arguments on an attribute macro that accepts only its annotated item.
pub(crate) fn no_arguments(tokens: TokenStream, macro_name: &str) -> Result<(), ParseError> {
    if let Some(token) = tokens.into_iter().next() {
        return Err(ParseError::new(
            token.span(),
            format!("{macro_name} takes no arguments"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, moxy::token::ToTokenStream};

    #[test]
    fn async_and_sync_policies_share_only_qualifier_checks() {
        let asynchronous = function_signature(&"async fn perform() {}".parse().unwrap()).unwrap();
        assert!(is_plain_async(&asynchronous));
        assert!(!is_plain_sync(&asynchronous));
        let synchronous = function_signature(&"fn perform() {}".parse().unwrap()).unwrap();
        assert!(is_plain_sync(&synchronous));
        assert!(!is_plain_async(&synchronous));
        for source in [
            "const fn perform() {}",
            "unsafe fn perform() {}",
            "async unsafe fn perform() {}",
            "extern \"C\" fn perform() {}",
            "async extern \"C\" fn perform() {}",
            "fn perform<T>() {}",
            "async fn perform<T>() {}",
        ] {
            let signature = function_signature(&source.parse().unwrap()).unwrap();
            assert!(
                !is_plain_sync(&signature) && !is_plain_async(&signature),
                "{source}"
            );
        }
        let signature = function_signature(
            &"async fn perform() where Concrete: Trait {}"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert!(is_plain_async(&signature));
    }

    #[test]
    fn schema_tables_preserve_paths_and_domain_errors() {
        let paths = nonempty_paths(
            &"wire::First, ::wire::Second,".parse().unwrap(),
            "response schemas",
        )
        .unwrap();
        assert!(paths.len() == 2);
        assert!(compact(&paths[0].to_token_stream()) == "wire::First");
        assert!(compact(&paths[1].to_token_stream()) == "::wire::Second");
        let error = nonempty_paths(&TokenStream::new(), "response schemas")
            .err()
            .expect("empty schema table rejected");
        assert!(error.to_string().contains("response schemas"));
        for invalid in ["Type extra", "Type;", "Type, 42"] {
            assert!(nonempty_paths(&invalid.parse().unwrap(), "response schemas").is_err());
        }
    }

    #[test]
    fn concrete_paths_reject_generics_and_trailing_expressions() {
        for valid in ["Type", "crate::module::Type", "::module::VALUE"] {
            let tokens = valid.parse().unwrap();
            assert!(plain_path(&tokens, Span::call_site(), "concrete path").is_ok());
        }
        for invalid in ["", "Type<T>", "Type + Other", "Type::method()"] {
            let tokens = invalid.parse().unwrap();
            assert!(plain_path(&tokens, Span::call_site(), "concrete path").is_err());
        }
    }

    #[test]
    fn named_clauses_keep_opaque_groups_and_reject_missing_values() {
        let tokens = "body { nested!(if ready { a() } else { b() }); }"
            .parse()
            .unwrap();
        let parser = Parser::from_tokens(&tokens);
        assert!(expect_name(&parser, "body", "body clause").unwrap() == "body");
        let body = grouped_value(&parser, "operation body").unwrap();
        assert!(body.delim.is_brace());
        assert!(compact(&body.tokens) == "nested!(ifready{a()}else{b()});");
        assert!(parser.is_empty());
        assert!(grouped_value(&parser, "missing value").is_err());
        let tokens = "wrong literal".parse().unwrap();
        let parser = Parser::from_tokens(&tokens);
        assert!(expect_name(&parser, "body", "body clause").is_err());
        assert!(grouped_value(&parser, "group expected").is_err());
    }
}
