//! Syntax and attribute helpers shared by the macros.

use moxy::{
    ast::{Field, ItemStruct, List, ParseError, Token, Type},
    token::{Ident, Span, Spanner, TokenStream, TokenTree},
};

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
