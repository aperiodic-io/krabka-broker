//! `#[derive(EnumStr)]`: see the crate documentation.

use moxy::{
    ast::{Attributed, Fields, ItemEnum, List, ParseError, Parser, Token},
    token::{LitStr, Spanner, TokenStream},
};

/// The value of a `key` or `key = name` argument: `None` for the bare key,
/// which asks for the default name.
struct FnName(Option<TokenStream>);

impl moxy::ast::FromMeta for FnName {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::Unit => Ok(Self(None)),
            moxy::ast::MetaContent::Expr { expr, .. } => Ok(Self(Some(expr.clone()))),
            moxy::ast::MetaContent::List(_) => Err(ParseError::new(
                meta.span(),
                "expected `key` or `key = name`",
            )),
        }
    }
}

/// The value of `alias = "a"` or `alias("a", "b")`.
struct Aliases(Vec<String>);

impl moxy::ast::FromMeta for Aliases {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::List(group) => {
                let parser = Parser::from_tokens(&group.tokens);
                let list = List::<LitStr, Token![,]>::parse_all(&parser)?;
                Ok(Self(
                    list.iter().map(|lit| lit.value().to_owned()).collect(),
                ))
            }
            _ => String::from_meta(meta).map(|alias| Self(vec![alias])),
        }
    }
}

/// The arguments of the `#[enum_str(...)]` enum attribute.
#[derive(Default, moxy::FromMeta)]
struct EnumArgs {
    /// How a variant name becomes its text when the variant names none.
    #[meta(default)]
    case: Option<String>,
    /// The name of the method that returns the text.
    #[meta(default)]
    as_str: Option<FnName>,
    /// The name of the method that parses the text, when there is one.
    #[meta(default)]
    parse: Option<FnName>,
    /// Add `pub const ALL: [Self; N]`.
    #[meta(default)]
    all: bool,
    /// Implement `prometheus_client::encoding::EncodeLabelValue`.
    #[meta(default)]
    label_value: bool,
}

/// The arguments of the `#[enum_str(...)]` variant attribute.
#[derive(moxy::FromMeta)]
struct VariantArgs {
    /// The variant's text, in place of the cased variant name.
    #[meta(default)]
    name: Option<String>,
    /// Other text that `parse` accepts for the variant.
    #[meta(default)]
    alias: Option<Aliases>,
}

/// `ident` in `case`: `snake_case`, `kebab-case`, `lowercase`, `UPPERCASE`,
/// or unchanged when there is no case.
fn apply_case(ident: &str, case: Option<&str>) -> Result<String, String> {
    let separated = |separator: char| {
        let mut text = String::new();
        for (index, character) in ident.chars().enumerate() {
            if character.is_uppercase() && index > 0 {
                text.push(separator);
            }
            text.extend(character.to_lowercase());
        }
        text
    };
    match case {
        None => Ok(ident.to_owned()),
        Some("snake_case") => Ok(separated('_')),
        Some("kebab-case") => Ok(separated('-')),
        Some("lowercase") => Ok(ident.to_lowercase()),
        Some("UPPERCASE") => Ok(ident.to_uppercase()),
        Some(other) => Err(format!(
            "unknown case `{other}`: expected `snake_case`, `kebab-case`, `lowercase` or `UPPERCASE`"
        )),
    }
}

/// Expands `#[derive(EnumStr)]` on `item`.
pub(crate) fn expand(item: ItemEnum) -> Result<TokenStream, ParseError> {
    let span = item.ident.span();
    let args = item.parse_meta::<EnumArgs>("enum_str")?.unwrap_or_default();
    let as_str = args
        .as_str
        .and_then(|name| name.0)
        .unwrap_or_else(|| moxy::template! { as_str });
    let parse = args
        .parse
        .map(|name| name.0.unwrap_or_else(|| moxy::template! { parse }));

    let mut name_arms = Vec::new();
    let mut parse_arms = Vec::new();
    let mut units = Vec::new();
    let mut has_fields = false;
    for variant in &item.variants.inner {
        let ident = &variant.ident;
        let variant_args = variant.parse_meta::<VariantArgs>("enum_str")?;
        let (name, aliases) = match variant_args {
            Some(VariantArgs { name, alias }) => (name, alias.map(|alias| alias.0)),
            None => (None, None),
        };
        let name = match name {
            Some(name) => name,
            None => apply_case(&ident.to_string(), args.case.as_deref())
                .map_err(|message| ParseError::new(span, &message))?,
        };
        let text = LitStr::new(&name, ident.span());
        let pattern = match &variant.fields {
            Fields::Named(_) => moxy::template! { Self::{{ ident }} { .. } },
            Fields::Unnamed(_) => moxy::template! { Self::{{ ident }}(..) },
            Fields::Unit => moxy::template! { Self::{{ ident }} },
        };
        name_arms.push(moxy::template! { {{ pattern }} => {{ text }}, });
        if variant.fields.is_unit() {
            let texts: Vec<LitStr> = std::iter::once(name)
                .chain(aliases.into_iter().flatten())
                .map(|text| LitStr::new(&text, ident.span()))
                .collect();
            parse_arms.push(moxy::template! {
                @for (index, text) in texts.iter().enumerate() {
                    @if index > 0 { | }
                    {{ text }}
                }
                => ::core::option::Option::Some(Self::{{ ident }}),
            });
            units.push(moxy::template! { Self::{{ ident }}, });
        } else {
            has_fields = true;
        }
    }
    if args.all && has_fields {
        return Err(ParseError::new(
            span,
            "`all` needs every variant to be a unit variant",
        ));
    }

    let ident = item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    let vis = item.vis;
    let receiver = if has_fields {
        moxy::template! { &self }
    } else {
        moxy::template! { self }
    };
    let count: TokenStream = units
        .len()
        .to_string()
        .parse()
        .map_err(|_| ParseError::new(span, "unreachable: a count lexes as a literal"))?;
    let all = args.all;
    let label_value = args.label_value;
    Ok(moxy::template! {
        impl {{ impl_generics }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            /// The text this variant stands for.
            #[must_use]
            {{ vis }} const fn {{ as_str }}({{ receiver }}) -> &'static str {
                match self {
                    @for arm in &name_arms { {{ arm }} }
                }
            }

            @if let Some(parse) = &parse {
                /// The variant whose text, or one of whose aliases, is `value`.
                #[must_use]
                {{ vis }} fn {{ parse }}(value: &str) -> ::core::option::Option<Self> {
                    match value {
                        @for arm in &parse_arms { {{ arm }} }
                        _ => ::core::option::Option::None,
                    }
                }
            }

            @if all {
                /// Every variant, in declaration order.
                {{ vis }} const ALL: [Self; {{ count }}] = [
                    @for unit in &units { {{ unit }} }
                ];
            }
        }

        @if label_value {
            impl {{ impl_generics }} ::prometheus_client::encoding::EncodeLabelValue for {{ ident }} {{ type_generics }} {{ where_clause }} {
                fn encode(
                    &self,
                    encoder: &mut ::prometheus_client::encoding::LabelValueEncoder<'_>,
                ) -> ::core::result::Result<(), ::core::fmt::Error> {
                    let text: &'static str = match self {
                        @for arm in &name_arms { {{ arm }} }
                    };
                    ::prometheus_client::encoding::EncodeLabelValue::encode(&text, encoder)
                }
            }
        }
    })
}
