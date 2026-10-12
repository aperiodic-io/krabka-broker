//! `#[derive(RefinedNewtype)]`: see the crate documentation.

use moxy::{
    ast::{Attributed, ItemStruct, ParseError},
    token::{Ident, LitStr, Spanner, ToTokenStream, TokenStream},
};

use crate::meta::{Tokens, inner_type};

/// The tokens inside a `key(...)` argument. A type with generic arguments
/// goes here rather than after `=`, where moxy reads an expression and stops
/// at the `<`.
struct Parenthesized(TokenStream);

impl moxy::ast::FromMeta for Parenthesized {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::List(group) => Ok(Self(group.tokens.clone())),
            _ => Err(ParseError::new(meta.span(), "expected `key(<type>)`")),
        }
    }
}

/// The arguments of the `#[refined(...)]` struct attribute.
#[derive(moxy::FromMeta)]
struct RefinedArgs {
    /// The `refined_type` rule whose `new` validates the value.
    rule: Parenthesized,
    /// The name of the method that returns the inner value.
    #[meta(default)]
    getter: Option<Tokens>,
    /// Return `String` errors from `new` rather than the rule's own error.
    #[meta(default)]
    string_error: bool,
    /// A prefix for every `new` error, as in `"<label>: <error>"`.
    #[meta(default)]
    label: Option<String>,
    /// The value `Default` validates and wraps.
    #[meta(rename = "default", default)]
    default_value: Option<Tokens>,
    /// Implement `FromStr` through the inner type's own parse.
    #[meta(default)]
    from_str: bool,
    /// Implement `Display` as the inner value's.
    #[meta(default)]
    display: bool,
    /// The name of a free function that parses text into the newtype.
    #[meta(default)]
    parse_fn: Option<Tokens>,
    /// The `krabka_units` quantity `new` takes as a whole number of its unit.
    #[meta(default)]
    quantity: Option<Tokens>,
    /// The name of a method that returns the value as its quantity.
    #[meta(default)]
    quantity_getter: Option<Tokens>,
}

/// A `krabka_units` quantity that a `quantity = <name>` newtype holds as a
/// whole number of one unit.
struct Quantity {
    /// The quantity type.
    ty: TokenStream,
    /// The `krabka_units::convert` trait with its conversions.
    ext: TokenStream,
    /// The method that rounds a quantity to a `raw` count of the unit.
    to_raw: TokenStream,
    /// The constructor from a `raw` count of the unit.
    from_raw: TokenStream,
    /// The integer `to_raw` returns and `from_raw` takes.
    raw: TokenStream,
    /// The `krabka_units::parse` function for the quantity's text.
    parse: TokenStream,
    /// The unit, for the error text.
    unit: &'static str,
}

impl Quantity {
    /// The quantity that `name` names.
    fn named(name: &Tokens) -> Result<Self, ParseError> {
        let (to_raw, from_raw, raw, parse, unit) = match name.0.to_string().as_str() {
            "ByteSize" => ("bytes_i64", "from_bytes_i64", "i64", "byte_size", "bytes"),
            "Time" => ("millis_i64", "from_millis", "i64", "time", "milliseconds"),
            "Frequency" => ("per_sec_u64", "from_per_sec_u64", "u64", "frequency", "Hz"),
            _ => {
                return Err(ParseError::new(
                    name.0.span(),
                    "`quantity` is `ByteSize`, `Time` or `Frequency`",
                ));
            }
        };
        let ident = |text: &str| Ident::new(text).with_span(name.0.span()).to_token_stream();
        let ext = ident(&format!("{}Ext", name.0));
        let parse = ident(parse);
        Ok(Self {
            ty: moxy::template! { ::krabka_units::{{ name.0 }} },
            ext: moxy::template! { ::krabka_units::convert::{{ ext }} },
            to_raw: ident(to_raw),
            from_raw: ident(from_raw),
            raw: ident(raw),
            parse: moxy::template! { ::krabka_units::parse::{{ parse }} },
            unit,
        })
    }
}

/// The doc text of the generated free parse function.
fn parse_fn_doc(name: &str) -> String {
    format!(
        " Parse a [`{name}`] from text.\n\n # Errors\n\n Returns an error when `value` does \
         not parse, or parses to a value that [`{name}::new`] refuses."
    )
}

/// The error text of a quantity that is not a whole number of `unit` that
/// fits in `inner`.
fn whole_unit_error(label: Option<&str>, unit: &str, inner: &str) -> String {
    let rule = format!("must be a whole number of {unit} that fits in {inner}");
    label.map_or_else(|| rule.clone(), |label| format!("{label}: {rule}"))
}

/// Expands `#[derive(RefinedNewtype)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let inner = inner_type(&item, "RefinedNewtype")?;
    let Some(args) = item.parse_meta::<RefinedArgs>("refined")? else {
        return Err(ParseError::new(
            item.ident.span(),
            "`RefinedNewtype` needs `#[refined(rule = <refined type>)]`",
        ));
    };
    let quantity = args.quantity.as_ref().map(Quantity::named).transpose()?;
    // A quantity that is not a whole number of its unit has no rule error to
    // return, so a quantity newtype's errors are always text.
    let string_error = args.string_error || quantity.is_some();
    if args.label.is_some() && !string_error {
        return Err(ParseError::new(
            item.ident.span(),
            "`label` prefixes a `String` error: add `string_error`",
        ));
    }
    if args.quantity_getter.is_some() && quantity.is_none() {
        return Err(ParseError::new(
            item.ident.span(),
            "`quantity_getter` needs `quantity = ByteSize|Time|Frequency`",
        ));
    }

    let ident = item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    let vis = item.vis;
    let rule = args.rule.0;
    let getter = args
        .getter
        .map_or_else(|| moxy::template! { into_value }, |getter| getter.0);
    let span = ident.span();
    let default_expect = LitStr::new(&format!("the default {ident} satisfies its rule"), span);
    let error = if string_error {
        moxy::template! { ::std::string::String }
    } else {
        moxy::template! { ::refined_type::result::Error<{{ inner }}> }
    };
    let map_error = match (&args.label, string_error) {
        (Some(label), _) => {
            let format = LitStr::new(&format!("{label}: {{error}}"), span);
            moxy::template! { .map_err(|error| ::std::format!({{ format }})) }
        }
        (None, true) => {
            moxy::template! { .map_err(|error| ::std::string::ToString::to_string(&error)) }
        }
        (None, false) => TokenStream::new(),
    };
    // Text through the quantity's or the inner type's parse, then through
    // `new`; both errors become `String`.
    let parser = quantity.as_ref().map_or_else(
        || moxy::template! { value.parse::<{{ inner }}>() },
        |quantity| {
            let parse = &quantity.parse;
            moxy::template! { {{ parse }}(value) }
        },
    );
    let validate = if string_error {
        moxy::template! { .and_then(<{{ ident }} {{ type_generics }}>::new) }
    } else {
        moxy::template! {
            .and_then(|value| <{{ ident }} {{ type_generics }}>::new(value).map_err(|error| ::std::string::ToString::to_string(&error)))
        }
    };
    let parse = moxy::template! {
        {{ parser }}
            .map_err(|error| ::std::string::ToString::to_string(&error))
            {{ validate }}
    };
    let parse_fn = args.parse_fn.map(|name| {
        let doc = LitStr::new(&parse_fn_doc(&ident.to_string()), span);
        (name.0, doc)
    });

    // `new` takes the field's type, or the quantity, which has to be a whole
    // number of its unit that the field holds before the rule sees it.
    let (new_input, to_inner) = match &quantity {
        None => (moxy::template! { {{ inner }} }, TokenStream::new()),
        Some(Quantity {
            ty,
            ext,
            to_raw,
            from_raw,
            raw,
            unit,
            ..
        }) => {
            let inner_name = moxy::template! { {{ inner }} }.to_string();
            let whole = LitStr::new(
                &whole_unit_error(args.label.as_deref(), unit, &inner_name),
                span,
            );
            let to_inner = moxy::template! {
                let raw = <{{ ty }} as {{ ext }}>::{{ to_raw }}(value);
                let value = if <{{ ty }} as {{ ext }}>::{{ from_raw }}(raw) == value {
                    <{{ inner }} as ::core::convert::TryFrom<{{ raw }}>>::try_from(raw).ok()
                } else {
                    ::core::option::Option::None
                }
                .ok_or_else(|| ::std::string::String::from({{ whole }}))?;
            };
            (moxy::template! { {{ ty }} }, to_inner)
        }
    };
    // The validated field as its quantity. `new` built the field from a
    // `raw` count, so it converts back.
    let as_quantity = quantity.as_ref().map(
        |Quantity {
             ty, ext, from_raw, raw, ..
         }| {
            moxy::template! {
                <{{ ty }} as {{ ext }}>::{{ from_raw }}(
                    <{{ raw }} as ::core::convert::TryFrom<{{ inner }}>>::try_from(self.0)
                        .unwrap_or_else(|_| ::core::unreachable!("`new` built the field from a raw count")),
                )
            }
        },
    );
    let quantity_getter = args
        .quantity_getter
        .map(|name| name.0)
        .zip(quantity.as_ref().map(|quantity| quantity.ty.clone()));

    let default_value = args.default_value.map(|tokens| tokens.0);
    let from_str = args.from_str;
    let display = match (&as_quantity, args.display) {
        (_, false) => None,
        (None, true) => Some(moxy::template! { ::core::fmt::Display::fmt(&self.0, formatter) }),
        (Some(as_quantity), true) => Some(moxy::template! {
            ::core::fmt::Display::fmt(&::krabka_units::fmt::Human::human({{ as_quantity }}), formatter)
        }),
    };
    Ok(moxy::template! {
        impl {{ impl_generics }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            /// Validate `value` against the type's rule and wrap it.
            ///
            /// # Errors
            ///
            /// Returns an error when `value` does not satisfy the rule.
            {{ vis }} fn new(value: {{ new_input }}) -> ::core::result::Result<Self, {{ error }}> {
                {{ to_inner }}
                <{{ rule }}>::new(value)
                    .map(|value| Self(value.into_value()))
                    {{ map_error }}
            }

            /// The validated value.
            #[must_use]
            {{ vis }} const fn {{ getter }}(self) -> {{ inner }} {
                self.0
            }

            @if let (Some((name, ty)), Some(as_quantity)) = (&quantity_getter, &as_quantity) {
                /// The validated value as its quantity.
                #[must_use]
                {{ vis }} fn {{ name }}(self) -> {{ ty }} {
                    {{ as_quantity }}
                }
            }
        }

        @if let Some(default_value) = &default_value {
            impl {{ impl_generics }} ::core::default::Default for {{ ident }} {{ type_generics }} {{ where_clause }} {
                fn default() -> Self {
                    Self::new({{ default_value }}).expect({{ default_expect }})
                }
            }
        }

        @if let Some(quantity) = &quantity {
            impl {{ impl_generics }} ::core::convert::TryFrom<{{ quantity.ty }}> for {{ ident }} {{ type_generics }} {{ where_clause }} {
                type Error = ::std::string::String;

                fn try_from(value: {{ quantity.ty }}) -> ::core::result::Result<Self, Self::Error> {
                    Self::new(value)
                }
            }
        }

        @if from_str {
            impl {{ impl_generics }} ::core::str::FromStr for {{ ident }} {{ type_generics }} {{ where_clause }} {
                type Err = ::std::string::String;

                fn from_str(value: &str) -> ::core::result::Result<Self, Self::Err> {
                    {{ parse }}
                }
            }
        }

        @if let Some(display) = &display {
            impl {{ impl_generics }} ::core::fmt::Display for {{ ident }} {{ type_generics }} {{ where_clause }} {
                fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    {{ display }}
                }
            }
        }

        @if let Some((name, doc)) = &parse_fn {
            #[doc = {{ doc }}]
            {{ vis }} fn {{ name }} {{ impl_generics }}(value: &str) -> ::core::result::Result<{{ ident }} {{ type_generics }}, ::std::string::String> {{ where_clause }} {
                {{ parse }}
            }
        }
    })
}
