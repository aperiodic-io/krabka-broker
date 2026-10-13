//! `#[derive(RefinedNewtype)]`: see the crate documentation.

use {
    crate::meta::{Tokens, inner_type},
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::{Ident, LitStr, Spanner, ToTokenStream, TokenStream},
    },
};

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
        let (to_raw, from_raw, raw, parse, unit) = match name.tokens.to_string().as_str() {
            "ByteSize" => ("bytes_i64", "from_bytes_i64", "i64", "byte_size", "bytes"),
            "Time" => ("millis_i64", "from_millis", "i64", "time", "milliseconds"),
            "Frequency" => ("per_sec_u64", "from_per_sec_u64", "u64", "frequency", "Hz"),
            _ => {
                return Err(ParseError::new(
                    name.tokens.span(),
                    "`quantity` is `ByteSize`, `Time` or `Frequency`",
                ));
            }
        };
        let ident = |text: &str| {
            Ident::new(text)
                .with_span(name.tokens.span())
                .to_token_stream()
        };
        let ext = ident(&format!("{}Ext", name.tokens));
        let parse = ident(parse);
        Ok(Self {
            ty: moxy::template! { ::krabka_units::{{ name.tokens }} },
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
        .map_or_else(|| moxy::template! { into_value }, |getter| getter.tokens);
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
        (name.tokens, doc)
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
        .map(|name| name.tokens)
        .zip(quantity.as_ref().map(|quantity| quantity.ty.clone()));

    let default_value = args.default_value.map(|tokens| tokens.tokens);
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

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::compact,
        assert2::assert,
        moxy::{
            ast::{File, Item, ItemStruct},
            token::{ToTokenStream, TokenStream},
        },
    };

    struct Expansion {
        tokens: TokenStream,
        items: File,
    }

    fn expanded(source: &str) -> Expansion {
        let item: ItemStruct = moxy::parse!({ source }).expect("valid newtype declaration");
        let tokens = expand(item).expect("valid refined policy");
        let items = moxy::parse!({ tokens.clone() }).expect("generated outer items");
        Expansion { tokens, items }
    }

    #[test]
    fn quantity_contracts_expose_checked_units_getters_defaults_and_text_parsers() {
        struct QuantityContract {
            name: &'static str,
            to_raw: &'static str,
            from_raw: &'static str,
            raw: &'static str,
            parser: &'static str,
            default: &'static str,
        }
        for case in [
            QuantityContract {
                name: "ByteSize",
                to_raw: "bytes_i64",
                from_raw: "from_bytes_i64",
                raw: "i64",
                parser: "byte_size",
                default: "::krabka_units::bytes(7)",
            },
            QuantityContract {
                name: "Time",
                to_raw: "millis_i64",
                from_raw: "from_millis",
                raw: "i64",
                parser: "time",
                default: "::krabka_units::millis(7)",
            },
            QuantityContract {
                name: "Frequency",
                to_raw: "per_sec_u64",
                from_raw: "from_per_sec_u64",
                raw: "u64",
                parser: "frequency",
                default: "::krabka_units::per_sec(7)",
            },
        ] {
            let source = format!(
                "#[refined(rule(GreaterI32<0>), quantity = {}, getter = raw, quantity_getter = quantity, parse_fn = parse_amount, default = {}, from_str, display)] pub struct Amount(i32);",
                case.name, case.default
            );
            let expansion = expanded(&source);
            let text = compact(&expansion.tokens);
            for signature in [
                format!(
                    "fnnew(value:::krabka_units::{})->::core::result::Result<Self,::std::string::String>",
                    case.name
                ),
                "constfnraw(self)->i32".to_owned(),
                format!("fnquantity(self)->::krabka_units::{}", case.name),
            ] {
                assert!(text.contains(&format!("pub{signature}")), "{signature}");
            }
            assert!(text.contains(&format!("TryFrom<::krabka_units::{}>forAmount", case.name)));
            assert!(text.contains(&format!(
                "as::krabka_units::convert::{}Ext>::{}(value)",
                case.name, case.to_raw
            )));
            assert!(text.contains(&format!(
                "as::krabka_units::convert::{}Ext>::{}(raw)==value",
                case.name, case.from_raw
            )));
            assert!(text.contains(&format!(
                "<i32as::core::convert::TryFrom<{}>>::try_from(raw).ok()",
                case.raw
            )));
            assert!(text.contains(&format!("Self::new({}).expect", case.default)));
            assert!(text.contains("impl::core::str::FromStrforAmount"));
            assert!(text.contains(&format!("::krabka_units::parse::{}(value)", case.parser)));
            assert!(text.contains("::krabka_units::fmt::Human::human"));
            let parser = expansion
                .items
                .items
                .iter()
                .find_map(|item| match item {
                    Item::Fn(function) if function.sig.ident == "parse_amount" => Some(function),
                    _ => None,
                })
                .expect("public text parser");
            assert!(compact(&parser.vis.to_token_stream()) == "pub");
            assert!(
                compact(&parser.sig.to_token_stream())
                    == "fnparse_amount(value:&str)->::core::result::Result<Amount,::std::string::String>"
            );
        }
    }

    #[test]
    fn scalar_policy_keeps_rule_errors_and_parses_through_the_inner_type() {
        let expansion = expanded(
            "#[refined(rule(MinMaxU32<0, 100>), from_str, parse_fn = parse_percent, default = 7)] pub struct Percent(u32);",
        );
        let text = compact(&expansion.tokens);
        assert!(text.contains(
            "fnnew(value:u32)->::core::result::Result<Self,::refined_type::result::Error<u32>>"
        ));
        assert!(text.contains("constfninto_value(self)->u32"));
        assert!(text.contains("value.parse::<u32>()"));
        assert!(text.contains("<Percent>::new(value).map_err"));
        assert!(text.contains("Self::new(7).expect"));
        assert!(text.contains(
            "fnparse_percent(value:&str)->::core::result::Result<Percent,::std::string::String>"
        ));
        assert!(!text.contains("TryFrom<"));
    }

    #[test]
    fn rejects_unsupported_quantities_and_getters_without_a_quantity() {
        struct RejectedPolicy {
            source: &'static str,
            diagnostic: &'static str,
        }
        for case in [
            RejectedPolicy {
                source: "#[refined(rule(GreaterI32<0>), quantity = DataRate)] struct Value(i32);",
                diagnostic: "`quantity` is `ByteSize`, `Time` or `Frequency`",
            },
            RejectedPolicy {
                source: "#[refined(rule(GreaterI32<0>), quantity_getter = quantity)] struct Value(i32);",
                diagnostic: "`quantity_getter` needs `quantity = ByteSize|Time|Frequency`",
            },
            RejectedPolicy {
                source: "#[refined(rule(GreaterI32<0>), label = \"value\")] struct Value(i32);",
                diagnostic: "`label` prefixes a `String` error: add `string_error`",
            },
        ] {
            let item: ItemStruct = moxy::parse!({ case.source }).unwrap();
            let Err(error) = expand(item) else {
                panic!("invalid refined policy was accepted");
            };
            assert!(error.to_string().contains(case.diagnostic));
        }
    }
}
