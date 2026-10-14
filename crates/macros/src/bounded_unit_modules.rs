//! Flat serde module declarations with one explicit magnitude policy per row.

use {
    crate::meta::{Tokens, plain_path},
    moxy::{
        ast::{Attributes, FromMeta, Meta, Parse, ParseError, Parser, Path, Token, Visibility},
        token::{Ident, LitStr, Spanner, TokenStream},
    },
    std::collections::HashSet,
};

#[derive(moxy::FromMeta)]
struct Bounds {
    codec: Tokens,
    bound: String,
    requirement: String,
}

struct UnitModule {
    attributes: Attributes,
    visibility: Visibility,
    name: Ident,
    quantity: Path,
    codec: Path,
    positive: bool,
    requirement: LitStr,
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut modules = Vec::new();
    let mut names = HashSet::new();
    while !parser.is_empty() {
        let attributes = Attributes::parse(&parser)?;
        let visibility = Visibility::parse(&parser)?;
        <Token![mod]>::parse(&parser)?;
        let name = Ident::parse(&parser)?;
        <Token![:]>::parse(&parser)?;
        let quantity = Path::parse(&parser)?;
        let group = moxy::token::Group::parse(&parser)?;
        if !group.delim.is_bracket() || !names.insert(name.text().to_owned()) {
            return Err(ParseError::new(
                name.span(),
                "expected distinct bounded modules with bracketed options",
            ));
        }
        <Token![;]>::parse(&parser)?;
        let metadata: Meta = moxy::parse!({
            moxy::template! { bounds({{ group.tokens }}) }
        })?;
        let options = Bounds::from_meta(&metadata)?;
        let positive = match options.bound.as_str() {
            "positive" => true,
            "nonnegative" => false,
            _ => {
                return Err(ParseError::new(
                    metadata.span(),
                    "bound must be positive or nonnegative",
                ));
            }
        };
        if options.requirement.is_empty() {
            return Err(ParseError::new(
                metadata.span(),
                "bounded quantity needs a requirement",
            ));
        }
        let codec = plain_path(
            &options.codec.tokens,
            metadata.span(),
            "quantity codec needs a plain module path",
        )?;
        let quantity = plain_path(
            &moxy::template! { {{ quantity }} },
            quantity.span(),
            "quantity needs a concrete path",
        )?;
        modules.push(UnitModule {
            attributes,
            visibility,
            name,
            quantity,
            codec,
            positive,
            requirement: LitStr::new(&options.requirement, metadata.span()),
        });
    }
    if modules.is_empty() {
        return Err(ParseError::new(
            input.span(),
            "declare at least one bounded unit module",
        ));
    }
    Ok(moxy::template! {
        @for module in &modules {
            {{ module.attributes }}
            {{ module.visibility }} mod {{ module.name }} {
                use ::krabka_units::{fmt::Human as _, prelude::*};
                use ::serde::{Deserializer, Serializer, de::Error as _};

                /// Writes the quantity as its human string form.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for a string.
                pub fn serialize<S: Serializer>(value: &{{ module.quantity }}, serializer: S) -> Result<S::Ok, S::Error> {
                    {{ module.codec }}::serialize(value, serializer)
                }

                /// Reads the quantity from its human string form and bounds it.
                ///
                /// # Errors
                ///
                /// If the value is not a quantity of this dimension carrying an
                /// explicit unit, or its magnitude is one this field cannot run.
                pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<{{ module.quantity }}, D::Error> {
                    let value = {{ module.codec }}::deserialize(deserializer)?;
                    if @if module.positive { value > {{ module.quantity }}::ZERO } @else { value >= {{ module.quantity }}::ZERO } {
                        return Ok(value);
                    }
                    Err(D::Error::custom(format!("must be {}, got {}", {{ module.requirement }}, value.human())))
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn explicit_bounds_emit_both_read_policies_and_delegate_serialization() {
        let output = compact(&expand(&"pub mod size: ByteSize [codec = units::size, bound = \"positive\", requirement = \"a size\"]; mod time: Time [codec = units::time, bound = \"nonnegative\", requirement = \"an extent\"];".parse().unwrap()).unwrap());
        assert!(output.contains("value>ByteSize::ZERO") && output.contains("value>=Time::ZERO"));
        assert!(output.contains("units::size::serialize(value,serializer)"));
    }

    #[yare::parameterized(
        empty = { "" },
        unsupported_bound = {
            "mod x: Time [codec = human, bound = \"signed\", requirement = \"time\"];",
        },
        nonpath_codec = {
            "mod x: Time [codec = human + other, bound = \"positive\", requirement = \"time\"];",
        },
        empty_requirement = {
            "mod x: Time [codec = human, bound = \"positive\", requirement = \"\"];",
        },
        wrong_delimiter = { "mod x: Time (codec = human);" },
        duplicate_module = {
            "mod x: Time [codec = human, bound = \"positive\", requirement = \"time\"]; mod x: Time [codec = human, bound = \"positive\", requirement = \"time\"];",
        },
    )]
    fn rejects_duplicate_modules_nonpaths_and_unsupported_policies(declaration: &str) {
        assert2::assert!(
            expand(&declaration.parse().unwrap()).is_err(),
            "{declaration}"
        );
    }
}
