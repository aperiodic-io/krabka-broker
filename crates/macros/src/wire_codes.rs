//! Public wire constants and ordered private guard tables from one declaration.

use {
    moxy::{
        ast::{Attributes, Parse, ParseError, Parser, Token},
        token::{Group, Ident, LitInt, Spanner, ToTokens, TokenStream},
    },
    std::collections::HashSet,
};

struct Code {
    attrs: Attributes,
    name: Ident,
    value: TokenStream,
}

impl Code {
    fn parse(parser: &Parser) -> Result<Self, ParseError> {
        let attrs = Attributes::parse(parser)?;
        let name = Ident::parse(parser)?;
        <Token![=]>::parse(parser)?;
        let negative = Option::<Token![-]>::parse(parser)?;
        let value = LitInt::parse(parser)?;
        let bound = if negative.is_some() { 32768 } else { 32767 };
        if value.value() > bound {
            return Err(ParseError::new(value.span(), "wire code must fit i16"));
        }
        <Token![;]>::parse(parser)?;
        Ok(Self {
            attrs,
            name,
            value: moxy::template! { {{ negative }} {{ value }} },
        })
    }
}

impl ToTokens for Code {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Self { attrs, name, value } = self;
        output.extend(moxy::template! {
            {{ attrs }}
            pub const {{ name }}: i16 = {{ value }};
        });
    }
}

struct Section {
    table: Ident,
    codes: Vec<Code>,
}

impl ToTokens for Section {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Self { table, codes } = self;
        let names: Vec<_> = codes.iter().map(|code| &code.name).collect();
        output.extend(moxy::template! {
            @for code in codes { {{ code }} }

            /// Every constant in this block, with the name a guard-test failure reports.
            #[cfg(test)]
            const {{ table }}: &[NamedCode] = &[
                @for name in &names {
                    NamedCode { name: stringify!({{ name }}), code: {{ name }} },
                }
            ];
        });
    }
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut tables = HashSet::new();
    let mut names = HashSet::new();
    let mut sections = Vec::new();
    for expected in ["release", "trunk", "private"] {
        let label = Ident::parse(&parser)?;
        if label.text() != expected {
            return Err(ParseError::new(
                label.span(),
                format!("expected `{expected}` code section"),
            ));
        }
        let table = Ident::parse(&parser)?;
        if !tables.insert(table.text().to_owned()) {
            return Err(ParseError::new(
                table.span(),
                "guard table declared more than once",
            ));
        }
        let group = Group::parse(&parser)?;
        if !group.delim.is_brace() {
            return Err(ParseError::new(group.span(), "code section needs braces"));
        }
        let rows = Parser::from_tokens(&group.tokens);
        let mut codes = Vec::new();
        while !rows.is_empty() {
            let code = Code::parse(&rows)?;
            if !names.insert(code.name.text().to_owned()) {
                return Err(ParseError::new(
                    code.name.span(),
                    "wire constant declared more than once",
                ));
            }
            codes.push(code);
        }
        sections.push(Section { table, codes });
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after the private code section"));
    }
    Ok(moxy::template! { @for section in &sections { {{ section }} } })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn signed_values_docs_and_private_guard_order_survive_generation() {
        let input = "release RELEASE { #[doc = \"negative\"] NEGATIVE = -1; MAX = 32767; MIN = -32768; } trunk TRUNK { FUTURE = 135; } private PRIVATE { LOCAL = 1000; }".parse().unwrap();
        let generated = compact(&expand(&input).unwrap());
        assert!(generated.contains("#[doc=\"negative\"]pubconstNEGATIVE:i16=-1;"));
        assert!(generated.contains("pubconstMIN:i16=-32768;"));
        assert!(generated.matches("#[cfg(test)]const").count() == 3);
        let expected = "constRELEASE:&[NamedCode]=&[NamedCode{name:stringify!(NEGATIVE),code:NEGATIVE},NamedCode{name:stringify!(MAX),code:MAX},NamedCode{name:stringify!(MIN),code:MIN},]";
        assert!(generated.contains(expected));
        assert!(!generated.contains("pubconstRELEASE"));
    }

    #[test]
    fn rejects_missing_sections_duplicates_bad_values_and_trailing_tokens() {
        for input in [
            "release R {} trunk T {}",
            "trunk T {} release R {} private P {}",
            "release R {} trunk R {} private P {}",
            "release R { A = 0; } trunk T { A = 1; } private P {}",
            "release R { A = 32768; } trunk T {} private P {}",
            "release R { A = -32769; } trunk T {} private P {}",
            "release R { A = 1.5; } trunk T {} private P {}",
            "release R { A = 1 + 2; } trunk T {} private P {}",
            "release R { A = 1 } trunk T {} private P {}",
            "release R [] trunk T {} private P {}",
            "release R {} trunk T {} private P {} extra",
        ] {
            assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
