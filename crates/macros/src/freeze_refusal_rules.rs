//! One exhaustive mutation inventory under visible runtime and logical headers.

use {
    crate::meta::{expect_name, function_signature, grouped_value, parameter_ident, plain_path},
    moxy::{
        ast::{FnParam, List, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, ToTokenStream, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
    std::collections::HashSet,
};

struct VariantRule {
    variant: Ident,
    refused: bool,
}

struct RuleList {
    name: &'static str,
    refused: bool,
}

struct Rules {
    ty: Path,
    variants: Vec<VariantRule>,
    bindings: Vec<Ident>,
}

fn declaration(input: &TokenStream) -> Result<Rules, ParseError> {
    let parser = Parser::from_tokens(input);
    let ty = Path::parse(&parser)?;
    plain_path(
        &ty.to_token_stream(),
        ty.span(),
        "expected one mutation enum path",
    )?;
    let _: Token![;] = Parse::parse(&parser)?;
    let mut variants = Vec::new();
    let mut seen = HashSet::new();
    for RuleList { name, refused } in [
        RuleList {
            name: "refuse",
            refused: true,
        },
        RuleList {
            name: "allow",
            refused: false,
        },
    ] {
        expect_name(&parser, name, "expected refuse and allow variant lists")?;
        let group = grouped_value(&parser, "expected bracketed variant list")?;
        if !group.delim.is_bracket() || group.tokens.is_empty() {
            return Err(ParseError::new(
                group.span(),
                "variant list must be nonempty brackets",
            ));
        }
        let names = List::<Ident, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
        for variant in names {
            if !seen.insert(variant.text().to_owned()) {
                return Err(ParseError::new(
                    variant.span(),
                    "mutation variant appears more than once",
                ));
            }
            variants.push(VariantRule { variant, refused });
        }
        let _: Token![;] = Parse::parse(&parser)?;
    }
    let mut bindings = Vec::new();
    for role in ["runtime", "model"] {
        expect_name(
            &parser,
            role,
            "expected runtime then model function headers",
        )?;
        let header = grouped_value(&parser, "expected braced function header")?;
        if !header.delim.is_brace() {
            return Err(ParseError::new(
                header.span(),
                "function header needs braces",
            ));
        }
        let signature = function_signature(&moxy::template! { {{ header.tokens }} {} })?;
        let inputs = &signature.params.inner.inputs;
        if signature.asyncness.is_some()
            || signature.safety.is_some()
            || signature.abi.is_some()
            || !signature.generics.params.is_empty()
            || inputs.len() != 1
            || signature
                .output
                .as_type()
                .is_none_or(|ty| ty.to_token_stream().to_string() != "bool")
            || (role == "runtime" && signature.constness.is_none())
        {
            return Err(ParseError::new(
                header.span(),
                "expected a concrete synchronous fn(kind: Enum) -> bool; runtime must be const",
            ));
        }
        let parameter = &inputs[0];
        let FnParam::Typed(parameter_type) = parameter else {
            return Err(ParseError::new(
                parameter.span(),
                "expected a named mutation parameter",
            ));
        };
        if parameter_type.ty.to_token_stream().to_string() != ty.to_token_stream().to_string() {
            return Err(ParseError::new(
                parameter.span(),
                "mutation parameter type must match the declared enum",
            ));
        }
        bindings.push(parameter_ident(parameter, "mutation parameter needs a name")?.clone());
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after model header"));
    }
    Ok(Rules {
        ty,
        variants,
        bindings,
    })
}

fn refusal_match(rules: &Rules, binding: &Ident) -> TokenStream {
    let ty = &rules.ty;
    moxy::template! {
        match {{ binding }} {
            @for rule in &rules.variants {
                {{ ty }}::{{ rule.variant }} => @if rule.refused { true } @else { false },
            }
        }
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let rules = declaration(&input.clone().into())?;
    let headers = input.into_iter().filter_map(|token| match token {
        TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => Some(group),
        _ => None,
    });
    let mut output = NativeTokenStream::new();
    for (header, binding) in headers.zip(&rules.bindings) {
        output.extend(header.stream());
        let body = refusal_match(&rules, binding);
        output.extend([TokenTree::Group(Group::new(Delimiter::Brace, body.into()))]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, refusal_match},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn validates_closed_inventory_and_typed_headers() {
        let valid = "Kind; refuse [Write,Delete]; allow [Read]; runtime { pub const fn rejects(kind: Kind) -> bool } model { fn logical(value: Kind) -> bool }";
        let rules = declaration(&valid.parse().unwrap()).unwrap();
        assert!(rules.variants.len() == 3);
        assert!(rules.variants[0].variant == "Write" && rules.variants[0].refused);
        assert!(rules.variants[1].variant == "Delete" && rules.variants[1].refused);
        assert!(rules.variants[2].variant == "Read" && !rules.variants[2].refused);
        assert!(rules.bindings[1] == "value");
        assert!(
            compact(&refusal_match(&rules, &rules.bindings[0]))
                == "matchkind{Kind::Write=>true,Kind::Delete=>true,Kind::Read=>false,}"
        );
        assert!(compact(&refusal_match(&rules, &rules.bindings[1])).starts_with("matchvalue{"));
    }

    #[yare::parameterized(
        duplicate_variant = { "duplicate_variant" },
        empty_allow_list = { "empty_allow_list" },
        async_runtime = { "async_runtime" },
        wrong_model_type = { "wrong_model_type" },
        wrong_return_type = { "wrong_return_type" },
    )]
    fn refuses_invalid_rule_headers(case: &str) {
        let valid = "Kind; refuse [Write,Delete]; allow [Read]; runtime { pub const fn rejects(kind: Kind) -> bool } model { fn logical(value: Kind) -> bool }";
        let invalid = match case {
            "duplicate_variant" => valid.replace("allow [Read]", "allow [Write]"),
            "empty_allow_list" => valid.replace("[Read]", "[]"),
            "async_runtime" => valid.replace("const fn", "async fn"),
            "wrong_model_type" => valid.replace("value: Kind", "value: Other"),
            "wrong_return_type" => valid.replace("-> bool", "-> i32"),
            _ => unreachable!(),
        };
        assert2::assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
    }
}
