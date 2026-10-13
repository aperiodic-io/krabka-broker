//! Flat per-case share API refusals, with native input and handler expressions.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups},
        sweep_loop::splice,
    },
    moxy::{
        ast::{List, Parse, ParseError, Parser, Token},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

struct Matrix {
    cases: Vec<Ident>,
    bindings: Vec<Ident>,
}

fn declaration(input: &TokenStream) -> Result<Matrix, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut declarations = Vec::new();
    for expected in ["case", "bind", "invoke"] {
        expect_name(&parser, expected, &format!("expected {expected} clause"))?;
        if expected != "invoke" {
            let group = grouped_value(&parser, "expected matrix binding tuple")?;
            if !group.delim.is_paren() {
                return Err(ParseError::new(
                    group.span(),
                    "matrix bindings need parentheses",
                ));
            }
            let names = List::<Ident, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
            if (expected == "case" && names.len() < 4) || (expected == "bind" && names.len() != 5) {
                return Err(ParseError::new(
                    group.span(),
                    "case needs label/authorizer/support/input.../expected; bind needs handle/directory/broker/context/response",
                ));
            }
            declarations.push(names.into_iter().collect::<Vec<_>>());
        }
        if expected != "bind" {
            let group = grouped_value(&parser, "expected native matrix expression")?;
            if !group.delim.is_bracket() || group.tokens.is_empty() {
                return Err(ParseError::new(
                    group.span(),
                    "expected nonempty bracketed matrix expression",
                ));
            }
        }
        let _: Token![;] = Parse::parse(&parser)?;
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected refusal matrix clause"));
    }
    let mut declarations = declarations.into_iter();
    Ok(Matrix {
        cases: declarations.next().unwrap(),
        bindings: declarations.next().unwrap(),
    })
}

fn template(matrix: &Matrix) -> TokenStream {
    let cases = &matrix.cases;
    let [case, authorizer, enabled, ..] = cases.as_slice() else {
        unreachable!("validated case count")
    };
    let expected = cases.last().unwrap();
    let [handle, directory, broker, context, response] = matrix.bindings.as_slice() else {
        unreachable!("validated fixture binding count")
    };
    moxy::template! {
        for (@for name in cases { {{ name }}, }) in __matrix_cases {
            let ({{ handle }}, {{ directory }}) = crate::test_support::start_share_broker().authorizer({{ authorizer }}).support({{ enabled }}).call().await;
            let {{ broker }} = {{ handle }}.broker_arc_for_test();
            let __share_principal = crate::test_support::principal("alice");
            let __share_peer = crate::test_support::peer();
            let {{ context }} = test_context(&__share_principal, &__share_peer);
            let {{ response }} = __matrix_invoke.await.expect("handle");
            ::assert2::assert!({{ response }} == {{ expected }}, "case: {}", {{ case }});
            {{ handle }}.shutdown().await;
        }
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let matrix = declaration(&input.clone().into())?;
    let mut groups = native_groups(input);
    let _case_bindings = groups.next();
    let cases = groups.next().expect("validated case expression");
    let _fixture_bindings = groups.next();
    let invoke = groups.next().expect("validated handler expression");
    Ok(splice(
        template(&matrix).into(),
        &[
            (
                "__matrix_cases".to_owned(),
                Group::new(Delimiter::None, cases.stream()),
            ),
            (
                "__matrix_invoke".to_owned(),
                Group::new(Delimiter::Parenthesis, invoke.stream()),
            ),
        ],
        &["__share_principal", "__share_peer"],
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, template},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn keeps_case_arity_and_shared_execution_order_explicit() {
        let matrix = declaration(&"case(label, authorizer, enabled, topic, partitions, expected) [cases::<1, 2>()]; bind(handle,dir,broker,ctx,response); invoke [dispatch(&broker, request(topic,&partitions), version,&ctx)];".parse().unwrap()).unwrap();
        let output = compact(&template(&matrix));
        assert!(
            output.contains(
                "for(label,authorizer,enabled,topic,partitions,expected,)in__matrix_cases"
            )
        );
        assert!(output.contains("handle.shutdown().await;"));
        for invalid in [
            "",
            "case(label,auth,expected) [cases]; bind(h,d,b,c,r); invoke [handle()];",
            "case(label,auth,enabled,expected) []; bind(h,d,b,c,r); invoke [handle()];",
            "case(label,auth,enabled,expected) [cases]; bind(h,d,b,c); invoke [handle()];",
            "case(label,auth,enabled,expected) [cases]; bind(h,d,b,c,r); invoke [];",
            "case(label,auth,enabled,expected) [cases]; bind(h,d,b,c,r); invoke [handle()]; extra",
        ] {
            assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
