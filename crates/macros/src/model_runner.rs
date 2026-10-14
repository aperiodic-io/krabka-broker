//! Bounded model runners with explicit signatures and native checks.

use {
    crate::meta::{
        BracketedFunction, Tokens, bracketed_function, function_signature, is_plain_sync,
        parameter_ident,
    },
    moxy::{
        ast::{FromMeta, Meta, Parse, ParseError, Parser, Path, Signature},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
};

#[derive(moxy::FromMeta)]
struct Runner {
    bfs: Tokens,
    pin: Tokens,
    depth: Tokens,
    states: Tokens,
    order: String,
    checker: Tokens,
}

fn options(metadata: &TokenStream) -> Result<Runner, ParseError> {
    let metadata: Meta = moxy::parse!({
        moxy::template! { model_runner({{ metadata }}) }
    })?;
    let runner = Runner::from_meta(&metadata)?;
    if !matches!(
        runner.order.as_str(),
        "properties_first" | "properties_last"
    ) {
        return Err(ParseError::new(
            metadata.span(),
            "runner order must be properties_first or properties_last",
        ));
    }
    for tokens in [&runner.bfs.tokens, &runner.pin.tokens] {
        let parser = Parser::from_tokens(tokens);
        let _: Path = Parse::parse(&parser)?;
        if !parser.is_empty() {
            return Err(parser.error("runner helper must be a function path"));
        }
    }
    let parser = Parser::from_tokens(&runner.checker.tokens);
    let _: Ident = Parse::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("checker needs one local binding"));
    }
    if runner.depth.tokens.is_empty() || runner.states.tokens.is_empty() {
        return Err(ParseError::new(
            metadata.span(),
            "runner depth and state bounds must be nonempty",
        ));
    }
    Ok(runner)
}

struct RunnerStatements {
    before: TokenStream,
    after: TokenStream,
}

fn statements(runner: Runner, signature: &Signature) -> Result<RunnerStatements, ParseError> {
    let parameters = &signature.params.inner.inputs;
    if !is_plain_sync(signature) || parameters.len() != 3 || !signature.output.is_default() {
        return Err(ParseError::new(
            signature.ident.span(),
            "runner needs a concrete synchronous function with model, label and pinned-count parameters and no return type",
        ));
    }
    let model = parameter_ident(&parameters[0], "runner model parameter needs a name")?;
    let label = parameter_ident(&parameters[1], "runner label parameter needs a name")?;
    let pinned = parameter_ident(&parameters[2], "runner pinned count needs a name")?;
    let Runner {
        bfs,
        pin,
        depth,
        states,
        order,
        checker,
    } = runner;
    let bfs = bfs.tokens;
    let pin = pin.tokens;
    let depth = depth.tokens;
    let states = states.tokens;
    let checker = checker.tokens;
    let before = moxy::template! { let {{ checker }} = {{ bfs }}({{ model }}, {{ label }}, {{ depth }}, {{ states }}); };
    let after = moxy::template! {
        @if order == "properties_first" { ::stateright::Checker::assert_properties(&{{ checker }}); }
        {{ pin }}(::stateright::Checker::unique_state_count(&{{ checker }}), {{ pinned }}, {{ label }});
        @if order == "properties_last" { ::stateright::Checker::assert_properties(&{{ checker }}); }
    };
    Ok(RunnerStatements { before, after })
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let BracketedFunction {
        header,
        metadata,
        body,
    } = bracketed_function(input, "model_runner")?;
    let mut empty_function = header.clone();
    empty_function.extend([TokenTree::Group(Group::new(
        Delimiter::Brace,
        NativeTokenStream::new(),
    ))]);
    let signature = function_signature(&empty_function.into())?;
    let RunnerStatements { before, after } = statements(options(&metadata.into())?, &signature)?;
    let mut generated: NativeTokenStream = before.into();
    generated.extend(body.stream());
    generated.extend(NativeTokenStream::from(after));
    let mut output = header;
    let mut replacement = Group::new(Delimiter::Brace, generated);
    replacement.set_span(body.span());
    output.extend([TokenTree::Group(replacement)]);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{RunnerStatements, options, statements},
        crate::meta::function_signature,
    };

    #[yare::parameterized(
        first = { "properties_first" },
        last = { "properties_last" },
    )]
    fn runner_policy_keeps_property_order_and_bounds(order: &str) {
        let header = "fn run(model: Model, label: &str, pinned: usize) {}"
            .parse()
            .unwrap();
        let signature = function_signature(&header).unwrap();
        let input = format!("bfs = run_bfs, pin = pin_count, depth = DEPTH, states = STATES, checker = checker, order = \"{order}\"").parse().unwrap();
        let RunnerStatements { before, after } =
            statements(options(&input).unwrap(), &signature).unwrap();
        assert2::assert!(before.to_string().contains("DEPTH"));
        assert2::assert!(before.to_string().contains("STATES"));
        let after = after.to_string();
        assert2::assert!(
            (after.find("assert_properties").unwrap() < after.find("pin_count").unwrap())
                == (order == "properties_first")
        );
    }

    #[yare::parameterized(
        unknown_order = {
            "bfs = bfs, pin = pin, depth = 1, states = 2, checker = checker, order = \"unknown\"",
        },
        computed_bfs = {
            "bfs = bfs + extra, pin = pin, depth = 1, states = 2, checker = checker, order = \"properties_first\"",
        },
        parenthesized_checker = {
            "bfs = bfs, pin = pin, depth = 1, states = 2, checker = (checker), order = \"properties_first\"",
        },
    )]
    fn runner_metadata_rejects_unknown_order_and_non_names(input: &str) {
        assert2::assert!(options(&input.parse().unwrap()).is_err(), "{input}");
    }
}
