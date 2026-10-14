//! Seed projections with typed bindings and opaque native assignment operations.

use {
    crate::meta::{edit_native_body, finish_braced_body},
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token},
        token::{Ident, Span, TokenStream},
    },
    proc_macro::{Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

struct Snapshot {
    state: Ident,
    members: Ident,
    current: Ident,
    targets: Ident,
    metadata: Path,
    assignment: Path,
    id: Ident,
    member: Ident,
}

impl Snapshot {
    fn parse(input: &TokenStream) -> Result<Self, ParseError> {
        let parser = Parser::from_tokens(input);
        let state = Ident::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        let members = Ident::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let current = Ident::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let targets = Ident::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        let metadata = Path::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let assignment = Path::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        <Token![|]>::parse(&parser)?;
        let id = Ident::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let member = Ident::parse(&parser)?;
        <Token![|]>::parse(&parser)?;
        finish_braced_body(&parser, "expected one final braced seed operation")?;
        Ok(Self {
            state,
            members,
            current,
            targets,
            metadata,
            assignment,
            id,
            member,
        })
    }

    fn statements(&self) -> TokenStream {
        let Self {
            state,
            members,
            current,
            targets,
            metadata,
            assignment,
            id,
            member,
        } = self;
        moxy::template! {
            let mut {{ members }} = ::std::collections::HashMap::new();
            let mut {{ targets }} = ::std::collections::HashMap::new();
            let mut {{ current }} = ::std::collections::HashMap::new();
            for ({{ id }}, {{ member }}) in &{{ state }}.members {
                {{ members }}.insert({{ id }}.clone(), {{ metadata }}({{ member }}));
                {{ current }}.insert({{ id }}.clone(), {{ assignment }}({{ member }}));
            }
        }
    }
}

struct Hydration {
    state: Ident,
    seed: Ident,
    member: Ident,
    current: Ident,
}

impl Hydration {
    fn parse(input: &TokenStream) -> Result<Self, ParseError> {
        let parser = Parser::from_tokens(input);
        let state = Ident::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let seed = Ident::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        let member = Ident::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let current = Ident::parse(&parser)?;
        finish_braced_body(&parser, "expected one final braced seed operation")?;
        Ok(Self {
            state,
            seed,
            member,
            current,
        })
    }

    fn statements(&self) -> TokenStream {
        let Self {
            state,
            seed,
            member,
            current,
        } = self;
        // The old declarative macro's internal id could not capture caller bindings.
        let id = Ident::new("mid").with_span(Span::mixed_site());
        moxy::template! {
            for ({{ id }}, {{ current }}) in {{ seed }}.current_per_member {
                if let Some({{ member }}) = {{ state }}.members.get_mut(&{{ id }}) {
                    {{ member }}.member_epoch = {{ current }}.member_epoch;
                    {{ member }}.previous_member_epoch = {{ current }}.previous_member_epoch;
                }
            }
        }
    }
}

fn take_operation(input: NativeTokenStream) -> Group {
    let Some(NativeTokenTree::Group(operation)) = input.into_iter().last() else {
        unreachable!("validated final operation")
    };
    operation
}

pub(crate) fn snapshot(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let header = Snapshot::parse(&input.clone().into())?;
    let native = take_operation(input);
    Ok(edit_native_body(header.statements().into(), |mut body| {
        // Retain the target operation's own block scope and all its native tokens.
        body.extend([NativeTokenTree::Group(native)]);
        body
    }))
}

pub(crate) fn hydrate(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let header = Hydration::parse(&input.clone().into())?;
    let native = take_operation(input);
    Ok(edit_native_body(header.statements().into(), |body| {
        edit_native_body(body, |mut restored| {
            // Hydration's caller statements share the successful lookup's scope.
            restored.extend(native.stream());
            restored
        })
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::{Hydration, Snapshot},
        assert2::assert,
        moxy::ast::ItemFn,
    };

    #[test]
    fn typed_headers_emit_complete_statement_lists_without_parsing_operation_bodies() {
        let snapshot = Snapshot::parse(&"state; metadata, current, target; codecs::meta, assignment; |id, member| { nested!(|&(_, value)| value); }".parse().unwrap()).unwrap();
        let emitted = snapshot.statements();
        let function: ItemFn = moxy::parse!({
            moxy::template! { fn projection() { {{ emitted }} } }
        })
        .unwrap();
        assert!(function.body.unwrap().stmts.len() == 4);
        let hydration = Hydration::parse(
            &"state, seed; member, current { nested!(|&(left, right)| left + right); }"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let emitted = hydration.statements();
        let function: ItemFn = moxy::parse!({
            moxy::template! { fn restore() { {{ emitted }} } }
        })
        .unwrap();
        assert!(function.body.unwrap().stmts.len() == 1);
    }

    #[yare::parameterized(
        too_few_bindings = { "state; a, b; metadata, assignment; |id, member| {}" },
        missing_assignment = { "state; a, b, c; metadata; |id, member| {}" },
        missing_member_binding = { "state; a, b, c; metadata, assignment; |id| {}" },
        non_block_operation = { "state; a, b, c; metadata, assignment; |id, member| ()" },
        trailing_operation = { "state; a, b, c; metadata, assignment; |id, member| {} extra" },
    )]
    fn rejects_incomplete_snapshot_bindings(source: &str) {
        assert2::assert!(
            Snapshot::parse(&source.parse().unwrap()).is_err(),
            "{source}"
        );
    }

    #[yare::parameterized(
        missing_block = { "state, seed; member, current" },
        missing_binding_comma = { "state, seed; member current {}" },
        non_block_body = { "state, seed; member, current []" },
        extra_block = { "state, seed; member, current {} {}" },
    )]
    fn rejects_incomplete_hydration_bindings(source: &str) {
        assert2::assert!(
            Hydration::parse(&source.parse().unwrap()).is_err(),
            "{source}"
        );
    }
}
