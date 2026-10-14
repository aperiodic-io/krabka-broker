//! Named partition operations delegate to the ordinary approval gate.

use moxy::{
    ast::{Attributes, Parse, ParseError, Parser, Token, Visibility},
    token::{Ident, TokenStream},
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let attributes = Attributes::parse(&parser)?;
    let visibility = Visibility::parse(&parser)?;
    let _: Token![fn] = Parse::parse(&parser)?;
    let name = Ident::parse(&parser)?;
    let _: Token![=] = Parse::parse(&parser)?;
    let action = Ident::parse(&parser)?;
    if !matches!(
        action.text(),
        "CancelReassignment" | "DeleteRecords" | "UncleanElectLeaders"
    ) {
        return Err(ParseError::new(
            action.span(),
            "expected a gated partition action",
        ));
    }
    let _: Token![;] = Parse::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("expected one named partition gate"));
    }
    Ok(moxy::template! {
        {{ attributes }}
        {{ visibility }} fn {{ name }}(
            image: &krabka_metadata::MetadataImage,
            config: &crate::config::BreakGlassConfig,
            topic: &str,
            partition: i32,
        ) -> Result<Option<krabka_metadata::MetadataRecord>, crate::break_glass::gate::BreakGlassDenial> {
            crate::handlers::partition_transition::authorize_partition(
                image, config, krabka_metadata::BreakGlassAction::{{ action }}, topic, partition,
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact};

    #[yare::parameterized(
        cancel_reassignment = { "CancelReassignment" },
        delete_records = { "DeleteRecords" },
        unclean_elect_leaders = { "UncleanElectLeaders" },
    )]
    fn all_actions_keep_visibility_docs_and_complete_gate_arguments(action: &str) {
        let input = format!("#[doc = \"ordered approval\"] pub(super) fn approve = {action};");
        let output = compact(&expand(&input.parse().unwrap()).unwrap());
        assert2::assert!(output.contains("#[doc=\"orderedapproval\"]pub(super)fnapprove("));
        assert2::assert!(output.contains(&format!("authorize_partition(image,config,krabka_metadata::BreakGlassAction::{action},topic,partition,")));
        assert2::assert!(expand(&"fn approve = DeleteRecords;".parse().unwrap()).is_ok());
    }

    #[yare::parameterized(
        empty = { "" },
        unknown_action = { "fn approve = Unknown;" },
        parameters = { "fn approve() = DeleteRecords;" },
        missing_semicolon = { "fn approve = DeleteRecords" },
        multiple_items = { "fn approve = DeleteRecords; fn other = DeleteRecords;" },
    )]
    fn rejects_unsupported_or_incomplete_declarations(invalid: &str) {
        assert2::assert!(expand(&invalid.parse().unwrap()).is_err(), "{invalid}");
    }
}
