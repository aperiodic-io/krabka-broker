//! Associated fixture row types for external share-state response schemas.

use moxy::{
    ast::{Parse, ParseError, Parser, Path, Token},
    token::TokenStream,
};

struct Schema {
    response: Path,
    topic: Path,
    partition: Path,
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut rows = Vec::new();
    while !parser.is_empty() {
        let response = Path::parse(&parser)?;
        <Token![=>]>::parse(&parser)?;
        let result = Path::parse(&parser)?;
        <Token![,]>::parse(&parser)?;
        let partition = Path::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        rows.push(Schema {
            response,
            topic: result,
            partition,
        });
    }
    if rows.is_empty() {
        return Err(parser.error("expected a share-state fixture schema table"));
    }
    Ok(moxy::template! {
        @for schema in rows {
            impl StateReply for {{ schema.response }} {
                type Partition = {{ schema.partition }};
                type Topic = {{ schema.topic }};

                fn topic(topic_id: ProtoUuid, partitions: Vec<Self::Partition>) -> Self::Topic {
                    {{ schema.topic }} { topic_id, partitions, unknown_tagged_fields: UnknownTaggedFields(vec![]) }
                }

                fn from_results(results: Vec<Self::Topic>) -> Self {
                    Self { results, unknown_tagged_fields: UnknownTaggedFields(vec![]) }
                }

                fn error_row(partition: i32, error_code: i16, message: Option<&str>) -> Self::Partition {
                    {{ schema.partition }} { partition, error_code, error_message: message.map(str::to_owned), ..Default::default() }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert, moxy::ast::ItemImpl};

    #[test]
    fn requires_complete_schema_triples_and_emits_owned_error_messages() {
        let output = expand(&"Reply => Topic, Partition;".parse().unwrap()).unwrap();
        assert!(compact(&output).contains("error_message:message.map(str::to_owned)"));
        let implementation: ItemImpl = moxy::parse!({ output }).unwrap();
        assert!(implementation.items.len() == 5);
        let row = implementation
            .items
            .iter()
            .find_map(|item| {
                item.as_fn()
                    .filter(|method| method.sig.ident == "error_row")
            })
            .unwrap();
        assert!(row.sig.params.inner.inputs.len() == 3);
        assert!(!row.sig.output.is_default());
        for input in [
            "",
            "Reply => Topic;",
            "Reply => Topic, Partition",
            "Reply => Topic, Partition; extra",
        ] {
            assert!(expand(&input.parse().unwrap()).is_err());
        }
    }
}
