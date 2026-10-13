//! Trait row adapters for external group-describe and share-state response schemas.

use {
    crate::meta::nonempty_paths,
    moxy::{ast::ParseError, token::TokenStream},
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let rows = nonempty_paths(input, "expected at least one group response type")?;
    Ok(moxy::template! {
        @for ty in rows {
            impl DescribedGroupRow for {{ ty }} {
                fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> Self {
                    Self { group_id: group_id.into(), error_code, error_message, ..Self::default() }
                }

                fn group_id(&self) -> &str { &self.group_id }

                fn error_code(&self) -> i16 { self.error_code }

                fn set_authorized_operations(&mut self, bits: i32) {
                    self.authorized_operations = bits;
                }
            }
        }
    })
}

pub(crate) fn partition_results(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let rows = nonempty_paths(input, "expected share-state response types")?;
    Ok(moxy::template! {
        @for response in rows {
            impl PartitionResults for {{ response }} {
                fn partition_results(self) -> Vec<PartitionRow> {
                    self.results.into_iter().flat_map(|topic| topic.partitions)
                        .map(|p| PartitionRow { partition: p.partition, error_code: p.error_code, error_message: p.error_message })
                        .collect()
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{expand, partition_results},
        assert2::assert,
        moxy::ast::ItemImpl,
    };

    #[test]
    fn requires_a_nonempty_comma_separated_type_table() {
        for input in ["", "Response Other", "Response;", "42", "Response, #"] {
            assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
        }
        assert!(expand(&"consumer::Row, share::Row,".parse().unwrap()).is_ok());
    }

    #[test]
    fn produces_a_consuming_partition_projection_and_requires_type_paths() {
        let output = partition_results(&"owned::Reply".parse().unwrap()).unwrap();
        let implementation: ItemImpl = moxy::parse!({ output }).unwrap();
        let method = implementation.items.first().unwrap().as_fn().unwrap();
        assert!(method.sig.ident == "partition_results");
        assert!(method.sig.params.inner.inputs.len() == 1);
        for source in ["", "Reply;", "Reply, 1", "Reply extra"] {
            assert!(
                partition_results(&source.parse().unwrap()).is_err(),
                "{source}"
            );
        }
    }
}
