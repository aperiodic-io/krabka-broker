//! Partition filtering from the visible endpoints of a local simulation message.

use {
    crate::meta::{field_ident, impl_block, named_fields},
    moxy::{
        ast::{ItemStruct, ParseError},
        token::{ToTokenStream, TokenStream},
    },
};

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = named_fields(&item, "PartitionMessage")?;
    let endpoint = |name| {
        fields.iter().find(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == name)
        })
    };
    let source = endpoint("src")
        .ok_or_else(|| ParseError::new(item.ident.span(), "PartitionMessage needs a src field"))?;
    let destination = endpoint("dst")
        .ok_or_else(|| ParseError::new(item.ident.span(), "PartitionMessage needs a dst field"))?;
    if !item.generics.params.is_empty()
        || source.ty.to_token_stream().to_string() != destination.ty.to_token_stream().to_string()
    {
        return Err(ParseError::new(
            item.ident.span(),
            "PartitionMessage needs concrete src and dst fields of the same type",
        ));
    }
    let source_name = field_ident(source, "PartitionMessage")?;
    let destination_name = field_ident(destination, "PartitionMessage")?;
    let node = &source.ty;
    let trait_prefix = moxy::template! { PartitionMessage<{{ node }}> for };
    let members = moxy::template! {
        fn touches(&self, node: {{ node }}) -> bool { self.{{ source_name }} == node || self.{{ destination_name }} == node }
    };
    Ok(impl_block(item, &trait_prefix, &members))
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::{RejectedStruct, assert_rejected_structs, compact},
        assert2::assert,
    };

    #[test]
    fn endpoints_must_be_present_and_have_the_same_concrete_type() {
        assert_rejected_structs(
            &[
                RejectedStruct {
                    label: "missing source",
                    declaration: "struct Message { dst: Node }",
                },
                RejectedStruct {
                    label: "missing destination",
                    declaration: "struct Message { src: Node }",
                },
                RejectedStruct {
                    label: "different coordinate types",
                    declaration: "struct Message { src: Node, dst: Other }",
                },
                RejectedStruct {
                    label: "generic message",
                    declaration: "struct Message<T> { src: T, dst: T }",
                },
            ],
            expand,
        );
    }

    #[test]
    fn endpoint_check_keeps_source_first_and_short_circuits() {
        let item = moxy::parse!("struct Message { src: Node, dst: Node, event: Event }").unwrap();
        assert!(compact(&expand(item).unwrap()).contains("self.src==node||self.dst==node"));
    }
}
