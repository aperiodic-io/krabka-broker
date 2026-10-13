//! The asymmetric read/write conversions of a locally owned share-state batch.

use {
    crate::meta::{Tokens, plain_path},
    moxy::{
        ast::{ItemStruct, ParseError},
        token::TokenStream,
    },
};

#[derive(moxy::FromMeta)]
struct WireArgs {
    read: Tokens,
    write: Tokens,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    use {
        crate::meta::named_fields,
        moxy::{ast::Attributed, token::ToTokenStream},
    };
    let fields = named_fields(&item, "WireStateBatch")?;
    if !item.generics.params.is_empty()
        || fields.len() != 4
        || [
            "first_offset",
            "last_offset",
            "delivery_state",
            "delivery_count",
        ]
        .iter()
        .any(|name| {
            !fields.iter().any(|field| {
                field
                    .ident
                    .as_ref()
                    .is_some_and(|ident| ident.text() == *name)
            })
        })
    {
        return Err(ParseError::new(
            item.ident.span(),
            "`WireStateBatch` needs a non-generic struct with first_offset, last_offset, delivery_state and delivery_count",
        ));
    }
    let args = item
        .parse_meta::<WireArgs>("wire_state_batch")?
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`WireStateBatch` needs `#[wire_state_batch(read = Type, write = Type)]`",
            )
        })?;
    let mut targets = Vec::new();
    for argument in [&args.read.0, &args.write.0] {
        let path = plain_path(
            argument,
            argument.span(),
            "batch targets must be type paths without arguments",
        )?;
        targets.push(path);
    }
    if targets[0].to_token_stream().to_string() == targets[1].to_token_stream().to_string() {
        return Err(ParseError::new(
            item.ident.span(),
            "read and write batch types must differ",
        ));
    }
    let conversions = [(&targets[0], false), (&targets[1], true)];
    let offset = fields
        .iter()
        .find(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == "first_offset")
        })
        .expect("validated first_offset")
        .ty
        .clone();
    let ident = item.ident;
    Ok(moxy::template! {
        @for (wire, from_borrowed) in conversions {
            impl ::core::convert::From<@if from_borrowed { & } {{ wire }}> for {{ ident }} {
                fn from(batch: @if from_borrowed { & } {{ wire }}) -> Self {
                    Self {
                        first_offset: {{ offset }}(batch.first_offset),
                        last_offset: {{ offset }}(batch.last_offset),
                        delivery_state: batch.delivery_state,
                        delivery_count: batch.delivery_count,
                    }
                }
            }
            impl ::core::convert::From<@if !from_borrowed { & } {{ ident }}> for {{ wire }} {
                fn from(batch: @if !from_borrowed { & } {{ ident }}) -> Self {
                    Self {
                        first_offset: batch.first_offset.0,
                        last_offset: batch.last_offset.0,
                        delivery_state: batch.delivery_state,
                        delivery_count: batch.delivery_count,
                        ..Self::default()
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::{assert_rejected_structs, compact},
        assert2::assert,
        moxy::ast::ItemStruct,
    };

    #[test]
    fn preserves_the_four_directional_receiver_contracts() {
        let item: ItemStruct = moxy::parse!("#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }").unwrap();
        let output = compact(&expand(item).unwrap());
        for implementation in [
            "From<read::Batch>forBatch",
            "From<&Batch>forread::Batch",
            "From<&write::Batch>forBatch",
            "From<Batch>forwrite::Batch",
        ] {
            assert!(output.contains(implementation), "{implementation}");
        }
        assert!(output.matches("..Self::default()").count() == 2);
    }

    #[test]
    fn rejects_wrong_shape_metadata_and_target_expressions() {
        assert_rejected_structs(
            &[
                (
                    "missing metadata",
                    "struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                ),
                (
                    "same targets",
                    "#[wire_state_batch(read = wire::Batch, write = wire::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                ),
                (
                    "target expression",
                    "#[wire_state_batch(read = read::Batch + other, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                ),
                (
                    "generic target",
                    "#[wire_state_batch(read = read::Batch<T>, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                ),
                (
                    "missing field",
                    "#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8 }",
                ),
                (
                    "tuple source",
                    "#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch(Offset);",
                ),
            ],
            expand,
        );
    }
}
