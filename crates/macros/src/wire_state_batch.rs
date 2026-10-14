//! The asymmetric read/write conversions of a locally owned share-state batch.

use {
    crate::meta::{Tokens, first_missing_named_field, plain_path},
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

struct Conversion<'a> {
    wire: &'a moxy::ast::Path,
    from_borrowed: bool,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    use {
        crate::meta::named_fields,
        moxy::{ast::Attributed, token::ToTokenStream},
    };
    let fields = named_fields(&item, "WireStateBatch")?;
    if !item.generics.params.is_empty()
        || fields.len() != 4
        || first_missing_named_field(fields, &[
            "first_offset",
            "last_offset",
            "delivery_state",
            "delivery_count",
        ])
        .is_some()
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
    for argument in [&args.read.tokens, &args.write.tokens] {
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
    let conversions = [
        Conversion {
            wire: &targets[0],
            from_borrowed: false,
        },
        Conversion {
            wire: &targets[1],
            from_borrowed: true,
        },
    ];
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
        @for conversion in conversions {
            impl ::core::convert::From<@if conversion.from_borrowed { & } {{ conversion.wire }}> for {{ ident }} {
                fn from(batch: @if conversion.from_borrowed { & } {{ conversion.wire }}) -> Self {
                    Self {
                        first_offset: {{ offset }}(batch.first_offset),
                        last_offset: {{ offset }}(batch.last_offset),
                        delivery_state: batch.delivery_state,
                        delivery_count: batch.delivery_count,
                    }
                }
            }
            impl ::core::convert::From<@if !conversion.from_borrowed { & } {{ ident }}> for {{ conversion.wire }} {
                fn from(batch: @if !conversion.from_borrowed { & } {{ ident }}) -> Self {
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
        crate::meta::{RejectedStruct, assert_rejected_structs, compact},
        assert2::assert,
        moxy::ast::ItemStruct,
    };

    fn generated_batch() -> String {
        let item: ItemStruct = moxy::parse!("#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }").unwrap();
        compact(&expand(item).unwrap())
    }

    #[test]
    fn preserves_both_default_tail_conversions() {
        let output = generated_batch();
        assert!(output.matches("..Self::default()").count() == 2);
    }

    #[yare::parameterized(
        read_into_owned = { "From<read::Batch>forBatch" },
        owned_into_read = { "From<&Batch>forread::Batch" },
        write_into_owned = { "From<&write::Batch>forBatch" },
        owned_into_write = { "From<Batch>forwrite::Batch" },
    )]
    fn preserves_each_directional_receiver_contract(implementation: &str) {
        assert2::assert!(
            generated_batch().contains(implementation),
            "{implementation}"
        );
    }

    #[test]
    fn rejects_wrong_shape_metadata_and_target_expressions() {
        assert_rejected_structs(
            &[
                RejectedStruct {
                    label: "missing metadata",
                    declaration: "struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                },
                RejectedStruct {
                    label: "same targets",
                    declaration: "#[wire_state_batch(read = wire::Batch, write = wire::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                },
                RejectedStruct {
                    label: "target expression",
                    declaration: "#[wire_state_batch(read = read::Batch + other, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                },
                RejectedStruct {
                    label: "generic target",
                    declaration: "#[wire_state_batch(read = read::Batch<T>, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8, delivery_count: i16 }",
                },
                RejectedStruct {
                    label: "missing field",
                    declaration: "#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch { first_offset: Offset, last_offset: Offset, delivery_state: i8 }",
                },
                RejectedStruct {
                    label: "tuple source",
                    declaration: "#[wire_state_batch(read = read::Batch, write = write::Batch)] struct Batch(Offset);",
                },
            ],
            expand,
        );
    }
}
