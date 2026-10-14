//! Share-state scalars in declaration order, followed by batches and message tags.

use {
    crate::meta::{field_ident, impl_block, named_fields},
    moxy::{
        ast::{Attributed, GenericArgument, ItemStruct, ParseError, PathArguments, Type},
        token::{Ident, LitStr, Spanner, ToTokenStream, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct ValueDocs {
    encode_doc: String,
    decode_doc: String,
}

#[derive(Clone, Copy)]
enum ScalarKind {
    I32,
    I64,
    Offset,
}

struct Scalar {
    ident: Ident,
    ty: Type,
    kind: ScalarKind,
}

#[derive(moxy::ToTokens)]
#[moxy(template {
    #[doc = {{ self.encode_doc }}]
    #[must_use]
    pub fn encode(&self) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::new();
        bytes::BufMut::put_i16(&mut buf, 0);
        @for field in &self.fields {
            @match (field.kind) {
                ScalarKind::I32 => { bytes::BufMut::put_i32(&mut buf, self.{{ field.ident }}); }
                ScalarKind::I64 => { bytes::BufMut::put_i64(&mut buf, self.{{ field.ident }}); }
                ScalarKind::Offset => { bytes::BufMut::put_i64(&mut buf, self.{{ field.ident }}.0); }
            }
        }
        put_batches(&mut buf, &self.state_batches);
        put_value_tags(&mut buf, self.delivery_complete_count);
        buf.freeze()
    }

    /// # Errors
    ///
    #[doc = {{ self.decode_doc }}]
    pub fn decode(mut buf: &[u8]) -> Result<Self, crate::error::BrokerError> {
        check_version(&mut buf)?;
        @for field in &self.fields {
            let {{ field.ident }} = @match (field.kind) {
                ScalarKind::I32 => { get_i32(&mut buf)? }
                ScalarKind::I64 => { get_i64(&mut buf)? }
                ScalarKind::Offset => { {{ field.ty }}(get_i64(&mut buf)?) }
            };
        }
        let state_batches = get_batches(&mut buf)?;
        let delivery_complete_count = get_value_tags(&mut buf)?;
        Ok(Self {
            @for field in &self.fields { {{ field.ident }}, }
            delivery_complete_count,
            state_batches,
        })
    }
})]
struct ValueMethods {
    fields: Vec<Scalar>,
    encode_doc: LitStr,
    decode_doc: LitStr,
}

fn is_plain_type(ty: &Type, name: &str) -> bool {
    ty.as_path().is_some_and(|path| {
        path.qself.is_none()
            && path.path.is_ident(name)
            && path
                .path
                .last()
                .is_some_and(|segment| matches!(segment.args, PathArguments::None))
    })
}

fn is_batches(ty: &Type) -> bool {
    ty.as_path().is_some_and(|path| {
        path.qself.is_none()
            && path.path.is_ident("Vec")
            && path.path.last().is_some_and(|segment| {
                let PathArguments::AngleBracketed(arguments) = &segment.args else {
                    return false;
                };
                arguments.args.len() == 1
                    && matches!(arguments.args.first(),
                    Some(GenericArgument::Type(inner)) if is_plain_type(inner, "StateBatch"))
            })
    })
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let docs = item.parse_meta::<ValueDocs>("share_state")?.ok_or_else(|| {
        ParseError::new(item.ident.span(), "`ShareStateValue` needs `#[share_state(encode_doc = \"...\", decode_doc = \"...\")]`")
    })?;
    let mut fields = Vec::new();
    let mut batches = false;
    let mut tags = false;
    for field in named_fields(&item, "ShareStateValue")? {
        let ident = field_ident(field, "ShareStateValue")?.clone();
        match ident.text() {
            "state_batches" if is_batches(&field.ty) => batches = true,
            "delivery_complete_count" if is_plain_type(&field.ty, "i32") => tags = true,
            "state_batches" | "delivery_complete_count" => {
                return Err(ParseError::new(
                    field.ty.span(),
                    "expected `state_batches: Vec<StateBatch>` and `delivery_complete_count: i32`",
                ));
            }
            _ => {
                let kind = if is_plain_type(&field.ty, "i32") {
                    ScalarKind::I32
                } else if is_plain_type(&field.ty, "i64") {
                    ScalarKind::I64
                } else if is_plain_type(&field.ty, "Offset") {
                    ScalarKind::Offset
                } else {
                    return Err(ParseError::new(
                        field.ty.span(),
                        "expected an i32, i64 or Offset scalar field",
                    ));
                };
                fields.push(Scalar {
                    ident,
                    ty: field.ty.clone(),
                    kind,
                });
            }
        }
    }
    if !batches || !tags {
        return Err(ParseError::new(
            item.ident.span(),
            "`ShareStateValue` needs `state_batches` and `delivery_complete_count` fields",
        ));
    }
    let methods = ValueMethods {
        fields,
        encode_doc: LitStr::new(&docs.encode_doc, item.ident.span()),
        decode_doc: LitStr::new(&docs.decode_doc, item.ident.span()),
    };
    Ok(impl_block(
        item,
        &TokenStream::new(),
        &methods.to_token_stream(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        wrong_epoch = {
            "epoch: u32, delivery_complete_count: i32, state_batches: Vec<StateBatch>",
        },
        missing_delivery = { "epoch: i32, state_batches: Vec<StateBatch>" },
        missing_batches = { "epoch: i32, delivery_complete_count: i32" },
        wrong_delivery = {
            "epoch: i32, delivery_complete_count: i64, state_batches: Vec<StateBatch>",
        },
        scalar_batches = { "epoch: i32, delivery_complete_count: i32, state_batches: StateBatch" },
        generic_epoch = {
            "epoch: i32<u8>, delivery_complete_count: i32, state_batches: Vec<StateBatch>",
        },
        two_batch_types = {
            "epoch: i32, delivery_complete_count: i32, state_batches: Vec<StateBatch, StateBatch>",
        },
        missing_delivery_and_batches = { "epoch: i32" },
    )]
    fn refuses_unknown_scalar_types_and_missing_or_invalid_trailers(fields: &str) {
        let input = format!(
            "#[share_state(encode_doc = \"encode\", decode_doc = \"decode\")] struct Value {{ {fields} }}"
        );
        let item: ItemStruct = moxy::parse!({ input }).unwrap();
        assert2::assert!(expand(item).is_err());
    }
}
