//! Flexible record framing around ordinary, explicitly ordered field codecs.

use {
    crate::{
        meta::{ItemBody, compact, edit_native_body, is_plain_sync, item_body},
        value_framing::decode_method,
    },
    moxy::{
        ast::{FnParam, FromMeta, ImplItem, ItemImpl, Meta, ParseError},
        token::{LitStr, Spanner, TokenStream, TokenTree},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

#[derive(moxy::FromMeta)]
struct ValueArgs {
    record: String,
}

fn wrappers(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    let options: Meta = moxy::parse!({
        moxy::template! { flexible_value({{ meta }}) }
    })?;
    let args = ValueArgs::from_meta(&options)?;
    let ItemBody {
        mut tokens,
        body_index: body,
    } = item_body(item.clone(), "flexible_value", TokenTree::is_keyword_impl)?;
    if body + 1 != tokens.len() {
        return Err(ParseError::new(item.span(), "expected one inherent impl"));
    }
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    // Parse signatures only. The original native method bodies are returned unchanged.
    let mut members = group.tokens.clone().into_inner();
    for token in &mut members {
        if let TokenTree::Group(group) = token
            && group.delim.is_brace()
        {
            group.tokens = TokenStream::new();
        }
    }
    group.tokens = members.into();
    let implementation: ItemImpl = moxy::parse!({ TokenStream::from(tokens) })?;
    if implementation.trait_ref.is_some() || implementation.unsafety.is_some() {
        return Err(ParseError::new(item.span(), "expected an inherent impl"));
    }
    let mut receiver = None;
    let mut decode = false;
    for member in &implementation.items.inner {
        let ImplItem::Fn(method) = member else {
            return Err(ParseError::new(
                member.span(),
                "expected private field codec methods",
            ));
        };
        let signature = &method.sig;
        if !method.vis.is_inherited() || !is_plain_sync(signature) {
            return Err(ParseError::new(
                signature.span(),
                "field codecs must be private synchronous methods",
            ));
        }
        let inputs = &signature.params.inner.inputs;
        match signature.ident.text() {
            "encode_fields" if receiver.is_none() && inputs.len() == 2 => {
                let Some(FnParam::Receiver(value)) = inputs.first() else {
                    return Err(ParseError::new(
                        signature.span(),
                        "encode_fields needs self or &self",
                    ));
                };
                if value.mutability.is_some() || value.lifetime.is_some() {
                    return Err(ParseError::new(
                        value.span(),
                        "encode_fields needs self or &self",
                    ));
                }
                let FnParam::Typed(buffer) = inputs.last().expect("two parameters") else {
                    return Err(ParseError::new(
                        signature.span(),
                        "expected buf: &mut BytesMut",
                    ));
                };
                if compact(&moxy::template! { {{ buffer.ty }} }) != "&mutBytesMut"
                    || !compact(&moxy::template! { {{ signature.output }} }).is_empty()
                {
                    return Err(ParseError::new(
                        signature.span(),
                        "expected fn encode_fields(self or &self, buf: &mut BytesMut)",
                    ));
                }
                receiver = Some(value.clone());
            }
            "decode_fields" if !decode && inputs.len() == 1 => {
                let Some(FnParam::Typed(buffer)) = inputs.first() else {
                    return Err(ParseError::new(
                        signature.span(),
                        "decode_fields must be an associated function",
                    ));
                };
                if compact(&moxy::template! { {{ buffer.ty }} }) != "&mut&[u8]"
                    || compact(&moxy::template! { {{ signature.output }} })
                        != "->Result<Self,BrokerError>"
                {
                    return Err(ParseError::new(
                        signature.span(),
                        "expected fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError>",
                    ));
                }
                decode = true;
            }
            _ => {
                return Err(ParseError::new(
                    signature.span(),
                    "expected one encode_fields and one decode_fields method",
                ));
            }
        }
    }
    let receiver = receiver.filter(|_| decode).ok_or_else(|| {
        ParseError::new(
            item.span(),
            "expected one encode_fields and one decode_fields method",
        )
    })?;
    let record = LitStr::new(&args.record, item.span());
    let decode = decode_method(&record, &moxy::template! {
        Self::decode_fields
    });
    Ok(moxy::template! {
        #[must_use]
        pub fn encode({{ receiver }}) -> ::bytes::Bytes {
            crate::coordinator::unified::persistence::flex::encode_value(
                |buf| self.encode_fields(buf),
            )
        }

        {{ decode }}
    })
}

pub(crate) fn expand(
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let suffix: NativeTokenStream = wrappers(meta, &item.clone().into())?.into();
    Ok(edit_native_body(item, |body| {
        body.into_iter().chain(suffix).collect()
    }))
}

#[cfg(test)]
mod tests {
    use {super::wrappers, crate::meta::compact};

    #[yare::parameterized(
        empty_impl = { "" },
        missing_decode = { "fn encode_fields(&self, buf: &mut BytesMut) {}" },
        mutable_encode_receiver = {
            "fn encode_fields(&mut self, buf: &mut BytesMut) {} fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {}",
        },
        public_encode_method = {
            "pub fn encode_fields(&self, buf: &mut BytesMut) {} fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {}",
        },
        borrowed_decode_buffer = {
            "fn encode_fields(&self, buf: &mut BytesMut) {} fn decode_fields(buf: &[u8]) -> Result<Self, BrokerError> {}",
        },
        duplicate_encode_method = {
            "fn encode_fields(&self, buf: &mut BytesMut) {} fn encode_fields(self, buf: &mut BytesMut) {} fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {}",
        },
    )]
    fn rejects_missing_duplicate_and_incompatible_field_codecs(methods: &str) {
        let item = format!("impl Value {{ {methods} }}").parse().unwrap();
        assert2::assert!(wrappers(&"record = \"Record\"".parse().unwrap(), &item).is_err());
    }

    #[yare::parameterized(
        shared_receiver = { "&self" },
        owned_receiver = { "self" },
    )]
    fn infers_receivers_without_parsing_complex_bodies(receiver: &str) {
        let item = format!("impl Value {{ fn encode_fields({receiver}, buf: &mut BytesMut) {{ module::write(buf, self.value); }} fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {{ return Err(module::error()); }} }}").parse().unwrap();
        let output = wrappers(&"record = \"Record\"".parse().unwrap(), &item).unwrap();
        let output = compact(&output);
        assert2::assert!(output.contains(&format!("pubfnencode({receiver})").replace(' ', "")));
        assert2::assert!(output.contains("Self::decode_fields"));
    }

    #[yare::parameterized(
        missing_metadata = { "" },
        numeric_record_name = { "record = 2" },
        unsupported_metadata_key = { "other = \"Record\"" },
    )]
    fn rejects_invalid_wrapper_metadata(meta: &str) {
        let item = "impl Value { fn encode_fields(&self, buf: &mut BytesMut) { module::write(buf, self.value); } fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> { return Err(module::error()); } }".parse().unwrap();
        assert2::assert!(wrappers(&meta.parse().unwrap(), &item).is_err());
    }
}
