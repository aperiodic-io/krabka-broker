//! One array field within the existing flexible version-zero value framing.

use {
    crate::{
        meta::{Tokens, field_ident, impl_block, named_fields},
        value_framing::decode_method,
    },
    moxy::{
        ast::{Attributed, ItemStruct, ParseError, Path},
        token::{LitStr, Spanner, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct ArrayCodec {
    record: String,
    encode: Tokens,
    decode: Tokens,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let codec = item.parse_meta::<ArrayCodec>("array_value")?.ok_or_else(|| {
        ParseError::new(item.ident.span(), "`FlexibleArrayValue` needs `#[array_value(record = \"KafkaRecord\", encode = writer, decode = reader)]`")
    })?;
    let fields = named_fields(&item, "FlexibleArrayValue")?;
    if fields.len() != 1 {
        return Err(ParseError::new(
            item.ident.span(),
            "`FlexibleArrayValue` needs exactly one named array field",
        ));
    }
    let array = fields.first().expect("one field");
    if !array
        .ty
        .as_path()
        .and_then(|path| path.path.last())
        .is_some_and(|segment| segment.ident == "Vec")
    {
        return Err(ParseError::new(
            array.ty.span(),
            "expected a Vec array field",
        ));
    }
    let field = field_ident(array, "FlexibleArrayValue")?.clone();
    let encode: Path = moxy::parse!({ codec.encode.0 })?;
    let decode: Path = moxy::parse!({ codec.decode.0 })?;
    let record = LitStr::new(&codec.record, item.ident.span());
    let decode = decode_method(&record, &moxy::template! {
        |buf| Ok(Self { {{ field }}: {{ decode }}(buf)? })
    });
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        #[must_use]
        pub fn encode(&self) -> bytes::Bytes {
            crate::coordinator::unified::persistence::flex::encode_value(|buf| {
                {{ encode }}(buf, &self.{{ field }});
            })
        }

        {{ decode }}
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        moxy::ast::{ImplItem, ItemImpl},
    };

    #[test]
    fn generic_arrays_keep_their_type_parameter_and_codec_methods() {
        let input = moxy::template! {
            #[array_value(record = "Assigned", encode = codecs::write, decode = codecs::read)]
            struct Assigned<T> { items: Vec<T> }
        };
        let output = expand(moxy::parse!({ input }).unwrap()).expect("valid array codec");
        let methods: ItemImpl = moxy::parse!({ output }).expect("complete implementation");
        assert!(methods.generics.params.len() == 1);
        assert!(methods.self_ty.as_path().unwrap().path.is_ident("Assigned"));
        assert!(
            methods
                .items
                .iter()
                .filter_map(ImplItem::as_fn)
                .map(|method| method.sig.ident.text())
                .eq(["encode", "decode"])
        );
    }

    #[test]
    fn refuses_missing_codecs_or_multiple_fields() {
        for input in [
            "struct Value { items: Vec<i32> }",
            "#[array_value(record = \"Record\", encode = write)] struct Value { items: Vec<i32> }",
            "#[array_value(record = \"Record\", encode = write, decode = read)] struct Value {}",
            "#[array_value(record = \"Record\", encode = write, decode = read)] struct Value { a: Vec<i32>, b: Vec<i32> }",
            "#[array_value(record = \"Record\", encode = write, decode = read)] struct Value { item: i32 }",
            "#[array_value(record = \"Record\", encode = 42, decode = read)] struct Value { items: Vec<i32> }",
        ] {
            let item: ItemStruct = moxy::parse!({ input }).unwrap();
            assert!(expand(item).is_err());
        }
    }
}
