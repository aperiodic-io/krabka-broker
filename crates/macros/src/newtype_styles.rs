use {
    crate::meta::inner_type,
    moxy::{
        ast::{ItemStruct, ParseError},
        token::{LitStr, TokenStream},
    },
};

fn tuple_struct(meta: TokenStream, item: TokenStream) -> Result<ItemStruct, ParseError> {
    if let Some(token) = meta.into_iter().next() {
        return Err(ParseError::new(token.span(), "expected no arguments"));
    }
    let item = moxy::parse!({ item } as ItemStruct)?;
    inner_type(&item, "newtype style")?;
    Ok(item)
}

pub(crate) fn scalar_id(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let item = tuple_struct(meta, item)?;
    Ok(moxy::template! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::derive_more::Display, ::derive_more::From, ::derive_more::Into,
            ::serde::Serialize, ::serde::Deserialize, ::krabka_macros::PrimitiveCmp)]
        #[serde(transparent)]
        {{ item }}
    })
}

pub(crate) fn test_scalar(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let item = tuple_struct(meta, item)?;
    Ok(moxy::template! {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq,
            ::derive_more::Display, ::derive_more::From, ::derive_more::Into)]
        {{ item }}
    })
}

pub(crate) fn hex_bytes_identity(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    let item = tuple_struct(meta, item)?;
    let debug = LitStr::new(&format!("{}({{}})", item.ident.text()), item.ident.span());
    Ok(moxy::template! {
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
            ::derive_more::Debug, ::derive_more::Display, ::serde::Serialize, ::serde::Deserialize)]
        #[display("{}", ::hex::encode(_0))]
        #[debug({{ debug }}, ::hex::encode(_0))]
        #[serde(transparent)]
        {{ item }}
    })
}

#[cfg(test)]
mod tests {
    use {super::tuple_struct, assert2::check};

    #[yare::parameterized(
        options = { "unexpected", "struct Id(u64);" },
        named = { "", "struct Id { value: u64 }" },
        two_fields = { "", "struct Id(u64, u64);" },
        unit = { "", "struct Id;" },
        enumeration = { "", "enum Id { Value }" },
    )]
    fn refuses_unsupported_shapes(options: &str, declaration: &str) {
        check!(tuple_struct(options.parse().unwrap(), declaration.parse().unwrap()).is_err());
    }
}
