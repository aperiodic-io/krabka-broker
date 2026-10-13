//! S3 endpoint and redacted credentials, preserving each configuration's docs.

use crate::{
    meta::{mode, named_body},
    prelude::*,
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let (endpoint_doc, access_doc, secret_doc) = match mode(meta, ["file", "runtime"])? {
        "file" => (
            moxy::template! { /// Optional custom endpoint URL (e.g. `MinIO` or Cloudflare R2).
            },
            moxy::template! {
                /// Explicit access key id. Falls back to the AWS credential chain
                /// (env vars, instance profile, …) when omitted.
            },
            moxy::template! {
                /// Explicit secret access key. Falls back to the AWS credential chain
                /// when omitted.
            },
        ),
        "runtime" => (
            moxy::template! {
                /// Optional custom endpoint URL, for example `http://minio:9000` or an R2
                /// endpoint.
            },
            moxy::template! {
                /// Optional explicit access key id. Without it, the backend falls back to
                /// the AWS credential chain.
            },
            moxy::template! {
                /// Optional explicit secret access key. Without it, the backend falls back
                /// to the AWS credential chain.
            },
        ),
        _ => unreachable!(),
    };
    let (mut tokens, body) = named_body(item, "s3_fields")?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let mut fields: Vec<_> = std::mem::take(&mut group.tokens).into_iter().collect();
    let marker = fields
        .iter()
        .position(|token| token.as_ident().is_some_and(|id| id == "allow_http"))
        .ok_or_else(|| {
            ParseError::new(Span::call_site(), "s3_fields requires an allow_http field")
        })?;
    let insertion = fields[..marker]
        .iter()
        .rposition(TokenTree::is_punct_comma)
        .map_or(0, |comma| comma + 1);
    fields.splice(insertion..insertion, moxy::template! {
        {{ endpoint_doc }} pub endpoint: Option<String>,
        {{ access_doc }}
        #[debug("{:?}", access_key_id.as_ref().map(|_| "***"))]
        pub access_key_id: Option<String>,
        {{ secret_doc }}
        #[debug("{:?}", secret_access_key.as_ref().map(|_| "***"))]
        pub secret_access_key: Option<String>,
    });
    group.tokens = fields.into();
    Ok(tokens.into())
}
