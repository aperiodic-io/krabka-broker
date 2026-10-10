//! Expand the shared file-upload method signature before async trait derives.

use moxy::{
    ast::ParseError,
    token::{Span, TokenStream, TokenTree},
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    crate::meta::no_arguments(meta, "object_ops")?;
    let (mut tokens, body) = crate::meta::item_body(item, "object_ops", |token| {
        token.is_keyword_trait() || token.is_keyword_impl()
    })?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let mut members = group.tokens.clone().into_iter().peekable();
    let mut output = TokenStream::new();
    let mut expanded = false;
    while let Some(token) = members.next() {
        if token.as_ident().is_some_and(|name| name == "put_from_path")
            && members.peek().is_some_and(TokenTree::is_punct_not)
        {
            members.next();
            let Some(TokenTree::Group(arguments)) = members.next() else {
                return Err(ParseError::new(
                    token.span(),
                    "expected put_from_path!(...)",
                ));
            };
            let arguments: Vec<_> = arguments.tokens.into_iter().collect();
            let ending = match arguments.as_slice() {
                [] => moxy::template! { ; },
                [TokenTree::Group(body)] if body.delim.is_brace() => arguments.into(),
                _ => {
                    return Err(ParseError::new(
                        token.span(),
                        "expected an empty declaration or one method body",
                    ));
                }
            };
            if members.peek().is_some_and(TokenTree::is_punct_semi) {
                members.next();
            }
            output.extend(moxy::template! {
                async fn put_from_path(
                    &self,
                    key: &Path,
                    src: &std::path::Path,
                    threshold: u64,
                    chunk_size: usize,
                    req: PutRequest,
                ) -> Result<PutOutcome, ObjectStoreError> {{ ending }}
            });
            if expanded {
                return Err(ParseError::new(
                    token.span(),
                    "put_from_path selected more than once",
                ));
            }
            expanded = true;
        } else {
            output.extend([token]);
        }
    }
    if !expanded {
        return Err(ParseError::new(
            Span::call_site(),
            "expected put_from_path!() or put_from_path!({ ... })",
        ));
    }
    group.tokens = output;
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::expand;

    #[test]
    fn preserves_declaration_docs_and_implementation_body() {
        for item in [
            "#[async_trait] trait Ops { #[doc = \"Upload\"] put_from_path!(); }",
            "#[async_trait] impl Ops for Client { put_from_path!({ self.upload(key, src, threshold, chunk_size, req).await }); }",
        ] {
            let output = expand("".parse().unwrap(), item.parse().unwrap()).unwrap();
            let output = crate::meta::compact(&output);
            assert!(output.contains("asyncfnput_from_path(&self,key:&Path,src:&std::path::Path,threshold:u64,chunk_size:usize,req:PutRequest,)"));
            assert!(!output.contains("put_from_path!"));
        }
    }

    #[test]
    fn rejects_missing_duplicate_or_malformed_methods() {
        for item in [
            "trait Ops {}",
            "trait Ops { put_from_path!(); put_from_path!(); }",
            "impl Ops for Client { put_from_path!(wrong); }",
        ] {
            assert!(expand("".parse().unwrap(), item.parse().unwrap()).is_err());
        }
    }
}
