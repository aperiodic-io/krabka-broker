//! Explicit delegation of an object-store wrapper's unchanged methods.

use moxy::{
    ast::ParseError,
    token::{Ident, Span, TokenStream, TokenTree},
};

use crate::meta::item_body;

fn methods(meta: TokenStream) -> Result<Vec<Ident>, ParseError> {
    let mut names = Vec::new();
    let mut expecting_name = true;
    for token in meta {
        if expecting_name {
            let TokenTree::Ident(name) = token else {
                return Err(ParseError::new(
                    token.span(),
                    "expected a delegated method name",
                ));
            };
            if names
                .iter()
                .any(|selected: &Ident| selected.text() == name.text())
            {
                return Err(ParseError::new(
                    name.span(),
                    "method selected more than once",
                ));
            }
            names.push(name);
        } else if !token.is_punct_comma() {
            return Err(ParseError::new(
                token.span(),
                "expected a comma between method names",
            ));
        }
        expecting_name = !expecting_name;
    }
    if names.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "select at least one delegated method",
        ));
    }
    Ok(names)
}

fn implementation(name: &Ident) -> Result<TokenStream, ParseError> {
    let implementation = match name.to_string().as_str() {
        "get_opts" => moxy::template! {
            async fn get_opts(
                &self,
                location: &::object_store::path::Path,
                options: ::object_store::GetOptions,
            ) -> ::object_store::Result<::object_store::GetResult> {
                self.inner.get_opts(location, options).await
            }
        },
        "put_multipart_opts" => moxy::template! {
            async fn put_multipart_opts(
                &self,
                location: &::object_store::path::Path,
                opts: ::object_store::PutMultipartOptions,
            ) -> ::object_store::Result<Box<dyn ::object_store::MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }
        },
        "delete_stream" => moxy::template! {
            fn delete_stream(
                &self,
                locations: ::futures_util::stream::BoxStream<'static, ::object_store::Result<::object_store::path::Path>>,
            ) -> ::futures_util::stream::BoxStream<'static, ::object_store::Result<::object_store::path::Path>> {
                self.inner.delete_stream(locations)
            }
        },
        "list" => moxy::template! {
            fn list(
                &self,
                prefix: Option<&::object_store::path::Path>,
            ) -> ::futures_util::stream::BoxStream<'static, ::object_store::Result<::object_store::ObjectMeta>> {
                self.inner.list(prefix)
            }
        },
        "list_with_delimiter" => moxy::template! {
            async fn list_with_delimiter(
                &self,
                prefix: Option<&::object_store::path::Path>,
            ) -> ::object_store::Result<::object_store::ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }
        },
        "copy_opts" => moxy::template! {
            async fn copy_opts(
                &self,
                from: &::object_store::path::Path,
                to: &::object_store::path::Path,
                options: ::object_store::CopyOptions,
            ) -> ::object_store::Result<()> {
                self.inner.copy_opts(from, to, options).await
            }
        },
        _ => {
            return Err(ParseError::new(
                name.span(),
                "unsupported delegated object-store method",
            ));
        }
    };
    Ok(implementation)
}

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let names = methods(meta)?;
    let (mut tokens, body) = item_body(item, "object_store_delegate", TokenTree::is_keyword_impl)?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let existing = Vec::from(group.tokens.clone());
    for name in names {
        if existing.windows(2).any(|pair| {
            pair[0].is_keyword_fn()
                && pair[1]
                    .as_ident()
                    .is_some_and(|item| item.text() == name.text())
        }) {
            return Err(ParseError::new(
                name.span(),
                "delegated method already has an implementation",
            ));
        }
        group.tokens.extend(implementation(&name)?);
    }
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::expand;

    #[test]
    fn preserves_custom_methods_and_generics() {
        let item = "#[async_trait::async_trait] impl<T: ObjectStore> ObjectStore for Wrapper<T> { async fn put_opts(&self) {} }";
        let output = expand(
            "get_opts, delete_stream, copy_opts,".parse().unwrap(),
            item.parse().unwrap(),
        )
        .unwrap()
        .to_string();
        assert!(output.contains("Wrapper < T >"));
        assert!(output.contains("async fn put_opts"));
        for method in ["get_opts", "delete_stream", "copy_opts"] {
            assert!(output.contains(&format!("fn {method}")), "{method}");
        }
    }

    #[test]
    fn rejects_unsupported_duplicate_and_existing_methods() {
        let item = "impl ObjectStore for Wrapper { fn list(&self) {} }";
        for meta in [
            "",
            "unknown",
            "get_opts get_opts",
            "get_opts, get_opts",
            "list",
        ] {
            assert!(
                expand(meta.parse().unwrap(), item.parse().unwrap()).is_err(),
                "{meta}"
            );
        }
    }
}
