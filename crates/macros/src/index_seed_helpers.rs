//! Flat helpers for testing the two real sparse sidecar indexes.

use {
    crate::meta::plain_path,
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token, Type},
        token::{Spanner, TokenStream},
    },
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let index = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let key = Type::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("expected one index type and its scalar key"));
    }
    let index = plain_path(
        &moxy::template! { {{ index }} },
        index.span(),
        "index fixture needs a concrete index type",
    )?;
    if !key
        .as_path()
        .is_some_and(|path| path.path.is_ident("u32") || path.path.is_ident("i64"))
    {
        return Err(ParseError::new(
            input.span(),
            "sparse-index fixture key must be u32 or i64",
        ));
    }
    Ok(moxy::template! {
        fn append_entries(index: &mut {{ index }}, entries: &[IndexSeed<{{ key }}>]) {
            for &IndexSeed { key, coordinate } in entries { index.append(key, coordinate).unwrap(); }
        }
        fn write_entries(path: &::std::path::Path, entries: &[IndexSeed<{{ key }}>]) {
            let mut index = {{ index }}::open(path).unwrap();
            append_entries(&mut index, entries);
            index.flush().unwrap();
        }
        fn written_index(name: &str, entries: &[IndexSeed<{{ key }}>]) -> TempPath {
            let dir = ::tempfile::tempdir().unwrap();
            let path = dir.path().join(name);
            write_entries(&path, entries);
            TempPath { dir, path }
        }
        fn populated_index(name: &str, entries: &[IndexSeed<{{ key }}>]) -> IndexFixture<{{ index }}> {
            use crate::index::index_fixture;
            let IndexFixture { dir, path, mut index } = index_fixture(name, {{ index }}::open);
            append_entries(&mut index, entries);
            IndexFixture { dir, path, index }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact};

    #[yare::parameterized(offset = { "OffsetIndex, u32" }, time = { "TimeIndex, i64" })]
    fn indexes_keep_both_key_types_and_flush_before_reopening(declaration: &str) {
        let output = compact(&expand(&declaration.parse().unwrap()).unwrap());
        assert2::assert!(output.contains("index.append(key,coordinate).unwrap()"));
        assert2::assert!(
            output.find("append_entries(&mutindex,entries)").unwrap()
                < output.find("index.flush().unwrap()").unwrap()
        );
        assert2::assert!(
            output.contains("TempPath{dir,path}")
                && output.contains("IndexFixture{dir,path,index}")
        );
    }

    #[yare::parameterized(
        generic_index = { "OffsetIndex<u32>, u32" },
        wrong_key_type = { "TimeIndex, String" },
        non_path = { "OffsetIndex + Other, u32" },
        extra_argument = { "TimeIndex, i64, extra" },
    )]
    fn indexes_reject_invalid_declarations(declaration: &str) {
        assert2::assert!(
            expand(&declaration.parse().unwrap()).is_err(),
            "{declaration}"
        );
    }
}
