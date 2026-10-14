//! Sparse-index construction and file operations, with shared open instrumentation.

use {
    crate::meta::{
        ItemBody, compact, first_missing_named_field, impl_block, item_body, named_fields,
    },
    moxy::{
        ast::{Attributed, FnParam, FromMeta, ItemFn, ItemStruct, Meta, ParseError},
        token::{LitStr, Spanner, TokenStream, TokenTree},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

enum Kind {
    Offset,
    Time,
}

impl FromMeta for Kind {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        match String::from_meta(meta)?.as_str() {
            "offset" => Ok(Self::Offset),
            "time" => Ok(Self::Time),
            _ => Err(ParseError::new(
                meta.span(),
                "expected offset or time index kind",
            )),
        }
    }
}

#[derive(moxy::FromMeta)]
struct IndexArgs {
    kind: Kind,
}

fn open_instrumentation() -> TokenStream {
    moxy::template! {
        #[tracing::instrument(
            level = "debug",
            skip_all,
            fields(path = %path.display(), entries = tracing::field::Empty),
            err,
        )]
    }
}

struct IndexPolicy {
    raw: TokenStream,
    invariant: &'static str,
    target: TokenStream,
    documentation: Option<&'static str>,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let options = item
        .parse_meta::<IndexArgs>("sparse_index")?
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`SparseIndex` needs `#[sparse_index(kind = \"offset\" | \"time\")]`",
            )
        })?;
    let fields = named_fields(&item, "SparseIndex")?;
    if let Some(required) = first_missing_named_field(fields, &["file", "io", "entries"]) {
        return Err(ParseError::new(
            item.ident.span(),
            format!("`SparseIndex` needs a `{required}` field"),
        ));
    }
    let IndexPolicy {
        raw,
        invariant,
        target,
        documentation,
    } = match options.kind {
        Kind::Offset => IndexPolicy {
            raw: moxy::template! { OffsetEntryRaw },
            invariant: "length is a multiple of OFFSET_ENTRY_SIZE and OffsetEntryRaw is Unaligned",
            target: moxy::template! { crate::io::IoTarget::OffsetIndex },
            documentation: Some(
                "Open or create an offset-index file. If the file exists, this method\nloads its entries into memory. If it does not exist, this method\ncreates an empty file.",
            ),
        },
        Kind::Time => IndexPolicy {
            raw: moxy::template! { TimeEntryRaw },
            invariant: "length is a multiple of TIME_ENTRY_SIZE and TimeEntryRaw is Unaligned",
            target: moxy::template! { crate::io::IoTarget::TimeIndex },
            documentation: None,
        },
    };
    let invariant = LitStr::new(invariant, item.ident.span());
    let documentation = documentation.map(|text| LitStr::new(text, item.ident.span()));
    let instrumentation = open_instrumentation();
    let methods = moxy::template! {
        @if let Some(documentation) = &documentation { #[doc = {{ documentation }}] }
        {{ instrumentation }}
        pub fn open(path: &Path) -> Result<Self, crate::error::LogError> {
            let crate::index::OpenedIndex { file, entries } =
                crate::index::load_index::<{{ raw }}, _>(
                    path, {{ invariant }}, |raw| krabka_verified::log_index::IndexEntry::new(raw.key.get(), raw.coordinate.get()),
                )?;
            Ok(Self { file, io: crate::io::file_io(), entries })
        }

        #[cfg(not(target_os = "wasi"))]
        pub(crate) fn flush_handle(&self) -> ::std::io::Result<::std::fs::File> {
            self.file.try_clone()
        }

        /// The number of entries the index holds, which decides when the
        /// segment is full under `segment.index.bytes`.
        #[must_use]
        pub fn entry_count(&self) -> usize { self.entries.len() }

        #[tracing::instrument(level = "debug", skip_all, err)]
        pub fn flush(&mut self) -> Result<(), crate::error::LogError> {
            self.io.sync_file({{ target }}, &self.file).map_err(crate::error::LogError::Io)
        }

        /// Route this index's writes and syncs through `io`.
        pub(crate) fn set_io(&mut self, io: ::std::sync::Arc<dyn crate::io::LogIo>) {
            self.io = io;
        }
    };
    Ok(impl_block(item, &TokenStream::new(), &methods))
}

fn validate_open(meta: &TokenStream, input: &TokenStream) -> Result<(), ParseError> {
    if !meta.is_empty() {
        return Err(ParseError::new(
            meta.span(),
            "`index_open` takes no arguments",
        ));
    }
    let ItemBody {
        mut tokens,
        body_index: body,
    } = item_body(input.clone(), "index_open", TokenTree::is_keyword_fn)?;
    if body + 1 != tokens.len() {
        return Err(ParseError::new(
            input.span(),
            "expected one index open method",
        ));
    }
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    group.tokens = TokenStream::new();
    let function: ItemFn = moxy::parse!({ TokenStream::from(tokens) })?;
    let parameters = &function.sig.params.inner.inputs;
    if function.sig.ident != "open"
        || function.sig.asyncness.is_some()
        || parameters.len() != 1
        || !matches!(parameters.first(), Some(FnParam::Typed(parameter))
            if compact(&moxy::template! { {{ parameter.pat }} }) == "path")
    {
        return Err(ParseError::new(
            function.sig.ident.span(),
            "expected fn open(path: ...)",
        ));
    }
    Ok(())
}

pub(crate) fn index_open(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    validate_open(&meta.into(), &TokenStream::from(item.clone()))?;
    let mut output: NativeTokenStream = open_instrumentation().into();
    // Preserve the original method and its nested expressions without AST round-tripping.
    output.extend(item);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{expand, validate_open},
        assert2::assert,
        moxy::{
            ast::{ItemImpl, ItemStruct},
            token::TokenStream,
        },
    };

    #[yare::parameterized(offset = { "offset" }, time = { "time" })]
    fn generates_the_complete_file_index_contract_for_both_kinds(kind: &str) {
        let source = format!(
            "#[sparse_index(kind = \"{kind}\")] struct DiskIndex {{ entries: Vec<IndexEntry<i64, u32>>, io: Arc<dyn LogIo>, file: File }}"
        );
        let declaration: ItemStruct = moxy::parse!({ source }).unwrap();
        let implementation: ItemImpl = moxy::parse!({ expand(declaration).unwrap() }).unwrap();
        let contract = ["open", "flush_handle", "entry_count", "flush", "set_io"];
        assert2::assert!(implementation.items.len() == contract.len());
        assert2::assert!(
            contract
                .into_iter()
                .all(|name| implementation.items.iter().any(|member| member
                    .as_fn()
                    .is_some_and(|method| method.sig.ident == name)))
        );
    }

    #[yare::parameterized(
        no_fields = { "struct MissingOptions {}" },
        missing_kind = { "#[sparse_index()] struct MissingKind {}" },
        unknown_kind = { "#[sparse_index(kind = \"stamp\")] struct OtherKind {}" },
        numeric_kind = { "#[sparse_index(kind = 1)] struct WrongType {}" },
        tuple_struct = { "#[sparse_index(kind = \"time\")] struct Tuple(File);" },
        missing_file = {
            "#[sparse_index(kind = \"offset\")] struct NoFile { io: Arc<dyn LogIo>, entries: Vec<IndexEntry<u32, u32>> }",
        },
        missing_io = {
            "#[sparse_index(kind = \"time\")] struct NoIo { file: File, entries: Vec<IndexEntry<i64, u32>> }",
        },
        missing_entries = {
            "#[sparse_index(kind = \"offset\")] struct NoEntries { file: File, io: Arc<dyn LogIo> }",
        },
    )]
    fn rejects_invalid_kind_layout_and_absent_fields(source: &str) {
        let declaration: ItemStruct = moxy::parse!({ source }).unwrap();
        assert2::assert!(expand(declaration).is_err());
    }

    #[test]
    fn instrumentation_checks_only_the_header_and_rejects_extra_input() {
        let empty = TokenStream::new();
        let valid: TokenStream = "pub fn open(path: PathBuf) -> Result<Self, Error> { let rows = values.map(|&value| nested!(value)); unknown_syntax!(); Ok(rows) }".parse().unwrap();
        assert!(validate_open(&empty, &valid).is_ok());
        assert!(validate_open(&"unused".parse().unwrap(), &valid).is_err());
    }

    #[yare::parameterized(
        wrong_name = { "fn load(path: PathBuf) {}" },
        wrong_argument = { "fn open(other: PathBuf) {}" },
        receiver = { "fn open(&self, path: &Path) {}" },
        async_fn = { "async fn open(path: PathBuf) {}" },
        struct_item = { "struct Index {}" },
        extra_item = { "fn open(path: PathBuf) {} fn extra() {}" },
    )]
    fn instrumentation_rejects_invalid_headers(source: &str) {
        let empty = TokenStream::new();
        assert2::assert!(validate_open(&empty, &source.parse().unwrap()).is_err());
    }
}
