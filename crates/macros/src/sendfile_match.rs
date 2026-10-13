//! Network operation and records-payload matches share one file-region platform policy.

use {
    moxy::{
        ast::{Expr, ExprMatch, MatchArm, Parse as _, ParseError, Parser, Pattern},
        token::{Spanner as _, ToTokenStream as _, TokenStream, TokenTree},
    },
    proc_macro::{TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
    std::ops::Range,
};

#[derive(Clone, Copy)]
enum MatchKind {
    WriteOp,
    RecordsPayload,
}

struct ArmRange {
    #[cfg(test)]
    parsed: Range<usize>,
    native: Range<usize>,
    gated: bool,
}

#[cfg(test)]
pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    expand_parsed(input, MatchKind::WriteOp)
}

#[cfg(test)]
pub(crate) fn records_payload(input: TokenStream) -> Result<TokenStream, ParseError> {
    expand_parsed(input, MatchKind::RecordsPayload)
}

pub(crate) fn expand_native(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    expand_original(input, MatchKind::WriteOp)
}

pub(crate) fn records_payload_native(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    expand_original(input, MatchKind::RecordsPayload)
}

fn expand_original(
    input: NativeTokenStream,
    kind: MatchKind,
) -> Result<NativeTokenStream, ParseError> {
    let parsed: TokenStream = input.clone().into();
    let ranges = arm_ranges(&parsed, kind)?;
    let mut original: Vec<_> = input.into_iter().collect();
    let Some(NativeTokenTree::Group(group)) = original.pop() else {
        return Err(ParseError::new(parsed.span(), "expected match arms"));
    };
    let source: Vec<_> = group.stream().into_iter().collect();
    let arms = render_arms(
        &source,
        ranges.iter().map(|arm| (&arm.native, arm.gated)),
        || {
            NativeTokenStream::from(platform_cfg())
                .into_iter()
                .collect()
        },
    );
    let mut replacement = proc_macro::Group::new(group.delimiter(), arms.into_iter().collect());
    replacement.set_span(group.span());
    original.push(NativeTokenTree::Group(replacement));
    Ok(original.into_iter().collect())
}

#[cfg(test)]
fn expand_parsed(input: TokenStream, kind: MatchKind) -> Result<TokenStream, ParseError> {
    let ranges = arm_ranges(&input, kind)?;
    let span = input.span();
    let mut original = input.into_inner();
    let Some(TokenTree::Group(mut group)) = original.pop() else {
        return Err(ParseError::new(span, "expected match arms"));
    };
    group.tokens = render_arms(
        &group.tokens,
        ranges.iter().map(|arm| (&arm.parsed, arm.gated)),
        || platform_cfg().into_iter().collect(),
    )
    .into_iter()
    .collect();
    original.push(TokenTree::Group(group));
    Ok(original.into())
}

/// Parse only to validate patterns and find arm boundaries. Emission copies the original tokens,
/// retaining attributes, guards, punctuation spacing, nested groups and their compiler spans.
fn arm_ranges(input: &TokenStream, kind: MatchKind) -> Result<Vec<ArmRange>, ParseError> {
    let context = match kind {
        MatchKind::WriteOp => "expected an exhaustive WriteOp match",
        MatchKind::RecordsPayload => "expected a RecordsPayload match",
    };
    let expression = parse_match(input, context)?;
    let Some(TokenTree::Group(group)) = input.last() else {
        return Err(ParseError::new(input.span(), "expected match arms"));
    };
    let parser = Parser::from_tokens(&group.tokens);
    let mut ranges = Vec::new();
    let mut variants = Vec::new();
    let mut gated = false;
    let (mut parsed_start, mut native_start) = (0, 0);
    while !parser.is_empty() {
        let start = parser.cursor();
        let arm = MatchArm::parse(&parser)?;
        let file = match kind {
            MatchKind::WriteOp => write_op_kind(&arm, &mut variants)?,
            MatchKind::RecordsPayload => {
                let (file, portable) = record_kinds(&arm.pat);
                if file && portable {
                    return Err(ParseError::new(
                        arm.span(),
                        "FileRegions must have its own match arm",
                    ));
                }
                file
            }
        };
        gated |= file;
        let consumed = parser.cursor().range(start);
        let parsed_end = parsed_start + consumed.len();
        // Moxy coalesces compound punctuation such as `=>` and `::`; each character was one
        // native punctuation token. Groups count once and their original contents stay intact.
        let native_end = native_start + consumed.iter().map(native_width).sum::<usize>();
        ranges.push(ArmRange {
            #[cfg(test)]
            parsed: parsed_start..parsed_end,
            native: native_start..native_end,
            gated: file,
        });
        (parsed_start, native_start) = (parsed_end, native_end);
    }
    match kind {
        MatchKind::WriteOp if variants.len() != 2 => Err(ParseError::new(
            expression.span(),
            "expected both Inline and File arms",
        )),
        MatchKind::RecordsPayload if !gated => Err(ParseError::new(
            expression.span(),
            "expected a FileRegions arm",
        )),
        _ => Ok(ranges),
    }
}

fn render_arms<'a, T: Clone>(
    source: &[T],
    ranges: impl Iterator<Item = (&'a Range<usize>, bool)>,
    cfg: impl Fn() -> Vec<T>,
) -> Vec<T> {
    let mut output = Vec::new();
    for (range, gated) in ranges {
        if gated {
            output.extend(cfg());
        }
        output.extend_from_slice(&source[range.clone()]);
    }
    output
}

fn native_width(token: &TokenTree) -> usize {
    match token {
        TokenTree::Punct(punct) => punct.as_str().len(),
        _ => 1,
    }
}

fn write_op_kind(arm: &MatchArm, variants: &mut Vec<String>) -> Result<bool, ParseError> {
    let Pattern::TupleStruct(pattern) = &arm.pat else {
        return Err(ParseError::new(
            arm.span(),
            "expected a WriteOp tuple variant",
        ));
    };
    let identifiers = path_identifiers(pattern.path.to_token_stream());
    let [.., owner, variant] = identifiers.as_slice() else {
        return Err(ParseError::new(
            arm.span(),
            "expected a qualified WriteOp variant",
        ));
    };
    if owner != "WriteOp"
        || !matches!(variant.as_str(), "Inline" | "File")
        || variants.contains(variant)
    {
        return Err(ParseError::new(
            arm.span(),
            "expected each WriteOp variant exactly once",
        ));
    }
    variants.push(variant.clone());
    Ok(variant == "File")
}

fn parse_match(input: &TokenStream, context: &str) -> Result<ExprMatch, ParseError> {
    match moxy::parse!({ input.clone() } as Expr)? {
        Expr::Match(expression) => Ok(expression),
        _ => Err(ParseError::new(input.span(), context)),
    }
}

fn path_identifiers(path: TokenStream) -> Vec<String> {
    path.into_iter()
        .filter_map(|token| match token {
            TokenTree::Ident(name) => Some(name.to_string()),
            _ => None,
        })
        .collect()
}

/// Recurse through `Result` wrappers and or-patterns, without gating a portable alternative.
fn record_kinds(pattern: &Pattern) -> (bool, bool) {
    match pattern {
        Pattern::TupleStruct(pattern) => {
            let identifiers = path_identifiers(pattern.path.to_token_stream());
            if let [.., owner, variant] = identifiers.as_slice()
                && owner == "RecordsPayload"
            {
                return (variant == "FileRegions", variant != "FileRegions");
            }
            combine_kinds(pattern.elems.iter())
        }
        Pattern::Or(pattern) => combine_kinds(pattern.cases.iter()),
        Pattern::Paren(pattern) => record_kinds(&pattern.content),
        Pattern::Reference(pattern) => record_kinds(&pattern.pat),
        _ => (false, true),
    }
}

fn combine_kinds<'a>(patterns: impl Iterator<Item = &'a Pattern>) -> (bool, bool) {
    patterns
        .map(record_kinds)
        .fold((false, false), |(file, portable), next| {
            (file || next.0, portable || next.1)
        })
}

pub(crate) fn platform_cfg() -> TokenStream {
    moxy::template! {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios",
            target_os = "tvos", target_os = "watchos", target_os = "freebsd", target_os = "dragonfly"))]
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    #[test]
    fn preserves_guards_attributes_and_async_arm_bodies() {
        let output = super::expand("match operation { #[cfg(feature = \"inline\")] WriteOp::Inline(bytes) if bytes.is_empty() => write(bytes).await?, WriteOp::File(region) => drain(region).await?, }".parse().unwrap()).unwrap().to_string();
        for retained in [
            "feature = \"inline\"",
            "if bytes . is_empty",
            "write (bytes) . await ?",
            "drain (region) . await ?",
            "target_os = \"dragonfly\"",
        ] {
            assert!(output.contains(retained), "{retained}: {output}");
        }
    }

    #[test]
    fn requires_an_exhaustive_write_op_match() {
        for input in [
            "value",
            "match value { _ => () }",
            "match value { WriteOp::Inline(b) => b }",
            "match value { Other::Inline(b) => b, Other::File(f) => f }",
        ] {
            assert!(super::expand(input.parse().unwrap()).is_err(), "{input}");
        }
    }
}

#[cfg(test)]
mod records_tests {
    use assert2::assert;

    #[test]
    fn retains_result_errors_guards_and_portable_alternatives() {
        let output = super::records_payload("match payload { Ok(RecordsPayload::Raw(b) | RecordsPayload::Legacy(b)) | Err(b) => portable(b), #[cfg(feature = \"files\")] Ok(RecordsPayload::FileRegions(r)) if !r.is_empty() => files(r).await, Ok(RecordsPayload::V2(b)) => batches(b), }".parse().unwrap()).unwrap().to_string();
        for retained in [
            "Err (b)",
            "feature = \"files\"",
            "if ! r . is_empty",
            "files (r) . await",
            "target_os = \"dragonfly\"",
        ] {
            assert!(output.contains(retained), "{retained}: {output}");
        }
        assert!(output.matches("target_os = \"linux\"").count() == 1);
    }

    #[test]
    fn rejects_an_arm_that_would_hide_a_portable_alternative() {
        assert!(super::records_payload("match payload { RecordsPayload::FileRegions(_) | RecordsPayload::Raw(_) => (), _ => (), }".parse().unwrap()).is_err());
    }
}
