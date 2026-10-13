//! `#[human_units]`: see the crate documentation.
//!
//! The struct is walked as tokens rather than parsed as an
//! [`ItemStruct`](moxy::ast::ItemStruct): moxy 0.5.3 reads the `:` of a `::`
//! inside generic arguments as a constraint, so it rejects a field typed
//! `Option<krabka_units::Time>`. Only the `#[serde(...)]` arguments go through
//! moxy's [`Meta`] parser.

use {
    crate::meta::{ItemBody, compact, named_body},
    moxy::{
        ast::{List, Meta, ParseError, Parser},
        token::{Group, TokenStream, TokenTree},
    },
};

/// A `krabka_units` value type that a config file writes as a human string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Time,
    ByteSize,
    Ratio,
}

impl Unit {
    /// The `krabka_units::serde_units::human` module for an `Option` of it.
    fn serde_module(self) -> &'static str {
        match self {
            Self::Time => "option_time",
            Self::ByteSize => "option_byte_size",
            Self::Ratio => "option_ratio",
        }
    }

    /// The `crate::file_config::schema_units` marker that documents it.
    fn schema_marker(self) -> &'static str {
        match self {
            Self::Time => "Duration",
            Self::ByteSize => "ByteSize",
            Self::Ratio => "Ratio",
        }
    }

    /// The attributes a field of `Option<unit>` gets. `default` is left out
    /// when the field already has a serde `default` of its own.
    fn attributes(self, with_default: bool) -> String {
        let default = if with_default { "default, " } else { "" };
        format!(
            "#[serde({default}with = \"krabka_units::serde_units::human::{}\")] \
             #[schemars(with = \"Option<crate::file_config::schema_units::{}>\")]",
            self.serde_module(),
            self.schema_marker(),
        )
    }
}

/// The last `::` segment of a path written without whitespace.
fn last_segment(path: &str) -> &str { path.rsplit("::").next().unwrap_or(path) }

/// The unit of a type written `Option<Time>`, `Option<ByteSize>` or
/// `Option<Ratio>` under any path prefix. `ty` carries no whitespace.
fn option_unit(ty: &str) -> Option<Unit> {
    let (option, inner) = ty.strip_suffix('>')?.split_once('<')?;
    if last_segment(option) != "Option" || inner.contains(['<', ',']) {
        return None;
    }
    match last_segment(inner) {
        "Time" => Some(Unit::Time),
        "ByteSize" => Some(Unit::ByteSize),
        "Ratio" => Some(Unit::Ratio),
        _ => None,
    }
}

/// Splits a brace-delimited field list at its top-level commas. A comma
/// between `<` and `>` belongs to a type such as `HashMap<K, V>`.
fn split_fields(tokens: &[TokenTree]) -> Vec<&[TokenTree]> {
    let mut fields = Vec::new();
    let mut depth = 0_usize;
    let mut start = 0;
    for (index, token) in tokens.iter().enumerate() {
        let arrow = index > 0 && tokens[index - 1].is_punct_minus();
        if token.is_punct_lt() {
            depth += 1;
        } else if token.is_punct_gt() && !arrow {
            depth = depth.saturating_sub(1);
        } else if token.is_punct_comma() && depth == 0 {
            fields.push(&tokens[start..index]);
            start = index + 1;
        }
    }
    if start < tokens.len() {
        fields.push(&tokens[start..]);
    }
    fields
}

/// The number of leading tokens of `field` that are `#[...]` attributes.
fn attributes_len(field: &[TokenTree]) -> usize {
    field
        .chunks(2)
        .take_while(|pair| {
            pair[0].is_punct_pound()
                && pair
                    .get(1)
                    .and_then(TokenTree::as_group)
                    .is_some_and(|group| group.delim.is_bracket())
        })
        .count()
        * 2
}

/// The argument names of every `#[serde(...)]` attribute in `attrs`.
fn serde_args(attrs: &[TokenTree]) -> Result<Vec<String>, ParseError> {
    let mut names = Vec::new();
    for attr in attrs.iter().filter_map(TokenTree::as_group) {
        let [path, TokenTree::Group(args)] = &attr.tokens[..] else {
            continue;
        };
        if path.as_ident().is_none_or(|ident| ident != "serde") {
            continue;
        }
        let args = List::<Meta, moxy::Token![,]>::parse_all(&Parser::from_tokens(&args.tokens))?;
        names.extend(
            args.iter()
                .filter_map(|arg| arg.path.as_ident().map(|ident| ident.text().to_owned())),
        );
    }
    Ok(names)
}

/// One field of the struct, with the human-unit attributes added when its
/// type calls for them and it does not already pick a serde codec.
fn annotate(field: &[TokenTree]) -> Result<Vec<TokenTree>, ParseError> {
    let (attrs, rest) = field.split_at(attributes_len(field));
    // A visibility's path sits inside a group, so the first top-level `:`
    // is the one after the field name.
    let Some(colon) = rest.iter().position(TokenTree::is_punct_colon) else {
        return Ok(field.to_vec());
    };
    let Some(unit) = option_unit(&compact(&TokenStream::from(&rest[colon + 1..]))) else {
        return Ok(field.to_vec());
    };
    let args = serde_args(attrs)?;
    let has = |name: &str| args.iter().any(|arg| arg == name);
    if has("with") || has("deserialize_with") || has("serialize_with") {
        return Ok(field.to_vec());
    }
    let added: TokenStream = unit.attributes(!has("default")).parse()?;
    Ok(attrs
        .iter()
        .cloned()
        .chain(added)
        .chain(rest.iter().cloned())
        .collect())
}

/// Expands `#[human_units]` on `item`.
pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    if let Some(argument) = meta.into_iter().next() {
        return Err(ParseError::new(
            argument.span(),
            "`#[human_units]` takes no arguments",
        ));
    }
    let ItemBody {
        mut tokens,
        body_index: body,
    } = named_body(item, "human_units")?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let comma: TokenStream = ",".parse()?;
    let mut fields = Vec::new();
    for field in split_fields(&group.tokens) {
        fields.extend(annotate(field)?);
        fields.extend(comma.iter().cloned());
    }
    *group = Group {
        delim: group.delim,
        span: group.span,
        tokens: fields.into(),
    };
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use {
        super::{Unit, expand, option_unit},
        crate::meta::compact,
        assert2::assert,
        moxy::token::TokenStream,
    };

    #[test]
    fn option_unit_matches_the_last_segment() {
        for (ty, unit) in [
            ("Option<Time>", Some(Unit::Time)),
            ("Option<krabka_units::Time>", Some(Unit::Time)),
            ("::std::option::Option<ByteSize>", Some(Unit::ByteSize)),
            ("Option<Ratio>", Some(Unit::Ratio)),
            ("Time", None),
            ("Vec<Time>", None),
            ("Option<String>", None),
            ("Option<Time<u8>>", None),
            ("Option<Vec<Time>>", None),
            ("HashMap<String,Time>", None),
        ] {
            assert!(option_unit(ty) == unit, "{ty}");
        }
    }

    fn expanded(item: &str) -> String {
        let item: TokenStream = item.parse().expect("item tokenizes");
        compact(&expand(TokenStream::new(), item).expect("expands"))
    }

    #[test]
    fn fields_get_attributes_by_type() {
        let out = expanded(
            "pub struct S<T> where T: Clone { \
               #[doc = \"Doc.\"] #[serde(rename = \"x\")] \
               pub(crate) a: Option<krabka_units::Time>, \
               b: HashMap<String, Option<Time>>, \
               c: Option<ByteSize> }",
        );
        assert!(
            out == "pubstructS<T>whereT:Clone{#[doc=\"Doc.\"]#[serde(rename=\"x\")]\
                    #[serde(default,with=\"krabka_units::serde_units::human::option_time\")]\
                    #[schemars(with=\"Option<crate::file_config::schema_units::Duration>\")]\
                    pub(crate)a:Option<krabka_units::Time>,\
                    b:HashMap<String,Option<Time>>,\
                    #[serde(default,with=\"krabka_units::serde_units::human::option_byte_size\")]\
                    #[schemars(with=\"Option<crate::file_config::schema_units::ByteSize>\")]\
                    c:Option<ByteSize>,}"
        );
    }

    #[test]
    fn existing_default_is_not_repeated() {
        let out = expanded("struct S { #[serde(default)] a: Option<Ratio> }");
        assert!(out.matches("default").count() == 1);
        assert!(out.contains("option_ratio"));
    }

    #[test]
    fn own_codec_is_left_alone() {
        for codec in ["with", "deserialize_with", "serialize_with"] {
            let source = format!("struct S {{ #[serde({codec} = \"m\")] a: Option<Time> }}");
            assert!(!expanded(&source).contains("schemars"), "{codec}");
        }
    }

    #[test]
    fn arguments_tuple_structs_and_enums_are_rejected() {
        for (meta, source) in [
            ("", "struct S(Option<Time>);"),
            ("", "enum E { A }"),
            ("x", "struct S {}"),
        ] {
            let meta: TokenStream = meta.parse().expect("meta tokenizes");
            let item: TokenStream = source.parse().expect("item tokenizes");
            assert!(expand(meta, item).is_err(), "{source}");
        }
    }
}
