//! Legacy group-record keys from visible enum variants and version metadata.

use {
    crate::meta::plain_path,
    moxy::{
        ast::{Attributed, FromMeta, ItemEnum, Meta, MetaContent, ParseError, Path},
        token::{Ident, LitStr, Spanner, ToTokenStream, TokenStream},
    },
    std::collections::HashSet,
};

struct Version(Path);

impl FromMeta for Version {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let MetaContent::Expr { expr, .. } = &meta.content else {
            return Err(ParseError::new(
                meta.span(),
                "record version needs a constant path",
            ));
        };
        let path = plain_path(
            expr,
            meta.span(),
            "record version needs one constant path without arguments",
        )?;
        Ok(Self(path))
    }
}

#[derive(moxy::FromMeta)]
struct KeyArgs {
    invalid: String,
}

#[derive(moxy::FromMeta)]
struct RecordArgs {
    version: Version,
}

struct Record {
    variant: Ident,
    version: Path,
    fields: Vec<Ident>,
}

pub(crate) fn expand(item: ItemEnum) -> Result<TokenStream, ParseError> {
    if !item.generics.params.is_empty() || item.variants.inner.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "GroupRecordKey needs a concrete nonempty enum",
        ));
    }
    let args = item.parse_meta::<KeyArgs>("group_key")?.ok_or_else(|| {
        ParseError::new(
            item.ident.span(),
            "declare #[group_key(invalid = \"message\")]",
        )
    })?;
    let invalid = LitStr::new(&args.invalid, item.ident.span());
    let mut records = Vec::new();
    let mut versions = HashSet::new();
    for variant in &item.variants.inner {
        let fields = variant.fields.as_named().ok_or_else(|| {
            ParseError::new(
                variant.ident.span(),
                "record variants need named string fields",
            )
        })?;
        let fields = &fields.fields.inner;
        if variant.discriminant.is_some() || !(1..=2).contains(&fields.len()) {
            return Err(ParseError::new(
                variant.ident.span(),
                "record keys need group_id and at most one member_id or regex",
            ));
        }
        let mut names = Vec::new();
        for (index, field) in fields.iter().enumerate() {
            let name = field.ident.as_ref().expect("named variant field");
            let expected_name = if index == 0 {
                name.text() == "group_id"
            } else {
                matches!(name.text(), "member_id" | "regex")
            };
            if !expected_name
                || !field
                    .ty
                    .as_path()
                    .is_some_and(|ty| ty.path.is_ident("String"))
            {
                return Err(ParseError::new(
                    name.span(),
                    "record fields must be group_id: String followed by member_id: String or regex: String",
                ));
            }
            names.push(name.clone());
        }
        let args = variant
            .parse_meta::<RecordArgs>("group_key")?
            .ok_or_else(|| {
                ParseError::new(
                    variant.ident.span(),
                    "each record variant needs #[group_key(version = CONST)]",
                )
            })?;
        let version = args.version.0;
        if !versions.insert(version.to_token_stream().to_string()) {
            return Err(ParseError::new(
                variant.ident.span(),
                "record version path appears more than once",
            ));
        }
        records.push(Record {
            variant: variant.ident.clone(),
            version,
            fields: names,
        });
    }
    let ident = item.ident;
    Ok(moxy::template! {
        impl {{ ident }} {
            /// The group named by any record family of this protocol.
            #[must_use]
            pub fn group_id(&self) -> &str {
                match self {
                    @for record in &records { Self::{{ record.variant }} { group_id, .. } => group_id, }
                }
            }

            fn parse(version: i16, mut buf: &[u8]) -> Result<Self, BrokerError> {
                let key = match version {
                    @for record in &records {
                        {{ record.version }} => Self::{{ record.variant }} {
                            @for field in &record.fields { {{ field }}: get_string(&mut buf)?, }
                        },
                    }
                    _ => return Err(BrokerError::Protocol(ProtocolError::InvalidValue({{ invalid }}))),
                };
                Ok(key)
            }

            fn encode(&self) -> Result<Bytes, BrokerError> {
                match self {
                    @for record in &records {
                        Self::{{ record.variant }} { @for field in &record.fields { {{ field }}, } } =>
                            encode_string_key({{ record.version }}, &[ @for field in &record.fields { {{ field }}, } ]),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn record_shapes_and_metadata_are_validated() {
        for declaration in [
            "#[group_key(invalid = \"bad\")] enum Key {}",
            "#[group_key(invalid = \"bad\")] enum Key<T> { #[group_key(version = V)] A { group_id: String } }",
            "enum Key { #[group_key(version = V)] A { group_id: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { A { group_id: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A(String) }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A { member_id: String, group_id: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A { group_id: i32 } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A { group_id: String, topic: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A { group_id: String, member_id: String, regex: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V)] A { group_id: String }, #[group_key(version = V)] B { group_id: String } }",
            "#[group_key(invalid = \"bad\")] enum Key { #[group_key(version = V + 1)] A { group_id: String } }",
        ] {
            let item = moxy::parse!({ declaration }).unwrap();
            assert!(expand(item).is_err(), "{declaration}");
        }
    }

    #[test]
    fn each_variant_keeps_its_version_and_string_order() {
        let item = moxy::parse!("#[group_key(invalid = \"bad version\")] enum Key { #[group_key(version = FIRST)] A { group_id: String }, #[group_key(version = SECOND)] B { group_id: String, regex: String } }").unwrap();
        let output = compact(&expand(item).unwrap());
        assert!(output.contains("encode_string_key(SECOND,&[group_id,regex,])"));
        assert!(output.find("FIRST=>").unwrap() < output.find("SECOND=>").unwrap());
        assert!(output.contains("InvalidValue(\"badversion\")"));
    }
}
