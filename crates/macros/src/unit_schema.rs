//! JSON Schema for explicit unit marker declarations.

use {
    crate::meta::impl_block,
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::{LitStr, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct SchemaArgs {
    format: String,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let options = item
        .parse_meta::<SchemaArgs>("unit_schema")?
        .ok_or_else(|| {
            ParseError::new(item.ident.span(), "UnitSchema needs a unit_schema format")
        })?;
    if item.fields.as_named().is_some()
        || item.fields.as_unnamed().is_some()
        || !item.generics.params.is_empty()
        || options.format.is_empty()
    {
        return Err(ParseError::new(
            item.ident.span(),
            "UnitSchema needs a concrete unit struct and a nonempty format",
        ));
    }
    let name = &item.ident;
    let format = LitStr::new(&options.format, item.ident.span());
    let methods = moxy::template! {
        fn schema_name() -> Cow<'static, str> { Cow::Borrowed(stringify!({{ name }})) }
        fn inline_schema() -> bool { true }
        fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
            json_schema!({ "type": "string", "format": {{ format }} })
        }
    };
    Ok(impl_block(
        item,
        &moxy::template! { JsonSchema for },
        &methods,
    ))
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn unit_marker_keeps_its_schema_name_and_declared_format() {
        let item =
            moxy::parse!("#[unit_schema(format = \"duration\")] pub struct Duration;").unwrap();
        let output = compact(&expand(item).unwrap());
        assert!(output.contains("Cow::Borrowed(stringify!(Duration))"));
        assert!(output.contains("\"format\":\"duration\""));
        assert!(output.contains("fninline_schema()->bool{true}"));
    }

    #[yare::parameterized(
        missing_attribute = { "struct Marker;" },
        empty_format = { "#[unit_schema(format = \"\")] struct Marker;" },
        generic = { "#[unit_schema(format = \"size\")] struct Marker<T>;" },
        named_fields = { "#[unit_schema(format = \"size\")] struct Marker {}" },
        tuple = { "#[unit_schema(format = \"size\")] struct Marker(u8);" },
    )]
    fn rejects_storage_generic_markers_and_missing_formats(declaration: &str) {
        assert2::assert!(
            expand(moxy::parse!({ declaration }).unwrap()).is_err(),
            "{declaration}"
        );
    }
}
