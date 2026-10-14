//! Display, parsing and string serde for the format crate's Kafka UUID wrappers.

use moxy::ast::{ItemStruct, ParseError};

pub(crate) fn expand(item: ItemStruct) -> Result<moxy::token::TokenStream, ParseError> {
    use crate::meta::inner_type;

    inner_type(&item, "KafkaUuid")?;
    if !item.generics.params.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "`KafkaUuid` needs a non-generic UUID wrapper",
        ));
    }
    let ident = item.ident;
    Ok(moxy::template! {
        impl ::core::fmt::Display for {{ ident }} {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.pad(&encode(self.0))
            }
        }

        impl ::core::str::FromStr for {{ ident }} {
            type Err = KafkaUuidError;

            /// Parses the Kafka form only, as Kafka's `Uuid.fromString` does.
            fn from_str(input: &str) -> Result<Self, Self::Err> {
                decode(input).map(Self)
            }
        }

        impl ::serde::Serialize for {{ ident }} {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for {{ ident }} {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <::std::string::String as ::serde::Deserialize<'de>>::deserialize(deserializer)?
                    .parse()
                    .map_err(<D::Error as ::serde::de::Error>::custom)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[yare::parameterized(
        unit_struct = { "struct Id;" },
        empty_named_struct = { "struct Id {}" },
        named_field = { "struct Id { value: Uuid }" },
        multiple_tuple_fields = { "struct Id(Uuid, Uuid);" },
        const_generic = { "struct Id<const DOMAIN: u8>(Uuid);" },
    )]
    fn refuses_non_newtypes_and_generic_wrappers(input: &str) {
        let item: moxy::ast::ItemStruct = moxy::parse!({ input }).unwrap();
        assert2::assert!(expand(item).is_err(), "{input}");
    }

    #[test]
    fn emits_the_four_trait_implementations_for_a_single_field_wrapper() {
        let item: moxy::ast::ItemStruct = moxy::parse!("pub struct Id(pub Uuid);").unwrap();
        let output = compact(&expand(item).unwrap());
        assert!(output.contains("DisplayforId"));
        assert!(output.contains("FromStrforId"));
        assert!(output.contains("SerializeforId"));
        assert!(output.contains("Deserialize<'de>forId"));
    }
}
