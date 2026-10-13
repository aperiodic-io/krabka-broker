//! Replay transitions for the three modern group seed protocols.

use {
    crate::meta::{impl_block, named_fields},
    moxy::{
        ast::{
            Attributed, Field, FromMeta, GenericArgument, ItemStruct, Meta, ParseError,
            PathArguments, Type,
        },
        token::{Ident, Spanner, TokenStream},
    },
};

struct ReplayProtocol(Ident);

impl FromMeta for ReplayProtocol {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let requested = String::from_meta(meta)?;
        let variant = [
            ("consumer", "Consumer"),
            ("share", "Share"),
            ("streams", "Streams"),
        ]
        .into_iter()
        .find_map(|(selector, variant)| (requested == selector).then_some(variant))
        .ok_or_else(|| {
            ParseError::new(
                meta.span(),
                "replay protocol must be consumer, share or streams",
            )
        })?;
        Ok(Self(Ident::new(variant).with_span(meta.span())))
    }
}

#[derive(moxy::FromMeta)]
struct ReplayOptions {
    protocol: ReplayProtocol,
}

fn record_type(field: &Field) -> Result<Type, ParseError> {
    let invalid = || {
        ParseError::new(
            field.ty.span(),
            "replay maps need `HashMap<String, Record>` fields",
        )
    };
    let segment = field
        .ty
        .as_path()
        .and_then(|ty| ty.path.last())
        .ok_or_else(invalid)?;
    let PathArguments::AngleBracketed(arguments) = &segment.args else {
        return Err(invalid());
    };
    if segment.ident != "HashMap" || arguments.args.len() != 2 {
        return Err(invalid());
    }
    let mut types = arguments.args.iter();
    let Some(GenericArgument::Type(key)) = types.next() else {
        return Err(invalid());
    };
    if !key.as_path().is_some_and(|ty| ty.path.is_ident("String")) {
        return Err(invalid());
    }
    match types.next() {
        Some(GenericArgument::Type(record)) => Ok(record.clone()),
        _ => Err(invalid()),
    }
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let options = item.parse_meta::<ReplayOptions>("replay_member_maps")?.ok_or_else(|| {
        ParseError::new(item.ident.span(), "`ReplayMemberMaps` needs `#[replay_member_maps(protocol = \"consumer\" | \"share\" | \"streams\")]`")
    })?;
    let fields = named_fields(&item, "ReplayMemberMaps")?;
    let map = |name| {
        fields
            .iter()
            .find(|field| field.ident.as_ref().is_some_and(|ident| ident == name))
            .ok_or_else(|| {
                ParseError::new(
                    item.ident.span(),
                    format!("`ReplayMemberMaps` needs a `{name}` field"),
                )
            })
            .and_then(record_type)
    };
    let metadata = map("members")?;
    let current = map("current_per_member")?;
    map("target_per_member")?;
    let protocol = options.protocol.0;
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        pub(super) fn install_current_member(
            &mut self,
            member_id: &str,
            value: {{ current }},
            uninitialized: impl FnOnce() -> {{ metadata }},
        ) {
            self.members.entry(member_id.into()).or_insert_with(uninitialized);
            self.current_per_member.insert(member_id.into(), value);
        }

        pub(super) fn member_tombstone(&self, member_id: &str) -> Result<bool, crate::error::BrokerError> {
            crate::coordinator::unified::replay_policy::member_tombstone(
                crate::coordinator::unified::replay_policy::ModernGroupType::{{ protocol }},
                member_id,
                self.members.contains_key(member_id).then(|| {
                    self.current_per_member.get(member_id).map_or(0, |current| current.member_epoch)
                }),
                self.target_per_member.contains_key(member_id),
            )
        }

        pub(super) fn remove_replayed_member(&mut self, member_id: &str) {
            self.members.remove(member_id);
            self.current_per_member.remove(member_id);
        }
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::compact,
        assert2::assert,
        moxy::{
            ast::{FnParam, GenericParam, ImplItem, ItemImpl, ItemStruct},
            token::ToTokenStream,
        },
    };

    #[test]
    fn emits_typed_replay_methods_for_each_protocol_and_preserves_generics() {
        for protocol in ["consumer", "share", "streams"] {
            let declaration = format!(
                r#"
                #[replay_member_maps(protocol = "{protocol}")]
                struct Seed<T: Clone> where T: Send {{
                    members: std::collections::HashMap<String, Metadata<T>>,
                    current_per_member: HashMap<String, ::records::Current<T>>,
                    target_per_member: HashMap<String, Target>,
                }}
            "#
            );
            let item: ItemStruct = moxy::parse!({ declaration }).unwrap();
            let implementation: ItemImpl = moxy::parse!({ expand(item).unwrap() }).unwrap();
            assert!(
                matches!(implementation.generics.params.first(), Some(GenericParam::Type(param)) if param.ident == "T")
            );
            assert!(implementation.generics.where_clause.is_some());
            assert!(implementation.items.len() == 3);
            let install = implementation
                .items
                .iter()
                .find_map(|item| {
                    item.as_fn()
                        .filter(|method| method.sig.ident == "install_current_member")
                })
                .unwrap();
            let Some(FnParam::Typed(value)) = install.sig.params.inner.inputs.iter().nth(2) else {
                panic!("typed current record parameter")
            };
            assert!(compact(&value.ty.to_token_stream()) == "::records::Current<T>");
            assert!(compact(&install.sig.to_token_stream()).contains("implFnOnce()->Metadata<T>"));
            for name in ["member_tombstone", "remove_replayed_member"] {
                assert!(
                    implementation
                        .items
                        .iter()
                        .filter_map(ImplItem::as_fn)
                        .any(|method| method.sig.ident == name)
                );
            }
        }
    }

    #[test]
    fn rejects_unknown_protocols_incomplete_maps_and_wrong_record_containers() {
        for (declaration, expected) in [
            ("struct Seed {}", "needs `#[replay_member_maps"),
            (
                "#[replay_member_maps(protocol = \"classic\")] struct Seed {}",
                "replay protocol",
            ),
            (
                "#[replay_member_maps(protocol = 1)] struct Seed {}",
                "string",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed();",
                "named fields",
            ),
            (
                "#[replay_member_maps(protocol = \"consumer\")] struct Seed {}",
                "members",
            ),
            (
                "#[replay_member_maps(protocol = \"consumer\")] struct Seed { members: HashMap<String, M> }",
                "current_per_member",
            ),
            (
                "#[replay_member_maps(protocol = \"streams\")] struct Seed { members: HashMap<String, M>, current_per_member: HashMap<String, C> }",
                "target_per_member",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed { members: Vec<M> }",
                "HashMap",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap }",
                "HashMap",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap<u32, M> }",
                "HashMap",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap<String> }",
                "HashMap",
            ),
            (
                "#[replay_member_maps(protocol = \"share\")] struct Seed { members: (String, M) }",
                "HashMap",
            ),
        ] {
            let item: ItemStruct = moxy::parse!({ declaration }).unwrap();
            assert!(
                expand(item).unwrap_err().to_string().contains(expected),
                "{declaration}"
            );
        }
    }
}
