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

struct ReplayProtocol {
    variant: Ident,
}

impl FromMeta for ReplayProtocol {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let requested = String::from_meta(meta)?;
        let variant = match requested.as_str() {
            "consumer" => "Consumer",
            "share" => "Share",
            "streams" => "Streams",
            _ => {
                return Err(ParseError::new(
                    meta.span(),
                    "replay protocol must be consumer, share or streams",
                ));
            }
        };
        Ok(Self {
            variant: Ident::new(variant).with_span(meta.span()),
        })
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
    let protocol = options.protocol.variant;
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
        moxy::{
            ast::{FnParam, GenericParam, ImplItem, ItemImpl, ItemStruct},
            token::ToTokenStream,
        },
    };

    fn expanded_protocol(protocol: &str) -> ItemImpl {
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
        moxy::parse!({ expand(item).unwrap() }).unwrap()
    }

    #[yare::parameterized(
        consumer_protocol = { "consumer" },
        share_protocol = { "share" },
        streams_protocol = { "streams" },
    )]
    fn emits_typed_replay_methods_for_each_protocol_and_preserves_generics(protocol: &str) {
        let implementation = expanded_protocol(protocol);
        assert2::assert!(
            matches!(implementation.generics.params.first(), Some(GenericParam::Type(param)) if param.ident == "T")
        );
        assert2::assert!(implementation.generics.where_clause.is_some());
        assert2::assert!(implementation.items.len() == 3);
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
        assert2::assert!(compact(&value.ty.to_token_stream()) == "::records::Current<T>");
        assert2::assert!(
            compact(&install.sig.to_token_stream()).contains("implFnOnce()->Metadata<T>")
        );
    }

    #[yare::parameterized(
        consumer_tombstone = { "consumer", "member_tombstone" },
        consumer_remove = { "consumer", "remove_replayed_member" },
        share_tombstone = { "share", "member_tombstone" },
        share_remove = { "share", "remove_replayed_member" },
        streams_tombstone = { "streams", "member_tombstone" },
        streams_remove = { "streams", "remove_replayed_member" },
    )]
    fn emits_each_protocol_method(protocol: &str, name: &str) {
        let implementation = expanded_protocol(protocol);
        assert2::assert!(
            implementation
                .items
                .iter()
                .filter_map(ImplItem::as_fn)
                .any(|method| method.sig.ident == name)
        );
    }

    #[yare::parameterized(
        missing_metadata = { "struct Seed {}", "needs `#[replay_member_maps" },
        unknown_protocol = {
            "#[replay_member_maps(protocol = \"classic\")] struct Seed {}",
            "replay protocol",
        },
        non_string_protocol = { "#[replay_member_maps(protocol = 1)] struct Seed {}", "string" },
        tuple_seed = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed();",
            "named fields",
        },
        missing_members = {
            "#[replay_member_maps(protocol = \"consumer\")] struct Seed {}",
            "members",
        },
        missing_current_map = {
            "#[replay_member_maps(protocol = \"consumer\")] struct Seed { members: HashMap<String, M> }",
            "current_per_member",
        },
        missing_target_map = {
            "#[replay_member_maps(protocol = \"streams\")] struct Seed { members: HashMap<String, M>, current_per_member: HashMap<String, C> }",
            "target_per_member",
        },
        vector_members = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed { members: Vec<M> }",
            "HashMap",
        },
        bare_hash_map = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap }",
            "HashMap",
        },
        wrong_hash_key = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap<u32, M> }",
            "HashMap",
        },
        missing_hash_value = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed { members: HashMap<String> }",
            "HashMap",
        },
        non_hash_map_members = {
            "#[replay_member_maps(protocol = \"share\")] struct Seed { members: (String, M) }",
            "HashMap",
        },
    )]
    fn rejects_unknown_protocols_incomplete_maps_and_wrong_record_containers(
        declaration: &str,
        expected: &str,
    ) {
        let item: ItemStruct = moxy::parse!({ declaration }).unwrap();
        assert2::assert!(
            expand(item).unwrap_err().to_string().contains(expected),
            "{declaration}"
        );
    }
}
