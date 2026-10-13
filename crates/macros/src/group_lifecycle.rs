//! Common group transitions with explicit protocol-specific policies.

use {
    crate::meta::{impl_block, named_fields},
    moxy::{
        ast::{Attributed, FromMeta, ItemStruct, Meta, ParseError},
        token::{LitStr, Spanner, TokenStream},
    },
};

#[derive(Clone, Copy)]
enum Protocol {
    Consumer,
    Share,
    Streams,
}

impl FromMeta for Protocol {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        match String::from_meta(meta)?.as_str() {
            "consumer" => Ok(Self::Consumer),
            "share" => Ok(Self::Share),
            "streams" => Ok(Self::Streams),
            _ => Err(ParseError::new(
                meta.span(),
                "expected consumer, share or streams protocol",
            )),
        }
    }
}

#[derive(moxy::FromMeta)]
struct LifecycleArgs {
    protocol: Protocol,
}

const STREAMS_BUMP: &str = "Increments the group epoch. This mirrors the share and consumer state
machines: a fresh epoch makes the assignment stale, so the method marks
the group dirty.";
const SHARE_EVICTION: &str = "Remove members whose `last_seen` is older than `session_timeout`, and
return the evicted member ids.";
const STREAMS_EVICTION: &str =
    "Removes members whose `last_seen` is older than `session_timeout` and
returns the evicted member ids. The method marks the group dirty if it
removed any member.";
const CONSUMER_FENCE: &str = "Removes every member whose rebalance timeout fired at `now` while the
member still had partitions to revoke. Returns the removed member ids,
sorted.

This is the fence of Kafka's `scheduleConsumerGroupRebalanceTimeout`
(`consumerGroupFenceMember`). The removal frees the partitions the
member did not revoke, so their new owners can take them.

A hosted classic member never sends `ConsumerGroupHeartbeat`, so no
heartbeat arms its timeout. This pass arms it the first time it sees
the member with partitions to revoke. Kafka bounds the same member with
its join and sync timers, which also use the rebalance timeout. The
pass also cancels the deadlines that no longer apply, so a past
deadline never stays armed.";
const STREAMS_FENCE: &str = "Removes every member whose rebalance timeout fired at `now` while it
was still at the epoch that armed it, and returns the removed ids,
sorted. This is the fence of Kafka's
`scheduleStreamsGroupRebalanceTimeout`.";

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let args = item.parse_meta::<LifecycleArgs>("group_lifecycle")?.ok_or_else(|| {
        ParseError::new(item.ident.span(), "`GroupLifecycle` needs `#[group_lifecycle(protocol = \"consumer\" | \"share\" | \"streams\")]`")
    })?;
    let assignment_delay = !matches!(args.protocol, Protocol::Streams);
    let dirty_on_bump = matches!(args.protocol, Protocol::Streams);
    let rebalance_fence = !matches!(args.protocol, Protocol::Share);
    let fields = named_fields(&item, "GroupLifecycle")?;
    let mut required = ["group_epoch", "members"]
        .into_iter()
        .chain(assignment_delay.then_some("assignment_timestamp_ms"))
        .chain(dirty_on_bump.then_some("dirty"));
    if let Some(missing) = required.find(|name| {
        !fields
            .iter()
            .any(|field| field.ident.as_ref().is_some_and(|ident| ident == *name))
    }) {
        return Err(ParseError::new(
            item.ident.span(),
            format!("`GroupLifecycle` needs a `{missing}` field"),
        ));
    }
    let doc = |text| LitStr::new(text, item.ident.span());
    let bump_doc = dirty_on_bump.then(|| doc(STREAMS_BUMP));
    let eviction_doc = match args.protocol {
        Protocol::Consumer => None,
        Protocol::Share => Some(doc(SHARE_EVICTION)),
        Protocol::Streams => Some(doc(STREAMS_EVICTION)),
    };
    let fence_doc = doc(if dirty_on_bump {
        STREAMS_FENCE
    } else {
        CONSUMER_FENCE
    });
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        @if let Some(documentation) = &bump_doc { #[doc = {{ documentation }}] }
        pub fn bump_epoch(&mut self) -> bool {
            let Some(epoch) = crate::metadata_epoch::next_i32(self.group_epoch) else {
                return false;
            };
            self.group_epoch = epoch;
            @if dirty_on_bump { self.dirty = true; }
            true
        }

        /// The members whose session expired at `now`, without removing them.
        #[must_use]
        pub fn expired_members(
            &self,
            now: ::std::time::Instant,
            session_timeout: ::std::time::Duration,
        ) -> Vec<String> {
            crate::coordinator::unified::expired_member_ids(
                self.members.iter().map(|(id, member)| (id.as_str(), member.last_seen)),
                now,
                session_timeout,
            )
        }

        @if let Some(documentation) = &eviction_doc { #[doc = {{ documentation }}] }
        pub fn evict_expired(
            &mut self,
            now: ::std::time::Instant,
            session_timeout: ::std::time::Duration,
        ) -> Vec<String> {
            let expired = self.expired_members(now, session_timeout);
            for member_id in &expired {
                self.remove_member(member_id);
            }
            expired
        }

        @if assignment_delay {
            /// Kafka's `GroupMetadataManager.canComputeNextTargetAssignment`, negated:
            /// `true` while the assignment `interval` holds the next target
            /// assignment back at `now_ms`.
            #[must_use]
            pub(crate) fn assignment_delayed(&self, interval: ::std::time::Duration, now_ms: i64) -> bool {
                !crate::coordinator::unified::member_helpers::can_compute_next_target_assignment(
                    self.assignment_timestamp_ms, interval, now_ms,
                )
            }
        }

        @if rebalance_fence {
            #[doc = {{ fence_doc }}]
            pub fn fence_rebalance_timeouts(&mut self, now: ::std::time::Instant) -> Vec<String> {
                let fenced = self.rebalance_timeouts_due(now);
                for member_id in &fenced {
                    self.remove_member(member_id);
                }
                fenced
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        assert2::assert,
        moxy::{
            ast::{Fields, GenericParam, ImplItem, ItemImpl, ItemStruct, WherePredicate},
            token::LitStr,
        },
    };

    fn declaration(protocol: &str) -> ItemStruct {
        let input = moxy::template! {
            #[group_lifecycle(protocol = {{ LitStr::new(protocol, moxy::token::Span::call_site()) }})]
            struct State<T> where T: Clone {
                group_epoch: i32,
                members: HashMap<String, Member>,
                unrelated: Vec<T>,
                @if protocol == "streams" { dirty: bool, }
                @else { assignment_timestamp_ms: i64, }
            }
        };
        moxy::parse!({ input }).unwrap()
    }

    #[test]
    fn each_protocol_emits_only_its_existing_lifecycle_contract() {
        for protocol in ["consumer", "share", "streams"] {
            let output = expand(declaration(protocol)).unwrap();
            let implementation: ItemImpl = moxy::parse!({ output }).unwrap();
            assert!(matches!(implementation.generics.params.first(),
                Some(GenericParam::Type(parameter)) if parameter.ident == "T"));
            assert!(
                matches!(implementation.generics.where_clause.as_ref().unwrap().predicates.first(),
                Some(WherePredicate::Type(predicate))
                    if predicate.bounded_ty.as_path().unwrap().path.is_ident("T")
                        && predicate.bounds.len() == 1)
            );
            let names: Vec<_> = implementation
                .items
                .iter()
                .filter_map(ImplItem::as_fn)
                .map(|method| method.sig.ident.text())
                .collect();
            assert!(names.contains(&"bump_epoch"));
            assert!(names.contains(&"expired_members"));
            assert!(names.contains(&"evict_expired"));
            assert!(names.contains(&"assignment_delayed") == (protocol != "streams"));
            assert!(names.contains(&"fence_rebalance_timeouts") == (protocol != "share"));
        }
    }

    #[test]
    fn refuses_missing_protocol_unknown_protocol_and_nonnamed_layouts() {
        for input in [
            "struct State { group_epoch: i32, members: HashMap<String, Member> }",
            "#[group_lifecycle()] struct State {}",
            "#[group_lifecycle(protocol = \"classic\")] struct State {}",
            "#[group_lifecycle(protocol = 0)] struct State {}",
            "#[group_lifecycle(protocol = \"consumer\")] struct State(i32);",
        ] {
            let item: ItemStruct = moxy::parse!({ input }).unwrap();
            assert!(expand(item).is_err());
        }
    }

    #[test]
    fn refuses_each_required_field_for_the_selected_protocol() {
        struct RequiredField {
            protocol: &'static str,
            missing: &'static str,
        }
        for case in [
            RequiredField {
                protocol: "consumer",
                missing: "group_epoch",
            },
            RequiredField {
                protocol: "consumer",
                missing: "members",
            },
            RequiredField {
                protocol: "share",
                missing: "assignment_timestamp_ms",
            },
            RequiredField {
                protocol: "streams",
                missing: "dirty",
            },
        ] {
            let missing = case.missing;
            let mut item = declaration(case.protocol);
            let Fields::Named(fields) = &mut item.fields else {
                panic!("named state fixture")
            };
            let mut members: Vec<_> = std::mem::take(&mut fields.fields.inner)
                .into_iter()
                .collect();
            let position = members
                .iter()
                .position(|field| field.ident.as_ref().is_some_and(|ident| ident == missing))
                .unwrap();
            members.remove(position);
            fields.fields.inner = members.into_iter().collect();
            assert!(expand(item).unwrap_err().to_string().contains(missing));
        }
    }
}
