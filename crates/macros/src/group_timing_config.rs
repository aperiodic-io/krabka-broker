//! Per-group timing overrides for consumer and share membership configurations.

use {
    crate::meta::{Tokens, impl_block, named_fields},
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::TokenStream,
    },
};

#[derive(moxy::FromMeta)]
struct TimingKeys {
    session: Tokens,
    heartbeat: Tokens,
    assignment: Tokens,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let keys = item.parse_meta::<TimingKeys>("group_timing")?.ok_or_else(|| {
        ParseError::new(
            item.ident.span(),
            "`GroupTimingConfig` needs `#[group_timing(session = KEY, heartbeat = KEY, assignment = KEY)]`",
        )
    })?;
    let fields = named_fields(&item, "GroupTimingConfig")?;
    for name in [
        "session_timeout",
        "heartbeat_interval",
        "assignment_interval",
        "min_session_timeout",
        "max_session_timeout",
        "min_heartbeat_interval",
        "max_heartbeat_interval",
    ] {
        if !fields.iter().any(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == name)
        }) {
            return Err(ParseError::new(
                item.ident.span(),
                format!("`GroupTimingConfig` needs a `{name}` field"),
            ));
        }
    }
    let session = keys.session.0;
    let heartbeat = keys.heartbeat.0;
    let assignment = keys.assignment.0;
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        /// The defaults with no assignment interval, for tests that assign each
        /// membership change at once.
        #[cfg(test)]
        pub(crate) fn assigning_at_once() -> Self {
            Self { assignment_interval: ::std::time::Duration::ZERO, ..Self::default() }
        }

        /// Apply this protocol's stored group timings within the broker bounds.
        /// A group with no valid timing override borrows the broker defaults.
        #[must_use]
        pub(crate) fn for_group(
            &self,
            overrides: Option<&::std::collections::BTreeMap<String, String>>,
        ) -> ::std::borrow::Cow<'_, Self> {
            use crate::coordinator::unified::config::{
                group_millis, MIN_ASSIGNMENT_INTERVAL, MAX_ASSIGNMENT_INTERVAL,
            };
            let session = group_millis(
                overrides, {{ session }}, self.min_session_timeout, self.max_session_timeout,
            );
            let heartbeat = group_millis(
                overrides, {{ heartbeat }}, self.min_heartbeat_interval, self.max_heartbeat_interval,
            );
            let assignment = group_millis(
                overrides, {{ assignment }}, MIN_ASSIGNMENT_INTERVAL, MAX_ASSIGNMENT_INTERVAL,
            );
            if session.is_none() && heartbeat.is_none() && assignment.is_none() {
                ::std::borrow::Cow::Borrowed(self)
            } else {
                let mut config = self.clone();
                config.session_timeout = session.unwrap_or(config.session_timeout);
                config.heartbeat_interval = heartbeat.unwrap_or(config.heartbeat_interval);
                config.assignment_interval = assignment.unwrap_or(config.assignment_interval);
                ::std::borrow::Cow::Owned(config)
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        moxy::ast::{Fields, GenericParam, ImplItem, ItemImpl, WherePredicate},
    };

    fn complete_config() -> ItemStruct {
        let input = moxy::template! {
            #[group_timing(
                session = keys::SESSION,
                heartbeat = keys::HEARTBEAT,
                assignment = keys::ASSIGNMENT,
            )]
            struct Config<T> where T: Clone + Default {
                unrelated: T,
                max_heartbeat_interval: Duration,
                assignment_interval: Duration,
                heartbeat_interval: Duration,
                min_session_timeout: Duration,
                session_timeout: Duration,
                max_session_timeout: Duration,
                min_heartbeat_interval: Duration,
            }
        };
        moxy::parse!({ input }).unwrap()
    }

    #[test]
    fn complete_configs_emit_valid_methods_with_the_original_generic_contract() {
        let output = expand(complete_config()).expect("complete timing config");
        let implementation: ItemImpl = moxy::parse!({ output }).expect("valid complete impl");
        assert!(implementation.trait_ref.is_none());
        assert!(
            implementation
                .self_ty
                .as_path()
                .unwrap()
                .path
                .is_ident("Config")
        );
        assert!(matches!(implementation.generics.params.first(),
            Some(GenericParam::Type(parameter)) if parameter.ident == "T"));
        let bounds = implementation.generics.where_clause.as_ref().unwrap();
        assert!(matches!(bounds.predicates.first(),
            Some(WherePredicate::Type(predicate))
                if predicate.bounded_ty.as_path().unwrap().path.is_ident("T")
                    && predicate.bounds.len() == 2));
        let methods: Vec<_> = implementation
            .items
            .iter()
            .filter_map(ImplItem::as_fn)
            .map(|method| method.sig.ident.text())
            .collect();
        assert!(methods == ["assigning_at_once", "for_group"]);
    }

    #[test]
    fn reports_each_missing_timing_field_instead_of_accepting_a_partial_config() {
        for missing in [
            "session_timeout",
            "heartbeat_interval",
            "assignment_interval",
            "min_session_timeout",
            "max_session_timeout",
            "min_heartbeat_interval",
            "max_heartbeat_interval",
        ] {
            let mut item = complete_config();
            let Fields::Named(fields) = &mut item.fields else {
                panic!("named config fixture")
            };
            fields.fields.inner = fields
                .fields
                .inner
                .iter()
                .filter(|field| field.ident.as_ref().is_none_or(|ident| ident != missing))
                .cloned()
                .collect();
            let error = expand(item).expect_err("missing timing field");
            assert!(error.to_string().contains(&format!("a `{missing}` field")));
        }
    }

    #[test]
    fn refuses_missing_keys_and_fields() {
        let without_keys: ItemStruct =
            moxy::parse!("struct Config { session_timeout: Duration }").unwrap();
        assert!(
            expand(without_keys)
                .unwrap_err()
                .to_string()
                .contains("#[group_timing")
        );
        let without_fields: ItemStruct = moxy::parse!(
            "#[group_timing(session = SESSION, heartbeat = HEARTBEAT, assignment = ASSIGNMENT)] struct Config {}"
        ).unwrap();
        assert!(
            expand(without_fields)
                .unwrap_err()
                .to_string()
                .contains("session_timeout")
        );
        let incomplete_keys: ItemStruct =
            moxy::parse!("#[group_timing(session = SESSION)] struct Config {}").unwrap();
        assert!(expand(incomplete_keys).is_err());
    }
}
