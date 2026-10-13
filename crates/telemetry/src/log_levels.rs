//! Runtime control of the stdout log filter, which is what Kafka's
//! `BROKER_LOGGER` config resource alters.
//!
//! A JVM broker keeps a live log4j2 `LoggerContext`. `kafka-configs
//! --entity-type broker-loggers --describe` reads every logger out of it, and
//! `--alter` writes one logger's level back into it. Neither call touches
//! cluster metadata: the change lives in the node that served the request and
//! is gone when that node restarts. This module is the same thing for
//! `tracing`.
//!
//! ## The model
//!
//! [`LogLevelController`] owns one editable [`EnvFilter`] behind a lock and
//! the *model* it was rendered from: a root level and a per-target level map.
//! Every level change mutates the model, re-renders it into a fresh
//! `EnvFilter`, swaps it in, and rebuilds the callsite interest cache so a
//! level *raise* takes effect at callsites `tracing` had already cached as
//! disabled. Mutation, render and swap happen under one lock, so a describe
//! never reports a level the live filter does not yet carry, and two
//! concurrent alterations cannot install their filters in the opposite order
//! from their model edits.
//!
//! The directives the model cannot express — a span or field directive such
//! as `[my_span{field=1}]=debug` — go into a second `EnvFilter` that is built
//! once and never replaced. `EnvFilter` keeps the state of the spans a
//! dynamic directive matches inside itself, and that state cannot be
//! transplanted into a new filter, so a rebuild would silence the events
//! inside a span that was already open. A callsite is enabled when either
//! filter enables it, which is exactly how one `EnvFilter` combines its
//! dynamic and static directives.
//!
//! [`LogLevelFilter`] is the [`Filter`] side of the same state. It delegates
//! to the carried filter and to whichever editable `EnvFilter` is current,
//! and records the target of every callsite it is asked about, which is how
//! the controller learns that a logger exists. `tracing` offers a new
//! callsite to every registered subscriber, so the recorded set covers the
//! whole process and not only the layer this filter is attached to. Together
//! with the targets the starting spec names, it answers to log4j2's
//! `LoggerContext.getLoggers()` plus the loggers a `log4j2.properties`
//! declares: a name stays a logger once it is known, whatever its level is
//! later set to or cleared back to.
//!
//! ## Levels
//!
//! Kafka's `LogLevelConfig.VALID_LOG_LEVELS` is `FATAL, ERROR, WARN, INFO,
//! DEBUG, TRACE`. `tracing` has no `FATAL`, so [`LogLevel::Fatal`] renders as
//! the `off` directive: no krabka event is more severe than `ERROR`, so a
//! target held at `FATAL` emits nothing, which is exactly what log4j2 does
//! with the same setting. The mapping round-trips, so a `FATAL` an operator
//! writes is the `FATAL` the describe reads back.

use {
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
    },
    tracing::{Metadata, span},
    tracing_core::{Interest, callsite},
    tracing_subscriber::{
        EnvFilter,
        filter::LevelFilter,
        layer::{Context, Filter},
    },
};

#[cfg(test)]
mod tests;

/// The name log4j2 and Kafka both give the level every unmatched target
/// falls back to. `kafka-configs` prints it beside the real loggers.
pub const ROOT_LOGGER: &str = "root";

/// The level names a `BROKER_LOGGER` value may carry, in the order Kafka
/// renders them into its rejection message. `RuntimeLoggerManager` builds that
/// list as `String.join(", ", sorted(LogLevelConfig.VALID_LOG_LEVELS))`, so a
/// caller joins these with `", "` and not with a bare comma.
pub const VALID_LOG_LEVELS: [&str; 6] = ["DEBUG", "ERROR", "FATAL", "INFO", "TRACE", "WARN"];

/// One level in Kafka's `BROKER_LOGGER` vocabulary.
///
/// The order is the log4j2 one, least verbose first, so `Trace > Info`.
///
/// `kafka_name` is the uppercase name Kafka puts on the wire, and
/// `from_kafka_name` parses a `BROKER_LOGGER` config value. Kafka tests the
/// value against a `Set<String>` of uppercase names, so a lowercase `info` is
/// rejected there and is rejected here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, krabka_macros::EnumStr)]
#[enum_str(case = "UPPERCASE", as_str = kafka_name, parse = from_kafka_name)]
pub enum LogLevel {
    /// Nothing krabka emits reaches this level, so it silences the target.
    Fatal,
    /// `ERROR` and above.
    Error,
    /// `WARN` and above.
    Warn,
    /// `INFO` and above.
    Info,
    /// `DEBUG` and above.
    Debug,
    /// Everything.
    Trace,
}

impl LogLevel {
    /// The `EnvFilter` level this renders as.
    const fn directive(self) -> &'static str {
        match self {
            Self::Fatal => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }

    /// Parse one `EnvFilter` level.
    ///
    /// `EnvFilter` hands the level of a directive to `LevelFilter`'s
    /// `FromStr`, which takes a case-insensitive name *or* a number from `0`
    /// to `5`, so `foo=4` is `foo=debug` and a bare `3` is the root at
    /// `INFO`. The empty level of a `foo=` directive never reaches it: the
    /// directive parser reads that as `TRACE` itself, which is not what
    /// `FromStr` would say. The name `EnvFilter` gives the level Kafka calls
    /// `FATAL` is `off`.
    fn from_directive(value: &str) -> Option<Self> {
        if value.is_empty() {
            return Some(Self::Trace);
        }
        if let Ok(number) = value.parse::<usize>() {
            return match number {
                0 => Some(Self::Fatal),
                1 => Some(Self::Error),
                2 => Some(Self::Warn),
                3 => Some(Self::Info),
                4 => Some(Self::Debug),
                5 => Some(Self::Trace),
                _ => None,
            };
        }
        match value.to_ascii_lowercase().as_str() {
            "off" => Some(Self::Fatal),
            "error" => Some(Self::Error),
            "warn" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }
}

/// The filter as a set of parts that can be edited and rendered back.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Model {
    /// The level a target with no matching directive gets. An `EnvFilter`
    /// with no bare-level directive disables everything it does not match,
    /// which is [`LogLevel::Fatal`].
    root: LogLevel,
    /// Explicit per-target levels. `EnvFilter` matches a target directive by
    /// string prefix and the longest match wins, so this map is read the same
    /// way. A target drops out of here when its level is cleared; it stays a
    /// logger, because [`Shared::known`] remembers the name.
    targets: BTreeMap<String, LogLevel>,
}

impl Model {
    /// Split an `EnvFilter` spec into the parts this module edits and the
    /// ones it only carries.
    ///
    /// A carried directive is a span or field directive, which is not a
    /// logger, or text `EnvFilter` itself rejects, which it drops with a
    /// warning either way.
    fn parse(spec: &str) -> (Self, Vec<String>) {
        let mut model = Self {
            root: LogLevel::Fatal,
            targets: BTreeMap::new(),
        };
        let mut carried = Vec::new();
        for directive in spec.split(',').map(str::trim).filter(|d| !d.is_empty()) {
            model.absorb(directive, &mut carried);
        }
        (model, carried)
    }

    /// Fold one directive into the model, or hand it to `carried` verbatim.
    fn absorb(&mut self, directive: &str, carried: &mut Vec<String>) {
        // A span directive (`[name{field}]=level`) or a field directive is
        // not a logger, so the model only carries it.
        if directive.contains('[') {
            carried.push(directive.to_owned());
            return;
        }
        match directive.split_once('=') {
            Some((target, level)) => match LogLevel::from_directive(level) {
                Some(level) if !target.is_empty() => {
                    self.targets.insert(target.to_owned(), level);
                }
                _ => carried.push(directive.to_owned()),
            },
            // A bare word is a level when it names one and a target held at
            // `TRACE` otherwise, which is how `EnvFilter` reads it.
            None => match LogLevel::from_directive(directive) {
                Some(level) => self.root = level,
                None => {
                    self.targets.insert(directive.to_owned(), LogLevel::Trace);
                }
            },
        }
    }

    /// Render the model back into an `EnvFilter` spec.
    fn render(&self) -> String {
        let mut parts = Vec::with_capacity(self.targets.len() + 1);
        parts.push(self.root.directive().to_owned());
        parts.extend(
            self.targets
                .iter()
                .map(|(target, level)| format!("{target}={}", level.directive())),
        );
        parts.join(",")
    }

    /// The level `target` resolves to: the longest directive that prefixes
    /// it, or the root level.
    fn effective(&self, target: &str) -> LogLevel {
        self.targets
            .iter()
            .filter(|(name, _)| target.starts_with(name.as_str()))
            .max_by_key(|(name, _)| name.len())
            .map_or(self.root, |(_, level)| *level)
    }
}

/// The state a [`LogLevelController`] and its [`LogLevelFilter`]s share.
#[derive(Debug)]
struct Shared {
    /// The editable filter. Replaced wholesale on a level change.
    filter: RwLock<EnvFilter>,
    /// The span and field directives the starting spec carried, in a filter
    /// of their own that no level change replaces. `EnvFilter` holds the
    /// state of the spans its dynamic directives match, and a replacement
    /// would start with none of it, so the events inside a span that was
    /// already open would stop matching.
    carried: EnvFilter,
    /// The parts [`Shared::filter`] was rendered from.
    model: RwLock<Model>,
    /// Every logger name this node has: the targets the starting spec named
    /// and the targets `tracing` has offered a callsite for.
    known: RwLock<BTreeSet<String>>,
}

/// Read a lock, taking the guard even when a panic poisoned it.
///
/// A poisoned lock must not take the log filter down with it: the state
/// behind it is a filter and a name map, and a torn write leaves neither in a
/// shape that can harm a reader.
fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Write a lock, taking the guard even when a panic poisoned it. See
/// [`read`].
fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

/// Record `name` as a logger this node has.
///
/// The read comes first because the common case is a name already known, and
/// a callsite registration must not queue behind a writer to find that out.
fn remember(shared: &Shared, name: &str) {
    if !read(&shared.known).contains(name) {
        write(&shared.known).insert(name.to_owned());
    }
}

/// The handle that reads and retargets the live log filter.
///
/// Cloning shares one filter: every clone reads and writes the same state,
/// which is what lets the broker hold one in its config while the installed
/// subscriber holds the [`LogLevelFilter`] side.
#[derive(Clone, Debug)]
pub struct LogLevelController {
    shared: Arc<Shared>,
}

/// The [`Filter`] side of a [`LogLevelController`].
///
/// Attach it to the layer whose verbosity `BROKER_LOGGER` should control:
/// `layer.with_filter(filter)`.
#[derive(Debug)]
pub struct LogLevelFilter {
    shared: Arc<Shared>,
}

impl LogLevelController {
    /// Build a controller over the `EnvFilter` spec `spec`, and the filter it
    /// drives.
    ///
    /// `spec` is the ordinary `RUST_LOG` syntax. Its target directives seed
    /// the logger list, the way the loggers named in a `log4j2.properties`
    /// are listed before anything has logged through them.
    #[must_use]
    pub fn new(spec: &str) -> (Self, LogLevelFilter) {
        let (model, carried) = Model::parse(spec);
        let known = model.targets.keys().cloned().collect();
        let shared = Arc::new(Shared {
            filter: RwLock::new(EnvFilter::new(model.render())),
            // The builder and not `EnvFilter::new`: an empty carried spec
            // must enable nothing, and `EnvFilter::new` would read it as the
            // default `ERROR` directive and enable everything at that level.
            carried: EnvFilter::builder().parse_lossy(carried.join(",")),
            model: RwLock::new(model),
            known: RwLock::new(known),
        });
        (
            Self {
                shared: Arc::clone(&shared),
            },
            LogLevelFilter { shared },
        )
    }

    /// Another filter over the same state, for a second layer.
    #[must_use]
    pub fn filter(&self) -> LogLevelFilter {
        LogLevelFilter {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Every logger this node knows, with its effective level.
    ///
    /// That is every target a callsite has been registered under, every
    /// target the starting spec named, and [`ROOT_LOGGER`].
    #[must_use]
    pub fn loggers(&self) -> BTreeMap<String, LogLevel> {
        let model = read(&self.shared.model);
        let mut out: BTreeMap<String, LogLevel> = read(&self.shared.known)
            .iter()
            .map(|target| (target.clone(), model.effective(target)))
            .collect();
        out.insert(ROOT_LOGGER.to_owned(), model.root);
        out
    }

    /// The effective level of one logger, or `None` when no such logger
    /// exists. Mirrors log4j2's `loggerExists` plus `getLevel`.
    #[must_use]
    pub fn level(&self, name: &str) -> Option<LogLevel> {
        let model = read(&self.shared.model);
        if name == ROOT_LOGGER {
            return Some(model.root);
        }
        read(&self.shared.known)
            .contains(name)
            .then(|| model.effective(name))
    }

    /// Whether a logger by this name exists on this node.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool { self.level(name).is_some() }

    /// Pin `name` to `level`, in this process only.
    ///
    /// [`ROOT_LOGGER`] sets the level every unmatched target falls back to.
    /// Any other name becomes a target directive, which also covers the
    /// targets below it: `krabka_broker` at `DEBUG` puts
    /// `krabka_broker::handlers::produce` at `DEBUG` too.
    pub fn set_level(&self, name: &str, level: LogLevel) {
        if name != ROOT_LOGGER {
            remember(&self.shared, name);
        }
        self.edit(|model| {
            if name == ROOT_LOGGER {
                model.root = level;
            } else {
                model.targets.insert(name.to_owned(), level);
            }
        });
    }

    /// Drop `name`'s own level so it inherits again, and report whether it
    /// had one.
    ///
    /// [`ROOT_LOGGER`] never has a level of its own to drop — it *is* the
    /// level everything else inherits — so it always reports `false`. Kafka
    /// refuses a DELETE of the root logger for that reason.
    #[must_use]
    pub fn clear_level(&self, name: &str) -> bool {
        if name == ROOT_LOGGER {
            return false;
        }
        let mut removed = false;
        self.edit(|model| removed = model.targets.remove(name).is_some());
        removed
    }

    /// Apply `mutate` to the model, re-render the filter, and make `tracing`
    /// re-ask every callsite.
    ///
    /// The rebuild is what makes a *raise* work: `tracing` caches a callsite
    /// the old filter disabled and never consults the filter for it again
    /// until the cache is dropped.
    fn edit(&self, mutate: impl FnOnce(&mut Model)) {
        {
            // The model lock is held across the install, so one alteration
            // at a time mutates, renders and swaps. Two that overlapped
            // could otherwise render `A` and `A+B` and install them in the
            // opposite order, leaving the filter at `A` while every describe
            // reported `A+B`.
            let mut model = write(&self.shared.model);
            mutate(&mut model);
            let spec = model.render();
            *write(&self.shared.filter) = EnvFilter::new(spec);
        }
        // Outside the locks: the rebuild calls back into
        // `LogLevelFilter::callsite_enabled`, which reads them.
        callsite::rebuild_interest_cache();
    }
}

/// The interest of a callsite either filter may enable.
///
/// `tracing` caches an `always` and never asks again, which is right only
/// when a filter admits the callsite unconditionally; it drops a `never`
/// callsite outright, which is right only when neither filter can admit it.
/// Everything in between is `sometimes`, and [`Filter::enabled`] settles it
/// per event.
fn either(a: &Interest, b: &Interest) -> Interest {
    if a.is_always() || b.is_always() {
        Interest::always()
    } else if a.is_never() && b.is_never() {
        Interest::never()
    } else {
        Interest::sometimes()
    }
}

impl<S> Filter<S> for LogLevelFilter {
    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        Filter::<S>::enabled(&self.shared.carried, meta, cx)
            || Filter::<S>::enabled(&*read(&self.shared.filter), meta, cx)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        remember(&self.shared, meta.target());
        either(
            &Filter::<S>::callsite_enabled(&self.shared.carried, meta),
            &Filter::<S>::callsite_enabled(&*read(&self.shared.filter), meta),
        )
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        // `None` is `EnvFilter` declining to bound the level, so it wins
        // over any bound the other filter offers.
        let carried = Filter::<S>::max_level_hint(&self.shared.carried)?;
        let editable = Filter::<S>::max_level_hint(&*read(&self.shared.filter))?;
        Some(carried.max(editable))
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, cx: Context<'_, S>) {
        Filter::<S>::on_new_span(&self.shared.carried, attrs, id, cx.clone());
        Filter::<S>::on_new_span(&*read(&self.shared.filter), attrs, id, cx);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, cx: Context<'_, S>) {
        Filter::<S>::on_record(&self.shared.carried, id, values, cx.clone());
        Filter::<S>::on_record(&*read(&self.shared.filter), id, values, cx);
    }

    fn on_enter(&self, id: &span::Id, cx: Context<'_, S>) {
        Filter::<S>::on_enter(&self.shared.carried, id, cx.clone());
        Filter::<S>::on_enter(&*read(&self.shared.filter), id, cx);
    }

    fn on_exit(&self, id: &span::Id, cx: Context<'_, S>) {
        Filter::<S>::on_exit(&self.shared.carried, id, cx.clone());
        Filter::<S>::on_exit(&*read(&self.shared.filter), id, cx);
    }

    fn on_close(&self, id: span::Id, cx: Context<'_, S>) {
        Filter::<S>::on_close(&self.shared.carried, id.clone(), cx.clone());
        Filter::<S>::on_close(&*read(&self.shared.filter), id, cx);
    }
}
