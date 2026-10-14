//! Behavior of the live log filter: what it lists, what it retargets, and
//! what a retarget does to the events a subscriber actually sees.

use {
    super::{LogLevel, LogLevelController, ROOT_LOGGER},
    assert2::{assert, check},
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    tracing_subscriber::{Layer, layer::SubscriberExt as _},
};

krabka_macros::capture_layer_fixture!(CaptureLayer, Captured, recover_poison);

/// A dispatcher and the capture it records into.
struct CapturingDispatch {
    dispatch: tracing::Dispatch,
    captured: Captured,
}

/// A dispatcher that captures through `controller`'s filter.
///
/// The `Dispatch` registers itself with `tracing` when it is built and stays
/// registered while it is alive, so a rebuild of the interest cache consults
/// it even between the `with_default` calls that make it current.
fn capturing_dispatch(controller: &LogLevelController) -> CapturingDispatch {
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(CaptureLayer(Arc::clone(&captured)).with_filter(controller.filter()));
    CapturingDispatch {
        dispatch: tracing::Dispatch::new(subscriber),
        captured,
    }
}

/// Everything the capture holds, drained.
fn drain(captured: &Captured) -> Vec<String> {
    std::mem::take(
        &mut *captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

#[yare::parameterized(
    fatal = { LogLevel::Fatal },
    error = { LogLevel::Error },
    warn = { LogLevel::Warn },
    info = { LogLevel::Info },
    debug = { LogLevel::Debug },
    trace = { LogLevel::Trace },
)]
fn kafka_level_names_round_trip(level: LogLevel) {
    check!(LogLevel::from_kafka_name(level.kafka_name()) == Some(level));
}

#[yare::parameterized(
    lowercase = { "info" },
    off = { "OFF" },
    empty = { "" },
)]
fn kafka_level_names_reject_non_levels(name: &str) {
    check!(LogLevel::from_kafka_name(name) == None);
}

#[test]
fn directives_in_the_spec_seed_the_logger_list() {
    let (controller, _filter) = LogLevelController::new("warn,krabka_broker=info,krabka_log=debug");

    let loggers = controller.loggers();
    assert!(loggers.get(ROOT_LOGGER) == Some(&LogLevel::Warn));
    assert!(loggers.get("krabka_broker") == Some(&LogLevel::Info));
    assert!(loggers.get("krabka_log") == Some(&LogLevel::Debug));
    // A target no directive names and no callsite has registered under is
    // not a logger yet.
    check!(controller.contains("never_seen_target") == false);
    check!(controller.level("never_seen_target") == None);
}

#[test]
fn a_spec_with_no_bare_level_holds_the_root_at_fatal() {
    // `EnvFilter` disables every target its directives do not name, and the
    // Kafka name for a level nothing krabka emits reaches is FATAL.
    let (controller, _filter) = LogLevelController::new("krabka_broker=info");
    assert!(controller.level(ROOT_LOGGER) == Some(LogLevel::Fatal));
}

#[test]
fn a_target_level_covers_the_targets_below_it() {
    let (controller, filter) = LogLevelController::new("info");
    let CapturingDispatch { dispatch, .. } = capturing_dispatch(&controller);
    drop(filter);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!(target: "seed_parent::child", "register the callsite");
    });
    assert!(controller.level("seed_parent::child") == Some(LogLevel::Info));

    controller.set_level("seed_parent", LogLevel::Trace);
    assert!(controller.level("seed_parent::child") == Some(LogLevel::Trace));
    assert!(controller.level(ROOT_LOGGER) == Some(LogLevel::Info));
}

#[test]
fn raising_a_level_enables_a_callsite_the_old_filter_had_disabled() {
    let (controller, _filter) = LogLevelController::new("info");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "raise_target", "before");
    });
    assert!(drain(&captured) == Vec::<String>::new());

    controller.set_level("raise_target", LogLevel::Debug);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "raise_target", "after");
    });
    assert!(drain(&captured) == vec!["raise_target:DEBUG".to_string()]);

    // The describe surface reports the same level the events prove.
    assert!(controller.level("raise_target") == Some(LogLevel::Debug));
}

#[test]
fn fatal_silences_a_target_and_reads_back_as_fatal() {
    let (controller, _filter) = LogLevelController::new("trace");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::error!(target: "silence_target", "before");
    });
    assert!(drain(&captured) == vec!["silence_target:ERROR".to_string()]);

    controller.set_level("silence_target", LogLevel::Fatal);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::error!(target: "silence_target", "after");
    });
    assert!(drain(&captured) == Vec::<String>::new());
    assert!(controller.level("silence_target") == Some(LogLevel::Fatal));
}

#[test]
fn clearing_a_level_puts_a_target_back_on_the_root() {
    let (controller, _filter) = LogLevelController::new("info,clear_target=debug");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "clear_target", "before");
    });
    assert!(drain(&captured) == vec!["clear_target:DEBUG".to_string()]);

    assert!(controller.clear_level("clear_target"));

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "clear_target", "after");
    });
    assert!(drain(&captured) == Vec::<String>::new());
    // The target is still a logger — a callsite has registered under it —
    // and it now reads the root level.
    assert!(controller.level("clear_target") == Some(LogLevel::Info));
    // Clearing it twice reports that there was nothing left to clear.
    check!(controller.clear_level("clear_target") == false);
}

#[test]
fn the_root_logger_has_no_level_of_its_own_to_clear() {
    let (controller, _filter) = LogLevelController::new("info");
    check!(controller.clear_level(ROOT_LOGGER) == false);
    check!(controller.level(ROOT_LOGGER) == Some(LogLevel::Info));

    controller.set_level(ROOT_LOGGER, LogLevel::Trace);
    check!(controller.level(ROOT_LOGGER) == Some(LogLevel::Trace));
}

#[test]
fn a_span_directive_survives_a_level_change() {
    // `[span]=level` is not a logger. It is carried verbatim, so a change to
    // an unrelated level must not drop it.
    let (controller, _filter) = LogLevelController::new("off,[carried_span]=debug");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    check!(controller.contains("[carried_span]=debug") == false);
    check!(controller.loggers().keys().all(|name| !name.contains('[')));

    controller.set_level("unrelated_target", LogLevel::Warn);

    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("carried_span");
        let _entered = span.enter();
        tracing::debug!(target: "inside_span_target", "still on");
    });
    assert!(drain(&captured).contains(&"inside_span_target:DEBUG".to_string()));
}

#[test]
fn a_span_open_before_a_level_change_keeps_its_directive() {
    // A dynamic directive matches through the state `EnvFilter` builds when
    // the span opens. A level change must not throw that state away, or the
    // events inside a span that is already open go quiet.
    let (controller, _filter) = LogLevelController::new("off,[open_span]=debug");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("open_span");
        let _entered = span.enter();
        tracing::debug!(target: "inside_open_span", "before");
        controller.set_level("unrelated_target", LogLevel::Warn);
        tracing::debug!(target: "inside_open_span", "after");
    });

    assert!(drain(&captured) == vec!["inside_open_span:DEBUG".to_string(); 2]);
}

#[yare::parameterized(
    numeric_fatal = { "numeric=0", LogLevel::Fatal },
    numeric_error = { "numeric=1", LogLevel::Error },
    numeric_warn = { "numeric=2", LogLevel::Warn },
    numeric_info = { "numeric=3", LogLevel::Info },
    numeric_debug = { "numeric=4", LogLevel::Debug },
    numeric_trace = { "numeric=5", LogLevel::Trace },
    named_debug = { "numeric=DEBUG", LogLevel::Debug },
    empty_level = { "numeric=", LogLevel::Trace },
)]
fn every_level_syntax_env_filter_takes_is_an_editable_level(
    directive: &'static str,
    expected: LogLevel,
) {
    // `EnvFilter` hands a directive's level to `LevelFilter`'s `FromStr`,
    // which reads a number from 0 to 5 as well as a name. The empty level of
    // a `target=` directive it reads itself, as TRACE.
    let (controller, _filter) = LogLevelController::new(&format!("info,{directive}"));
    check!(
        controller.loggers().get("numeric") == Some(&expected),
        "{directive}"
    );

    // A bare number is the root level, the same as a bare name.
    let (controller, _filter) = LogLevelController::new("4");
    check!(controller.level(ROOT_LOGGER) == Some(LogLevel::Debug));
    check!(controller.contains("4") == false);
}

#[test]
fn a_numeric_directive_does_not_outrank_a_later_alteration() {
    // `RUST_LOG=info,lower_target=4` is `lower_target` at DEBUG. Lowering it
    // to WARN has to silence its DEBUG events and read back as WARN.
    let (controller, _filter) = LogLevelController::new("info,lower_target=4");
    let CapturingDispatch { dispatch, captured } = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "lower_target", "before");
    });
    assert!(drain(&captured) == vec!["lower_target:DEBUG".to_string()]);
    assert!(controller.level("lower_target") == Some(LogLevel::Debug));

    controller.set_level("lower_target", LogLevel::Warn);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "lower_target", "after");
    });
    assert!(drain(&captured) == Vec::<String>::new());
    assert!(controller.level("lower_target") == Some(LogLevel::Warn));
}

#[test]
fn a_level_change_is_one_update_of_the_model_and_the_filter() {
    // Two alterations that overlapped could render `A` and `A+B` and install
    // them in the opposite order, leaving the live filter at `A` while every
    // describe reported `A+B`. Blocking the install proves the two halves
    // are one update: while no filter can be installed, no describe may
    // report the level that install would carry.
    let (controller, _filter) = LogLevelController::new("info");
    let install_blocked = super::write(&controller.shared.filter);

    let altering = std::thread::spawn({
        let controller = controller.clone();
        move || controller.set_level("serialized_target", LogLevel::Debug)
    });
    let stop = Arc::new(AtomicBool::new(false));
    let (reported, observed) = std::sync::mpsc::channel();
    let describing = std::thread::spawn({
        let controller = controller.clone();
        let stop = Arc::clone(&stop);
        move || {
            // Only the altered level counts. `set_level` records the name as
            // a logger before it touches the model, so a describe that lands
            // in that window reads the root level, which is not the level
            // this install would carry.
            while !stop.load(Ordering::Relaxed) {
                if controller.level("serialized_target") == Some(LogLevel::Debug) {
                    let _ = reported.send(());
                    return;
                }
                std::thread::yield_now();
            }
        }
    });

    let seen = observed.recv_timeout(std::time::Duration::from_secs(2));
    check!(
        seen == Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "a describe reported DEBUG while the filter carrying it could not be installed"
    );

    drop(install_blocked);
    stop.store(true, Ordering::Relaxed);
    altering.join().expect("the altering thread panicked");
    describing.join().expect("the describing thread panicked");
    assert!(controller.level("serialized_target") == Some(LogLevel::Debug));
}
