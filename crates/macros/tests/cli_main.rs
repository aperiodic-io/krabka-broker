//! The `main` that `cli_main!` writes, run in a child copy of this test
//! binary so that its `std::process::exit` ends the child and not the suite.

use {assert2::assert, std::process::Command};

/// The exit code a child's `run_from_args` returns. Its presence marks the
/// child.
const CHILD_EXIT: &str = "KRABKA_CLI_MAIN_CHILD_EXIT";
/// The runtime flavor a child's `run_from_args` expects to run on.
const CHILD_FLAVOR: &str = "KRABKA_CLI_MAIN_CHILD_FLAVOR";

mod fake_cli {
    use assert2::assert;

    /// Checks that the generated `main` passed the process arguments through,
    /// installed a tracing subscriber and built the runtime the parent asked
    /// for, then answers the exit code the parent asked for. A failed check
    /// panics, which exits the child with 101.
    pub async fn run_from_args(args: std::env::ArgsOs) -> i32 {
        tokio::task::yield_now().await;
        assert!(args.eq(std::env::args_os()));
        assert!(tracing::dispatcher::has_been_set());
        let flavor = format!("{:?}", tokio::runtime::Handle::current().runtime_flavor());
        assert!(std::env::var(super::CHILD_FLAVOR).ok() == Some(flavor));
        std::env::var(super::CHILD_EXIT)
            .expect("child exit code")
            .parse()
            .expect("an i32")
    }
}

mod default_runtime {
    krabka_macros::cli_main!(super::fake_cli);

    pub fn run() { main(); }
}

mod single_threaded {
    krabka_macros::cli_main!(super::fake_cli, flavor = "current_thread");

    pub fn run() { main(); }
}

/// Runs `test` in a child copy of this binary whose `run_from_args` expects
/// `flavor` and returns `code`, and answers the child's exit code.
fn child_exit_code(test: &str, flavor: &str, code: i32) -> Option<i32> {
    Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", test])
        .env(CHILD_EXIT, code.to_string())
        .env(CHILD_FLAVOR, flavor)
        .stdout(std::process::Stdio::null())
        .status()
        .expect("child test")
        .code()
}

fn child_test_name(run: fn()) -> String {
    if std::env::var_os(CHILD_EXIT).is_some() {
        run();
    }
    std::thread::current()
        .name()
        .expect("test thread name")
        .to_owned()
}

#[yare::parameterized(
    success = { 0 },
    ordinary_failure = { 1 },
    usage_error = { 2 },
)]
fn main_exits_with_what_run_from_args_returns(code: i32) {
    let test = child_test_name(default_runtime::run);
    assert2::assert!(child_exit_code(&test, "MultiThread", code) == Some(code));
}

#[test]
fn main_panics_when_run_from_args_uses_the_wrong_runtime() {
    let test = child_test_name(default_runtime::run);
    // The child panics, and exits with 101, when the runtime is not the one
    // it expects.
    assert!(child_exit_code(&test, "CurrentThread", 0) == Some(101));
}

#[test]
fn runtime_arguments_reach_tokio_main() {
    let test = child_test_name(single_threaded::run);
    assert!(child_exit_code(&test, "CurrentThread", 3) == Some(3));
}
