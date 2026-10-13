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

#[test]
fn main_exits_with_what_run_from_args_returns() {
    if std::env::var_os(CHILD_EXIT).is_some() {
        default_runtime::run();
    }
    let test = "main_exits_with_what_run_from_args_returns";
    for code in [0, 1, 2] {
        assert!(child_exit_code(test, "MultiThread", code) == Some(code));
    }
    // The child panics, and exits with 101, when the runtime is not the one
    // it expects.
    assert!(child_exit_code(test, "CurrentThread", 0) == Some(101));
}

#[test]
fn runtime_arguments_reach_tokio_main() {
    if std::env::var_os(CHILD_EXIT).is_some() {
        single_threaded::run();
    }
    let test = "runtime_arguments_reach_tokio_main";
    assert!(child_exit_code(test, "CurrentThread", 3) == Some(3));
}
