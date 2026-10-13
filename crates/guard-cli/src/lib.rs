//! Administers krabka topic write freezes and break-glass proposals.
//!
//! A write freeze is a broker-owned state where the cluster is up, every read
//! works, and the broker refuses every new client write to a topic. A
//! break-glass proposal is a standing authorization that two different people
//! agreed to, which a privileged transition spends. This is the operator's side
//! of both: freeze a scope, read the registry back and prove it, lift a freeze
//! with an approval, and open, approve, withdraw and read the proposals.
//!
//! It is one command for one incident. It is a library as well as a binary so
//! tests call [`run_from_args`] in process: a test that spawns the binary needs
//! a Cargo working tree to build it from, and a Bazel test sandbox has none.
//! That is the same reason `krabka-barrier` and `krabka-format` are libraries.
//!
//! # The two properties that matter
//!
//! `--sign-with` never leaves the machine. It takes a PKCS#8 Ed25519 key file,
//! builds the canonical signing bytes here, signs them here, and puts only the
//! `key_id` and the detached signature on the wire. The private key never
//! reaches a broker, so a broker cannot make a signature in an operator's name.
//!
//! `freeze list --verify-signatures` checks the registry here as well, against
//! operator public keys on this machine. That makes the operator's own machine,
//! and not the broker that served the rows, the thing that says the registry is
//! authentic.
//!
//! # Exit codes
//!
//! A runbook branches on `$?`, so every number means one thing across this tool
//! and `krabka-barrier`. See [`EXIT_REFUSED`], [`EXIT_UNREACHABLE`],
//! [`EXIT_MISMATCH`], [`EXIT_NO_APPROVAL`] and [`EXIT_BAD_SIGNATURE`].
//!
//! # The api keys
//!
//! Every subcommand speaks one krabka-private api key, in the 1015 to 1019
//! range. A JVM `AdminClient` cannot send those. The freeze is visible to the
//! JVM tools another way: `DescribeConfigs` reports a read-only `write.freeze`
//! key for every topic, so `kafka-configs --describe` shows it.

use {self::command::dispatch, clap::Parser};

mod cli;
mod command;
mod failure;
mod report;
pub mod signing;
pub mod verify;

#[cfg(test)]
mod tests;

pub use self::{
    cli::{
        Action, BreakGlassCommand, Cli, Command, FreezeCommand, FreezeSigningArgs, ScopeArgs,
        action_name, pattern_name,
    },
    report::{code_name, exit_for_code},
    verify::{CheckedEntry, Unproved, VerifyOutcome},
};

/// The exit code for a request the broker refused.
pub const EXIT_REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
pub const EXIT_UNREACHABLE: i32 = 2;
/// The exit code for a registry the local trust set does not match.
///
/// It carries `krabka-barrier`'s meaning: the tool asked the cluster for
/// something and what came back does not agree with what the operator holds. A
/// registry entry that names an operator key this machine does not have is that
/// disagreement, and it is not the same answer as a signature that failed. This
/// one says the tool could not check.
pub const EXIT_MISMATCH: i32 = 3;
/// The exit code for an action that needs a break-glass approval which does not
/// exist.
///
/// This is the code a runbook branches on. It separates "go and get a second
/// person" from every other refusal.
pub const EXIT_NO_APPROVAL: i32 = 4;
/// The exit code for a signature that did not verify, or that the broker needed
/// and did not get.
///
/// It keeps "the tool could not check" apart from "the tool checked and the
/// answer is wrong". KFC-5's verifier draws the same distinction.
pub const EXIT_BAD_SIGNATURE: i32 = 5;

krabka_macros::parsed_cli_entrypoint!({
    /// Run the tool from an argv-style iterator, returning its exit code.
    ///
    /// `0` means the broker accepted the request. `1` means it refused one, and the
    /// reason is on stderr. `2` means the broker could not be reached, so nothing
    /// is known about the outcome. `3` means the registry names an operator key
    /// this machine does not hold. `4` means the action needs a break-glass
    /// approval that does not exist. `5` means a signature did not verify.
    ///
    /// # Panics
    ///
    /// Panics if `argv` does not parse, which for a caller passing a literal
    /// argument list is a bug in that list rather than a runtime condition.
});

/// Run one parsed command.
pub async fn run(cli: Cli) -> i32 {
    let client =
        krabka_macros::connect_cli_client!(&cli.bootstrap_server, "krabka-guard", EXIT_UNREACHABLE);
    match dispatch(&client, cli.command).await {
        Ok(code) => code,
        Err(failure) => {
            eprintln!("{}", failure.message());
            failure.exit_code()
        }
    }
}
