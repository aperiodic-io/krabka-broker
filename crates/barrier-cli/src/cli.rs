//! The tool's command line: the `clap` types and the argument conversions.
//!
//! `Cli` and `Command` spell every flag each barrier subcommand takes, and
//! `parse_time` and `Time::millis_i64` are the two halves of the one conversion
//! the wire forces: an operator writes a duration in whatever unit reads best,
//! and the barrier apis carry milliseconds. Parsing is separate from sending
//! so a test can prove a command line reaches the right shape without a
//! broker.

use {
    clap::{Parser, Subcommand},
    krabka_units::Time,
};

/// The tool's command line.
///
/// Shared by the binary and by [`crate::run_from_args`], so both accept
/// exactly the same flags.
#[krabka_macros::bootstrap_cli_fields]
#[derive(Parser)]
#[command(
    name = "krabka-barrier",
    version,
    about = "Define barrier groups, trigger cuts, and verify a cut against the log"
)]
pub struct Cli {}

/// The subcommands, one per barrier api.
#[derive(Subcommand)]
pub enum Command {
    /// Create a barrier group, or update one that exists.
    Define {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// A topic the group cuts across. Repeat for each one.
        #[arg(long = "topic", required = true)]
        topics: Vec<String>,
        /// How often the coordinator injects a cut on its own. Takes any
        /// time unit, so `500ms`, `30s`, `5m` and `1h` all work. A number
        /// with no unit is refused. Omit for on-demand only.
        #[arg(long, value_parser = parse_time)]
        interval: Option<Time>,
        /// How many cuts the group keeps before the oldest is tombstoned.
        #[arg(long, default_value_t = 100)]
        retained_cuts: i32,
    },
    /// Delete a barrier group and its cuts.
    Delete {
        /// The group's name.
        #[arg(long)]
        group: String,
    },
    /// Print a group's definition and its latest epoch.
    Describe {
        /// A group to describe. Repeat for each one, or omit for every group.
        #[arg(long = "group")]
        groups: Vec<String>,
    },
    /// Inject a cut now and print the offsets it took.
    Trigger {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// How long the coordinator retries a partition that has no marker
        /// yet. Takes any time unit, the same as `--interval`. The broker
        /// clamps this to its own configured ceiling. Omit to take the
        /// broker's default.
        #[arg(long, value_parser = parse_time)]
        timeout: Option<Time>,
    },
    /// Print a group's retained cuts.
    List {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// Skip every cut below this epoch.
        #[arg(long, default_value_t = 0)]
        from_epoch: i64,
        /// How many cuts to print. `-1` prints every retained cut.
        #[arg(long, default_value_t = -1)]
        max_results: i32,
    },
    /// Read the log at a cut's offsets and prove each one holds that cut's
    /// marker.
    ///
    /// A cut is only worth as much as the markers behind it. This fetches the
    /// batch at every offset the cut names and checks it is a barrier control
    /// batch carrying this group and this epoch.
    Verify {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// The epoch to verify.
        #[arg(long)]
        epoch: i64,
    },
}

/// Parse a time argument.
///
/// Delegates to `krabka_units`, so every unit the broker's own configuration
/// accepts works here too: `ns`, `us`, `ms`, `s`, `m`, `h`, `d` and their long
/// forms.
///
/// A number with no unit is refused, and only `0` is exempt. That is the units
/// crate's rule and it is the right one here: `--timeout 30` from someone who
/// meant milliseconds would otherwise wait thirty seconds without complaining.
fn parse_time(raw: &str) -> Result<Time, String> {
    krabka_units::parse::time(raw).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, krabka_units::convert::TimeExt as _};

    krabka_macros::duration_parser_fixture!(
        {
            /// A time argument takes any unit the broker's own configuration takes,
            /// so an operator never has to convert to milliseconds by hand, and a
            /// number with no unit is refused rather than guessed at.

        },
        [
            ("5m", Some(300_000)),
            ("1h", Some(3_600_000)),
            ("2d", Some(172_800_000)),
            ("250us", Some(0)),
            ("1 minute", Some(60_000)),
            // A unit is required, so a number alone cannot be read as the
            // wrong scale. Zero is the one exemption, having no scale.
            ("90", None),
            ("0", Some(0)),
            ("banana", None),
            ("", None),
        ]
    );

    krabka_macros::bootstrap_parser_fixture!(["krabka-barrier", "describe"], [
        "krabka-barrier",
        "-b",
        "localhost:9092",
        "describe"
    ]);

    /// A group with no periodic injection is the default, and it reaches the
    /// wire as the -1 the broker reads as "on demand only".
    #[test]
    fn define_without_an_interval_asks_for_on_demand_only() {
        let cli = Cli::try_parse_from([
            "krabka-barrier",
            "-b",
            "localhost:9092",
            "define",
            "--group",
            "g",
            "--topic",
            "orders",
        ])
        .expect("parses");
        let Command::Define {
            interval,
            retained_cuts,
            topics,
            ..
        } = cli.command
        else {
            panic!("expected define");
        };
        check!(interval == None);
        check!(retained_cuts == 100);
        check!(topics == vec!["orders".to_owned()]);
    }

    /// `list` defaults to every retained cut, matching the request default the
    /// wire carries.
    #[test]
    fn list_defaults_to_every_retained_cut() {
        let cli = Cli::try_parse_from([
            "krabka-barrier",
            "-b",
            "localhost:9092",
            "list",
            "--group",
            "g",
        ])
        .expect("parses");
        let Command::List {
            max_results,
            from_epoch,
            ..
        } = cli.command
        else {
            panic!("expected list");
        };
        check!(max_results == -1);
        check!(from_epoch == 0);
    }
}
