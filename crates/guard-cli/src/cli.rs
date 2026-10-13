//! The command line this tool accepts, and the values its arguments resolve to.
//!
//! `clap` derives the parser from these types, so the doc comment beside a
//! field is the help text an operator reads. Two flag groups say more than the
//! parser can state on its own: `--topic` against `--prefix`, and the three
//! signing flags that travel together. A resolver turns each group into one
//! value, and it refuses the combination the parser already rules out rather
//! than falling back to a default that would mean something else.

use {
    super::failure::Failure,
    clap::{Args, Parser, Subcommand, ValueEnum},
    krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
    krabka_units::Time,
    std::path::PathBuf,
};

/// The tool's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
///
/// [`run_from_args`]: crate::run_from_args
#[krabka_macros::bootstrap_cli_fields]
#[derive(Parser)]
#[command(
    name = "krabka-guard",
    version,
    about = "Freeze topic writes, lift a freeze, and run the break-glass two-person rule"
)]
pub struct Cli {}

/// The two halves of the tool.
#[derive(Subcommand)]
pub enum Command {
    /// Administer the topic write-freeze registry.
    Freeze {
        /// What to do to the registry.
        #[command(subcommand)]
        command: FreezeCommand,
    },
    /// Administer the break-glass two-person rule.
    #[command(name = "break-glass")]
    BreakGlass {
        /// What to do to the proposals.
        #[command(subcommand)]
        command: BreakGlassCommand,
    },
}

/// The freeze registry subcommands.
#[derive(Subcommand)]
pub enum FreezeCommand {
    /// Freeze a topic or a topic-name prefix.
    ///
    /// A freeze is the safe direction, so one command sets it. The broker takes
    /// an unsigned freeze unless `freeze.require_signature` is on, and an
    /// unsigned entry is an attestation rather than a proof.
    Set {
        /// The scope to freeze.
        #[command(flatten)]
        scope: ScopeArgs,
        /// Free text that says why. The broker keeps it in the metadata log and
        /// in the audit event.
        #[arg(long)]
        reason: String,
        /// The detached operator signature, if the operator makes one.
        #[command(flatten)]
        signing: FreezeSigningArgs,
    },
    /// Print the live registry entries.
    List {
        /// Only print the entries whose scope is exactly this.
        #[arg(long)]
        scope: Option<String>,
        /// Check every returned entry against operator public keys on this
        /// machine, rather than taking the broker's word for the registry.
        #[arg(long, requires = "operator_keys")]
        verify_signatures: bool,
        /// A TOML file carrying the `[[operator_keys]]` block, which may be the
        /// `broker.toml` the cluster runs on.
        #[arg(long)]
        operator_keys: Option<PathBuf>,
    },
    /// Lift a freeze.
    ///
    /// A thaw is the dangerous direction. The broker refuses one that names no
    /// approved break-glass proposal, and it refuses one that carries no
    /// signature whatever its configuration says.
    Clear {
        /// The scope to thaw. It must name the entry exactly: a freeze on
        /// `--prefix tenant-a.` is not lifted by naming one topic under it.
        #[command(flatten)]
        scope: ScopeArgs,
        /// The approved break-glass proposal that authorizes the thaw.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
        /// Free text that says why the freeze is lifted. The metadata log and
        /// the audit event keep it, so the record names the reason as well as
        /// the person.
        #[arg(long, default_value = "")]
        reason: String,
        /// The detached operator signature. A thaw always needs one.
        #[command(flatten)]
        signing: FreezeSigningArgs,
    },
}

/// The break-glass subcommands.
#[derive(Subcommand)]
pub enum BreakGlassCommand {
    /// Open a proposal, which a second person then approves.
    Propose {
        /// The transition the proposal authorizes.
        #[arg(long, value_enum)]
        action: Action,
        /// What the transition applies to: a topic, a broker id, or a
        /// partition, depending on the action.
        #[arg(long)]
        target: String,
        /// Free text that says why the transition is needed.
        #[arg(long)]
        reason: String,
        /// How long the proposal stays open. Takes any time unit, so `30m`,
        /// `1h` and `90s` all work, and a number with no unit is refused. The
        /// broker caps it at `break_glass.proposal_ttl`. Omit to take that
        /// value.
        #[arg(long, value_parser = parse_time)]
        ttl: Option<Time>,
    },
    /// Add one approval to a proposal.
    ///
    /// The broker refuses the proposer, a principal outside
    /// `break_glass.approvers`, and a principal that already approved. Those
    /// three checks are what make this a two-person rule rather than a
    /// two-click rule.
    Approve {
        /// The proposal to approve.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
        /// The PKCS#8 Ed25519 key that signs the approval. The broker demands
        /// one for every action in `break_glass.signed_actions`.
        #[arg(long, requires = "key_id")]
        sign_with: Option<PathBuf>,
        /// The operator key that `--sign-with` holds, as `[[operator_keys]]`
        /// names it.
        #[arg(long, requires = "sign_with")]
        key_id: Option<String>,
    },
    /// Withdraw a proposal, so nothing can spend it.
    ///
    /// A withdraw rides the approve api key with the request's `withdraw` flag
    /// set. Approve and withdraw both name a proposal that exists and both act
    /// on it, so they share one request. A propose names no proposal, because a
    /// propose is what creates one.
    Withdraw {
        /// The proposal to withdraw.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
    },
    /// Print the proposals and their approvals.
    List {
        /// Drop the proposals that a transition consumed, that an operator
        /// withdrew, and that expired.
        #[arg(long)]
        pending: bool,
        /// Print this proposal alone.
        #[arg(long, value_parser = parse_uuid)]
        proposal: Option<uuid::Uuid>,
    },
}

/// The scope a freeze command acts on.
///
/// Exactly one of the two is given. A literal scope names one topic, and a
/// prefixed scope names every topic whose name starts with it, including one
/// the cluster creates later. The two words are Kafka's ACL pattern types, so a
/// namespace freeze uses the vocabulary an operator knows from `kafka-acls`.
#[derive(Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct ScopeArgs {
    /// One topic, by name.
    #[arg(long)]
    pub topic: Option<String>,
    /// Every topic whose name starts with this prefix.
    #[arg(long)]
    pub prefix: Option<String>,
}

/// The detached operator signature on a freeze or a thaw.
///
/// The three flags travel together. The broker binds the signature to the
/// principal on the connection and to the principal that `[[operator_keys]]`
/// binds the key to, and both of those have to be the name the signed bytes
/// carry. Only the operator knows what their listener authenticates them as, so
/// the tool asks rather than guesses.
#[derive(Args, Debug, Clone)]
pub struct FreezeSigningArgs {
    /// The PKCS#8 Ed25519 key file. It is read here, used here, and never
    /// sent.
    #[arg(long, requires_all = ["key_id", "principal"])]
    pub sign_with: Option<PathBuf>,
    /// The operator key that `--sign-with` holds, as `[[operator_keys]]` names
    /// it.
    #[arg(long, requires = "sign_with")]
    pub key_id: Option<String>,
    /// The principal the broker authenticates on this connection, which is the
    /// author the record carries.
    #[arg(long, requires = "sign_with")]
    pub principal: Option<String>,
}

/// A gated transition that a break-glass proposal authorizes.
///
/// The wire values are the values of the broker's own action type. The names
/// here are the same names `break_glass.signed_actions` and the audit event
/// use, spelled with dashes as a command line spells a word.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Lift a topic write freeze.
    ThawTopicFreeze,
    /// Elect a leader that is not in the in-sync replica set.
    UncleanElectLeaders,
    /// Recover a partition from a replica that may be behind.
    UncleanRecovery,
    /// Remove a broker from the cluster.
    UnregisterBroker,
    /// Cancel a partition reassignment that is in flight.
    CancelReassignment,
    /// Delete a topic.
    DeleteTopic,
    /// Delete records from the head of a partition.
    DeleteRecords,
}

impl Action {
    /// The wire value the private api keys carry for this action.
    #[must_use]
    pub fn wire(self) -> i8 {
        match self {
            Action::ThawTopicFreeze => 1,
            Action::UncleanElectLeaders => 2,
            Action::UncleanRecovery => 3,
            Action::UnregisterBroker => 4,
            Action::CancelReassignment => 5,
            Action::DeleteTopic => 6,
            Action::DeleteRecords => 7,
        }
    }
}

/// The name of an action wire value, in the spelling every krabka surface uses.
#[must_use]
pub fn action_name(wire: i8) -> &'static str {
    match wire {
        1 => "thaw_topic_freeze",
        2 => "unclean_elect_leaders",
        3 => "unclean_recovery",
        4 => "unregister_broker",
        5 => "cancel_reassignment",
        6 => "delete_topic",
        7 => "delete_records",
        _ => "unknown",
    }
}

/// The word for a pattern-type wire value.
#[must_use]
pub fn pattern_name(pattern_type: i8) -> &'static str {
    match pattern_type {
        PATTERN_TYPE_LITERAL => "literal",
        PATTERN_TYPE_PREFIXED => "prefixed",
        _ => "unknown",
    }
}

/// Parse a time argument.
///
/// Delegates to `krabka_units`, so every unit the broker's own configuration
/// accepts works here too: `ns`, `us`, `ms`, `s`, `m`, `h`, `d` and their long
/// forms.
///
/// A number with no unit is refused, and only `0` is exempt. That is the units
/// crate's rule and it is the right one here: `--ttl 30` from someone who meant
/// minutes would otherwise open a proposal for thirty milliseconds.
fn parse_time(raw: &str) -> Result<Time, String> {
    krabka_units::parse::time(raw).map_err(|e| e.to_string())
}

/// Parse a proposal id.
fn parse_uuid(raw: &str) -> Result<uuid::Uuid, String> {
    uuid::Uuid::parse_str(raw).map_err(|e| format!("{raw:?} is not a proposal id: {e}"))
}

impl ScopeArgs {
    /// The scope these arguments name.
    ///
    /// # Errors
    ///
    /// Returns a refusal when neither or both of `--topic` and `--prefix` are
    /// given. The parser already rules that out, and this keeps the function
    /// total rather than defaulting to an empty prefix that would name every
    /// topic in the cluster.
    pub(super) fn resolve(&self) -> Result<Scope, Failure> {
        match (self.topic.as_deref(), self.prefix.as_deref()) {
            (Some(topic), None) => Ok(Scope {
                name: topic.to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
            }),
            (None, Some(prefix)) => Ok(Scope {
                name: prefix.to_owned(),
                pattern_type: PATTERN_TYPE_PREFIXED,
            }),
            _ => Err(Failure::Refused(
                "name exactly one of --topic and --prefix".to_owned(),
            )),
        }
    }
}

/// One resolved freeze scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Scope {
    /// A topic name, or a topic-name prefix.
    pub(super) name: String,
    /// `3` literal, `4` prefixed.
    pub(super) pattern_type: i8,
}

impl FreezeSigningArgs {
    /// The signing material, when the operator asked for a signature.
    ///
    /// # Errors
    ///
    /// Returns a refusal when `--sign-with` is given without both of the other
    /// two. The parser already rules that out, and this keeps the function
    /// total.
    pub(super) fn resolve(&self) -> Result<Option<FreezeSigning<'_>>, Failure> {
        let Some(key_path) = self.sign_with.as_deref() else {
            return Ok(None);
        };
        match (self.key_id.as_deref(), self.principal.as_deref()) {
            (Some(key_id), Some(principal)) => Ok(Some(FreezeSigning {
                key_path,
                key_id,
                principal,
            })),
            _ => Err(Failure::Refused(
                "--sign-with also needs --key-id and --principal".to_owned(),
            )),
        }
    }
}

/// The resolved signing material for one freeze or thaw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FreezeSigning<'a> {
    /// The PKCS#8 key file, which never leaves this machine.
    pub(super) key_path: &'a std::path::Path,
    /// The key id that the request carries.
    pub(super) key_id: &'a str,
    /// The author name the signed bytes carry.
    pub(super) principal: &'a str,
}

#[cfg(test)]
mod tests;
