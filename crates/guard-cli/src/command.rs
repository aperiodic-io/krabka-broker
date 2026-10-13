//! The request one subcommand sends, and the answer it reports.
//!
//! A signature covers what the broker holds and never what the caller supplies,
//! so a signed command reads the cluster id, or the stored proposal, back from
//! the cluster before it signs anything. Every path here returns a [`Failure`]
//! for a step that stopped before a broker answered, and an exit code for one
//! that a broker answered.

use {
    super::{
        cli::{Action, BreakGlassCommand, Command, FreezeCommand, FreezeSigningArgs, ScopeArgs},
        failure::Failure,
        report::{
            described_error, print_freeze, print_proposal, report_error, report_set_freeze,
            report_verify,
        },
        signing, verify,
    },
    krabka_protocol::{
        krabka::{
            break_glass as bg,
            freeze::{self as api, PATTERN_TYPE_ANY},
        },
        owned::describe_cluster_request::DescribeClusterRequest,
        primitives::uuid::Uuid as WireUuid,
    },
    krabka_units::{Time, convert::TimeExt as _},
};

// Commands share transport errors and the broker's ordinary refusal report.
// Their success responses retain each command's distinct interpretation.
macro_rules! checked_response {
    ($client:expr, $request:expr) => {{
        let response = $client.send($request).await.map_err(Failure::from)?;
        if response.error_code != 0 {
            return Ok(report_error(
                response.error_code,
                response.error_message.as_deref(),
            ));
        }
        response
    }};
}

/// Send one command's request and print its response.
///
/// # Errors
///
/// Returns the [`Failure`] of a step that stopped before a broker answered: a
/// key file that cannot be read, a proposal that cannot be looked up, or a
/// request that did not complete.
pub(super) async fn dispatch(
    client: &krabka_client_core::Client,
    command: Command,
) -> Result<i32, Failure> {
    match command {
        Command::Freeze { command } => match command {
            FreezeCommand::Set {
                scope,
                reason,
                signing,
            } => set_freeze(client, &scope, reason, &signing, true, uuid::Uuid::nil()).await,
            FreezeCommand::Clear {
                scope,
                proposal,
                reason,
                signing,
            } => set_freeze(client, &scope, reason, &signing, false, proposal).await,
            FreezeCommand::List {
                scope,
                verify_signatures,
                operator_keys,
            } => list_freezes(client, scope, verify_signatures, operator_keys.as_deref()).await,
        },
        Command::BreakGlass { command } => match command {
            BreakGlassCommand::Propose {
                action,
                target,
                reason,
                ttl,
            } => propose(client, action, target, reason, ttl).await,
            BreakGlassCommand::Approve {
                proposal,
                sign_with,
                key_id,
            } => approve(client, proposal, sign_with.as_deref(), key_id.as_deref()).await,
            BreakGlassCommand::Withdraw { proposal } => withdraw(client, proposal).await,
            BreakGlassCommand::List { pending, proposal } => {
                list_proposals(client, pending, proposal).await
            }
        },
    }
}

/// Set or lift one freeze.
///
/// The two directions share this because they share a request. `frozen` and the
/// proposal are what separate them, and both are inside the signed bytes, so a
/// signature captured from a freeze cannot be replayed as the thaw.
async fn set_freeze(
    client: &krabka_client_core::Client,
    scope: &ScopeArgs,
    reason: String,
    signing: &FreezeSigningArgs,
    frozen: bool,
    proposal: uuid::Uuid,
) -> Result<i32, Failure> {
    let scope = scope.resolve()?;
    let set_at_ms = now_ms();
    let proposal_id = WireUuid(*proposal.as_bytes());

    let (key_id, signature) = match signing.resolve()? {
        None => (String::new(), Vec::new()),
        Some(signing) => {
            // The cluster id is inside the signed bytes, so a signature made
            // here cannot be replayed into a second cluster. It is read before
            // the signature is made and never after.
            let cluster_id = cluster_id(client).await?;
            let signer = signing::load_signer(signing.key_path, signing.key_id)
                .map_err(Failure::Signature)?;
            let bytes = signing::freeze_signing_bytes(&signing::FreezeSigningInput {
                cluster_id: &cluster_id,
                pattern_type: scope.pattern_type,
                scope: &scope.name,
                frozen,
                reason: &reason,
                set_by: signing.principal,
                set_at_ms,
                proposal_id: proposal_id.0,
            });
            (signing.key_id.to_owned(), signer.sign(&bytes))
        }
    };

    let request = api::SetTopicFreezeRequest {
        scope: scope.name,
        pattern_type: scope.pattern_type,
        frozen,
        reason,
        proposal_id,
        set_at_ms,
        key_id,
        signature,
        ..api::SetTopicFreezeRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    Ok(report_set_freeze(&response, frozen))
}

/// Read the registry, and prove it when the operator asks.
async fn list_freezes(
    client: &krabka_client_core::Client,
    scope: Option<String>,
    verify_signatures: bool,
    operator_keys: Option<&std::path::Path>,
) -> Result<i32, Failure> {
    let request = api::DescribeTopicFreezesRequest {
        scope_filter: scope,
        pattern_type_filter: PATTERN_TYPE_ANY,
        ..api::DescribeTopicFreezesRequest::default()
    };
    let response = checked_response!(client, request);
    let Some(operator_keys) = operator_keys.filter(|_| verify_signatures) else {
        for freeze in &response.freezes {
            print_freeze(freeze, None);
        }
        return Ok(0);
    };
    let keys = verify::load_trust_set(operator_keys).map_err(Failure::Signature)?;
    let cluster_id = cluster_id(client).await?;
    let outcome = verify::verify_registry(&cluster_id, &keys, &response.freezes);
    Ok(report_verify(&response.freezes, &outcome))
}

/// Open a proposal.
async fn propose(
    client: &krabka_client_core::Client,
    action: Action,
    target: String,
    reason: String,
    ttl: Option<Time>,
) -> Result<i32, Failure> {
    let request = bg::ProposeBreakGlassRequest {
        action: action.wire(),
        target,
        reason,
        // Zero asks the broker for `break_glass.proposal_ttl`.
        ttl_ms: ttl.map_or(0, Time::millis_i64),
        ..bg::ProposeBreakGlassRequest::default()
    };
    let response = checked_response!(client, request);
    println!(
        "proposal {}",
        uuid::Uuid::from_bytes(response.proposal_id.0)
    );
    println!("expires  {}", response.expires_at_ms);
    Ok(0)
}

/// Add one approval to a proposal.
///
/// A signature covers the proposal the broker holds, and not one the caller
/// supplies, so a signed approval reads the proposal back first and signs what
/// is stored.
async fn approve(
    client: &krabka_client_core::Client,
    proposal: uuid::Uuid,
    sign_with: Option<&std::path::Path>,
    key_id: Option<&str>,
) -> Result<i32, Failure> {
    let proposal_id = WireUuid(*proposal.as_bytes());
    let (key_id, signature) = match (sign_with, key_id) {
        (Some(path), Some(key_id)) => {
            let stored = read_proposal(client, proposal_id).await?;
            let signer = signing::load_signer(path, key_id).map_err(Failure::Signature)?;
            let bytes = signing::approval_signing_bytes(&signing::ApprovalSigningInput {
                proposal_id: stored.proposal_id.0,
                action: stored.action,
                target: &stored.target,
                proposer: &stored.proposer,
                created_at_ms: stored.created_at_ms,
                expires_at_ms: stored.expires_at_ms,
            });
            (key_id.to_owned(), signer.sign(&bytes))
        }
        _ => (String::new(), Vec::new()),
    };
    let request = bg::ApproveBreakGlassRequest {
        proposal_id,
        key_id,
        signature,
        withdraw: false,
        ..bg::ApproveBreakGlassRequest::default()
    };
    let response = checked_response!(client, request);
    println!(
        "approvals {} of {}",
        response.approvals_held, response.approvals_required
    );
    Ok(0)
}

/// Withdraw a proposal.
///
/// This rides api key 1018 with the `withdraw` flag set, and never key 1017.
async fn withdraw(
    client: &krabka_client_core::Client,
    proposal: uuid::Uuid,
) -> Result<i32, Failure> {
    let request = bg::ApproveBreakGlassRequest {
        proposal_id: WireUuid(*proposal.as_bytes()),
        withdraw: true,
        ..bg::ApproveBreakGlassRequest::default()
    };
    let _response = checked_response!(client, request);
    println!("withdrawn {proposal}");
    Ok(0)
}

/// Print the proposals and their approvals.
async fn list_proposals(
    client: &krabka_client_core::Client,
    pending: bool,
    proposal: Option<uuid::Uuid>,
) -> Result<i32, Failure> {
    let request = bg::DescribeBreakGlassRequest {
        pending_only: pending,
        proposal_id: proposal.map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes())),
        ..bg::DescribeBreakGlassRequest::default()
    };
    let response = checked_response!(client, request);
    for stored in &response.proposals {
        print_proposal(stored);
    }
    Ok(0)
}

/// Read one stored proposal, so a signature covers what the broker holds.
async fn read_proposal(
    client: &krabka_client_core::Client,
    proposal_id: WireUuid,
) -> Result<bg::DescribedBreakGlassProposal, Failure> {
    let request = bg::DescribeBreakGlassRequest {
        proposal_id,
        ..bg::DescribeBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Err(Failure::Refused(format!(
            "cannot read the proposal to sign it: {}",
            described_error(response.error_code, response.error_message.as_deref())
        )));
    }
    let id = uuid::Uuid::from_bytes(proposal_id.0);
    response
        .proposals
        .into_iter()
        .find(|stored| stored.proposal_id == proposal_id)
        .ok_or_else(|| Failure::Refused(format!("the cluster holds no proposal {id}")))
}

/// Read the cluster id that a freeze signature covers.
async fn cluster_id(client: &krabka_client_core::Client) -> Result<String, Failure> {
    let response = client
        .send(DescribeClusterRequest::default())
        .await
        .map_err(Failure::from)?;
    if response.error_code != 0 {
        return Err(Failure::Refused(format!(
            "cannot read the cluster id: {}",
            described_error(response.error_code, response.error_message.as_deref())
        )));
    }
    Ok(response.cluster_id)
}

/// The current wall clock, in milliseconds since the Unix epoch.
///
/// A signed record carries it, and the broker refuses one outside
/// `freeze.signature_max_skew` of its own clock. A clock before the epoch
/// saturates at zero, which the broker then refuses as far outside the window.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| i64::try_from(since.as_millis()).unwrap_or(0))
}
