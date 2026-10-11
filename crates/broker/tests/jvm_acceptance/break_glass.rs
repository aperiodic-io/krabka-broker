//! Break-glass setup through the host-side private APIs.

use crate::support::sasl_client;

pub(crate) async fn propose(
    bootstrap: &str,
    operator: (&str, &str),
    action: i8,
    target: &str,
    reason: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    use krabka_protocol::krabka::break_glass::ProposeBreakGlassRequest;
    let client = sasl_client(bootstrap, operator.0, operator.1).await;
    let response = client
        .send(ProposeBreakGlassRequest {
            action,
            target: target.to_owned(),
            reason: reason.to_owned(),
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    let code = response.error_code;
    let message = response.error_message;
    assert2::assert!(code == 0, "propose: code={code} message={message:?}");
    response.proposal_id
}

pub(crate) async fn approve(
    bootstrap: &str,
    operator: (&str, &str),
    proposal_id: krabka_protocol::primitives::uuid::Uuid,
) -> (i32, i32) {
    use krabka_protocol::krabka::break_glass::ApproveBreakGlassRequest;
    let client = sasl_client(bootstrap, operator.0, operator.1).await;
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id,
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    let code = response.error_code;
    let message = response.error_message;
    let who = operator.0;
    assert2::assert!(
        code == 0,
        "approve as {who}: code={code} message={message:?}"
    );
    (response.approvals_held, response.approvals_required)
}
