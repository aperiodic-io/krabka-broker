//! The owner-or-renewer gate on Renew and Expire, over the wire.
//!
//! Kafka trunk's `DelegationTokenControlManager.allowedToRenew` lets only the
//! token's owner or a listed renewer renew or expire it, and answers
//! `DELEGATION_TOKEN_OWNER_MISMATCH` (63) to anyone else, super users
//! included. A super user that mints a token for another owner and
//! must manage it afterwards lists itself as a renewer.

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    renew_delegation_token_request::RenewDelegationTokenRequest,
};

use crate::{
    DELEGATION_TOKEN_OWNER_MISMATCH,
    cluster::{wait_for_token, wait_for_token_gone},
    rpc::{
        send_create_delegation_token, send_expire_delegation_token, send_renew_delegation_token,
    },
};

/// Super user `admin` mints two tokens owned by `alice`: one without
/// renewers and one that lists `admin`. On the first, Renew and Expire from
/// `admin` answer 63 and change nothing; on the second, both succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn super_user_renews_and_expires_only_as_a_listed_renewer() {
    let (handle, _dir, addr) = crate::cluster::start_admin_alice().await;

    let result: Result<(), String> = async {
        let mut admin = crate::wire::admin_plain(addr).await?;

        let mut hmacs = Vec::new();
        for (correlation_id, renewers) in [
            (100, vec![]),
            (
                101,
                vec![CreatableRenewers {
                    principal_type: "User".to_string(),
                    principal_name: "admin".to_string(),
                    ..Default::default()
                }],
            ),
        ] {
            let create_resp = send_create_delegation_token(
                &mut admin,
                correlation_id,
                &CreateDelegationTokenRequest {
                    owner_principal_type: Some("User".to_string()),
                    owner_principal_name: Some("alice".to_string()),
                    max_lifetime_ms: -1,
                    renewers,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| format!("CreateDelegationToken(admin for alice): {e}"))?;
            if create_resp.error_code != 0 {
                return Err(format!(
                    "Create for another owner must succeed for a super user; got code={}",
                    create_resp.error_code,
                ));
            }
            let token = wait_for_token(&handle, &create_resp.token_id).await;
            assert!(token.owner.name == "alice");
            hmacs.push((
                create_resp.token_id.clone(),
                create_resp.hmac.clone(),
                create_resp,
            ));
        }
        let (unlisted_id, unlisted_hmac, unlisted) = hmacs.remove(0);
        let (listed_id, listed_hmac, listed) = hmacs.remove(0);

        // Not a renewer: both refused with 63, and the token is untouched.
        let renew_resp = send_renew_delegation_token(
            &mut admin,
            200,
            &RenewDelegationTokenRequest {
                hmac: unlisted_hmac.clone(),
                renew_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(admin, not a renewer): {e}"))?;
        check!(renew_resp.error_code == DELEGATION_TOKEN_OWNER_MISMATCH);
        let expire_resp = send_expire_delegation_token(
            &mut admin,
            201,
            &ExpireDelegationTokenRequest {
                hmac: unlisted_hmac,
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(admin, not a renewer): {e}"))?;
        check!(expire_resp.error_code == DELEGATION_TOKEN_OWNER_MISMATCH);
        let untouched = wait_for_token(&handle, &unlisted_id).await;
        check!(untouched.expiry_timestamp_ms == unlisted.expiry_timestamp_ms);

        // A listed renewer: Renew succeeds within the max timestamp, and
        // Expire with a negative period deletes the token.
        let renew_resp = send_renew_delegation_token(
            &mut admin,
            300,
            &RenewDelegationTokenRequest {
                hmac: listed_hmac.clone(),
                renew_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(admin, renewer): {e}"))?;
        check!(renew_resp.error_code == 0);
        check!(renew_resp.expiry_timestamp_ms >= listed.expiry_timestamp_ms);
        check!(renew_resp.expiry_timestamp_ms <= listed.max_timestamp_ms);
        let expire_resp = send_expire_delegation_token(
            &mut admin,
            301,
            &ExpireDelegationTokenRequest {
                hmac: listed_hmac,
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(admin, renewer): {e}"))?;
        check!(expire_resp.error_code == 0);

        drop(admin);
        wait_for_token_gone(&handle, &listed_id).await;
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("renewer gate test failed: {msg}");
    }
}
