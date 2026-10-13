use {super::*, crate::network::auth::AuthenticatedSnapshot, assert2::assert, std::sync::Arc};

#[derive(Debug)]
struct CompletingIntrospection {
    clock: Arc<AtomicI64>,
    completed_ms: i64,
    expiry_seconds: i64,
}

#[async_trait::async_trait]
impl krabka_security::IntrospectionClient for CompletingIntrospection {
    async fn introspect(
        &self,
        _token: &str,
    ) -> Result<serde_json::Value, krabka_security::IntrospectionError> {
        tokio::task::yield_now().await;
        self.clock.store(self.completed_ms, Ordering::Release);
        Ok(serde_json::json!({"active": true, "sub": "alice", "exp": self.expiry_seconds}))
    }

    async fn userinfo(
        &self,
        _token: &str,
    ) -> Result<Option<serde_json::Value>, krabka_security::IntrospectionError> {
        Ok(None)
    }
}

#[tokio::test]
async fn oauth_completion_uses_the_current_clock_for_lifetime_and_expiry() {
    let started = 1_000_000;
    let expiry = 2_000_000;
    let request = oauthbearer_client_response("opaque-token");
    for reauth in [false, true] {
        for completed in [started + 500, expiry, expiry + 500, started - 1] {
            let clock = Arc::new(AtomicI64::new(started));
            let client = Arc::new(CompletingIntrospection {
                clock: Arc::clone(&clock),
                completed_ms: completed,
                expiry_seconds: expiry / 1000,
            });
            let validator = krabka_security::OAuthBearerValidator::Introspection(
                krabka_security::IntrospectionValidator {
                    client,
                    principal_claim_name: "sub".into(),
                    custom_claim_check: None,
                    call_userinfo: false,
                    allowable_clock_skew: secs(0),
                    expected_audience: None,
                    fallback_user_name_claim: None,
                    fallback_user_name_prefix: None,
                    groups_claim: None,
                    groups_claim_delimiter: None,
                },
            );
            let mut auth = if reauth {
                ConnectionAuth::Reauthenticating {
                    previous: AuthenticatedSnapshot {
                        principal: Principal {
                            name: "alice".into(),
                            auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                            groups: vec![],
                        },
                        mechanism: SaslMechanism::OAuthBearer,
                        expires_at_ms: Some(expiry),
                        authenticated_via_token: false,
                    },
                    exchange: SaslExchange::OAuthBearer,
                    pending_token_expiry_ms: None,
                }
            } else {
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::OAuthBearer,
                    exchange: SaslExchange::OAuthBearer,
                    pending_token_expiry_ms: None,
                }
            };
            let response = handle_authenticate_oauthbearer(
                &request,
                &mut auth,
                &validator,
                || clock.load(Ordering::Acquire),
                None,
            )
            .await;
            if completed < started || completed >= expiry {
                assert!(
                    !auth.is_authenticated(),
                    "reauth={reauth} completed={completed}"
                );
                assert_success_authenticate_response(
                    &response,
                    br#"{"status":"invalid_token"}"#,
                    0,
                );
                assert!(matches!(
                    auth,
                    ConnectionAuth::Negotiating {
                        exchange: SaslExchange::OAuthBearerFailed,
                        ..
                    } | ConnectionAuth::Reauthenticating {
                        exchange: SaslExchange::OAuthBearerFailed,
                        ..
                    }
                ));
                let reply = SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from_static(b"\x01"),
                    ..Default::default()
                };
                let failure = handle_authenticate_oauthbearer(
                    &reply,
                    &mut auth,
                    &validator,
                    || clock.load(Ordering::Acquire),
                    None,
                )
                .await;
                assert_failed_authenticate_response(
                    &failure,
                    Some(r#"{"status":"invalid_token"}"#),
                );
                if reauth {
                    assert!(matches!(auth, ConnectionAuth::Authenticated {
                        expires_at_ms: Some(at), authenticated_via_token: false, ..
                    } if at == expiry));
                    assert!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
                }
            } else {
                assert_success_authenticate_response(&response, b"", expiry - completed);
                assert!(
                    matches!(auth, ConnectionAuth::Authenticated { expires_at_ms: Some(at), .. } if at == expiry)
                );
            }
        }
    }
}
