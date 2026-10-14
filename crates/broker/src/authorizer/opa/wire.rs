//! OPA's Kafka-plugin action and request-context envelope (v1.5.1).
//! See the published [serializers](https://github.com/open-policy-agent/opa-kafka-plugin/blob/v1.5.1/src/main/scala/org/openpolicyagent/kafka/OpaAuthorizer.scala).
//! The authorization interface supplies the principal and client address;
//! Kafka connection headers, listener names and security protocols are not
//! available here and are omitted from `requestContext`. Policies may use
//! the supplied context fields and the complete single-resource action.

use {
    super::OpaAuthorizer,
    krabka_authz::{AuthorizationRequest, AuthorizationResult, jdk_host_address},
    krabka_metadata::{AclOperation, ResourceType},
    serde::{Deserialize, Serialize},
};

/// Outer envelope accepted by OPA's REST decision API.
#[derive(Debug, Serialize)]
struct OpaRequest<'a> {
    input: OpaInput<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaInput<'a> {
    action: OpaAction<'a>,
    request_context: OpaRequestContext<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaAction<'a> {
    operation: &'static str,
    resource_pattern: OpaResource<'a>,
    resource_reference_count: u32,
    log_if_allowed: bool,
    log_if_denied: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaRequestContext<'a> {
    principal: OpaPrincipal<'a>,
    client_address: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaPrincipal<'a> {
    principal_type: &'static str,
    name: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaResource<'a> {
    resource_type: &'static str,
    name: &'a str,
    pattern_type: &'static str,
    unknown: bool,
}

/// Decision payload returned by OPA. The endpoint must return
/// `{"result": true|false}`. Anything else parses as an error, and the
/// caller falls through to [`OpaAuthorizer::error_decision`].
#[derive(Debug, Deserialize)]
struct OpaResponse {
    result: bool,
}

impl OpaAuthorizer {
    /// POST the request to OPA and translate the boolean response into
    /// the binary decision. Any HTTP-level or JSON-level error falls through
    /// to [`Self::error_decision`], which honours `allow_on_error`.
    pub(super) async fn call_opa(&self, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        let body = OpaRequest {
            input: OpaInput {
                action: OpaAction {
                    operation: operation_str(req.operation),
                    resource_pattern: OpaResource {
                        resource_type: resource_type_str(req.resource_type),
                        name: req.resource_name,
                        pattern_type: "LITERAL",
                        unknown: false,
                    },
                    resource_reference_count: 1,
                    log_if_allowed: true,
                    log_if_denied: true,
                },
                request_context: OpaRequestContext {
                    principal: OpaPrincipal {
                        principal_type: "User",
                        name: &req.principal.name,
                    },
                    // RequestContextSerializer uses InetAddress.toString().
                    client_address: format!("/{}", jdk_host_address(req.host.ip())),
                },
            },
        };
        match self.http_client.post(&self.url).json(&body).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => self.parse_opa_response(resp).await,
                Err(e) => {
                    tracing::warn!(error = %e, url = %self.url, "OPA HTTP status failed");
                    self.error_decision()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, url = %self.url, "OPA HTTP call failed");
                self.error_decision()
            }
        }
    }

    async fn parse_opa_response(&self, resp: reqwest::Response) -> AuthorizationResult {
        match resp.json::<OpaResponse>().await {
            Ok(r) => {
                if r.result {
                    AuthorizationResult::Allow
                } else {
                    AuthorizationResult::Deny
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, url = %self.url, "OPA response parse failed");
                self.error_decision()
            }
        }
    }
}

/// Kafka enum names used by the OPA Kafka plugin's `ActionSerializer`.
fn operation_str(op: AclOperation) -> &'static str {
    match op {
        AclOperation::All => "ALL",
        AclOperation::Read => "READ",
        AclOperation::Write => "WRITE",
        AclOperation::Create => "CREATE",
        AclOperation::Delete => "DELETE",
        AclOperation::Alter => "ALTER",
        AclOperation::Describe => "DESCRIBE",
        AclOperation::ClusterAction => "CLUSTER_ACTION",
        AclOperation::DescribeConfigs => "DESCRIBE_CONFIGS",
        AclOperation::AlterConfigs => "ALTER_CONFIGS",
        AclOperation::IdempotentWrite => "IDEMPOTENT_WRITE",
        AclOperation::TwoPhaseCommit => "TWO_PHASE_COMMIT",
        AclOperation::CreateTokens => "CREATE_TOKENS",
        AclOperation::DescribeTokens => "DESCRIBE_TOKENS",
    }
}

/// Kafka enum names used by the plugin's `ResourcePatternSerializer`.
fn resource_type_str(t: ResourceType) -> &'static str {
    match t {
        ResourceType::Topic => "TOPIC",
        ResourceType::Group => "GROUP",
        ResourceType::Cluster => "CLUSTER",
        ResourceType::TransactionalId => "TRANSACTIONAL_ID",
        ResourceType::DelegationToken => "DELEGATION_TOKEN",
        ResourceType::User => "USER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OPA input's `operation` string must be the Kafka enum name,
    /// including KIP-939's `TWO_PHASE_COMMIT`. This pins the mapping, so the
    /// test catches a regression or a blanket mutation of `operation_str`.
    #[yare::parameterized(
        read = { AclOperation::Read, "READ" },
        write = { AclOperation::Write, "WRITE" },
        two_phase_commit = { AclOperation::TwoPhaseCommit, "TWO_PHASE_COMMIT" },
    )]
    fn operation_str_maps_kafka_names(op: AclOperation, want: &str) {
        assert2::assert!(operation_str(op) == want, "{op:?}");
    }
}
