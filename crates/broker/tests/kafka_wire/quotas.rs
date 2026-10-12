//! Wire drivers for the client-quota admin APIs, `AlterClientQuotas` and
//! `DescribeClientQuotas`, each driven over its own authenticated SASL/PLAIN
//! connection.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{Decode, Encode};

use super::{round_trip, sasl_plain_authenticate};
use crate::{
    kafka_wire::{ExchangeSetup as WireExchangeSetup, PlainAuthenticationSetup},
    support::wire::HeaderEncoding as RequestHeaderEncoding,
};

pub type QuotaEntity = Vec<(String, Option<String>)>;
pub type QuotaOperations = Vec<(String, f64, bool)>;
pub type QuotaEntries = Vec<(QuotaEntity, QuotaOperations)>;

#[derive(Clone, Copy, Default)]
pub enum QuotaValidation {
    #[default]
    Apply,
    ValidateOnly,
}

#[derive(Clone, Copy, Default)]
pub enum QuotaFilterMatching {
    #[default]
    ContainsComponents,
    Strict,
}

#[derive(krabka_macros::FieldDefaults)]
pub struct QuotaAlterSetup<'a> {
    #[default(PlainAuthenticationSetup::admin("krabka-broker-test"))]
    pub authentication: PlainAuthenticationSetup<'a>,
    pub validation: QuotaValidation,
}

#[derive(krabka_macros::FieldDefaults)]
pub struct QuotaDescribeSetup<'a> {
    #[default(PlainAuthenticationSetup::admin("krabka-broker-test"))]
    pub authentication: PlainAuthenticationSetup<'a>,
    pub matching: QuotaFilterMatching,
}

#[derive(Clone, Copy)]
pub struct QuotaValue(pub f64);
impl Default for QuotaValue {
    fn default() -> Self {
        Self(1024.0)
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct UserQuotaSetup<'a> {
    #[default(PlainAuthenticationSetup::admin("krabka-broker-test"))]
    pub authentication: PlainAuthenticationSetup<'a>,
    #[default("alice")]
    pub user: &'a str,
    #[default("producer_byte_rate")]
    pub key: &'a str,
    pub value: QuotaValue,
}

impl UserQuotaSetup<'_> {
    fn alteration_entries(&self) -> QuotaEntries {
        vec![(
            vec![("user".into(), Some(self.user.into()))],
            vec![(self.key.into(), self.value.0, false)],
        )]
    }

    fn alteration_options(&self) -> QuotaAlterSetup<'_> {
        QuotaAlterSetup {
            authentication: self.authentication.clone(),
            ..Default::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for AlterClientQuotas and DescribeClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drives `AlterClientQuotas` with `api_key=49` over a SASL/PLAIN connection.
///
/// `entries` is a list of `(entity_components, ops)` where:
/// - `entity_components` is `Vec<(entity_type, entity_name)>`, e.g.
///   `vec![("user".into(), Some("alice".into()))]`
/// - `ops` is `Vec<(key, value, remove)>`, e.g.
///   `vec![("producer_byte_rate".into(), 1024.0, false)]`
///
/// Returns the per-entry `(entity, error_code)` pairs.
pub async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    entries: QuotaEntries,
    setup: &QuotaAlterSetup<'_>,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    use krabka_protocol::owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    };

    let req = AlterClientQuotasRequest {
        entries: entries
            .into_iter()
            .map(|(entity_parts, ops)| EntryData {
                entity: entity_parts
                    .into_iter()
                    .map(|(entity_type, entity_name)| EntityData {
                        entity_type,
                        entity_name,
                        ..Default::default()
                    })
                    .collect(),
                ops: ops
                    .into_iter()
                    .map(|(key, value, remove)| OpData {
                        key,
                        value,
                        remove,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        validate_only: matches!(setup.validation, QuotaValidation::ValidateOnly),
        ..Default::default()
    };

    let version: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, &setup.authentication)
        .await
        .expect("SASL authenticate for AlterClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode AlterClientQuotas");
    let resp_bytes = round_trip(
        &mut stream,
        WireExchangeSetup {
            api_key: WireApiKey(49),
            version: WireApiVersion(version),
            client_id: setup.authentication.client_id,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("AlterClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterClientQuotasResponse::decode(&mut cur, version)
        .expect("decode AlterClientQuotasResponse");

    resp.entries
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            (entity, e.error_code)
        })
        .collect()
}

/// Drives `DescribeClientQuotas` with `api_key=48` over a SASL/PLAIN connection.
///
/// `components` is a list of `(entity_type, match_type, match_value)`:
/// - `match_type`: 0=EXACT, 1=DEFAULT, 2=ANY
///
/// Returns the list of `(entity, values)` pairs from the response.
pub async fn drive_describe_client_quotas_sasl(
    addr: SocketAddr,
    components: Vec<(String, i8, Option<String>)>,
    setup: &QuotaDescribeSetup<'_>,
) -> Vec<(Vec<(String, Option<String>)>, Vec<(String, f64)>)> {
    use krabka_protocol::owned::{
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::DescribeClientQuotasResponse,
    };

    let req = DescribeClientQuotasRequest {
        components: components
            .into_iter()
            .map(|(entity_type, match_type, match_)| ComponentData {
                entity_type,
                match_type,
                match_,
                ..Default::default()
            })
            .collect(),
        strict: matches!(setup.matching, QuotaFilterMatching::Strict),
        ..Default::default()
    };

    let version: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, &setup.authentication)
        .await
        .expect("SASL authenticate for DescribeClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode DescribeClientQuotas");
    let resp_bytes = round_trip(
        &mut stream,
        WireExchangeSetup {
            api_key: WireApiKey(48),
            version: WireApiVersion(version),
            client_id: setup.authentication.client_id,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("DescribeClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeClientQuotasResponse::decode(&mut cur, version)
        .expect("decode DescribeClientQuotasResponse");

    assert!(resp.error_code == 0, "DescribeClientQuotas top-level error");

    resp.entries
        .unwrap_or_default()
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            let values = e.values.into_iter().map(|v| (v.key, v.value)).collect();
            (entity, values)
        })
        .collect()
}

/// Set one named user's quota through the authenticated admin API, then await its image.
pub async fn set_user_quota(
    broker: &krabka_broker::BrokerHandle,
    addr: SocketAddr,
    setup: UserQuotaSetup<'_>,
) {
    let entity: krabka_metadata::EntityKey = vec![("user".into(), Some(setup.user.into()))];
    let response = drive_alter_client_quotas_sasl(
        addr,
        setup.alteration_entries(),
        &setup.alteration_options(),
    )
    .await;
    assert!(response[0].1 == 0, "alter quota must succeed");
    broker
        .wait_for_image(|image| {
            image
                .client_quotas()
                .get(&entity)
                .and_then(|quota| quota.get(setup.key))
                == Some(&setup.value.0)
        })
        .await;
}

/// Alter one user's quota, retaining the caller's authentication and client id.
///
/// # Panics
/// Panics if the original authentication, encoding or wire exchange fails.
pub async fn alter_user_quota(
    addr: SocketAddr,
    setup: UserQuotaSetup<'_>,
) -> Vec<(QuotaEntity, i16)> {
    drive_alter_client_quotas_sasl(
        addr,
        setup.alteration_entries(),
        &setup.alteration_options(),
    )
    .await
}
