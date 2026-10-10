//! The registry lookup and the bounded, expiring cache in front of it.
//!
//! This is where a schema id turns into an answer: a cache read, and on a miss
//! one or two registry calls whose result — positive or negative — is stored
//! with the instant it goes stale. The record check itself is in `check`, and
//! keeping the two apart means the caching policy, including the shorter TTL
//! for a registry that could not answer, reads in one place.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use krabka_schema_serde::{
    error::SchemaSerdeError, registry::model::SchemaReference, subject::SchemaKind,
};
use krabka_units::convert::TimeExt as _;
use qubit_clock::WallClock as _;

use super::{SchemaValidator, UNAVAILABLE_TTL_MS, reject::RejectReason};
use crate::{metrics::BrokerMetrics, schema_validation::ValidationMode, time_util};

const MAX_REFERENCE_SCHEMAS: usize = 32;
const MAX_REFERENCE_BYTES: usize = 1024 * 1024;

/// What the registry said about one schema id.
#[derive(Debug, Clone)]
pub(super) struct SchemaEntry {
    /// Every subject this id is registered under. The subject check is a
    /// membership test against this set.
    pub(super) subjects: HashSet<String>,
    /// The schema text and its format. Only [`ValidationMode::Full`] needs it,
    /// so it is fetched on the first `Full` check for this id and not before —
    /// an `Id`-mode topic never pays for the second registry call.
    pub(super) body: Option<Arc<SchemaBody>>,
}

#[derive(Debug, Clone)]
pub(super) struct SchemaBody {
    pub(super) kind: SchemaKind,
    pub(super) schema: String,
    pub(super) references: HashMap<String, String>,
    pub(super) reference_order: Vec<String>,
}

struct ReferenceClosure {
    sources: HashMap<String, String>,
    order: Vec<String>,
    resolving: HashSet<(String, i32)>,
    fetched_count: usize,
    fetched_bytes: usize,
    remaining_bytes: usize,
}

/// One cached answer, positive or negative, with the instant it goes stale.
#[derive(Debug, Clone)]
pub(super) struct Cached {
    /// `Err` is a negative cache entry: the id is not registered, or the
    /// registry could not say. Both are worth remembering for the TTL.
    entry: Result<SchemaEntry, RejectReason>,
    expires_at_ms: i64,
}

impl SchemaValidator {
    /// The cached answer for `id`, fetching it when absent or stale.
    ///
    /// `mode` decides how much is fetched: `Full` also needs the schema text,
    /// and an entry cached by an earlier `Id` check does not carry it.
    pub(super) async fn entry(
        &self,
        id: u32,
        subject: &str,
        mode: ValidationMode,
        metrics: &BrokerMetrics,
    ) -> Result<SchemaEntry, RejectReason> {
        let now = time_util::epoch_millis(self.clock.now());
        // Expired entries are not evicted eagerly; the read just declines
        // them. Lazy eviction is good enough at LRU capacities in the tens of
        // thousands, and it is what the OPA cache does.
        let hit = {
            let mut cache = self.cache.lock().expect("schema cache mutex poisoned");
            cache
                .get(&id)
                .filter(|cached| cached.expires_at_ms > now)
                .cloned()
        };
        if let Some(cached) = hit {
            match cached.entry {
                Ok(entry) if !entry.subjects.contains(subject) => {
                    metrics.record_schema_cache_hit();
                    return Ok(entry);
                }
                // A `Full` check needs the text; an entry without one was
                // cached by an `Id` check and has to be completed.
                Ok(entry) if mode == ValidationMode::Id || entry.body.is_some() => {
                    metrics.record_schema_cache_hit();
                    return Ok(entry);
                }
                // Cached by an `Id` check while this is `Full`: the text is
                // missing, so this costs a registry round trip like any other
                // miss and is counted as one.
                Ok(_) => {}
                Err(reason) => {
                    metrics.record_schema_cache_hit();
                    return Err(reason);
                }
            }
        }

        metrics.record_schema_cache_miss();
        let fetched = tokio::time::timeout(self.operation_timeout, self.fetch(id, subject, mode))
            .await
            .unwrap_or_else(|_| {
                Err(RejectReason::RegistryUnavailable(format!(
                    "schema registry operation exceeded {:?}",
                    self.operation_timeout
                )))
            });
        // A registry that could not answer is remembered only briefly, so the
        // next produce re-asks instead of inheriting a stale outage. Bounded by
        // `expire_after` so a shorter configured TTL still wins.
        let ttl_ms = if matches!(fetched, Err(RejectReason::RegistryUnavailable(_))) {
            UNAVAILABLE_TTL_MS.min(self.expire_after.millis_i64())
        } else {
            self.expire_after.millis_i64()
        };
        let expires_at_ms = now.saturating_add(ttl_ms);
        {
            let mut cache = self.cache.lock().expect("schema cache mutex poisoned");
            cache.put(
                id,
                Cached {
                    entry: fetched.clone(),
                    expires_at_ms,
                },
            );
        }
        fetched
    }

    /// Ask the registry about `id`.
    async fn fetch(
        &self,
        id: u32,
        subject: &str,
        mode: ValidationMode,
    ) -> Result<SchemaEntry, RejectReason> {
        let bindings = self
            .client
            .subject_versions_for_id(id)
            .await
            .map_err(|e| Self::fetch_error(id, &e))?;
        let subjects: HashSet<_> = bindings.into_iter().map(|b| b.subject).collect();

        if !subjects.contains(subject) {
            return Ok(SchemaEntry {
                subjects,
                body: None,
            });
        }

        let body = if mode == ValidationMode::Full {
            let fetched = self
                .client
                .schema_by_id(id)
                .await
                .map_err(|e| Self::fetch_error(id, &e))?;
            if fetched.schema.len() > MAX_REFERENCE_BYTES {
                return Err(RejectReason::RegistryRejected {
                    kind: krabka_verified::SchemaFailureKind::Permanent,
                    detail: format!("schema closure exceeds {MAX_REFERENCE_BYTES} bytes"),
                });
            }
            let (references, reference_order) = self
                .reference_sources_ordered(
                    &fetched.references,
                    MAX_REFERENCE_BYTES - fetched.schema.len(),
                )
                .await
                .map_err(|e| Self::fetch_error(id, &e))?;
            Some(Arc::new(SchemaBody {
                kind: fetched.kind,
                schema: fetched.schema,
                references,
                reference_order,
            }))
        } else {
            None
        };

        Ok(SchemaEntry { subjects, body })
    }

    async fn reference_sources_ordered(
        &self,
        references: &[SchemaReference],
        remaining_bytes: usize,
    ) -> Result<(HashMap<String, String>, Vec<String>), SchemaSerdeError> {
        let mut closure = ReferenceClosure {
            sources: HashMap::new(),
            order: Vec::new(),
            resolving: HashSet::new(),
            fetched_count: 0,
            fetched_bytes: 0,
            remaining_bytes,
        };
        self.resolve_reference_sources(references, &mut closure, 0)
            .await?;
        Ok((closure.sources, closure.order))
    }

    async fn resolve_reference_sources(
        &self,
        references: &[SchemaReference],
        closure: &mut ReferenceClosure,
        depth: usize,
    ) -> Result<(), SchemaSerdeError> {
        const MAX_REFERENCE_DEPTH: usize = 32;
        if !references.is_empty() && depth >= MAX_REFERENCE_DEPTH {
            return Err(SchemaSerdeError::Schema(format!(
                "schema reference depth exceeds {MAX_REFERENCE_DEPTH}"
            )));
        }
        for reference in references {
            if closure.sources.contains_key(&reference.name) {
                continue;
            }
            let key = (reference.subject.clone(), reference.version);
            if !closure.resolving.insert(key.clone()) {
                return Err(SchemaSerdeError::Schema(format!(
                    "cyclic schema reference at {} version {}",
                    reference.subject, reference.version
                )));
            }
            if closure.fetched_count >= MAX_REFERENCE_SCHEMAS {
                return Err(SchemaSerdeError::Schema(format!(
                    "schema closure exceeds {MAX_REFERENCE_SCHEMAS} references"
                )));
            }
            closure.fetched_count += 1;
            let fetched = self
                .client
                .schema_by_subject_version(&reference.subject, reference.version)
                .await?;
            closure.fetched_bytes = closure
                .fetched_bytes
                .checked_add(fetched.schema.len())
                .filter(|bytes| *bytes <= closure.remaining_bytes)
                .ok_or_else(|| {
                    SchemaSerdeError::Schema(format!(
                        "schema closure exceeds {MAX_REFERENCE_BYTES} bytes"
                    ))
                })?;
            Box::pin(self.resolve_reference_sources(&fetched.references, closure, depth + 1))
                .await?;
            closure.resolving.remove(&key);
            closure
                .sources
                .insert(reference.name.clone(), fetched.schema);
            closure.order.push(reference.name.clone());
        }
        Ok(())
    }

    /// Turn a registry failure into the reason it stands for.
    ///
    /// A 404 is an unknown ID. Transport errors, request timeouts, throttling,
    /// and 5xx responses are transient; other statuses and malformed success
    /// responses fail closed.
    fn fetch_error(id: u32, error: &SchemaSerdeError) -> RejectReason {
        use krabka_verified::SchemaFailureKind::{Malformed, Permanent};

        match error {
            SchemaSerdeError::RegistryStatus { status: 404, .. } => RejectReason::UnknownId(id),
            SchemaSerdeError::RegistryTransport(_)
            | SchemaSerdeError::RegistryStatus {
                status: 408 | 429 | 500..=599,
                ..
            } => RejectReason::RegistryUnavailable(error.to_string()),
            SchemaSerdeError::RegistryDecode(_) => RejectReason::RegistryRejected {
                kind: Malformed,
                detail: error.to_string(),
            },
            _ => RejectReason::RegistryRejected {
                kind: Permanent,
                detail: error.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use assert2::{assert, check};
    use krabka_schema_serde::subject::Role;
    use krabka_units::{millis, minutes, secs};
    use qubit_clock::ManualMonotonicClock;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::schema_validation::validator::test_support::{
        KNOWN_ID, framed, no_metrics, registry, response_endpoint, unavailable_ttl_ms, validator,
        value_check,
    };

    fn clocked_validator(uri: String) -> (Arc<ManualMonotonicClock>, SchemaValidator) {
        let timeline = ManualMonotonicClock::new_shared();
        let clock = timeline.new_wall_clock(SystemTime::now());
        let validator =
            SchemaValidator::with_clock(uri, false, 100, minutes(5), secs(5), None, clock)
                .expect("validator");
        (timeline, validator)
    }

    #[tokio::test]
    async fn an_unregistered_id_is_rejected_and_the_rejection_is_cached() {
        let server = MockServer::start().await;
        // One call for two checks: the negative answer is cached, so a
        // produce storm against one bad id costs one registry call.
        response_endpoint(
            &server,
            "/schemas/ids/99/versions",
            ResponseTemplate::new(404),
            Some(1),
        )
        .await;

        let v = validator(server.uri());
        let field = framed(99, b"anything");
        for _ in 0..2 {
            let got = value_check(&v, ValidationMode::Id, &field).await;
            assert!(let Err(reason) = got);
            check!(reason.label() == "unknown_id", "{reason}");
        }
    }

    #[tokio::test]
    async fn a_second_check_of_the_same_id_is_a_cache_hit() {
        // `expect(1)`: the mock fails the test if the second check calls it.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        for _ in 0..3 {
            check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
        }
    }

    #[tokio::test]
    async fn a_cache_entry_expires_on_its_ttl() {
        // Two calls: one before the TTL passes and one after.
        let server = registry(2).await;
        let timeline = ManualMonotonicClock::new_shared();
        let clock = timeline.new_wall_clock(SystemTime::now());
        let v =
            SchemaValidator::with_clock(server.uri(), false, 100, millis(10), secs(5), None, clock)
                .expect("validator");
        let field = framed(KNOWN_ID, b"anything");

        check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
        // Past the TTL on a controlled timeline, so the expiry is an assertion
        // and not a race against a real sleep. The wall clock is anchored to
        // that timeline, so advancing it moves the cache's stamp with it.
        timeline
            .advance(Duration::from_millis(50))
            .expect("manual time moves forward");
        check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
    }

    async fn check_id(v: &SchemaValidator, field: &[u8], metrics: &BrokerMetrics) {
        check!(
            v.check("orders", Role::Value, ValidationMode::Id, field, metrics)
                .await
                .is_ok()
        );
    }

    async fn reference_endpoint(
        server: &MockServer,
        index: usize,
        references: Option<serde_json::Value>,
        expected_calls: u64,
    ) {
        let mut body = serde_json::json!({
            "subject": format!("subject-{index}"),
            "version": 1,
            "id": index,
            "schema": r#"{"type":"record","name":"Leaf","fields":[]}"#,
        });
        if let Some(references) = references {
            body["references"] = references;
        }
        response_endpoint(
            server,
            &format!("/subjects/subject-{index}/versions/1"),
            ResponseTemplate::new(200).set_body_json(body),
            Some(expected_calls),
        )
        .await;
    }

    #[tokio::test]
    async fn the_cache_counters_move_on_a_miss_then_a_hit() {
        // `registry(1)` allows exactly one call to `/versions`, so the second
        // check is served from the cache. Without a counter on each path these
        // two gauges stay at zero for the life of the broker, which is what
        // this asserts against.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        let metrics = BrokerMetrics::new();

        check_id(&v, &field, &metrics).await;
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 0);

        check_id(&v, &field, &metrics).await;
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 1);
    }

    #[tokio::test]
    async fn a_full_cache_hit_shares_the_schema_closure() {
        let server = registry(1).await;
        let v = validator(server.uri());
        let metrics = no_metrics();
        let first = v
            .entry(KNOWN_ID, "orders-value", ValidationMode::Full, &metrics)
            .await
            .expect("first lookup")
            .body
            .expect("full lookup has a body");
        let second = v
            .entry(KNOWN_ID, "orders-value", ValidationMode::Full, &metrics)
            .await
            .expect("cached lookup")
            .body
            .expect("cached full lookup has a body");

        check!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn a_registry_that_could_not_answer_is_re_asked_after_the_short_ttl() {
        // The registry fails once and then recovers. A five-minute
        // `expire_after` must not keep rejecting for five minutes: a negative
        // entry for an unreachable registry carries its own short TTL, so the
        // next produce re-asks instead of inheriting the outage.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}/versions"),
            ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])),
            None,
        )
        .await;

        let (timeline, v) = clocked_validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");

        let got = value_check(&v, ValidationMode::Id, &field).await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "registry_unavailable", "{reason}");

        timeline
            .advance(Duration::from_millis(unavailable_ttl_ms() + 1))
            .expect("manual time moves forward");
        check!(
            value_check(&v, ValidationMode::Id, &field).await.is_ok(),
            "the registry recovered, and the short negative TTL let the broker re-ask"
        );
    }

    #[tokio::test]
    async fn an_unregistered_id_stays_cached_past_the_unavailable_ttl() {
        // The short TTL is only for a registry that could not answer. A 404 is
        // the registry answering, so it keeps the full `expire_after` —
        // otherwise a produce storm against one bad id would re-ask every two
        // seconds. `expect(1)` is the assertion: the second check never asked.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let (timeline, v) = clocked_validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");

        for _ in 0..2 {
            let got = value_check(&v, ValidationMode::Id, &field).await;
            assert!(let Err(reason) = got);
            check!(reason.label() == "unknown_id", "{reason}");
            timeline
                .advance(Duration::from_millis(unavailable_ttl_ms() + 1))
                .expect("manual time moves forward");
        }
    }

    #[tokio::test]
    async fn a_reference_chain_may_end_at_the_depth_limit() {
        const DEPTH: usize = 32;
        let server = MockServer::start().await;
        for index in 0..DEPTH {
            let references = if index + 1 == DEPTH {
                serde_json::json!([])
            } else {
                serde_json::json!([{
                    "name": format!("Ref{}", index + 1),
                    "subject": format!("subject-{}", index + 1),
                    "version": 1
                }])
            };
            reference_endpoint(&server, index, Some(references), 1).await;
        }

        let v = validator(server.uri());
        let root = [SchemaReference {
            name: "Ref0".to_owned(),
            subject: "subject-0".to_owned(),
            version: 1,
        }];
        let result = v
            .reference_sources_ordered(&root, MAX_REFERENCE_BYTES)
            .await;
        assert!(let Ok((sources, order)) = result);
        check!(sources.len() == DEPTH);
        check!(order.len() == DEPTH);
    }

    #[tokio::test]
    async fn a_reference_closure_is_bounded_by_schema_count() {
        let server = MockServer::start().await;
        let mut root = Vec::new();
        for index in 0..=MAX_REFERENCE_SCHEMAS {
            root.push(SchemaReference {
                name: format!("Ref{index}"),
                subject: format!("subject-{index}"),
                version: 1,
            });
            reference_endpoint(
                &server,
                index,
                None,
                u64::from(index < MAX_REFERENCE_SCHEMAS),
            )
            .await;
        }

        let result = validator(server.uri())
            .reference_sources_ordered(&root, MAX_REFERENCE_BYTES)
            .await;
        assert!(let Err(error) = result);
        check!(
            error
                .to_string()
                .contains("schema closure exceeds 32 references"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_reference_closure_is_bounded_by_total_bytes() {
        let server = MockServer::start().await;
        response_endpoint(
            &server,
            "/subjects/large/versions/1",
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "large",
                "version": 1,
                "id": 7,
                "schema": "x".repeat(MAX_REFERENCE_BYTES + 1)
            })),
            Some(1),
        )
        .await;
        let root = [SchemaReference {
            name: "large".to_owned(),
            subject: "large".to_owned(),
            version: 1,
        }];

        let result = validator(server.uri())
            .reference_sources_ordered(&root, MAX_REFERENCE_BYTES)
            .await;
        assert!(let Err(error) = result);
        check!(
            error
                .to_string()
                .contains("schema closure exceeds 1048576 bytes"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn one_deadline_bounds_the_whole_registry_operation() {
        let server = MockServer::start().await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}/versions"),
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_json(serde_json::json!([
                    {"subject": "orders-value", "version": 1}
                ])),
            Some(1),
        )
        .await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}"),
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_json(serde_json::json!({"schema": r#""string""#})),
            Some(1),
        )
        .await;
        let v = SchemaValidator::new(server.uri(), false, 100, minutes(1), millis(80))
            .expect("validator");

        let result = value_check(&v, ValidationMode::Full, &framed(KNOWN_ID, b"a")).await;
        assert!(let Err(reason) = result);
        check!(reason.label() == "registry_unavailable", "{reason}");
    }
}
