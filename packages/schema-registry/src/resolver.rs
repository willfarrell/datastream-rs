// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Cached Confluent Schema Registry lookups by schema id
//! (`GET /schemas/ids/{id}`). Same cache/eviction/dedup semantics as
//! datastream-aws `aws_glue_schema_registry_resolver`, keyed by schema id and
//! adding the registry's references.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use datastream_core::{CancellationToken, Error, Result, Value};
use futures::future::{BoxFuture, FutureExt, Shared};
use reqwest::Client;
use url::Url;

const DEFAULT_MAX_CACHE_SIZE: usize = 1000;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BODY_SIZE: usize = 1_048_576; // 1MiB

#[derive(Clone, Debug, PartialEq)]
pub struct ConfluentSchema {
    pub schema_id: u32,
    /// Schema text as stored (Avro/JSON Schema JSON, or `.proto` source).
    pub schema_definition: Option<String>,
    /// `"AVRO"` (the registry omits it for Avro), `"PROTOBUF"` or `"JSON"`.
    pub data_format: String,
    /// The registry's `{ name, subject, version }` references, as returned.
    pub references: Vec<Value>,
}

/// A failed lookup; `status` is set for a non-2xx response.
#[derive(Clone, Debug)]
pub struct ConfluentSchemaRegistryError {
    pub message: String,
    pub status: Option<u16>,
}

impl ConfluentSchemaRegistryError {
    fn new(message: String) -> Self {
        Self {
            message,
            status: None,
        }
    }
}

impl std::fmt::Display for ConfluentSchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ConfluentSchemaRegistryError {}

#[derive(Default, Clone, Debug)]
pub struct ConfluentSchemaRegistryOptions {
    /// Registry base URL, e.g. `https://registry.example.com`.
    pub url: Option<String>,
    /// Request headers, e.g. `Authorization`.
    pub headers: Vec<(String, String)>,
    /// How long a lookup stays cached (default: forever).
    pub cache_expiry: Option<Duration>,
    /// Oldest entries are evicted past this many (default 1000; 0 disables
    /// caching).
    pub max_cache_size: Option<usize>,
    /// Per-lookup timeout, covering the response body (default 30s).
    pub timeout: Option<Duration>,
    /// Cancelling it aborts in-flight lookups.
    pub signal: Option<CancellationToken>,
    /// Response body limit (default 1MiB; `usize::MAX` for none).
    pub max_body_size: Option<usize>,
    pub client: Option<Client>,
}

type Lookup = std::result::Result<ConfluentSchema, ConfluentSchemaRegistryError>;
type Pending = Shared<BoxFuture<'static, Lookup>>;

#[derive(Default)]
struct State {
    cache: HashMap<u32, (ConfluentSchema, Option<Instant>)>,
    order: VecDeque<u32>,
    // Parallel lookups for the same id share one request.
    inflight: HashMap<u32, Pending>,
}

impl State {
    // ponytail: O(n) scan of the FIFO order; fine for the default 1000 entries.
    fn remove(&mut self, id: u32) {
        if self.cache.remove(&id).is_some() {
            self.order.retain(|key| *key != id);
        }
    }
}

pub struct ConfluentSchemaRegistryResolver {
    client: Client,
    base: Url,
    base_path: String,
    headers: Vec<(String, String)>,
    cache_expiry: Option<Duration>,
    max_cache_size: usize,
    timeout: Duration,
    signal: CancellationToken,
    max_body_size: usize,
    state: Mutex<State>,
}

pub fn confluent_schema_registry_resolver(
    options: ConfluentSchemaRegistryOptions,
) -> Result<ConfluentSchemaRegistryResolver> {
    let base = options
        .url
        .as_deref()
        .and_then(|url| Url::parse(url).ok())
        .ok_or("confluentSchemaRegistryResolver: url must be an absolute URL")?;
    // Append to the path, not the string: a base with a query string
    // (?tenant=a) must keep it after /schemas/ids/{id}.
    let base_path = base.path().trim_end_matches('/').to_string();
    Ok(ConfluentSchemaRegistryResolver {
        client: options.client.unwrap_or_default(),
        base,
        base_path,
        headers: options.headers,
        cache_expiry: options.cache_expiry,
        max_cache_size: options.max_cache_size.unwrap_or(DEFAULT_MAX_CACHE_SIZE),
        timeout: options.timeout.unwrap_or(DEFAULT_TIMEOUT),
        signal: options.signal.unwrap_or_default(),
        max_body_size: options.max_body_size.unwrap_or(DEFAULT_MAX_BODY_SIZE),
        state: Mutex::default(),
    })
}

impl ConfluentSchemaRegistryResolver {
    pub async fn resolve(&self, schema_id: u32) -> Result<ConfluentSchema> {
        let pending = {
            let mut state = self.lock();
            if let Some((value, expires)) = state.cache.get(&schema_id) {
                if !expires.is_some_and(|expires| expires <= Instant::now()) {
                    return Ok(value.clone());
                }
            }
            // Drop the stale entry so the refreshed one is queued at the tail,
            // keeping eviction true-FIFO.
            state.remove(schema_id);
            let lookup = self.lookup(schema_id);
            let inflight = state.inflight.entry(schema_id);
            inflight.or_insert_with(|| lookup.boxed().shared()).clone()
        };
        let result = pending.await;
        let mut state = self.lock();
        // A failure (or timeout) must not stay in flight: the next call retries.
        state.inflight.remove(&schema_id);
        let value = result.map_err(Error::from)?;
        // max_cache_size 0 disables caching; evicting down to it would
        // otherwise still keep this entry.
        if self.max_cache_size > 0 {
            state.remove(schema_id);
            while state.order.len() >= self.max_cache_size {
                let Some(oldest) = state.order.pop_front() else {
                    break;
                };
                state.cache.remove(&oldest);
            }
            let expires = self.cache_expiry.map(|expiry| Instant::now() + expiry);
            state.cache.insert(schema_id, (value.clone(), expires));
            state.order.push_back(schema_id);
        }
        Ok(value)
    }

    fn lookup(&self, schema_id: u32) -> impl std::future::Future<Output = Lookup> + Send + 'static {
        let mut url = self.base.clone();
        url.set_path(&format!("{}/schemas/ids/{schema_id}", self.base_path));
        let mut request = self.client.get(url);
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        let (signal, timeout, max) = (self.signal.clone(), self.timeout, self.max_body_size);
        async move {
            // The timeout also covers reading the body, so a registry that
            // stalls mid-response is bounded too.
            tokio::select! {
                _ = signal.cancelled() => Err(ConfluentSchemaRegistryError::new("Aborted".into())),
                result = tokio::time::timeout(timeout, fetch_schema(request, schema_id, max)) => {
                    result.unwrap_or_else(|_| {
                        Err(ConfluentSchemaRegistryError::new(format!(
                            "confluentSchemaRegistryResolver: schema {schema_id} lookup timed out ({}ms)",
                            timeout.as_millis()
                        )))
                    })
                }
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

async fn fetch_schema(request: reqwest::RequestBuilder, schema_id: u32, max: usize) -> Lookup {
    let fail = |message: String| ConfluentSchemaRegistryError::new(message);
    let mut response = request.send().await.map_err(|e| fail(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        // Dropping the response releases the connection; the body is not needed.
        return Err(ConfluentSchemaRegistryError {
            message: format!(
                "confluentSchemaRegistryResolver: schema {schema_id} lookup failed (HTTP {})",
                status.as_u16()
            ),
            status: Some(status.as_u16()),
        });
    }
    // Buffering a body of any size would trust the registry; it is a trust
    // boundary, so cap it.
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| fail(e.to_string()))? {
        if body.len().saturating_add(chunk.len()) > max {
            return Err(fail(format!(
                "confluentSchemaRegistryResolver: schema {schema_id} response exceeds maxBodySize ({max} bytes)"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return Err(fail(format!(
            "confluentSchemaRegistryResolver: schema {schema_id} lookup returned no body"
        )));
    }
    let body: Value = serde_json::from_slice(&body).map_err(|e| fail(e.to_string()))?;
    // The registry omits schemaType for Avro, its original (default) format,
    // and references when the schema has none.
    Ok(ConfluentSchema {
        schema_id,
        schema_definition: body["schema"].as_str().map(str::to_string),
        data_format: body["schemaType"].as_str().unwrap_or("AVRO").to_string(),
        references: body["references"].as_array().cloned().unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    const AVRO: &str = r#"{"type":"string"}"#;

    fn options(server: &MockServer) -> ConfluentSchemaRegistryOptions {
        ConfluentSchemaRegistryOptions {
            url: Some(server.uri()),
            ..Default::default()
        }
    }

    fn resolver(options: ConfluentSchemaRegistryOptions) -> ConfluentSchemaRegistryResolver {
        confluent_schema_registry_resolver(options).unwrap()
    }

    /// Answers every lookup with an Avro schema.
    async fn registry() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "schema": AVRO })))
            .mount(&server)
            .await;
        server
    }

    // The ids requested, in order.
    async fn requested(server: &MockServer) -> Vec<String> {
        let requests: Vec<Request> = server.received_requests().await.unwrap();
        requests
            .iter()
            .map(|r| r.url.path().rsplit('/').next().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn gets_schemas_ids_and_returns_schema_metadata() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/42"))
            .and(header("authorization", "Basic abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "schema": AVRO })))
            .expect(1)
            .mount(&server)
            .await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            headers: vec![("Authorization".into(), "Basic abc".into())],
            ..options(&server)
        });
        assert_eq!(
            resolve.resolve(42).await.unwrap(),
            ConfluentSchema {
                schema_id: 42,
                schema_definition: Some(AVRO.into()),
                data_format: "AVRO".into(),
                references: vec![],
            }
        );
    }

    #[tokio::test]
    async fn returns_references_and_maps_schema_type() {
        let server = MockServer::start().await;
        let references = json!([{ "name": "Address", "subject": "address", "version": 3 }]);
        Mock::given(path("/schemas/ids/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({ "schema": "syntax", "schemaType": "PROTOBUF", "references": references }),
            ))
            .mount(&server)
            .await;
        Mock::given(path("/schemas/ids/2"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "schema": "{}", "schemaType": "JSON" })),
            )
            .mount(&server)
            .await;
        let resolve = resolver(options(&server));
        let schema = resolve.resolve(1).await.unwrap();
        assert_eq!(schema.data_format, "PROTOBUF");
        assert_eq!(json!(schema.references), references);
        assert_eq!(resolve.resolve(2).await.unwrap().data_format, "JSON");
    }

    #[tokio::test]
    async fn keeps_the_base_url_path_and_query_string() {
        let server = registry().await;
        let uri = server.uri();
        for (url, id) in [
            (format!("{uri}/?tenant=a"), 7),
            (format!("{uri}/registry?tenant=a&b=1"), 8),
            (format!("{uri}?tenant=a"), 9),
            // A trailing slash is dropped.
            (format!("{uri}/registry/"), 10),
        ] {
            let resolve = resolver(ConfluentSchemaRegistryOptions {
                url: Some(url),
                ..Default::default()
            });
            resolve.resolve(id).await.unwrap();
        }
        let requests = server.received_requests().await.unwrap();
        let urls: Vec<String> = requests
            .iter()
            .map(|r| r.url[url::Position::BeforePath..].to_string())
            .collect();
        assert_eq!(
            urls,
            [
                "/schemas/ids/7?tenant=a",
                "/registry/schemas/ids/8?tenant=a&b=1",
                "/schemas/ids/9?tenant=a",
                "/registry/schemas/ids/10",
            ]
        );
    }

    #[test]
    fn rejects_a_missing_or_relative_url() {
        for url in [None, Some("/schemas".to_string()), Some(String::new())] {
            let e = confluent_schema_registry_resolver(ConfluentSchemaRegistryOptions {
                url,
                ..Default::default()
            })
            .err()
            .unwrap();
            assert_eq!(
                e.to_string(),
                "confluentSchemaRegistryResolver: url must be an absolute URL"
            );
        }
    }

    #[tokio::test]
    async fn accepts_both_ends_of_the_id_range() {
        let server = registry().await;
        let resolve = resolver(options(&server));
        assert_eq!(resolve.resolve(0).await.unwrap().schema_id, 0);
        assert_eq!(resolve.resolve(u32::MAX).await.unwrap().schema_id, u32::MAX);
        assert_eq!(requested(&server).await, ["0", "4294967295"]);
    }

    #[tokio::test]
    async fn times_out_then_retries() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "schema": AVRO })))
            .mount(&server)
            .await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            timeout: Some(Duration::from_millis(20)),
            ..options(&server)
        });
        let e = resolve.resolve(1).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "confluentSchemaRegistryResolver: schema 1 lookup timed out (20ms)"
        );
        // The timed-out lookup does not stay in flight.
        assert_eq!(resolve.resolve(1).await.unwrap().schema_id, 1);
    }

    #[tokio::test]
    async fn aborts_when_signal_is_cancelled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let signal = CancellationToken::new();
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            signal: Some(signal.clone()),
            ..options(&server)
        });
        let (result, ()) = tokio::join!(resolve.resolve(1), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            signal.cancel();
        });
        assert_eq!(result.unwrap_err().to_string(), "Aborted");
    }

    // A response whose JSON body is exactly `size` bytes.
    fn sized(size: usize) -> ResponseTemplate {
        let body = json!({ "schema": "a".repeat(size - 13) }).to_string();
        assert_eq!(body.len(), size);
        ResponseTemplate::new(200).set_body_string(body)
    }

    #[tokio::test]
    async fn caps_the_response_body_at_1mib_by_default() {
        let server = MockServer::start().await;
        Mock::given(path("/schemas/ids/1"))
            .respond_with(sized(1_048_576))
            .mount(&server)
            .await;
        Mock::given(path("/schemas/ids/2"))
            .respond_with(sized(1_048_577))
            .mount(&server)
            .await;
        let resolve = resolver(options(&server));
        let schema = resolve.resolve(1).await.unwrap();
        assert_eq!(schema.schema_definition.unwrap().len(), 1_048_576 - 13);
        assert_eq!(
            resolve.resolve(2).await.unwrap_err().to_string(),
            "confluentSchemaRegistryResolver: schema 2 response exceeds maxBodySize (1048576 bytes)"
        );
    }

    #[tokio::test]
    async fn honors_max_body_size() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(sized(2_000_000))
            .mount(&server)
            .await;
        let e = resolver(ConfluentSchemaRegistryOptions {
            max_body_size: Some(20),
            ..options(&server)
        })
        .resolve(3)
        .await
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            "confluentSchemaRegistryResolver: schema 3 response exceeds maxBodySize (20 bytes)"
        );
        // usize::MAX reads any size.
        let schema = resolver(ConfluentSchemaRegistryOptions {
            max_body_size: Some(usize::MAX),
            ..options(&server)
        })
        .resolve(3)
        .await
        .unwrap();
        assert_eq!(schema.schema_definition.unwrap().len(), 2_000_000 - 13);
    }

    #[tokio::test]
    async fn rejects_a_2xx_response_with_no_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let e = resolver(options(&server)).resolve(4).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "confluentSchemaRegistryResolver: schema 4 lookup returned no body"
        );
    }

    #[tokio::test]
    async fn errors_with_the_status_and_does_not_cache_a_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "schema": AVRO })))
            .mount(&server)
            .await;
        let resolve = resolver(options(&server));
        let e = resolve.resolve(5).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "confluentSchemaRegistryResolver: schema 5 lookup failed (HTTP 404)"
        );
        let e = e.downcast_ref::<ConfluentSchemaRegistryError>().unwrap();
        assert_eq!(e.status, Some(404));
        assert_eq!(resolve.resolve(5).await.unwrap().schema_id, 5);
        assert_eq!(requested(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn caches_indefinitely_by_default_and_dedupes_in_flight() {
        let server = registry().await;
        let resolve = resolver(options(&server));
        let (a, b) = tokio::join!(resolve.resolve(1), resolve.resolve(1));
        assert_eq!(a.unwrap(), b.unwrap());
        resolve.resolve(1).await.unwrap();
        resolve.resolve(2).await.unwrap();
        assert_eq!(requested(&server).await, ["1", "2"]);
    }

    #[tokio::test]
    async fn respects_cache_expiry() {
        let server = registry().await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            cache_expiry: Some(Duration::from_millis(50)),
            ..options(&server)
        });
        resolve.resolve(1).await.unwrap();
        resolve.resolve(1).await.unwrap();
        assert_eq!(requested(&server).await.len(), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        resolve.resolve(1).await.unwrap();
        assert_eq!(requested(&server).await.len(), 2);

        // A zero expiry is a finite TTL, so every lookup refetches.
        let server = registry().await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            cache_expiry: Some(Duration::ZERO),
            ..options(&server)
        });
        resolve.resolve(1).await.unwrap();
        resolve.resolve(1).await.unwrap();
        assert_eq!(requested(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn evicts_the_oldest_entry_past_max_cache_size() {
        let server = registry().await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            max_cache_size: Some(2),
            ..options(&server)
        });
        for id in [1, 2, 3, 2, 3, 1, 3] {
            resolve.resolve(id).await.unwrap();
        }
        // 3 evicted 1; refetching 1 evicted 2.
        assert_eq!(requested(&server).await, ["1", "2", "3", "1"]);
    }

    #[tokio::test]
    async fn max_cache_size_zero_does_not_cache() {
        let server = registry().await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            max_cache_size: Some(0),
            ..options(&server)
        });
        resolve.resolve(1).await.unwrap();
        resolve.resolve(1).await.unwrap();
        assert_eq!(requested(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn requeues_a_refreshed_key_at_the_fifo_tail() {
        let server = registry().await;
        let resolve = resolver(ConfluentSchemaRegistryOptions {
            cache_expiry: Some(Duration::from_millis(100)),
            max_cache_size: Some(3),
            ..options(&server)
        });
        resolve.resolve(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        resolve.resolve(2).await.unwrap();
        tokio::time::sleep(Duration::from_millis(70)).await;
        resolve.resolve(1).await.unwrap(); // stale -> refreshed, now newest
        resolve.resolve(3).await.unwrap();
        resolve.resolve(4).await.unwrap(); // evicts the oldest: 2
        resolve.resolve(1).await.unwrap(); // still cached
        assert_eq!(requested(&server).await, ["1", "2", "1", "3", "4"]);
    }
}
