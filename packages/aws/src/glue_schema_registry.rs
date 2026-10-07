// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Cached Glue Schema Registry lookups by schema version id.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_sdk_glue::Client;
use aws_smithy_types::error::display::DisplayErrorContext;
use datastream_core::{Error, Result};
use futures::future::{BoxFuture, FutureExt, Shared};

#[derive(Clone, Debug, PartialEq)]
pub struct GlueSchema {
    pub schema_version_id: Option<String>,
    pub schema_definition: Option<String>,
    pub data_format: Option<String>,
}

#[derive(Default, Clone, Debug)]
pub struct AwsGlueSchemaRegistryOptions {
    /// How long a lookup stays cached (default: forever).
    pub cache_expiry: Option<Duration>,
    /// Oldest entries are evicted past this many (default 1000).
    pub max_cache_size: Option<usize>,
}

type Pending = Shared<BoxFuture<'static, Result<GlueSchema, String>>>;

#[derive(Default)]
struct State {
    cache: HashMap<String, (GlueSchema, Option<Instant>)>,
    order: VecDeque<String>,
    // Parallel lookups for the same id share one GetSchemaVersion call.
    inflight: HashMap<String, Pending>,
}

impl State {
    // ponytail: O(n) scan of the FIFO order; fine for the default 1000 entries.
    fn remove(&mut self, id: &str) {
        if self.cache.remove(id).is_some() {
            self.order.retain(|key| key != id);
        }
    }
}

pub struct AwsGlueSchemaRegistryResolver {
    client: Client,
    cache_expiry: Option<Duration>,
    max_cache_size: usize,
    state: Mutex<State>,
}

pub fn aws_glue_schema_registry_resolver(
    client: Client,
    options: AwsGlueSchemaRegistryOptions,
) -> AwsGlueSchemaRegistryResolver {
    AwsGlueSchemaRegistryResolver {
        client,
        cache_expiry: options.cache_expiry,
        max_cache_size: options.max_cache_size.unwrap_or(1000),
        state: Mutex::default(),
    }
}

impl AwsGlueSchemaRegistryResolver {
    pub async fn resolve(&self, schema_version_id: &str) -> Result<GlueSchema> {
        if schema_version_id.is_empty() {
            return Err("awsGlueSchemaRegistryResolver: schemaVersionId required".into());
        }
        let pending = {
            let mut state = self.lock();
            if let Some((value, expires)) = state.cache.get(schema_version_id) {
                if !expires.is_some_and(|expires| expires <= Instant::now()) {
                    return Ok(value.clone());
                }
            }
            state.remove(schema_version_id);
            let request = self
                .client
                .get_schema_version()
                .schema_version_id(schema_version_id);
            let fetch = async move {
                let output = request
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
                Ok::<_, String>(GlueSchema {
                    schema_version_id: output.schema_version_id,
                    schema_definition: output.schema_definition,
                    data_format: output.data_format.map(|f| f.as_str().to_string()),
                })
            };
            let inflight = state.inflight.entry(schema_version_id.to_string());
            inflight.or_insert_with(|| fetch.boxed().shared()).clone()
        };
        let result = pending.await;
        let mut state = self.lock();
        state.inflight.remove(schema_version_id);
        let value = result.map_err(Error::from)?;
        state.remove(schema_version_id);
        while state.order.len() >= self.max_cache_size {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            state.cache.remove(&oldest);
        }
        let expires = self.cache_expiry.map(|expiry| Instant::now() + expiry);
        let key = schema_version_id.to_string();
        state.cache.insert(key.clone(), (value.clone(), expires));
        state.order.push_back(key);
        Ok(value)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_glue::operation::get_schema_version::GetSchemaVersionOutput;
    use aws_sdk_glue::types::DataFormat;
    use aws_smithy_mocks::{mock, mock_client, Rule, RuleMode};

    fn echo_client() -> (Client, Rule) {
        let rule = mock!(Client::get_schema_version).then_compute_output(|req| {
            GetSchemaVersionOutput::builder()
                .schema_version_id(req.schema_version_id().unwrap_or_default())
                .schema_definition("syntax = \"proto3\";")
                .data_format(DataFormat::Protobuf)
                .build()
        });
        (
            mock_client!(aws_sdk_glue, RuleMode::MatchAny, [&rule]),
            rule,
        )
    }

    fn resolver(
        client: Client,
        cache_expiry: Option<Duration>,
        max_cache_size: Option<usize>,
    ) -> AwsGlueSchemaRegistryResolver {
        aws_glue_schema_registry_resolver(
            client,
            AwsGlueSchemaRegistryOptions {
                cache_expiry,
                max_cache_size,
            },
        )
    }

    #[tokio::test]
    async fn fetches_and_caches_schema_metadata() {
        let (client, rule) = echo_client();
        let resolve = resolver(client, None, None);
        let schema = resolve.resolve("v1").await.unwrap();
        assert_eq!(
            schema,
            GlueSchema {
                schema_version_id: Some("v1".into()),
                schema_definition: Some("syntax = \"proto3\";".into()),
                data_format: Some("PROTOBUF".into()),
            }
        );
        resolve.resolve("v1").await.unwrap();
        resolve.resolve("v1").await.unwrap();
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn respects_cache_expiry() {
        let (client, rule) = echo_client();
        let resolve = resolver(client, Some(Duration::from_millis(1)), None);
        resolve.resolve("v1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        resolve.resolve("v1").await.unwrap();
        assert_eq!(rule.num_calls(), 2);

        // A zero expiry is a finite TTL, so every lookup refetches.
        let (client, rule) = echo_client();
        let resolve = resolver(client, Some(Duration::ZERO), None);
        resolve.resolve("v1").await.unwrap();
        resolve.resolve("v1").await.unwrap();
        assert_eq!(rule.num_calls(), 2);

        let (client, rule) = echo_client();
        let resolve = resolver(client, Some(Duration::from_secs(60)), None);
        resolve.resolve("v1").await.unwrap();
        resolve.resolve("v1").await.unwrap();
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn dedupes_concurrent_lookups() {
        let (client, rule) = echo_client();
        let resolve = resolver(client, None, None);
        let (a, b, c) = tokio::join!(
            resolve.resolve("v1"),
            resolve.resolve("v1"),
            resolve.resolve("v1")
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(c.unwrap().schema_version_id.as_deref(), Some("v1"));
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn evicts_oldest_entry_past_max_cache_size() {
        let (client, rule) = echo_client();
        let resolve = resolver(client, None, Some(2));
        for id in ["a", "b", "c", "a"] {
            resolve.resolve(id).await.unwrap();
        }
        // "a" was evicted by "c", then refetched (evicting "b").
        assert_eq!(rule.num_calls(), 4);
        resolve.resolve("c").await.unwrap();
        assert_eq!(rule.num_calls(), 4);
        resolve.resolve("b").await.unwrap();
        assert_eq!(rule.num_calls(), 5);
    }

    #[tokio::test]
    async fn rejects_empty_id() {
        let (client, rule) = echo_client();
        let e = resolver(client, None, None).resolve("").await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsGlueSchemaRegistryResolver: schemaVersionId required"
        );
        assert_eq!(rule.num_calls(), 0);
    }
}
