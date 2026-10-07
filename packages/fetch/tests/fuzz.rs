// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::BTreeMap;
use std::sync::OnceLock;

use datastream_core::stream_to_array;
use datastream_fetch::{fetch_readable_stream, FetchChunk, FetchOptions, FetchStreamOptions};
use proptest::prelude::*;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn body() -> Value {
    json!({
        "data": [{"id": 1}, {"id": 2}],
        "items": [{"key": "a"}],
        "nested": {"deep": {"values": [{"n": 1}]}},
    })
}

/// One runtime and mock server for every case: `/fuzz` returns `body()`,
/// `/echo` returns its query string as a JSON object.
fn server() -> &'static (Runtime, MockServer) {
    static SERVER: OnceLock<(Runtime, MockServer)> = OnceLock::new();
    SERVER.get_or_init(|| {
        let rt = Runtime::new().unwrap();
        let server = rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/fuzz"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body()))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/echo"))
                .respond_with(|request: &Request| {
                    let query: Map<String, Value> = request
                        .url
                        .query_pairs()
                        .map(|(k, v)| (k.into_owned(), Value::String(v.into_owned())))
                        .collect();
                    ResponseTemplate::new(200).set_body_json(query)
                })
                .mount(&server)
                .await;
            server
        });
        (rt, server)
    })
}

/// One shared connection pool. Without it every case opens a fresh client and
/// socket, and thousands of cases exhaust the OS's ephemeral ports.
fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new).clone()
}

fn options(route: &str, data_path: &str) -> FetchOptions {
    FetchOptions {
        url: format!("{}{route}", server().1.uri()),
        client: Some(client()),
        data_path: Some(data_path.into()),
        rate_limit: Some(0.0),
        ..Default::default()
    }
}

fn fetch(configs: Vec<FetchOptions>, concurrency: usize) -> Vec<FetchChunk> {
    server().0.block_on(async {
        let stream = fetch_readable_stream(
            configs,
            FetchStreamOptions {
                concurrency: Some(concurrency),
                signal: None,
            },
        );
        stream_to_array(stream, None).await.unwrap()
    })
}

/// What `data_path` selects from `body()`: array items, a single value, or nothing.
fn expected(data_path: &str) -> Vec<FetchChunk> {
    let mut data = body();
    for key in data_path.split('.').filter(|k| !k.is_empty()) {
        data = match data.get(key) {
            Some(value) => value.clone(),
            None => return Vec::new(),
        };
    }
    match data {
        Value::Array(items) => items.into_iter().map(FetchChunk::Json).collect(),
        value => vec![FetchChunk::Json(value)],
    }
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_fetch_readable_stream_data_path(data_path in prop::sample::select(vec!["", "data", "items", "nested.deep.values"])) {
        prop_assert_eq!(fetch(vec![options("/fuzz", data_path)], 1), expected(data_path));
    }

    // Every qs key and value reaches the server intact.
    #[test]
    fn fuzz_fetch_readable_stream_qs(qs in prop::collection::btree_map(".{0,10}", ".{0,20}", 0..5)) {
        let config = FetchOptions {
            qs: qs.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect(),
            ..options("/echo", "")
        };
        let echoed = fetch(vec![config], 1);
        let echoed: BTreeMap<String, String> = match &echoed[..] {
            [FetchChunk::Json(Value::Object(map))] => map
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                .collect(),
            other => panic!("{other:?}"),
        };
        prop_assert_eq!(echoed, qs);
    }

    // Output stays in input order whatever the concurrency.
    #[test]
    fn fuzz_fetch_readable_stream_configs(
        data_paths in prop::collection::vec(prop::sample::select(vec!["", "data", "items"]), 1..6),
        concurrency in 1..4usize,
    ) {
        let configs = data_paths.iter().map(|p| options("/fuzz", p)).collect();
        let want: Vec<FetchChunk> = data_paths.iter().flat_map(|p| expected(p)).collect();
        prop_assert_eq!(fetch(configs, concurrency), want);
    }

    #[test]
    fn fuzz_fetch_readable_stream_data_path_segments(
        segments in prop::collection::vec(prop::sample::select(vec!["nested", "deep", "values", "data", "items"]), 1..5),
    ) {
        let data_path = segments.join(".");
        prop_assert_eq!(fetch(vec![options("/fuzz", &data_path)], 1), expected(&data_path));
    }
}
