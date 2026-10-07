// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, StreamExt};
use datastream_fetch::{
    fetch_readable_stream, fetch_writable_stream, FetchOptions, FetchStreamOptions,
};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const ITEMS: usize = 1_000;
const PAGE_SIZE: usize = 100;

fn page(offset: usize, size: usize) -> Value {
    let data: Vec<Value> = (offset..offset + size)
        .map(|i| json!({"id": i, "name": format!("item_{i}"), "value": i as f64 / 7.0}))
        .collect();
    json!({ "data": data })
}

async fn mock_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/json-single"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(0, ITEMS)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/json-paginated"))
        .respond_with(|request: &Request| {
            let offset: usize = request
                .url
                .query_pairs()
                .find(|(k, _)| k == "$offset")
                .and_then(|(_, v)| v.parse().ok())
                .unwrap_or(0);
            let size = PAGE_SIZE.min(ITEMS.saturating_sub(offset));
            ResponseTemplate::new(200).set_body_json(page(offset, size))
        })
        .mount(&server)
        .await;
    let rows: Vec<String> = (0..ITEMS)
        .map(|i| format!("{i},item_{i},{}", i as f64 / 7.0))
        .collect();
    Mock::given(method("GET"))
        .and(path("/csv"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(format!("id,name,value\n{}\n", rows.join("\n")), "text/csv"),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"uploaded": true})))
        .mount(&server)
        .await;
    server
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(mock_server());
    let options = |route: &str| FetchOptions {
        url: format!("{}{route}", server.uri()),
        data_path: Some("data".into()),
        rate_limit: Some(0.0),
        ..Default::default()
    };
    let drain = |o: FetchOptions| async move {
        let mut stream = fetch_readable_stream(vec![o], FetchStreamOptions::default());
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
    };

    c.bench_function(
        &format!("fetchResponseStream (single JSON)/{ITEMS} items, single page"),
        |b| b.to_async(&rt).iter(|| drain(options("/json-single"))),
    );
    c.bench_function(
        &format!("fetchResponseStream (paginated JSON)/{ITEMS} items, {PAGE_SIZE} per page"),
        |b| {
            b.to_async(&rt).iter(|| {
                drain(FetchOptions {
                    offset_param: Some("$offset".into()),
                    offset_amount: Some(PAGE_SIZE as i64),
                    ..options("/json-paginated")
                })
            })
        },
    );
    c.bench_function(
        &format!("fetchResponseStream (CSV)/{ITEMS} rows CSV"),
        |b| b.to_async(&rt).iter(|| drain(options("/csv"))),
    );

    let upload = vec![b'x'; 1024 * 1024];
    c.bench_function("fetchWritableStream/upload 1MB string", |b| {
        b.to_async(&rt).iter(|| async {
            let input = create_readable_stream([upload.clone()]);
            let o = FetchOptions {
                method: Some("POST".into()),
                ..options("/upload")
            };
            fetch_writable_stream(input, o, None).await.unwrap()
        })
    });
}

criterion_group!(index, benches);
criterion_main!(index);
