// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_avro::{
    avro_decode_stream, avro_encode_stream, AvroDecodeOptions, AvroEncodeOptions, AvroType,
};
use datastream_core::{create_readable_stream, stream_to_array, Value};
use serde_json::json;
use tokio::runtime::Runtime;

const ITEMS: usize = 10_000;

fn msg_type() -> AvroType {
    AvroType::parse(&json!({
        "type": "record",
        "name": "Msg",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "double"},
        ],
    }))
    .unwrap()
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ty = msg_type();
    let objects: Vec<Value> = (0..ITEMS)
        .map(|i| json!({"id": i, "name": format!("item_{i}"), "value": i as f64 * 1.5}))
        .collect();
    let encoded: Vec<Vec<u8>> = objects.iter().map(|o| ty.encode(o).unwrap()).collect();

    c.bench_function("avroEncodeStream/10000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let options = AvroEncodeOptions {
                schema: ty.clone().into(),
            };
            let stream =
                avro_encode_stream(create_readable_stream(objects.clone()), options).unwrap();
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("avroDecodeStream/10000 records", |b| {
        b.to_async(&rt).iter(|| async {
            let options = AvroDecodeOptions {
                schema: ty.clone().into(),
                max_message_size: None,
            };
            let stream =
                avro_decode_stream(create_readable_stream(encoded.clone()), options).unwrap();
            stream_to_array(stream, None).await.unwrap()
        })
    });
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = benches
}
criterion_main!(index);
