// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_arrow::arrow_schema::{DataType, Field, Schema, SchemaRef};
use datastream_arrow::{
    arrow_batch_from_object_stream, arrow_detect_schema_stream, arrow_to_array_stream,
    arrow_to_object_stream, ArrowBatchOptions, ArrowDetectSchemaOptions, RecordBatch,
};
use datastream_core::{create_readable_stream, stream_to_array, DataStream, Value};
use serde_json::json;
use tokio::runtime::Runtime;

const ITEMS: usize = 10_000;

fn batches(objects: &[Value], schema: &SchemaRef, batch_size: usize) -> DataStream<RecordBatch> {
    let options = ArrowBatchOptions {
        schema: Some(schema.clone().into()),
        batch_size: Some(batch_size),
    };
    arrow_batch_from_object_stream(create_readable_stream(objects.to_vec()), options).unwrap()
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let objects: Vec<Value> = (0..ITEMS)
        .map(|i| json!({"id": i, "name": format!("user_{i}")}))
        .collect();

    c.bench_function("arrowDetectSchemaStream/10000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let options = ArrowDetectSchemaOptions {
                sample_size: Some(100),
                ..Default::default()
            };
            let (stream, _) =
                arrow_detect_schema_stream(create_readable_stream(objects.clone()), options);
            stream_to_array(stream, None).await.unwrap()
        })
    });
    let mut group = c.benchmark_group("arrowBatchFromObjectStream");
    for batch_size in [1_000, 100] {
        group.bench_function(format!("10000 objects, batchSize {batch_size}"), |b| {
            b.to_async(&rt).iter(|| async {
                stream_to_array(batches(&objects, &schema, batch_size), None)
                    .await
                    .unwrap()
            })
        });
    }
    group.finish();
    c.bench_function("arrowToObjectStream/10000 objects, batchSize 1000", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = arrow_to_object_stream(batches(&objects, &schema, 1_000));
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("arrowToArrayStream/10000 objects, batchSize 1000", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = arrow_to_array_stream(batches(&objects, &schema, 1_000));
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("object roundtrip/10000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let options = ArrowDetectSchemaOptions {
                sample_size: Some(100),
                ..Default::default()
            };
            let (stream, detected) =
                arrow_detect_schema_stream(create_readable_stream(objects.clone()), options);
            let options = ArrowBatchOptions {
                schema: Some(detected.lazy_schema()),
                batch_size: Some(1_000),
            };
            let stream = arrow_batch_from_object_stream(stream, options).unwrap();
            stream_to_array(arrow_to_object_stream(stream), None)
                .await
                .unwrap()
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
