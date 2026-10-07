// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, stream_to_array, DataStream};
use datastream_schema_registry::{
    confluent_frame_stream, confluent_unframe_stream, glue_frame_stream, glue_unframe_stream,
    ConfluentFrameOptions, ConfluentUnframeOptions, GlueFrameOptions, GlueUnframeOptions,
};
use tokio::runtime::Runtime;

const COUNT: usize = 10_000;
const SCHEMA_ID: u32 = 42;
const SCHEMA_VERSION_ID: &str = "12345678-1234-1234-1234-1234567890ab";

fn messages() -> Vec<Vec<u8>> {
    (0..COUNT)
        .map(|i| (0..64).map(|j| ((i + j) % 256) as u8).collect())
        .collect()
}

fn confluent_frame(input: DataStream<Vec<u8>>) -> DataStream<Vec<u8>> {
    let options = ConfluentFrameOptions {
        schema_id: Some(SCHEMA_ID),
        ..Default::default()
    };
    confluent_frame_stream(input, options).unwrap().0
}

fn glue_frame(input: DataStream<Vec<u8>>) -> DataStream<Vec<u8>> {
    let options = GlueFrameOptions {
        schema_version_id: Some(SCHEMA_VERSION_ID.into()),
        ..Default::default()
    };
    glue_frame_stream(input, options).unwrap().0
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let messages = messages();
    let confluent_framed = rt
        .block_on(stream_to_array(
            confluent_frame(create_readable_stream(messages.clone())),
            None,
        ))
        .unwrap();
    let glue_framed = rt
        .block_on(stream_to_array(
            glue_frame(create_readable_stream(messages.clone())),
            None,
        ))
        .unwrap();

    c.bench_function("confluentFrameStream/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = confluent_frame(create_readable_stream(messages.clone()));
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("confluentUnframeStream/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let (stream, _) = confluent_unframe_stream(
                create_readable_stream(confluent_framed.clone()),
                ConfluentUnframeOptions::default(),
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("confluent roundtrip/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let framed = confluent_frame(create_readable_stream(messages.clone()));
            let (stream, _) = confluent_unframe_stream(framed, ConfluentUnframeOptions::default());
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("glueFrameStream/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = glue_frame(create_readable_stream(messages.clone()));
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("glueUnframeStream/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let (stream, _) = glue_unframe_stream(
                create_readable_stream(glue_framed.clone()),
                GlueUnframeOptions::default(),
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("glue roundtrip/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let framed = glue_frame(create_readable_stream(messages.clone()));
            let (stream, _) = glue_unframe_stream(framed, GlueUnframeOptions::default());
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
