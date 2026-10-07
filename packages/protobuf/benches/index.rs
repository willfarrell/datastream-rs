// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, stream_to_array, Value};
use datastream_protobuf::prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
use datastream_protobuf::prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
};
use datastream_protobuf::prost_reflect::{DescriptorPool, MessageDescriptor};
use datastream_protobuf::{
    protobuf_decode_stream, protobuf_encode_stream, protobuf_length_prefix_frame_stream,
    protobuf_length_prefix_unframe_stream, ProtobufDecodeOptions, ProtobufEncodeOptions,
    ProtobufLengthPrefixUnframeOptions,
};
use serde_json::json;
use tokio::runtime::Runtime;

const ITEMS: usize = 10_000;

fn msg_type() -> MessageDescriptor {
    let field = |name: &str, number, kind: Type| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(kind as i32),
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("msg.proto".into()),
        package: Some("bench".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Msg".into()),
            field: vec![
                field("id", 1, Type::Int32),
                field("name", 2, Type::String),
                field("value", 3, Type::Double),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file).unwrap();
    pool.get_message_by_name("bench.Msg").unwrap()
}

fn encode_options(desc: &MessageDescriptor) -> ProtobufEncodeOptions {
    ProtobufEncodeOptions {
        type_: desc.clone().into(),
    }
}

fn decode_options(desc: &MessageDescriptor) -> ProtobufDecodeOptions<Vec<u8>> {
    ProtobufDecodeOptions {
        type_: desc.clone().into(),
        max_output_size: None,
    }
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let desc = msg_type();
    let objects: Vec<Value> = (0..ITEMS)
        .map(|i| json!({"id": i, "name": format!("item_{i}"), "value": i as f64 * 1.5}))
        .collect();
    let encoded = rt
        .block_on(stream_to_array(
            protobuf_encode_stream(
                create_readable_stream(objects.clone()),
                encode_options(&desc),
            ),
            None,
        ))
        .unwrap();
    let framed = rt
        .block_on(stream_to_array(
            protobuf_length_prefix_frame_stream(create_readable_stream(encoded.clone())),
            None,
        ))
        .unwrap();

    c.bench_function("protobufEncodeStream/10000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = protobuf_encode_stream(
                create_readable_stream(objects.clone()),
                encode_options(&desc),
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("protobufDecodeStream/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = protobuf_decode_stream(
                create_readable_stream(encoded.clone()),
                decode_options(&desc),
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("protobuf roundtrip/10000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = protobuf_encode_stream(
                create_readable_stream(objects.clone()),
                encode_options(&desc),
            );
            let stream = protobuf_decode_stream(stream, decode_options(&desc));
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function("protobuf framing roundtrip/10000 messages", |b| {
        b.to_async(&rt).iter(|| async {
            let stream =
                protobuf_length_prefix_frame_stream(create_readable_stream(encoded.clone()));
            let stream = protobuf_length_prefix_unframe_stream(
                stream,
                ProtobufLengthPrefixUnframeOptions::default(),
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });
    c.bench_function(
        "protobufLengthPrefixUnframeStream/10000 framed messages",
        |b| {
            b.to_async(&rt).iter(|| async {
                let stream = protobuf_length_prefix_unframe_stream(
                    create_readable_stream(framed.clone()),
                    ProtobufLengthPrefixUnframeOptions::default(),
                );
                stream_to_array(stream, None).await.unwrap()
            })
        },
    );
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
