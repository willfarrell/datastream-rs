// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{create_readable_stream, stream_to_array, Result, Value};
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
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

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
        package: Some("fuzz".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Msg".into()),
            field: vec![field("id", 1, Type::Int32), field("name", 2, Type::String)],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file).unwrap();
    pool.get_message_by_name("fuzz.Msg").unwrap()
}

async fn encode(input: Vec<Value>) -> Vec<Vec<u8>> {
    let options = ProtobufEncodeOptions {
        type_: msg_type().into(),
    };
    let stream = protobuf_encode_stream(create_readable_stream(input), options);
    stream_to_array(stream, None).await.unwrap()
}

async fn decode(input: Vec<Vec<u8>>, max_output_size: Option<usize>) -> Result<Vec<Value>> {
    let options = ProtobufDecodeOptions {
        type_: msg_type().into(),
        max_output_size,
    };
    stream_to_array(
        protobuf_decode_stream(create_readable_stream(input), options),
        None,
    )
    .await
}

async fn unframe(chunks: Vec<Vec<u8>>, max_message_size: Option<usize>) -> Result<Vec<Vec<u8>>> {
    let options = ProtobufLengthPrefixUnframeOptions { max_message_size };
    stream_to_array(
        protobuf_length_prefix_unframe_stream(create_readable_stream(chunks), options),
        None,
    )
    .await
}

/// Re-split `bytes` at arbitrary points so frames straddle chunk boundaries.
fn rechunk(bytes: Vec<u8>, cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut cuts: Vec<usize> = cuts.iter().map(|c| c % (bytes.len() + 1)).collect();
    cuts.sort_unstable();
    let mut chunks = Vec::new();
    let mut start = 0;
    for cut in cuts.into_iter().chain([bytes.len()]) {
        chunks.push(bytes[start..cut].to_vec());
        start = cut;
    }
    chunks
}

fn byte_chunks() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..8)
}

proptest! {
    #[test]
    fn fuzz_encode_decode_roundtrip(
        input in prop::collection::vec((any::<i32>(), ".{0,32}"), 0..16),
    ) {
        let objects: Vec<Value> = input
            .iter()
            .map(|(id, name)| json!({"id": id, "name": name}))
            .collect();
        let decoded = block_on(decode(block_on(encode(objects.clone())), None)).unwrap();
        prop_assert_eq!(decoded, objects);
    }

    #[test]
    fn fuzz_decode_random_bytes(chunks in byte_chunks()) {
        // Arbitrary bytes may fail to decode, but must never panic, and every
        // decoded message is an object.
        if let Ok(values) = block_on(decode(chunks, None)) {
            prop_assert!(values.iter().all(Value::is_object));
        }
    }

    #[test]
    fn fuzz_decode_max_output_size(
        input in prop::collection::vec((any::<i32>(), ".{0,32}"), 1..16),
        max in 0usize..512,
    ) {
        let objects: Vec<Value> = input
            .iter()
            .map(|(id, name)| json!({"id": id, "name": name}))
            .collect();
        let encoded = block_on(encode(objects));
        let total: usize = encoded.iter().map(Vec::len).sum();
        match block_on(decode(encoded, Some(max))) {
            Ok(_) => prop_assert!(total <= max),
            Err(e) => {
                prop_assert!(total > max);
                prop_assert!(e.to_string().contains("maxOutputSize"));
            }
        }
    }

    #[test]
    fn fuzz_unframe_random_bytes(chunks in byte_chunks()) {
        let total: usize = chunks.iter().map(Vec::len).sum();
        if let Ok(messages) = block_on(unframe(chunks, None)) {
            // Each message costs at least a one-byte prefix plus its body.
            let used: usize = messages.iter().map(|m| m.len() + 1).sum();
            prop_assert!(used <= total);
        }
    }

    #[test]
    fn fuzz_unframe_max_message_size(chunks in byte_chunks(), max in 0usize..64) {
        if let Ok(messages) = block_on(unframe(chunks, Some(max))) {
            prop_assert!(messages.iter().all(|m| m.len() <= max));
        }
    }

    #[test]
    fn fuzz_frame_unframe_roundtrip(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 0..8),
        cuts in prop::collection::vec(any::<usize>(), 0..8),
    ) {
        let framed = block_on(stream_to_array(
            protobuf_length_prefix_frame_stream(create_readable_stream(payloads.clone())),
            None,
        ))
        .unwrap();
        let chunks = rechunk(framed.concat(), &cuts);
        prop_assert_eq!(block_on(unframe(chunks, None)).unwrap(), payloads);
    }
}
