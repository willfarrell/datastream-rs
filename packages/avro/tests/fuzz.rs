// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_avro::{
    avro_decode_stream, avro_encode_stream, AvroDecodeOptions, AvroEncodeOptions, AvroType,
};
use datastream_core::{create_readable_stream, stream_to_array, Result, Value};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn schema() -> Value {
    json!({
        "type": "record",
        "name": "Msg",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "tag", "type": ["null", "string"]},
        ],
    })
}

async fn encode(input: Vec<Value>) -> Result<Vec<Vec<u8>>> {
    let options = AvroEncodeOptions {
        schema: schema().into(),
    };
    stream_to_array(
        avro_encode_stream(create_readable_stream(input), options)?,
        None,
    )
    .await
}

async fn decode(input: Vec<Vec<u8>>, max_message_size: Option<usize>) -> Result<Vec<Value>> {
    let options = AvroDecodeOptions {
        schema: schema().into(),
        max_message_size,
    };
    stream_to_array(
        avro_decode_stream(create_readable_stream(input), options)?,
        None,
    )
    .await
}

fn records() -> impl Strategy<Value = Vec<Value>> {
    prop::collection::vec(
        (any::<i32>(), ".{0,32}", prop::option::of(".{0,16}")),
        0..16,
    )
    .prop_map(|records| {
        records
            .into_iter()
            .map(|(id, name, tag)| json!({"id": id, "name": name, "tag": tag}))
            .collect()
    })
}

fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_map(Value::from),
        ".{0,8}".prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            prop::collection::btree_map("(id|name|tag|.{0,4})", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn byte_chunks() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..8)
}

proptest! {
    #[test]
    fn fuzz_encode_decode_roundtrip(input in records()) {
        let decoded = block_on(decode(block_on(encode(input.clone())).unwrap(), None)).unwrap();
        prop_assert_eq!(decoded, input);
    }

    #[test]
    fn fuzz_encode_arbitrary_values(input in prop::collection::vec(json_value(), 1..8)) {
        // Arbitrary values mostly fail to match, but must never panic, and
        // whatever encodes decodes back to a record.
        match block_on(encode(input)) {
            Ok(encoded) => {
                let decoded = block_on(decode(encoded, None)).unwrap();
                prop_assert!(decoded.iter().all(Value::is_object));
            }
            Err(e) => prop_assert!(e
                .to_string()
                .starts_with("avroEncodeStream: record does not match schema at $")),
        }
    }

    #[test]
    fn fuzz_decode_random_bytes(chunks in byte_chunks()) {
        if let Ok(values) = block_on(decode(chunks, None)) {
            prop_assert!(values.iter().all(Value::is_object));
        }
    }

    #[test]
    fn fuzz_decode_max_message_size(input in records(), max in 0usize..64) {
        let encoded = block_on(encode(input)).unwrap();
        let largest = encoded.iter().map(Vec::len).max().unwrap_or(0);
        match block_on(decode(encoded, Some(max))) {
            Ok(_) => prop_assert!(largest <= max),
            Err(e) => {
                prop_assert!(largest > max);
                prop_assert!(e.to_string().contains("maxMessageSize"));
            }
        }
    }

    #[test]
    fn fuzz_decode_random_bytes_with_containers(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        // Arrays, maps and recursion under random bytes: errors, never a
        // panic, overflow, or runaway allocation.
        let ty = AvroType::parse(&json!({
            "type": "record",
            "name": "Tree",
            "fields": [
                {"name": "children", "type": {"type": "array", "items": "Tree"}},
                {"name": "labels", "type": {"type": "map", "values": ["null", "Tree"]}},
            ],
        }))
        .unwrap();
        if let Ok(value) = ty.decode(&bytes) {
            prop_assert!(ty.encode(&value).is_ok());
        }
    }
}
