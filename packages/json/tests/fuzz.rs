// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{
    create_readable_stream, create_readable_stream_from_string, stream_to_array, stream_to_string,
    DataStream, Value,
};
use datastream_json::{
    json_format_stream, json_parse_stream, ndjson_format_stream, ndjson_parse_stream,
    JsonFormatOptions,
};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn chunked(input: String, size: usize) -> DataStream<String> {
    create_readable_stream_from_string(input, Some(size)).unwrap()
}

// Floats are dyadic (exactly representable) so the text roundtrip is exact
// without serde_json's `float_roundtrip` feature.
fn json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<i32>().prop_map(|n| Value::from(f64::from(n) / 8.0)),
        any::<String>().prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            prop::collection::btree_map(any::<String>(), inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn values() -> impl Strategy<Value = Vec<Value>> {
    prop::collection::vec(json(), 0..20)
}

proptest! {
    #[test]
    fn fuzz_ndjson_parse_stream_string(input in any::<String>(), size in 1usize..32) {
        let (stream, _errors) = ndjson_parse_stream(chunked(input, size), Default::default());
        prop_assert!(block_on(stream_to_array(stream, None)).is_ok());
    }

    #[test]
    fn fuzz_ndjson_parse_stream_ndjson_like(input in values(), size in 1usize..32) {
        let text: Vec<String> = input.iter().map(Value::to_string).collect();
        let (stream, errors) = ndjson_parse_stream(chunked(text.join("\n"), size), Default::default());
        let output = block_on(stream_to_array(stream, None)).unwrap();
        prop_assert_eq!(output, input);
        prop_assert_eq!(errors.get(), json!({}));
    }

    #[test]
    fn fuzz_ndjson_roundtrip(input in values(), size in 1usize..64) {
        let text = block_on(stream_to_string(
            ndjson_format_stream(create_readable_stream(input.clone()), JsonFormatOptions::default()),
            None,
        ))
        .unwrap();
        prop_assert_eq!(text.lines().count(), input.len());
        let (stream, _) = ndjson_parse_stream(chunked(text, size), Default::default());
        prop_assert_eq!(block_on(stream_to_array(stream, None)).unwrap(), input);
    }

    #[test]
    fn fuzz_json_parse_stream_string(input in any::<String>(), size in 1usize..32) {
        let (stream, _errors) = json_parse_stream(chunked(input, size), Default::default());
        // Malformed input may error; it must not panic or hang.
        let _ = block_on(stream_to_array(stream, None));
    }

    #[test]
    fn fuzz_json_parse_stream_array(input in values(), size in 1usize..32) {
        let (stream, errors) =
            json_parse_stream(chunked(Value::from(input.clone()).to_string(), size), Default::default());
        prop_assert_eq!(block_on(stream_to_array(stream, None)).unwrap(), input);
        prop_assert_eq!(errors.get(), json!({}));
    }

    #[test]
    fn fuzz_json_format_roundtrip(input in values(), space in prop::option::of(0usize..12), size in 1usize..64) {
        let text = block_on(stream_to_string(
            json_format_stream(create_readable_stream(input.clone()), JsonFormatOptions { space }),
            None,
        ))
        .unwrap();
        prop_assert_eq!(&serde_json::from_str::<Value>(&text).unwrap(), &Value::from(input.clone()));
        let (stream, _) = json_parse_stream(chunked(text, size), Default::default());
        prop_assert_eq!(block_on(stream_to_array(stream, None)).unwrap(), input);
    }
}
