// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{
    create_pass_through_stream, create_readable_stream, create_readable_stream_from_bytes,
    create_readable_stream_from_string, create_transform_stream, noop, noop_flush, pipeline,
    stream_to_array, stream_to_buffer, stream_to_object, stream_to_string, Map, Value,
};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_map(Value::from),
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

fn object() -> impl Strategy<Value = Map<String, Value>> {
    prop::collection::btree_map(any::<String>(), json(), 0..4).prop_map(|m| m.into_iter().collect())
}

proptest! {
    #[test]
    fn fuzz_create_readable_stream_from_string(input in any::<String>(), size in 1usize..64) {
        let chunks = block_on(stream_to_array(
            create_readable_stream_from_string(input.clone(), Some(size)).unwrap(),
            None,
        ))
        .unwrap();
        prop_assert!(chunks.iter().all(|c| !c.is_empty() && c.chars().count() <= size));
        prop_assert_eq!(chunks.concat(), input);
    }

    #[test]
    fn fuzz_create_readable_stream_from_bytes(input in any::<Vec<u8>>(), size in 1usize..64) {
        let output = block_on(stream_to_buffer(
            create_readable_stream_from_bytes(input.clone(), Some(size)).unwrap(),
            None,
        ))
        .unwrap();
        prop_assert_eq!(output, input);
    }

    #[test]
    fn fuzz_create_readable_stream_array_of_strings(input in any::<Vec<String>>()) {
        let output = block_on(stream_to_array(create_readable_stream(input.clone()), None)).unwrap();
        prop_assert_eq!(output, input);
    }

    #[test]
    fn fuzz_create_readable_stream_array_of_anything(input in prop::collection::vec(json(), 0..16)) {
        let output = block_on(stream_to_array(create_readable_stream(input.clone()), None)).unwrap();
        prop_assert_eq!(output, input);
    }

    #[test]
    fn fuzz_stream_to_string(input in any::<Vec<String>>()) {
        let output = block_on(stream_to_string(create_readable_stream(input.clone()), None)).unwrap();
        prop_assert_eq!(output, input.concat());
    }

    #[test]
    fn fuzz_stream_to_array_max_buffer_size(input in any::<Vec<u8>>(), max in 0usize..16) {
        let output = block_on(stream_to_array(create_readable_stream(input.clone()), Some(max)));
        prop_assert_eq!(output.is_ok(), input.len() <= max);
    }

    #[test]
    fn fuzz_stream_to_object(input in prop::collection::vec(object(), 1..5)) {
        let chunks: Vec<Value> = input.iter().cloned().map(Value::Object).collect();
        let output = block_on(stream_to_object(create_readable_stream(chunks), None)).unwrap();
        let mut expected = Map::new();
        for map in input {
            expected.extend(map);
        }
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn fuzz_pipeline_pass_through(input in any::<Vec<String>>()) {
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = seen.clone();
        let stream = create_pass_through_stream(
            create_readable_stream(input.clone()),
            move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            noop,
        );
        let output = block_on(stream_to_array(stream, None)).unwrap();
        prop_assert_eq!(seen.load(Ordering::Relaxed), input.len());
        prop_assert_eq!(output, input);
    }

    #[test]
    fn fuzz_pipeline_transform(input in any::<Vec<String>>(), copies in 0usize..4) {
        let stream = create_transform_stream(
            create_readable_stream(input.clone()),
            move |chunk: String, enqueue: &mut Vec<String>| {
                enqueue.extend(std::iter::repeat_n(chunk, copies));
                Ok(())
            },
            noop_flush,
        );
        let output = block_on(stream_to_array(stream, None)).unwrap();
        prop_assert_eq!(output.len(), input.len() * copies);
        let result = block_on(pipeline(create_readable_stream(input), &[])).unwrap();
        prop_assert!(result.is_empty());
    }
}
