// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;

use datastream_core::{create_readable_stream, pipeline, stream_to_array, DataStream, Map, Value};
use datastream_object::{
    object_batch_stream, object_count_stream, object_from_entries_stream, object_key_join_stream,
    object_key_map_stream, object_key_value_stream, object_key_values_stream, object_omit_stream,
    object_pick_stream, object_skip_consecutive_duplicates_stream, object_to_entries_stream,
    ObjectBatchOptions, ObjectCountOptions, ObjectEntriesOptions, ObjectKeyJoinOptions,
    ObjectKeyMapOptions, ObjectKeyValueOptions, ObjectKeyValuesOptions, ObjectKeysOptions,
};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn collect(stream: DataStream<Value>) -> Vec<Value> {
    block_on(stream_to_array(stream, None)).unwrap()
}

// Short keys from a small alphabet so picks, maps and duplicates collide.
fn key() -> impl Strategy<Value = String> {
    "[a-c]{0,2}"
}

fn json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_map(Value::from),
        any::<String>().prop_map(Value::from),
    ];
    leaf.prop_recursive(2, 16, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(Value::from),
            prop::collection::btree_map(key(), inner, 0..3)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn flat_object() -> impl Strategy<Value = Map<String, Value>> {
    prop::collection::btree_map(key(), "[xy]{0,1}".prop_map(Value::from), 0..4)
        .prop_map(|m| m.into_iter().collect())
}

fn objects() -> impl Strategy<Value = Vec<Value>> {
    prop::collection::vec(flat_object().prop_map(Value::Object), 1..8)
}

proptest! {
    #[test]
    fn fuzz_object_count_stream(input in prop::collection::vec(json(), 0..16)) {
        let (stream, result) =
            object_count_stream(create_readable_stream(input.clone()), ObjectCountOptions::default());
        let output = block_on(pipeline(stream, &[&result])).unwrap();
        prop_assert_eq!(&output["count"], &json!(input.len()));
    }

    #[test]
    fn fuzz_object_pick_and_omit_stream(input in objects(), keys in prop::collection::vec(key(), 1..5)) {
        let options = ObjectKeysOptions { keys: keys.clone() };
        let picked = collect(object_pick_stream(create_readable_stream(input.clone()), options.clone()));
        let omitted = collect(object_omit_stream(create_readable_stream(input.clone()), options));
        for ((original, pick), omit) in input.iter().zip(&picked).zip(&omitted) {
            let (pick, omit) = (pick.as_object().unwrap(), omit.as_object().unwrap());
            prop_assert!(pick.keys().all(|k| keys.contains(k)));
            prop_assert!(omit.keys().all(|k| !keys.contains(k)));
            let mut merged = pick.clone();
            merged.extend(omit.clone());
            prop_assert_eq!(&Value::Object(merged), original);
        }
    }

    #[test]
    fn fuzz_object_key_map_stream(input in objects(), keys in prop::collection::hash_map(key(), key(), 0..4)) {
        let output = collect(object_key_map_stream(
            create_readable_stream(input.clone()),
            ObjectKeyMapOptions { keys: keys.clone() },
        ));
        for (original, mapped) in input.iter().zip(&output) {
            let expected: std::collections::BTreeSet<String> = original
                .as_object()
                .unwrap()
                .keys()
                .map(|k| keys.get(k).cloned().unwrap_or_else(|| k.clone()))
                .collect();
            let actual: std::collections::BTreeSet<String> =
                mapped.as_object().unwrap().keys().cloned().collect();
            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn fuzz_object_batch_stream(
        input in prop::collection::vec(("[ab]", json()), 1..16),
        max_batch_size in prop::option::of(1usize..4),
    ) {
        let input: Vec<Value> = input
            .into_iter()
            .map(|(group, value)| json!({"group": group, "value": value}))
            .collect();
        let output = collect(object_batch_stream(
            create_readable_stream(input.clone()),
            ObjectBatchOptions { keys: vec!["group".into()], max_batch_size },
        ));
        let batches: Vec<&Vec<Value>> = output.iter().map(|b| b.as_array().unwrap()).collect();
        let flattened: Vec<Value> = batches.iter().flat_map(|b| b.iter().cloned()).collect();
        prop_assert_eq!(flattened, input);
        for batch in &batches {
            prop_assert!(!batch.is_empty());
            prop_assert!(batch.len() <= max_batch_size.unwrap_or(usize::MAX));
            prop_assert!(batch.iter().all(|row| row["group"] == batch[0]["group"]));
        }
        if max_batch_size.is_none() {
            for pair in batches.windows(2) {
                prop_assert_ne!(&pair[0][0]["group"], &pair[1][0]["group"]);
            }
        }
    }

    #[test]
    fn fuzz_object_key_value_stream(input in prop::collection::vec((any::<String>(), json()), 1..8)) {
        let rows: Vec<Value> = input.iter().map(|(k, v)| json!({"k": k, "v": v})).collect();
        let output = collect(object_key_value_stream(
            create_readable_stream(rows),
            ObjectKeyValueOptions { key: "k".into(), value: "v".into() },
        ));
        for ((k, v), row) in input.iter().zip(&output) {
            let mut expected = Map::new();
            expected.insert(k.clone(), v.clone());
            prop_assert_eq!(row, &Value::Object(expected));
        }
    }

    #[test]
    fn fuzz_object_key_values_stream(input in prop::collection::vec((any::<String>(), json(), json()), 1..8)) {
        let rows: Vec<Value> = input.iter().map(|(k, a, b)| json!({"k": k, "a": a, "b": b})).collect();
        let output = collect(object_key_values_stream(
            create_readable_stream(rows),
            ObjectKeyValuesOptions { key: "k".into(), values: Some(vec!["a".into(), "b".into()]) },
        ));
        for ((k, a, b), row) in input.iter().zip(&output) {
            let mut expected = Map::new();
            expected.insert(k.clone(), json!({"a": a, "b": b}));
            prop_assert_eq!(row, &Value::Object(expected));
        }
    }

    #[test]
    fn fuzz_object_key_join_stream(
        input in prop::collection::vec((any::<String>(), any::<String>(), json()), 1..8),
        separator in ".{1,3}",
    ) {
        let rows: Vec<Value> = input
            .iter()
            .map(|(first, last, other)| json!({"first": first, "last": last, "other": other}))
            .collect();
        let output = collect(object_key_join_stream(
            create_readable_stream(rows),
            ObjectKeyJoinOptions {
                keys: vec![("name".into(), vec!["first".into(), "last".into()])],
                separator: Some(separator.clone()),
            },
        ));
        for ((first, last, other), row) in input.iter().zip(&output) {
            prop_assert_eq!(row, &json!({"name": format!("{first}{separator}{last}"), "other": other}));
        }
    }

    #[test]
    fn fuzz_object_from_entries_roundtrip(
        keys in prop::collection::hash_set(any::<String>(), 1..5),
        values in prop::collection::vec(json(), 5),
    ) {
        let keys: Vec<String> = keys.into_iter().collect();
        let entries = Value::Array(values[..keys.len()].to_vec());
        let options = ObjectEntriesOptions { keys: keys.clone().into() };
        let objects = collect(object_from_entries_stream(
            create_readable_stream(vec![entries.clone()]),
            options.clone(),
        ));
        let row = objects[0].as_object().unwrap();
        let expected: HashMap<&String, &Value> = keys.iter().zip(entries.as_array().unwrap()).collect();
        prop_assert_eq!(row.iter().collect::<HashMap<_, _>>(), expected);
        let back = collect(object_to_entries_stream(create_readable_stream(objects), options));
        prop_assert_eq!(&back[0], &entries);
    }

    #[test]
    fn fuzz_object_skip_consecutive_duplicates_stream(input in prop::collection::vec(flat_object().prop_map(Value::Object), 0..16)) {
        let output = collect(object_skip_consecutive_duplicates_stream(create_readable_stream(input.clone())));
        let mut expected = input;
        expected.dedup();
        prop_assert_eq!(output, expected);
    }
}
