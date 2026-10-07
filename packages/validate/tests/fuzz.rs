// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::BTreeSet;

use datastream_core::{create_readable_stream, stream_to_array, Map, Value};
use datastream_validate::{
    transpile_schema, validate_stream, CompiledSchema, TranspileOptions, ValidateOptions,
};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn schema() -> CompiledSchema {
    let schema = json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "number" },
            "active": { "type": "boolean" },
        },
        "required": ["name"],
        "additionalProperties": true,
    });
    transpile_schema(&schema, TranspileOptions::default()).unwrap()
}

/// Validate `input`, returning the emitted rows and the indexes of failed rows.
fn validate(input: Vec<Value>, on_error_enqueue: bool) -> (Vec<Value>, BTreeSet<i64>) {
    let options = ValidateOptions {
        schema: Some(schema().into()),
        on_error_enqueue: Some(on_error_enqueue),
        ..Default::default()
    };
    let (stream, result) = validate_stream(create_readable_stream(input), options).unwrap();
    let output = block_on(stream_to_array(stream, None)).unwrap();
    let failed = result
        .get()
        .as_object()
        .unwrap()
        .values()
        .flat_map(|error| error["idx"].as_array().unwrap().clone())
        .map(|idx| idx.as_i64().unwrap())
        .collect();
    (output, failed)
}

fn any_json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        ".{0,16}".prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 32, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::from),
            prop::collection::btree_map("[a-z]{0,6}", inner, 0..6)
                .prop_map(|m| Value::Object(m.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

fn valid_row() -> impl Strategy<Value = Value> {
    (".{0,16}", any::<Option<i32>>(), any::<Option<bool>>()).prop_map(|(name, age, active)| {
        let mut row = json!({ "name": name });
        if let Some(age) = age {
            row["age"] = json!(age);
        }
        if let Some(active) = active {
            row["active"] = json!(active);
        }
        row
    })
}

proptest! {
    #[test]
    fn fuzz_validate_valid_objects(input in prop::collection::vec(valid_row(), 1..16)) {
        let (output, failed) = validate(input.clone(), true);
        prop_assert!(failed.is_empty());
        prop_assert_eq!(output, input);
    }

    #[test]
    fn fuzz_validate_anything(input in prop::collection::vec(any_json(), 1..16)) {
        // With onErrorEnqueue every row is emitted, valid or not.
        let len = input.len();
        let (output, failed) = validate(input, true);
        prop_assert_eq!(output.len(), len);
        prop_assert!(failed.iter().all(|&idx| (0..len as i64).contains(&idx)));
    }

    #[test]
    fn fuzz_validate_drops_invalid(input in prop::collection::vec(any_json(), 1..16)) {
        // Without onErrorEnqueue each row is either emitted or reported, never both.
        let len = input.len();
        let (output, failed) = validate(input, false);
        prop_assert_eq!(output.len() + failed.len(), len);
    }
}
