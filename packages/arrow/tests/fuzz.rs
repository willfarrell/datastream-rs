// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_arrow::{
    arrow_batch_from_array_stream, arrow_batch_from_object_stream, arrow_detect_schema_stream,
    arrow_to_array_stream, arrow_to_object_stream, ArrowBatchOptions, ArrowDetectSchemaOptions,
};
use datastream_core::{create_readable_stream, stream_to_array, Map, Result, Value};
use proptest::prelude::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

/// Detect a schema over every row, then batch (50 rows per batch) and unbatch.
fn roundtrip(input: Vec<Value>, arrays: bool) -> Result<Option<Vec<Value>>> {
    block_on(async move {
        let options = ArrowDetectSchemaOptions {
            sample_size: Some(input.len()),
            ..Default::default()
        };
        let (stream, detected) = arrow_detect_schema_stream(create_readable_stream(input), options);
        let rows = stream_to_array(stream, None).await?;
        let schema = detected.schema().unwrap();
        if schema.fields().is_empty() {
            return Ok(None);
        }
        let options = ArrowBatchOptions {
            schema: Some(schema.into()),
            batch_size: Some(50),
        };
        let input = create_readable_stream(rows);
        let output = if arrays {
            arrow_to_array_stream(arrow_batch_from_array_stream(input, options)?)
        } else {
            arrow_to_object_stream(arrow_batch_from_object_stream(input, options)?)
        };
        Ok(Some(stream_to_array(output, None).await?))
    })
}

/// A value of one column type. Floats always have a fraction so detection
/// never mistakes the column for Int32; strings are never empty, which
/// detection treats like null.
fn typed_value(kind: u8) -> BoxedStrategy<Value> {
    match kind {
        0 => any::<i32>().prop_map(Value::from).boxed(),
        1 => (any::<i32>(), 1u32..1000)
            .prop_map(|(i, f)| Value::from(f64::from(i) + f64::from(f) / 1000.0))
            .boxed(),
        2 => any::<bool>().prop_map(Value::from).boxed(),
        _ => ".{1,12}".prop_map(Value::from).boxed(),
    }
}

/// Rows whose columns each hold one type (or are missing / null).
fn typed_rows() -> impl Strategy<Value = Vec<Vec<Option<Value>>>> {
    prop::collection::vec(0u8..4, 1..5).prop_flat_map(|kinds| {
        let row: Vec<_> = kinds
            .into_iter()
            .map(|kind| prop::option::of(typed_value(kind)))
            .collect();
        prop::collection::vec(row, 1..120)
    })
}

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        ".{0,8}".prop_map(Value::from),
    ]
}

proptest! {
    #[test]
    fn fuzz_detect_schema_random_objects(
        input in prop::collection::vec(prop::collection::btree_map("[a-e]", scalar(), 0..5), 0..40),
    ) {
        let input: Vec<Value> = input
            .into_iter()
            .map(|row| Value::Object(row.into_iter().collect::<Map<_, _>>()))
            .collect();
        let expected = input.clone();
        let options = ArrowDetectSchemaOptions { sample_size: Some(10), ..Default::default() };
        let (stream, detected) = arrow_detect_schema_stream(create_readable_stream(input), options);
        // Detection passes every row through unchanged.
        prop_assert_eq!(block_on(stream_to_array(stream, None)).unwrap(), expected.clone());
        prop_assert_eq!(detected.schema().is_some(), !expected.is_empty());
    }

    #[test]
    fn fuzz_batch_random_objects(
        input in prop::collection::vec(prop::collection::btree_map("[a-e]", scalar(), 0..5), 1..80),
    ) {
        // Mixed types per column may be rejected, but only with a clear error.
        let input = input
            .into_iter()
            .map(|row| Value::Object(row.into_iter().collect::<Map<_, _>>()))
            .collect();
        if let Err(e) = roundtrip(input, false) {
            prop_assert!(e.to_string().contains("cannot append"), "{e}");
        }
    }

    #[test]
    fn fuzz_object_roundtrip(rows in typed_rows(), use_null: bool) {
        let names = ["a", "b", "c", "d"];
        let input: Vec<Value> = rows
            .iter()
            .map(|row| {
                let mut object = Map::new();
                for (name, value) in names.iter().zip(row) {
                    match value {
                        Some(value) => { object.insert((*name).into(), value.clone()); }
                        None if use_null => { object.insert((*name).into(), Value::Null); }
                        None => {}
                    }
                }
                Value::Object(object)
            })
            .collect();
        // Columns that never appear are not in the schema; the rest come back
        // on every row, null where missing.
        let present: Vec<&str> = names
            .iter()
            .copied()
            .filter(|name| input.iter().any(|row| row.get(name).is_some()))
            .collect();
        let expected: Vec<Value> = rows
            .iter()
            .map(|row| {
                Value::Object(
                    names
                        .iter()
                        .zip(row)
                        .filter(|(name, _)| present.contains(name))
                        .map(|(name, value)| ((*name).into(), value.clone().unwrap_or(Value::Null)))
                        .collect(),
                )
            })
            .collect();
        match roundtrip(input, false).unwrap() {
            Some(output) => prop_assert_eq!(output, expected),
            None => prop_assert!(present.is_empty()),
        }
    }

    #[test]
    fn fuzz_array_roundtrip(rows in typed_rows()) {
        let width = rows[0].len();
        let input: Vec<Value> = rows
            .iter()
            .map(|row| Value::Array(row.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect()))
            .collect();
        let output = roundtrip(input.clone(), true).unwrap().unwrap();
        prop_assert!(output.iter().all(|row| row.as_array().unwrap().len() == width));
        prop_assert_eq!(output, input);
    }
}
