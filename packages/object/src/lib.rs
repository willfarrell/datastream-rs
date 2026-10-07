// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Object manipulation streams, ported from `@datastream/object`.
//!
//! Chunks are `serde_json::Value`s. Where JS reads `chunk[key]`, a numeric key
//! also indexes into arrays, and a missing value becomes `null`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use datastream_core::{
    create_pass_through_stream, create_readable_stream, create_transform_stream, deep_equal, noop,
    noop_flush, DataStream, Map, Result, StreamExt, StreamResult, Value,
};
use serde_json::json;

/// `chunk[key]`: an object field, or an array index when `key` is a number.
fn field<'a>(chunk: &'a Value, key: &str) -> Option<&'a Value> {
    match chunk {
        Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => chunk.get(key),
    }
}

fn field_or_null(chunk: &Value, key: &str) -> Value {
    field(chunk, key).cloned().unwrap_or(Value::Null)
}

/// JS `String(value)`, used where a value becomes an object key.
fn key_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    }
}

/// JS `Array.prototype.join` item: null and undefined become "".
fn join_string(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        v => key_string(v),
    }
}

fn into_object(chunk: Value) -> Result<Map<String, Value>> {
    match chunk {
        Value::Object(map) => Ok(map),
        _ => Err("Expected chunk to be object".into()),
    }
}

/// Keys known up front, or resolved once on the first chunk (JS `keys: () => [...]`).
#[derive(Clone)]
pub enum Keys {
    List(Vec<String>),
    Lazy(Arc<dyn Fn() -> Vec<String> + Send + Sync>),
}

impl Keys {
    pub fn lazy(f: impl Fn() -> Vec<String> + Send + Sync + 'static) -> Self {
        Self::Lazy(Arc::new(f))
    }
    fn resolve(&self) -> Vec<String> {
        match self {
            Self::List(keys) => keys.clone(),
            Self::Lazy(f) => f(),
        }
    }
}

impl Default for Keys {
    fn default() -> Self {
        Self::List(Vec::new())
    }
}

impl fmt::Debug for Keys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::List(keys) => f.debug_tuple("List").field(keys).finish(),
            Self::Lazy(_) => f.write_str("Lazy(..)"),
        }
    }
}

impl<S: Into<String>> From<Vec<S>> for Keys {
    fn from(keys: Vec<S>) -> Self {
        Self::List(keys.into_iter().map(Into::into).collect())
    }
}

// *** Readable *** //

pub fn object_readable_stream<I>(input: I) -> DataStream<Value>
where
    I: IntoIterator<Item = Value>,
    I::IntoIter: Send + 'static,
{
    create_readable_stream(input)
}

// *** Count *** //

#[derive(Default, Clone, Debug)]
pub struct ObjectCountOptions {
    pub result_key: Option<String>,
}

/// Count chunks. Result key defaults to "count".
pub fn object_count_stream<T: Send + 'static>(
    input: DataStream<T>,
    options: ObjectCountOptions,
) -> (DataStream<T>, StreamResult) {
    let result = StreamResult::new(
        options.result_key.unwrap_or_else(|| "count".into()),
        json!(0),
    );
    let r = result.clone();
    let mut count: u64 = 0;
    let stream = create_pass_through_stream(
        input,
        move |_| {
            count += 1;
            r.set(json!(count));
            Ok(())
        },
        noop,
    );
    (stream, result)
}

// *** Batch / pivot *** //

#[derive(Default, Clone, Debug)]
pub struct ObjectBatchOptions {
    pub keys: Vec<String>,
    pub max_batch_size: Option<usize>,
}

/// Group consecutive chunks sharing the same `keys` values into arrays.
pub fn object_batch_stream(
    mut input: DataStream<Value>,
    options: ObjectBatchOptions,
) -> DataStream<Value> {
    let ObjectBatchOptions {
        keys,
        max_batch_size,
    } = options;
    let max = max_batch_size.unwrap_or(usize::MAX);
    Box::pin(try_stream! {
        let mut previous_id: Option<Vec<Value>> = None;
        let mut batch = Vec::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let id: Vec<Value> = keys.iter().map(|k| field_or_null(&chunk, k)).collect();
            if previous_id.as_ref() != Some(&id) {
                if !batch.is_empty() {
                    yield Value::Array(std::mem::take(&mut batch));
                }
                previous_id = Some(id);
            }
            batch.push(chunk);
            if batch.len() >= max {
                yield Value::Array(std::mem::take(&mut batch));
            }
        }
        if !batch.is_empty() {
            yield Value::Array(batch);
        }
    })
}

#[derive(Default, Clone, Debug)]
pub struct ObjectPivotLongToWideOptions {
    pub keys: Vec<String>,
    pub value_param: String,
    pub delimiter: Option<String>,
}

/// Turn each batch (from [`object_batch_stream`]) into one row with a column
/// per `keys` combination (joined by `delimiter`, default " ").
pub fn object_pivot_long_to_wide_stream(
    input: DataStream<Value>,
    options: ObjectPivotLongToWideOptions,
) -> DataStream<Value> {
    let ObjectPivotLongToWideOptions {
        keys,
        value_param,
        delimiter,
    } = options;
    let delimiter = delimiter.unwrap_or_else(|| " ".into());
    create_transform_stream(
        input,
        move |chunks, enqueue| {
            let Value::Array(chunks) = chunks else {
                return Err("Expected chunk to be array, use with objectBatchStream".into());
            };
            let mut row = chunks
                .first()
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            for chunk in &chunks {
                let key_param = keys
                    .iter()
                    .map(|k| join_string(field(chunk, k)))
                    .collect::<Vec<_>>()
                    .join(delimiter.as_str());
                row.insert(key_param, field_or_null(chunk, &value_param));
            }
            row.retain(|k, _| !keys.contains(k) && *k != value_param);
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectPivotWideToLongOptions {
    pub keys: Vec<String>,
    pub key_param: Option<String>,
    pub value_param: Option<String>,
}

/// Emit one row per present `keys` column, as `{...rest, keyParam, valueParam}`.
pub fn object_pivot_wide_to_long_stream(
    input: DataStream<Value>,
    options: ObjectPivotWideToLongOptions,
) -> DataStream<Value> {
    let keys = options.keys;
    let key_param = options.key_param.unwrap_or_else(|| "keyParam".into());
    let value_param = options.value_param.unwrap_or_else(|| "valueParam".into());
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let chunk = into_object(chunk)?;
            let rest: Map<String, Value> = chunk
                .iter()
                .filter(|(k, _)| !keys.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for key in &keys {
                if let Some(value) = chunk.get(key) {
                    let mut row = rest.clone();
                    row.insert(key_param.clone(), json!(key));
                    row.insert(value_param.clone(), value.clone());
                    enqueue.push(Value::Object(row));
                }
            }
            Ok(())
        },
        noop_flush,
    )
}

// *** Keys *** //

#[derive(Default, Clone, Debug)]
pub struct ObjectKeyValueOptions {
    pub key: String,
    pub value: String,
}

/// `{[chunk[key]]: chunk[value]}`.
pub fn object_key_value_stream(
    input: DataStream<Value>,
    options: ObjectKeyValueOptions,
) -> DataStream<Value> {
    let ObjectKeyValueOptions { key, value } = options;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let mut row = Map::new();
            row.insert(
                key_string(field(&chunk, &key)),
                field_or_null(&chunk, &value),
            );
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectKeyValuesOptions {
    pub key: String,
    pub values: Option<Vec<String>>,
}

/// `{[chunk[key]]: chunk}`, or only the `values` fields when given.
pub fn object_key_values_stream(
    input: DataStream<Value>,
    options: ObjectKeyValuesOptions,
) -> DataStream<Value> {
    let ObjectKeyValuesOptions { key, values } = options;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let name = key_string(field(&chunk, &key));
            let value = match &values {
                None => chunk,
                Some(values) => Value::Object(
                    values
                        .iter()
                        .map(|k| (k.clone(), field_or_null(&chunk, k)))
                        .collect(),
                ),
            };
            let mut row = Map::new();
            row.insert(name, value);
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectKeyJoinOptions {
    /// New key -> old keys whose values are joined into it (old keys are removed).
    pub keys: Vec<(String, Vec<String>)>,
    /// Defaults to "," like JS `Array.prototype.join`.
    pub separator: Option<String>,
}

pub fn object_key_join_stream(
    input: DataStream<Value>,
    options: ObjectKeyJoinOptions,
) -> DataStream<Value> {
    let keys = options.keys;
    let separator = options.separator.unwrap_or_else(|| ",".into());
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let mut row = into_object(chunk)?;
            let joined: Vec<String> = keys
                .iter()
                .map(|(_, old_keys)| {
                    old_keys
                        .iter()
                        .map(|k| join_string(row.get(k)))
                        .collect::<Vec<_>>()
                        .join(separator.as_str())
                })
                .collect();
            for ((new_key, old_keys), value) in keys.iter().zip(joined) {
                row.retain(|k, _| !old_keys.contains(k));
                row.insert(new_key.clone(), Value::String(value));
            }
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectKeyMapOptions {
    /// Old key -> new key. Unlisted keys are kept as is.
    pub keys: HashMap<String, String>,
}

pub fn object_key_map_stream(
    input: DataStream<Value>,
    options: ObjectKeyMapOptions,
) -> DataStream<Value> {
    let keys = options.keys;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let row: Map<String, Value> = into_object(chunk)?
                .into_iter()
                .map(|(k, v)| (keys.get(&k).cloned().unwrap_or(k), v))
                .collect();
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectValueMapOptions {
    pub key: String,
    /// Old value (as a string) -> new value. Unlisted values are kept as is.
    pub values: Map<String, Value>,
}

pub fn object_value_map_stream(
    input: DataStream<Value>,
    options: ObjectValueMapOptions,
) -> DataStream<Value> {
    let ObjectValueMapOptions { key, values } = options;
    create_transform_stream(
        input,
        move |mut chunk, enqueue| {
            if let Some(value) = values.get(&key_string(chunk.get(&key))) {
                if let Some(row) = chunk.as_object_mut() {
                    row.insert(key.clone(), value.clone());
                }
            }
            enqueue.push(chunk);
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ObjectKeysOptions {
    pub keys: Vec<String>,
}

fn filter_keys(input: DataStream<Value>, keys: Vec<String>, keep: bool) -> DataStream<Value> {
    let keys: HashSet<String> = keys.into_iter().collect();
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let row: Map<String, Value> = into_object(chunk)?
                .into_iter()
                .filter(|(k, _)| keys.contains(k) == keep)
                .collect();
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

/// Keep only `keys`.
pub fn object_pick_stream(
    input: DataStream<Value>,
    options: ObjectKeysOptions,
) -> DataStream<Value> {
    filter_keys(input, options.keys, true)
}

/// Drop `keys`.
pub fn object_omit_stream(
    input: DataStream<Value>,
    options: ObjectKeysOptions,
) -> DataStream<Value> {
    filter_keys(input, options.keys, false)
}

// *** Entries *** //

#[derive(Default, Clone, Debug)]
pub struct ObjectEntriesOptions {
    pub keys: Keys,
}

/// Array chunk -> object: `{[keys[i]]: chunk[i]}`.
pub fn object_from_entries_stream(
    input: DataStream<Value>,
    options: ObjectEntriesOptions,
) -> DataStream<Value> {
    let source = options.keys;
    let mut resolved: Option<Vec<String>> = None;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let keys = resolved.get_or_insert_with(|| source.resolve());
            let row: Map<String, Value> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.clone(), chunk.get(i).cloned().unwrap_or(Value::Null)))
                .collect();
            enqueue.push(Value::Object(row));
            Ok(())
        },
        noop_flush,
    )
}

/// Object chunk -> array: `keys.map((key) => chunk[key])`.
pub fn object_to_entries_stream(
    input: DataStream<Value>,
    options: ObjectEntriesOptions,
) -> DataStream<Value> {
    let source = options.keys;
    let mut resolved: Option<Vec<String>> = None;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            let keys = resolved.get_or_insert_with(|| source.resolve());
            enqueue.push(Value::Array(
                keys.iter().map(|k| field_or_null(&chunk, k)).collect(),
            ));
            Ok(())
        },
        noop_flush,
    )
}

// *** Duplicates *** //

/// Drop chunks equal to the previous chunk. Comparison is structural (and
/// ignores key order), which covers the JS `isNestedObject` option.
pub fn object_skip_consecutive_duplicates_stream(input: DataStream<Value>) -> DataStream<Value> {
    let mut previous: Option<Value> = None;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            if !previous.as_ref().is_some_and(|p| deep_equal(p, &chunk)) {
                previous = Some(chunk.clone());
                enqueue.push(chunk);
            }
            Ok(())
        },
        noop_flush,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{pipeline, stream_to_array};

    fn from(input: Value) -> DataStream<Value> {
        let Value::Array(items) = input else {
            panic!("expected array")
        };
        create_readable_stream(items)
    }

    async fn collect(stream: DataStream<Value>) -> Value {
        Value::Array(stream_to_array(stream, None).await.unwrap())
    }

    fn keys(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    #[tokio::test]
    async fn readable_reads_chunks() {
        let input = json!([{"a": "1"}, {"b": "2"}, {"c": "3"}]);
        let Value::Array(items) = input.clone() else {
            unreachable!()
        };
        assert_eq!(collect(object_readable_stream(items)).await, input);
        assert_eq!(collect(object_readable_stream(Vec::new())).await, json!([]));
    }

    #[tokio::test]
    async fn count_counts_chunks() {
        let (stream, count) =
            object_count_stream(create_readable_stream(["1", "2", "3"]), Default::default());
        let output = pipeline(stream, &[&count]).await.unwrap();
        assert_eq!(count.key(), "count");
        assert_eq!(count.get(), json!(3));
        assert_eq!(output["count"], json!(3));

        let options = ObjectCountOptions {
            result_key: Some("object".into()),
        };
        let (stream, count) = object_count_stream(create_readable_stream(["1", "2", "3"]), options);
        let output = pipeline(stream, &[&count]).await.unwrap();
        assert_eq!(output["object"], json!(3));
    }

    #[tokio::test]
    async fn batch_by_key() {
        let input = json!([
            {"a": "1", "b": "2"}, {"a": "1", "b": "2"}, {"a": "2", "b": "3"},
            {"a": "3", "b": "4"}, {"a": "3", "b": "5"}
        ]);
        let options = ObjectBatchOptions {
            keys: keys(&["a"]),
            ..Default::default()
        };
        assert_eq!(
            collect(object_batch_stream(from(input), options)).await,
            json!([
                [{"a": "1", "b": "2"}, {"a": "1", "b": "2"}],
                [{"a": "2", "b": "3"}],
                [{"a": "3", "b": "4"}, {"a": "3", "b": "5"}]
            ])
        );
    }

    #[tokio::test]
    async fn batch_by_index() {
        let input = json!([["1", "1"], ["1", "2"], ["2", "3"], ["3", "4"], ["3", "5"]]);
        let options = ObjectBatchOptions {
            keys: keys(&["0"]),
            ..Default::default()
        };
        assert_eq!(
            collect(object_batch_stream(from(input), options)).await,
            json!([
                [["1", "1"], ["1", "2"]],
                [["2", "3"]],
                [["3", "4"], ["3", "5"]]
            ])
        );
    }

    #[tokio::test]
    async fn batch_max_size() {
        let input: Vec<Value> = (0..9).map(|i| json!({"a": "same", "b": i})).collect();
        let options = ObjectBatchOptions {
            keys: keys(&["a"]),
            max_batch_size: Some(3),
        };
        let output = stream_to_array(
            object_batch_stream(create_readable_stream(input), options),
            None,
        )
        .await
        .unwrap();
        let sizes: Vec<usize> = output.iter().map(|b| b.as_array().unwrap().len()).collect();
        assert_eq!(sizes, [3, 3, 3]);
    }

    #[tokio::test]
    async fn batch_keeps_distinct_tuples_apart() {
        let input = json!([{"x": "a b", "y": "c"}, {"x": "a", "y": "b c"}]);
        let options = ObjectBatchOptions {
            keys: keys(&["x", "y"]),
            ..Default::default()
        };
        assert_eq!(
            collect(object_batch_stream(from(input), options)).await,
            json!([[{"x": "a b", "y": "c"}], [{"x": "a", "y": "b c"}]])
        );
        let options = ObjectBatchOptions {
            keys: keys(&["a"]),
            ..Default::default()
        };
        assert_eq!(
            collect(object_batch_stream(from(json!([])), options)).await,
            json!([])
        );
    }

    #[tokio::test]
    async fn pivot_long_to_wide() {
        let input = json!([
            {"a": "1", "b": "l", "v": 1, "u": "m"}, {"a": "1", "b": "w", "v": 2, "u": "m"},
            {"a": "2", "b": "w", "v": 3, "u": "m"}, {"a": "3", "b": "l", "v": 4, "u": "m"},
            {"a": "3", "b": "w", "v": 5, "u": "m"}
        ]);
        let batch = ObjectBatchOptions {
            keys: keys(&["a"]),
            ..Default::default()
        };
        let pivot = ObjectPivotLongToWideOptions {
            keys: keys(&["b", "u"]),
            value_param: "v".into(),
            ..Default::default()
        };
        let stream =
            object_pivot_long_to_wide_stream(object_batch_stream(from(input), batch), pivot);
        let output = collect(stream).await;
        assert_eq!(
            output,
            json!([{"a": "1", "l m": 1, "w m": 2}, {"a": "2", "w m": 3}, {"a": "3", "l m": 4, "w m": 5}])
        );
        // Key order matches JS.
        let first: Vec<&String> = output[0].as_object().unwrap().keys().collect();
        assert_eq!(first, ["a", "l m", "w m"]);
    }

    #[tokio::test]
    async fn pivot_long_to_wide_custom_delimiter() {
        let input = json!([[{"a": "1", "b": "l", "u": "m", "v": 1}, {"a": "1", "b": "w", "u": "m", "v": 2}]]);
        let pivot = ObjectPivotLongToWideOptions {
            keys: keys(&["b", "u"]),
            value_param: "v".into(),
            delimiter: Some("-".into()),
        };
        assert_eq!(
            collect(object_pivot_long_to_wide_stream(from(input), pivot)).await,
            json!([{"a": "1", "l-m": 1, "w-m": 2}])
        );
    }

    #[tokio::test]
    async fn pivot_long_to_wide_rejects_non_array() {
        let input = json!([{"a": "1", "b": "l", "v": 1, "u": "m"}]);
        let pivot = ObjectPivotLongToWideOptions {
            keys: keys(&["b", "u"]),
            value_param: "v".into(),
            ..Default::default()
        };
        let e = pipeline(object_pivot_long_to_wide_stream(from(input), pivot), &[])
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Expected chunk to be array, use with objectBatchStream"
        );
    }

    #[tokio::test]
    async fn pivot_wide_to_long() {
        let input = json!([{"a": "1", "l m": 1, "w m": 2}, {"a": "2", "w m": 3}, {"a": "3", "l m": 4, "w m": 5}]);
        let options = ObjectPivotWideToLongOptions {
            keys: keys(&["l m", "w m", "a m"]),
            key_param: Some("b u".into()),
            value_param: Some("v".into()),
        };
        assert_eq!(
            collect(object_pivot_wide_to_long_stream(from(input), options)).await,
            json!([
                {"a": "1", "b u": "l m", "v": 1}, {"a": "1", "b u": "w m", "v": 2},
                {"a": "2", "b u": "w m", "v": 3}, {"a": "3", "b u": "l m", "v": 4},
                {"a": "3", "b u": "w m", "v": 5}
            ])
        );
    }

    #[tokio::test]
    async fn pivot_wide_to_long_defaults() {
        let input = json!([{"id": 1, "a": {"x": 10}, "b": 20}]);
        let options = ObjectPivotWideToLongOptions {
            keys: keys(&["a", "b"]),
            ..Default::default()
        };
        assert_eq!(
            collect(object_pivot_wide_to_long_stream(from(input), options)).await,
            json!([
                {"id": 1, "keyParam": "a", "valueParam": {"x": 10}},
                {"id": 1, "keyParam": "b", "valueParam": 20}
            ])
        );
    }

    #[tokio::test]
    async fn key_value() {
        let input = json!([{"a": "1", "b": "2", "c": "3"}]);
        let options = ObjectKeyValueOptions {
            key: "a".into(),
            value: "b".into(),
        };
        assert_eq!(
            collect(object_key_value_stream(from(input), options)).await,
            json!([{"1": "2"}])
        );
    }

    #[tokio::test]
    async fn key_values() {
        let input = json!([{"a": "1", "b": "2", "c": "3"}]);
        let options = ObjectKeyValuesOptions {
            key: "a".into(),
            values: None,
        };
        assert_eq!(
            collect(object_key_values_stream(from(input.clone()), options)).await,
            json!([{"1": {"a": "1", "b": "2", "c": "3"}}])
        );
        let options = ObjectKeyValuesOptions {
            key: "a".into(),
            values: Some(keys(&["b"])),
        };
        assert_eq!(
            collect(object_key_values_stream(from(input), options)).await,
            json!([{"1": {"b": "2"}}])
        );
    }

    #[tokio::test]
    async fn key_join() {
        let input =
            json!([{"firstName": "John", "lastName": "Doe", "age": 30, "nested": {"v": 1}}]);
        let options = ObjectKeyJoinOptions {
            keys: vec![("fullName".into(), keys(&["firstName", "lastName"]))],
            separator: Some(" ".into()),
        };
        let output = collect(object_key_join_stream(from(input), options)).await;
        assert_eq!(
            output,
            json!([{"age": 30, "nested": {"v": 1}, "fullName": "John Doe"}])
        );
        let order: Vec<&String> = output[0].as_object().unwrap().keys().collect();
        assert_eq!(order, ["age", "nested", "fullName"]);
    }

    #[tokio::test]
    async fn key_join_default_separator_and_missing() {
        let input = json!([{"a": 1, "c": null}]);
        let options = ObjectKeyJoinOptions {
            keys: vec![("x".into(), keys(&["a", "b", "c"]))],
            separator: None,
        };
        assert_eq!(
            collect(object_key_join_stream(from(input), options)).await,
            json!([{"x": "1,,"}])
        );
    }

    #[tokio::test]
    async fn key_map() {
        let input = json!([{"a": 1, "b": 2, "c": 3}]);
        let options = ObjectKeyMapOptions {
            keys: [("a", "x"), ("b", "y")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        assert_eq!(
            collect(object_key_map_stream(from(input), options)).await,
            json!([{"x": 1, "y": 2, "c": 3}])
        );
    }

    #[tokio::test]
    async fn value_map() {
        let input = json!([{"status": "active"}, {"status": "inactive"}, {"status": "unknown"}]);
        let Value::Object(values) = json!({"active": 1, "inactive": 0}) else {
            unreachable!()
        };
        let options = ObjectValueMapOptions {
            key: "status".into(),
            values,
        };
        assert_eq!(
            collect(object_value_map_stream(from(input), options)).await,
            json!([{"status": 1}, {"status": 0}, {"status": "unknown"}])
        );
    }

    #[tokio::test]
    async fn pick_and_omit() {
        let input = json!([{"a": 1, "b": 2, "c": 3}]);
        let options = ObjectKeysOptions {
            keys: keys(&["a", "c"]),
        };
        assert_eq!(
            collect(object_pick_stream(from(input.clone()), options)).await,
            json!([{"a": 1, "c": 3}])
        );
        let options = ObjectKeysOptions { keys: keys(&["b"]) };
        assert_eq!(
            collect(object_omit_stream(from(input), options)).await,
            json!([{"a": 1, "c": 3}])
        );
    }

    #[tokio::test]
    async fn non_object_chunk_errors() {
        let options = ObjectKeysOptions { keys: keys(&["a"]) };
        let e = pipeline(object_pick_stream(from(json!([1])), options), &[])
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Expected chunk to be object");
    }

    #[tokio::test]
    async fn from_entries() {
        let input = json!([[1, 2, 3], [4, 5]]);
        let options = ObjectEntriesOptions {
            keys: vec!["a", "b", "c"].into(),
        };
        assert_eq!(
            collect(object_from_entries_stream(from(input.clone()), options)).await,
            json!([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": null}])
        );
        let options = ObjectEntriesOptions {
            keys: Keys::lazy(|| keys(&["a", "b", "c"])),
        };
        assert_eq!(
            collect(object_from_entries_stream(from(input), options)).await,
            json!([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": null}])
        );
    }

    #[tokio::test]
    async fn to_entries() {
        let input = json!([{"a": 1, "b": 2, "c": 3}, {"a": 4, "c": 6}]);
        let options = ObjectEntriesOptions {
            keys: vec!["a", "b", "c"].into(),
        };
        assert_eq!(
            collect(object_to_entries_stream(from(input.clone()), options)).await,
            json!([[1, 2, 3], [4, null, 6]])
        );
        let options = ObjectEntriesOptions {
            keys: Keys::lazy(|| keys(&["c", "a"])),
        };
        assert_eq!(
            collect(object_to_entries_stream(from(input.clone()), options)).await,
            json!([[3, 1], [6, 4]])
        );
        let options = ObjectEntriesOptions::default();
        assert_eq!(
            collect(object_to_entries_stream(from(input), options)).await,
            json!([[], []])
        );
    }

    #[tokio::test]
    async fn lazy_keys_resolve_once() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        let options = ObjectEntriesOptions {
            keys: Keys::lazy(move || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                vec!["a".into()]
            }),
        };
        collect(object_to_entries_stream(
            from(json!([{"a": 1}, {"a": 2}])),
            options,
        ))
        .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn skip_consecutive_duplicates() {
        let input = json!([{"a": 1}, {"b": 2}, {"b": 2}, {"c": 3}, {"a": 1}]);
        assert_eq!(
            collect(object_skip_consecutive_duplicates_stream(from(input))).await,
            json!([{"a": 1}, {"b": 2}, {"c": 3}, {"a": 1}])
        );
        let input = json!([
            {"a": 1, "b": {"c": 2, "d": 3}}, {"b": {"d": 3, "c": 2}, "a": 1}, {"a": 1, "b": {"c": 2, "d": 4}}
        ]);
        assert_eq!(
            collect(object_skip_consecutive_duplicates_stream(from(input))).await,
            json!([{"a": 1, "b": {"c": 2, "d": 3}}, {"a": 1, "b": {"c": 2, "d": 4}}])
        );
    }
}
