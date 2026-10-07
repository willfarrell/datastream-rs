// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, pipeline, stream_to_array, DataStream, Map, Value};
use datastream_object::{
    object_batch_stream, object_count_stream, object_from_entries_stream, object_key_join_stream,
    object_key_map_stream, object_key_value_stream, object_omit_stream, object_pick_stream,
    object_pivot_wide_to_long_stream, object_skip_consecutive_duplicates_stream,
    object_value_map_stream, ObjectBatchOptions, ObjectCountOptions, ObjectEntriesOptions,
    ObjectKeyJoinOptions, ObjectKeyMapOptions, ObjectKeyValueOptions, ObjectKeysOptions,
    ObjectPivotWideToLongOptions, ObjectValueMapOptions,
};
use tokio::runtime::Runtime;

// The JS bench uses 100K; 10K keeps `cargo bench` to a couple of minutes.
const ITEMS: usize = 10_000;
const COLS: usize = 10;

fn generate_objects(rows: usize, cols: usize) -> Vec<Value> {
    (0..rows)
        .map(|r| {
            let map: Map<String, Value> = (0..cols)
                .map(|c| (format!("col{c}"), Value::from(format!("val_{r}_{c}"))))
                .collect();
            Value::Object(map)
        })
        .collect()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn bench(
    c: &mut Criterion,
    runtime: &Runtime,
    name: &str,
    input: &[Value],
    f: impl Fn(DataStream<Value>) -> DataStream<Value>,
) {
    c.bench_function(name, |b| {
        b.to_async(runtime).iter(|| async {
            stream_to_array(f(create_readable_stream(input.to_vec())), None)
                .await
                .unwrap()
        })
    });
}

fn benches(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let objects = generate_objects(ITEMS, COLS);
    let three = strings(&["col0", "col1", "col2"]);

    c.bench_function("objectCountStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let (stream, result) = object_count_stream(
                create_readable_stream(objects.clone()),
                ObjectCountOptions::default(),
            );
            pipeline(stream, &[&result]).await.unwrap()
        })
    });

    bench(
        c,
        &runtime,
        "objectPickStream, pick 3 keys",
        &objects,
        |s| {
            object_pick_stream(
                s,
                ObjectKeysOptions {
                    keys: three.clone(),
                },
            )
        },
    );
    bench(
        c,
        &runtime,
        "objectOmitStream, omit 3 keys",
        &objects,
        |s| {
            object_omit_stream(
                s,
                ObjectKeysOptions {
                    keys: three.clone(),
                },
            )
        },
    );

    let renames: HashMap<String, String> = (0..3)
        .map(|i| (format!("col{i}"), format!("renamed{i}")))
        .collect();
    bench(
        c,
        &runtime,
        "objectKeyMapStream, rename 3 keys",
        &objects,
        |s| {
            object_key_map_stream(
                s,
                ObjectKeyMapOptions {
                    keys: renames.clone(),
                },
            )
        },
    );

    bench(c, &runtime, "objectKeyValueStream", &objects, |s| {
        object_key_value_stream(
            s,
            ObjectKeyValueOptions {
                key: "col0".into(),
                value: "col1".into(),
            },
        )
    });

    let values: Map<String, Value> = (0..ITEMS)
        .map(|i| (format!("val_{i}_0"), Value::from(format!("mapped_{i}"))))
        .collect();
    bench(c, &runtime, "objectValueMapStream", &objects, |s| {
        object_value_map_stream(
            s,
            ObjectValueMapOptions {
                key: "col0".into(),
                values: values.clone(),
            },
        )
    });

    bench(
        c,
        &runtime,
        "objectKeyJoinStream, join 3 keys",
        &objects,
        |s| {
            object_key_join_stream(
                s,
                ObjectKeyJoinOptions {
                    keys: vec![("combined".into(), three.clone())],
                    separator: Some("-".into()),
                },
            )
        },
    );

    let arrays: Vec<Value> = (0..ITEMS)
        .map(|r| {
            Value::from(
                (0..COLS)
                    .map(|c| format!("val_{r}_{c}"))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let keys: Vec<String> = (0..COLS).map(|i| format!("col{i}")).collect();
    bench(
        c,
        &runtime,
        "objectFromEntriesStream, arrays -> objects",
        &arrays,
        |s| {
            object_from_entries_stream(
                s,
                ObjectEntriesOptions {
                    keys: keys.clone().into(),
                },
            )
        },
    );

    let grouped: Vec<Value> = objects
        .iter()
        .enumerate()
        .map(|(i, obj)| {
            let mut obj = obj.clone();
            obj["group"] = Value::from(format!("group_{}", i / (ITEMS / 100)));
            obj
        })
        .collect();
    bench(
        c,
        &runtime,
        "objectBatchStream, ~100 batches",
        &grouped,
        |s| {
            object_batch_stream(
                s,
                ObjectBatchOptions {
                    keys: vec!["group".into()],
                    max_batch_size: None,
                },
            )
        },
    );

    bench(
        c,
        &runtime,
        "objectPivotWideToLongStream, pivot 3 keys",
        &objects,
        |s| {
            object_pivot_wide_to_long_stream(
                s,
                ObjectPivotWideToLongOptions {
                    keys: three.clone(),
                    ..Default::default()
                },
            )
        },
    );

    bench(
        c,
        &runtime,
        "objectSkipConsecutiveDuplicatesStream, all unique",
        &objects,
        object_skip_consecutive_duplicates_stream,
    );
    let duplicated: Vec<Value> = objects
        .iter()
        .flat_map(|obj| [obj.clone(), obj.clone()])
        .take(ITEMS)
        .collect();
    bench(
        c,
        &runtime,
        "objectSkipConsecutiveDuplicatesStream, 50% duplicates",
        &duplicated,
        object_skip_consecutive_duplicates_stream,
    );
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = benches
}
criterion_main!(index);
