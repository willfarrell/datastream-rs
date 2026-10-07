// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{
    create_pass_through_stream, create_readable_stream, create_readable_stream_from_string,
    create_transform_stream, create_writable_stream, noop, noop_flush, stream_to_array, Map, Value,
};

const ITEMS: usize = 10_000;
const COLS: usize = 10;

fn generate_string(rows: usize, cols: usize) -> String {
    let header: Vec<String> = (0..cols).map(|c| format!("col{c}")).collect();
    let mut out = header.join(",") + "\r\n";
    for r in 0..rows {
        let row: Vec<String> = (0..cols).map(|c| format!("val_{r}_{c}")).collect();
        out += &(row.join(",") + "\r\n");
    }
    out
}

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

fn benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let big_string = generate_string(ITEMS, COLS);
    let objects = generate_objects(ITEMS, COLS);

    c.bench_function("readable -> streamToArray (string, 16KB chunks)", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_readable_stream_from_string(big_string.clone(), None).unwrap();
            stream_to_array(stream, None).await.unwrap()
        })
    });

    c.bench_function("readable -> streamToArray (objects)", |b| {
        b.to_async(&runtime).iter(|| async {
            stream_to_array(create_readable_stream(objects.clone()), None)
                .await
                .unwrap()
        })
    });

    c.bench_function("readable -> transform(identity) -> streamToArray", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_transform_stream(
                create_readable_stream(objects.clone()),
                |chunk, enqueue: &mut Vec<Value>| {
                    enqueue.push(chunk);
                    Ok(())
                },
                noop_flush,
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });

    // Simulates csvParseStream: ~16KB string chunks in, ~147 rows out per chunk.
    let row: Vec<String> = (0..COLS).map(|c| format!("val_0_{c}")).collect();
    let total = big_string.len();
    c.bench_function("readable -> transform(1->N) -> streamToArray", |b| {
        b.to_async(&runtime).iter(|| async {
            let row = row.clone();
            let stream = create_transform_stream(
                create_readable_stream_from_string(big_string.clone(), None).unwrap(),
                move |chunk: String, enqueue: &mut Vec<Vec<String>>| {
                    let count = (chunk.len() * ITEMS).div_ceil(total);
                    enqueue.extend(std::iter::repeat_n(row.clone(), count));
                    Ok(())
                },
                noop_flush,
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });

    c.bench_function("readable -> passThrough -> streamToArray", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_pass_through_stream(
                create_readable_stream(objects.clone()),
                |_| Ok(()),
                noop,
            );
            stream_to_array(stream, None).await.unwrap()
        })
    });

    c.bench_function("readable -> writable (pipeline)", |b| {
        b.to_async(&runtime).iter(|| async {
            create_writable_stream(create_readable_stream(objects.clone()), |_| Ok(()), noop)
                .await
                .unwrap()
        })
    });
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
