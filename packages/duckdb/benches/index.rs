// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, Value};
use datastream_duckdb::duckdb::arrow::array::{ArrayRef, Int32Array, StringArray};
use datastream_duckdb::duckdb::arrow::datatypes::{DataType, Field, Schema};
use datastream_duckdb::duckdb::arrow::record_batch::RecordBatch;
use datastream_duckdb::{
    duckdb_appender_stream, duckdb_arrow_insert_stream, duckdb_connect, Db, DuckdbOptions,
};
use serde_json::json;
use tokio::runtime::Runtime;

const N: usize = 10_000;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ])
}

/// A fresh table per iteration so every run inserts into an empty table.
fn next_table(db: &Db, prefix: &str, counter: &AtomicUsize, create: bool) -> String {
    let table = format!("{prefix}_{}", counter.fetch_add(1, Ordering::Relaxed));
    if create {
        db.lock()
            .unwrap()
            .execute_batch(&format!("CREATE TABLE {table} (id INTEGER, name VARCHAR)"))
            .unwrap();
    }
    table
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = duckdb_connect(None, &[]).unwrap();
    let counter = AtomicUsize::new(0);
    let rows: Vec<Value> = (0..N)
        .map(|i| json!({"id": i, "name": format!("user_{i}")}))
        .collect();
    let schema = Arc::new(schema());
    let batches: Vec<RecordBatch> = (0..N)
        .collect::<Vec<_>>()
        .chunks(1_000)
        .map(|chunk| {
            let ids: ArrayRef = Arc::new(
                chunk
                    .iter()
                    .map(|&i| Some(i as i32))
                    .collect::<Int32Array>(),
            );
            let names: ArrayRef = Arc::new(
                chunk
                    .iter()
                    .map(|i| Some(format!("user_{i}")))
                    .collect::<StringArray>(),
            );
            RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap()
        })
        .collect();

    c.bench_function("duckdbAppenderStream/10000 object rows", |b| {
        b.to_async(&rt).iter(|| {
            let table = next_table(&db, "bench_appender", &counter, true);
            let options = DuckdbOptions {
                db: db.clone(),
                table,
                schema: None,
            };
            let input = create_readable_stream(rows.clone());
            async move { duckdb_appender_stream(input, options).await.unwrap() }
        })
    });
    c.bench_function(
        "duckdbArrowInsertStream/10000 rows via Arrow batches (batchSize=1000)",
        |b| {
            b.to_async(&rt).iter(|| {
                let table = next_table(&db, "bench_arrow", &counter, true);
                let options = DuckdbOptions {
                    db: db.clone(),
                    table,
                    schema: None,
                };
                let input = create_readable_stream(batches.clone());
                async move { duckdb_arrow_insert_stream(input, options).await.unwrap() }
            })
        },
    );
    c.bench_function(
        "duckdbAppenderStream (schema + auto-create)/10000 rows, schema provided",
        |b| {
            b.to_async(&rt).iter(|| {
                let table = next_table(&db, "bench_appender_schema", &counter, false);
                let options = DuckdbOptions {
                    db: db.clone(),
                    table,
                    schema: Some(self::schema()),
                };
                let input = create_readable_stream(rows.clone());
                async move { duckdb_appender_stream(input, options).await.unwrap() }
            })
        },
    );
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = benches
}
criterion_main!(index);
