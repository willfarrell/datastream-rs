// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use datastream_core::{create_readable_stream, Value};
use datastream_duckdb::duckdb::arrow::array::{ArrayRef, Int32Array, StringArray};
use datastream_duckdb::duckdb::arrow::datatypes::{DataType, Field, Schema};
use datastream_duckdb::duckdb::arrow::record_batch::RecordBatch;
use datastream_duckdb::{
    duckdb_appender_stream, duckdb_arrow_insert_stream, duckdb_connect, Db, DuckdbOptions,
};
use proptest::prelude::*;
use serde_json::json;

type Rows = Vec<(Option<i32>, Option<String>)>;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    // The appender runs on a blocking thread, so a full runtime is needed.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap()
        .block_on(future)
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn create_table(db: &Db, table: &str) -> datastream_duckdb::duckdb::Result<()> {
    db.lock().unwrap().execute_batch(&format!(
        "CREATE TABLE {} (id INTEGER, name VARCHAR)",
        quote(table)
    ))
}

fn select(db: &Db, table: &str) -> Rows {
    let conn = db.lock().unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT id, name FROM {} ORDER BY rowid",
            quote(table)
        ))
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    rows.map(Result::unwrap).collect()
}

fn options(db: &Db, table: &str) -> DuckdbOptions {
    DuckdbOptions {
        db: db.clone(),
        table: table.into(),
        schema: None,
    }
}

fn rows() -> impl Strategy<Value = Rows> {
    prop::collection::vec((any::<Option<i32>>(), prop::option::of(".{0,20}")), 1..20)
}

fn loose_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<i64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        ".{0,8}".prop_map(Value::from),
        any::<bool>().prop_map(Value::from),
    ]
}

proptest! {
    #[test]
    fn fuzz_appender_roundtrip(rows in rows(), as_arrays: bool) {
        let db = duckdb_connect(None, &[]).unwrap();
        create_table(&db, "fuzz").unwrap();
        let input: Vec<Value> = rows
            .iter()
            .map(|(id, name)| if as_arrays { json!([id, name]) } else { json!({"id": id, "name": name}) })
            .collect();
        block_on(duckdb_appender_stream(create_readable_stream(input), options(&db, "fuzz"))).unwrap();
        prop_assert_eq!(select(&db, "fuzz"), rows);
    }

    #[test]
    fn fuzz_appender_random_values(input in prop::collection::vec((loose_value(), loose_value()), 1..20)) {
        // Values DuckDB can't cast fail the stream; otherwise every row lands.
        let db = duckdb_connect(None, &[]).unwrap();
        create_table(&db, "fuzz").unwrap();
        let len = input.len();
        let input: Vec<Value> = input
            .into_iter()
            .map(|(id, name)| json!({"id": id, "name": name}))
            .collect();
        match block_on(duckdb_appender_stream(create_readable_stream(input), options(&db, "fuzz"))) {
            Ok(()) => prop_assert_eq!(select(&db, "fuzz").len(), len),
            Err(e) => prop_assert!(!e.to_string().is_empty()),
        }
    }

    #[test]
    fn fuzz_appender_random_table_name(table in ".{0,30}", rows in rows()) {
        let db = duckdb_connect(None, &[]).unwrap();
        let created = !table.is_empty() && create_table(&db, &table).is_ok();
        let input: Vec<Value> = rows
            .iter()
            .map(|(id, name)| json!({"id": id, "name": name}))
            .collect();
        let result = block_on(duckdb_appender_stream(create_readable_stream(input), options(&db, &table)));
        if table.is_empty() {
            prop_assert!(result.unwrap_err().to_string().contains("identifier must be a non-empty string"));
        } else if created {
            // Quoting must hold for any name: the rows land in that exact table.
            result.unwrap();
            prop_assert_eq!(select(&db, &table), rows);
        } else {
            prop_assert!(result.is_err());
        }
    }

    #[test]
    fn fuzz_arrow_insert_roundtrip(rows in rows(), batch_size in 1usize..8) {
        let db = duckdb_connect(None, &[]).unwrap();
        create_table(&db, "fuzz").unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batches: Vec<RecordBatch> = rows
            .chunks(batch_size)
            .map(|chunk| {
                let ids: ArrayRef = Arc::new(chunk.iter().map(|r| r.0).collect::<Int32Array>());
                let names: ArrayRef =
                    Arc::new(chunk.iter().map(|r| r.1.as_deref()).collect::<StringArray>());
                RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap()
            })
            .collect();
        block_on(duckdb_arrow_insert_stream(create_readable_stream(batches), options(&db, "fuzz"))).unwrap();
        prop_assert_eq!(select(&db, "fuzz"), rows);
    }
}
