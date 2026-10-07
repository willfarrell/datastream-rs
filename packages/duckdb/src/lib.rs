// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! DuckDB writable streams, ported from `@datastream/duckdb`.
//!
//! DuckDB calls block, so each stream appends on a blocking thread and the
//! async side forwards chunks to it over a bounded channel.

use std::sync::{Arc, Mutex};

use datastream_core::{DataStream, Error, Result, StreamExt, Value, DEFAULT_HIGH_WATER_MARK};
use duckdb::arrow::datatypes::{DataType, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::Value as DuckValue;
use duckdb::{appender_params_from_iter, Appender, Config, Connection};
use tokio::sync::mpsc;

pub use duckdb;

/// A connection shared between streams.
pub type Db = Arc<Mutex<Connection>>;

#[derive(Clone, Debug)]
pub struct DuckdbOptions {
    pub db: Db,
    pub table: String,
    /// Used to CREATE TABLE when `table` does not exist yet.
    pub schema: Option<Schema>,
}

/// Open a database. `path` defaults to `":memory:"`; `options` are DuckDB settings.
pub fn duckdb_connect(path: Option<&str>, options: &[(&str, &str)]) -> Result<Db> {
    let path = path.unwrap_or(":memory:");
    // An empty path would silently open an in-memory database, masking the bug.
    if path.is_empty() {
        return Err(
            r#"duckdb: path must be a non-empty string (use ":memory:" for in-memory)"#.into(),
        );
    }
    let mut config = Config::default();
    for (key, value) in options {
        config = config.with(key, value)?;
    }
    let conn = if path == ":memory:" {
        Connection::open_in_memory_with_flags(config)?
    } else {
        Connection::open_with_flags(path, config)?
    };
    Ok(Arc::new(Mutex::new(conn)))
}

// DuckDB has no parameter binding for identifiers, so names are interpolated
// into SQL. Double embedded quotes so a crafted name can't break out.
fn quote_ident(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err("duckdb: identifier must be a non-empty string".into());
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

// Only a missing-table error means "does not exist"; anything else (lock,
// permission, ...) must propagate instead of triggering CREATE TABLE.
fn is_missing_table_error(message: &str) -> bool {
    let message = message.to_lowercase();
    message.contains("does not exist")
        || message.contains("not found")
        || message.contains("catalog error")
}

fn table_exists(conn: &Connection, quoted: &str) -> Result<bool> {
    match conn.execute_batch(&format!("SELECT 1 FROM {quoted} LIMIT 0")) {
        Ok(()) => Ok(true),
        Err(e) if is_missing_table_error(&e.to_string()) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn arrow_type_to_duckdb_sql(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(TimeUnit::Second, _) => "TIMESTAMP_S",
        DataType::Timestamp(TimeUnit::Millisecond, _) => "TIMESTAMP_MS",
        DataType::Timestamp(TimeUnit::Microsecond, _) => "TIMESTAMP",
        DataType::Timestamp(TimeUnit::Nanosecond, _) => "TIMESTAMP_NS",
        _ => "VARCHAR",
    }
}

// Create the table from `schema` if needed, then return the PHYSICAL column
// order: the appender is positional, so the schema order can't be trusted.
fn ensure_table_and_columns(
    conn: &Connection,
    table: &str,
    schema: Option<&Schema>,
) -> Result<Vec<String>> {
    let quoted = quote_ident(table)?;
    if let Some(schema) = schema {
        if !table_exists(conn, &quoted)? {
            let cols = schema
                .fields()
                .iter()
                .map(|f| {
                    Ok(format!(
                        "{} {}",
                        quote_ident(f.name())?,
                        arrow_type_to_duckdb_sql(f.data_type())
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            conn.execute_batch(&format!("CREATE TABLE {quoted} ({})", cols.join(", ")))?;
        }
    }
    let mut stmt = conn.prepare(&format!("DESCRIBE {quoted}"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(columns)
}

fn to_duckdb_value(value: Option<&Value>) -> DuckValue {
    match value {
        None | Some(Value::Null) => DuckValue::Null,
        Some(Value::Bool(b)) => DuckValue::Boolean(*b),
        Some(Value::Number(n)) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => DuckValue::BigInt(i),
            (_, Some(u)) => DuckValue::UBigInt(u),
            _ => DuckValue::Double(n.as_f64().unwrap_or(f64::NAN)),
        },
        Some(Value::String(s)) => DuckValue::Text(s.clone()),
        // Nested values are stored as JSON text.
        Some(other) => DuckValue::Text(other.to_string()),
    }
}

// Drain `input` into an appender on a blocking thread. Nothing (not even
// CREATE TABLE) happens until the first chunk. The appender is flushed and
// released when the input ends, errors, or a write fails.
async fn append_stream<T, F>(
    mut input: DataStream<T>,
    options: DuckdbOptions,
    mut append: F,
) -> Result<()>
where
    T: Send + 'static,
    F: FnMut(&mut Appender<'_>, &[String], T) -> Result<()> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<T>(DEFAULT_HIGH_WATER_MARK);
    let worker = tokio::task::spawn_blocking(move || -> Result<()> {
        let Some(mut chunk) = rx.blocking_recv() else {
            return Ok(());
        };
        let conn = options.db.lock().unwrap_or_else(|e| e.into_inner());
        let columns = ensure_table_and_columns(&conn, &options.table, options.schema.as_ref())?;
        let mut appender = conn.appender(&options.table)?;
        loop {
            append(&mut appender, &columns, chunk)?;
            match rx.blocking_recv() {
                Some(next) => chunk = next,
                None => break,
            }
        }
        appender.flush()?;
        Ok(())
    });

    let mut upstream: Result<()> = Ok(());
    while let Some(chunk) = input.next().await {
        match chunk {
            // A closed channel means the worker failed; its error is returned below.
            Ok(chunk) => {
                if tx.send(chunk).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                upstream = Err(e);
                break;
            }
        }
    }
    drop(tx);
    let written = worker.await.map_err(Error::from)?;
    upstream.and(written)
}

/// Append object rows (matched to columns by name) or array rows (by position).
pub async fn duckdb_appender_stream(
    input: DataStream<Value>,
    options: DuckdbOptions,
) -> Result<()> {
    append_stream(input, options, |appender, columns, row| {
        let cells = columns.iter().enumerate().map(|(i, column)| {
            to_duckdb_value(match &row {
                Value::Array(items) => items.get(i),
                Value::Object(map) => map.get(column),
                _ => None,
            })
        });
        appender.append_row(appender_params_from_iter(cells))?;
        Ok(())
    })
    .await
}

/// Append Arrow record batches; columns are matched by position.
pub async fn duckdb_arrow_insert_stream(
    input: DataStream<RecordBatch>,
    options: DuckdbOptions,
) -> Result<()> {
    let table = options.table.clone();
    append_stream(input, options, move |appender, columns, batch| {
        if batch.num_columns() != columns.len() {
            return Err(format!(
                r#"duckdb: record batch column count ({}) does not match table "{table}" column count ({})"#,
                batch.num_columns(),
                columns.len()
            )
            .into());
        }
        appender.append_record_batch(batch)?;
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_channel, create_readable_stream};
    use duckdb::arrow::array::{ArrayRef, Int32Array, StringArray};
    use duckdb::arrow::datatypes::Field;
    use serde_json::json;

    type Rows = Vec<(Option<i32>, Option<String>)>;

    fn setup_table() -> Db {
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .unwrap();
        db
    }

    fn select(db: &Db, sql: &str) -> Rows {
        let conn = db.lock().unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<duckdb::Result<Rows>>()
            .unwrap();
        rows
    }

    fn select_all(db: &Db) -> Rows {
        select(db, "SELECT id, name FROM users ORDER BY id")
    }

    fn rows(pairs: &[(i32, &str)]) -> Rows {
        pairs
            .iter()
            .map(|(i, n)| (Some(*i), Some(n.to_string())))
            .collect()
    }

    fn options(db: &Db, table: &str, schema: Option<Schema>) -> DuckdbOptions {
        DuckdbOptions {
            db: db.clone(),
            table: table.into(),
            schema,
        }
    }

    fn users_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn reversed_schema() -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ])
    }

    fn batch(ids: Vec<Option<i32>>, names: Vec<Option<&str>>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ];
        RecordBatch::try_new(Arc::new(users_schema()), columns).unwrap()
    }

    #[tokio::test]
    async fn appender_inserts_object_rows() {
        let db = setup_table();
        let input = create_readable_stream([
            json!({"id": 1, "name": "alice"}),
            json!({"name": "bob", "id": 2}),
        ]);
        duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice"), (2, "bob")]));
    }

    #[tokio::test]
    async fn appender_inserts_array_rows() {
        let db = setup_table();
        let input = create_readable_stream([json!([1, "alice"]), json!([2, "bob"])]);
        duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice"), (2, "bob")]));
    }

    #[tokio::test]
    async fn appender_handles_nulls() {
        let db = setup_table();
        let input = create_readable_stream([
            json!({"id": 1, "name": null}),
            json!({"id": null}),
            json!([2]),
        ]);
        duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        assert_eq!(
            select_all(&db),
            vec![(Some(1), None), (Some(2), None), (None, None)]
        );
    }

    #[tokio::test]
    async fn appender_creates_table_from_schema() {
        let db = duckdb_connect(None, &[]).unwrap();
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, options(&db, "users", Some(users_schema())))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn appender_with_schema_and_existing_table_skips_create() {
        let db = setup_table();
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, options(&db, "users", Some(users_schema())))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn appender_does_nothing_for_empty_input() {
        let db = duckdb_connect(None, &[]).unwrap();
        let input = create_readable_stream(Vec::<Value>::new());
        duckdb_appender_stream(input, options(&db, "users", Some(users_schema())))
            .await
            .unwrap();
        let conn = db.lock().unwrap();
        assert!(!table_exists(&conn, "\"users\"").unwrap());
    }

    #[tokio::test]
    async fn appender_escapes_table_name() {
        let db = setup_table();
        let table = r#"users"; DROP TABLE users; --"#;
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, options(&db, table, Some(users_schema())))
            .await
            .unwrap();
        // users survives and the crafted name is just a (weird) table name.
        assert!(select_all(&db).is_empty());
        let sql = format!("SELECT id, name FROM {}", quote_ident(table).unwrap());
        assert_eq!(select(&db, &sql), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn appender_rejects_empty_table_name() {
        let db = setup_table();
        let input = create_readable_stream([json!({"id": 1})]);
        let e = duckdb_appender_stream(input, options(&db, "", None))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "duckdb: identifier must be a non-empty string"
        );
    }

    #[tokio::test]
    async fn appender_maps_objects_by_physical_column_order() {
        let db = setup_table();
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, options(&db, "users", Some(reversed_schema())))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn appender_write_failure_errors_and_releases_connection() {
        let db = setup_table();
        let input = create_readable_stream([json!({"id": "not a number", "name": "alice"})]);
        assert!(duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .is_err());
        // The appender and lock were released: the connection is still usable.
        let input = create_readable_stream([json!({"id": 2, "name": "bob"})]);
        duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(2, "bob")]));
    }

    #[tokio::test]
    async fn appender_upstream_error_after_init_propagates() {
        let db = setup_table();
        let (tx, input) = create_readable_channel(None);
        tx.push(json!({"id": 1, "name": "alice"})).unwrap();
        tx.error("upstream boom".into()).await;
        drop(tx);
        let e = duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "upstream boom");
        // The appender was closed (flushing what it had) and the lock released.
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn appender_missing_table_without_schema_errors() {
        let db = duckdb_connect(None, &[]).unwrap();
        let input = create_readable_stream([json!({"id": 1})]);
        let e = duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap_err();
        assert!(is_missing_table_error(&e.to_string()), "{e}");
    }

    #[tokio::test]
    async fn arrow_inserts_all_record_batches() {
        let db = setup_table();
        let input = create_readable_stream([
            batch(vec![Some(1), Some(2)], vec![Some("alice"), Some("bob")]),
            batch(vec![Some(3), None], vec![None, Some("dan")]),
        ]);
        duckdb_arrow_insert_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        let mut expected = rows(&[(1, "alice"), (2, "bob")]);
        expected.push((Some(3), None));
        expected.push((None, Some("dan".into())));
        assert_eq!(select_all(&db), expected);
    }

    #[tokio::test]
    async fn arrow_creates_table_from_schema() {
        let db = duckdb_connect(None, &[]).unwrap();
        let input = create_readable_stream([batch(vec![Some(1)], vec![Some("alice")])]);
        duckdb_arrow_insert_stream(input, options(&db, "users", Some(users_schema())))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[tokio::test]
    async fn arrow_maps_columns_by_physical_order() {
        let db = setup_table();
        let input = create_readable_stream([batch(
            vec![Some(1), Some(2)],
            vec![Some("alice"), Some("bob")],
        )]);
        duckdb_arrow_insert_stream(input, options(&db, "users", Some(reversed_schema())))
            .await
            .unwrap();
        assert_eq!(select_all(&db), rows(&[(1, "alice"), (2, "bob")]));
    }

    #[tokio::test]
    async fn arrow_column_count_mismatch_errors() {
        let db = setup_table();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let one_col = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let input = create_readable_stream([one_col]);
        let e = duckdb_arrow_insert_stream(input, options(&db, "users", None))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            r#"duckdb: record batch column count (1) does not match table "users" column count (2)"#
        );
        assert!(select_all(&db).is_empty());
    }

    #[tokio::test]
    async fn arrow_upstream_error_after_init_propagates() {
        let db = setup_table();
        let (tx, input) = create_readable_channel(None);
        tx.push(batch(vec![Some(1)], vec![Some("alice")])).unwrap();
        tx.error("upstream batch boom".into()).await;
        drop(tx);
        let e = duckdb_arrow_insert_stream(input, options(&db, "users", None))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "upstream batch boom");
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
    }

    #[test]
    fn arrow_type_mapping() {
        let cases = [
            (DataType::Boolean, "BOOLEAN"),
            (DataType::Int8, "TINYINT"),
            (DataType::Int16, "SMALLINT"),
            (DataType::Int32, "INTEGER"),
            (DataType::Int64, "BIGINT"),
            (DataType::UInt8, "UTINYINT"),
            (DataType::UInt16, "USMALLINT"),
            (DataType::UInt32, "UINTEGER"),
            (DataType::UInt64, "UBIGINT"),
            (DataType::Float32, "REAL"),
            (DataType::Float64, "DOUBLE"),
            (DataType::Date32, "DATE"),
            (DataType::Date64, "DATE"),
            (DataType::Timestamp(TimeUnit::Second, None), "TIMESTAMP_S"),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                "TIMESTAMP_MS",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "TIMESTAMP",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "TIMESTAMP_NS",
            ),
            (DataType::Utf8, "VARCHAR"),
            (DataType::Null, "VARCHAR"),
        ];
        for (data_type, sql) in cases {
            assert_eq!(arrow_type_to_duckdb_sql(&data_type), sql, "{data_type:?}");
        }
    }

    #[test]
    fn quote_ident_escapes_and_rejects_empty() {
        assert_eq!(quote_ident("users").unwrap(), r#""users""#);
        assert_eq!(quote_ident(r#"a"b"#).unwrap(), r#""a""b""#);
        let e = quote_ident("").unwrap_err();
        assert_eq!(
            e.to_string(),
            "duckdb: identifier must be a non-empty string"
        );
    }

    #[test]
    fn missing_table_error_detection() {
        assert!(is_missing_table_error("Table with name x does not exist!"));
        assert!(is_missing_table_error("table NOT FOUND"));
        assert!(is_missing_table_error("Catalog Error: ..."));
        // The full phrase is required, not a prefix.
        assert!(!is_missing_table_error("does not"));
        assert!(!is_missing_table_error(
            "IO Error: Could not set lock on file"
        ));
        assert!(!is_missing_table_error(""));
    }

    #[test]
    fn json_values_map_to_duckdb_values() {
        assert_eq!(to_duckdb_value(None), DuckValue::Null);
        assert_eq!(to_duckdb_value(Some(&json!(null))), DuckValue::Null);
        assert_eq!(
            to_duckdb_value(Some(&json!(true))),
            DuckValue::Boolean(true)
        );
        assert_eq!(to_duckdb_value(Some(&json!(-1))), DuckValue::BigInt(-1));
        assert_eq!(
            to_duckdb_value(Some(&json!(u64::MAX))),
            DuckValue::UBigInt(u64::MAX)
        );
        assert_eq!(to_duckdb_value(Some(&json!(1.5))), DuckValue::Double(1.5));
        assert_eq!(
            to_duckdb_value(Some(&json!("a"))),
            DuckValue::Text("a".into())
        );
        assert_eq!(
            to_duckdb_value(Some(&json!({"a": [1]}))),
            DuckValue::Text(r#"{"a":[1]}"#.into())
        );
    }

    #[test]
    fn connect_defaults_to_memory_and_rejects_empty_path() {
        let db = duckdb_connect(None, &[("threads", "1")]).unwrap();
        db.lock().unwrap().execute_batch("SELECT 1").unwrap();
        assert!(duckdb_connect(Some(":memory:"), &[]).is_ok());
        let e = duckdb_connect(Some(""), &[]).unwrap_err();
        assert_eq!(
            e.to_string(),
            r#"duckdb: path must be a non-empty string (use ":memory:" for in-memory)"#
        );
        assert!(duckdb_connect(None, &[("not_a_real_setting", "1")]).is_err());
    }

    #[test]
    fn connect_opens_a_file() {
        let path =
            std::env::temp_dir().join(format!("datastream-duckdb-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let db = duckdb_connect(path.to_str(), &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch("CREATE TABLE t (a INTEGER)")
            .unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }
}
