// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! DuckDB query readable and writable streams, ported from `@datastream/duckdb`.
//!
//! DuckDB calls block, so each stream runs its queries on a blocking thread and
//! the async side exchanges rows/chunks with it over a bounded channel. A stream
//! holds the [`Db`] lock while it runs, so other users of the same `Db` wait
//! for it; give concurrent work its own connection (`Connection::try_clone`).

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datastream_core::{
    CancellationToken, DataStream, Error, Map, Result, StreamExt, Value, DEFAULT_HIGH_WATER_MARK,
};
use duckdb::arrow::array::{ArrayRef, AsArray};
use duckdb::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Int64Type, Schema, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::{TimeUnit as DuckTimeUnit, Value as DuckValue};
use duckdb::{
    appender_params_from_iter, params_from_iter, Appender, Config, Connection, InterruptHandle,
};
use tokio::sync::mpsc;

pub use duckdb;

/// A connection shared between streams.
pub type Db = Arc<Mutex<Connection>>;

/// A schema, or a function returning one when the first chunk arrives (e.g.
/// one detected upstream). `None` from the function means no schema.
#[derive(Clone)]
pub enum DuckdbSchemaInput {
    Static(Schema),
    Lazy(Arc<dyn Fn() -> Option<Schema> + Send + Sync>),
}

impl From<Schema> for DuckdbSchemaInput {
    fn from(schema: Schema) -> Self {
        Self::Static(schema)
    }
}

impl fmt::Debug for DuckdbSchemaInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(schema) => f.debug_tuple("Static").field(schema).finish(),
            Self::Lazy(_) => f.write_str("Lazy"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DuckdbOptions {
    pub db: Db,
    pub table: String,
    /// Used to CREATE TABLE when `table` does not exist yet.
    pub schema: Option<DuckdbSchemaInput>,
}

#[derive(Clone, Debug)]
pub struct DuckdbQueryOptions {
    pub db: Db,
    pub sql: String,
    /// Values bound to positional `?` params in order.
    pub params: Vec<Value>,
    /// Cancelling interrupts the running query and fails the stream with "Aborted".
    pub signal: Option<CancellationToken>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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

/// A table column, as DESCRIBE reports it.
struct TableColumn {
    name: String,
    sql_type: String,
}

// Create the table from `schema` if needed, then return the PHYSICAL column
// order: the appender is positional, so the schema order can't be trusted.
fn ensure_table_and_columns(
    conn: &Connection,
    table: &str,
    schema: Option<&Schema>,
) -> Result<Vec<TableColumn>> {
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
        .query_map([], |row| {
            Ok(TableColumn {
                name: row.get(0)?,
                sql_type: row.get(1)?,
            })
        })?
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

// The failed BEGIN keeps DuckDB's reason as its source.
#[derive(Debug)]
struct BeginError(duckdb::Error);

impl fmt::Display for BeginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("duckdb: could not begin the write stream's transaction; use a connection with no open transaction")
    }
}

impl std::error::Error for BeginError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

enum Write<T> {
    Chunk(T),
    // The input ended cleanly. A channel closed without it (upstream error, or
    // the writer future dropped) rolls back.
    End,
}

// Append every chunk, returning true when the input ended cleanly. The
// appender is dropped (flushing into the transaction) before returning.
fn append_all<T, F>(
    conn: &Connection,
    options: &DuckdbOptions,
    mut chunk: T,
    rx: &mut mpsc::Receiver<Write<T>>,
    append: &mut F,
) -> Result<bool>
where
    F: FnMut(&mut Appender<'_>, &[TableColumn], T) -> Result<()>,
{
    // A lazy schema resolves now, inside the transaction, once data has arrived.
    let schema = match &options.schema {
        Some(DuckdbSchemaInput::Static(schema)) => Some(schema.clone()),
        Some(DuckdbSchemaInput::Lazy(f)) => f(),
        None => None,
    };
    let columns = ensure_table_and_columns(conn, &options.table, schema.as_ref())?;
    let mut appender = conn.appender(&options.table)?;
    loop {
        append(&mut appender, &columns, chunk)?;
        match rx.blocking_recv() {
            Some(Write::Chunk(next)) => chunk = next,
            Some(Write::End) => break,
            None => return Ok(false),
        }
    }
    appender.flush()?;
    Ok(true)
}

// Drain `input` into an appender on a blocking thread, inside one transaction
// so a failed or dropped pipeline commits nothing (the appender auto-flushes
// every 204,800 rows, which can't be undone otherwise). Nothing (not even
// BEGIN or CREATE TABLE) happens until the first chunk; COMMIT runs after the
// appender is flushed and released. DuckDB has no nested transactions, so a
// connection that already has one open is refused (its failed BEGIN also
// aborts the caller's transaction).
async fn append_stream<T, F>(
    mut input: DataStream<T>,
    options: DuckdbOptions,
    mut append: F,
) -> Result<()>
where
    T: Send + 'static,
    F: FnMut(&mut Appender<'_>, &[TableColumn], T) -> Result<()> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Write<T>>(DEFAULT_HIGH_WATER_MARK);
    let worker = tokio::task::spawn_blocking(move || -> Result<()> {
        let Some(Write::Chunk(chunk)) = rx.blocking_recv() else {
            return Ok(());
        };
        let conn = lock(&options.db);
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(BeginError)?;
        match append_all(&conn, &options, chunk, &mut rx, &mut append) {
            // DuckDB rolls a failed COMMIT back itself.
            Ok(true) => Ok(conn.execute_batch("COMMIT")?),
            failed => {
                // A failed ROLLBACK is dropped: the stream is already failing
                // with the error that matters.
                let _ = conn.execute_batch("ROLLBACK");
                failed.map(drop)
            }
        }
    });

    let mut upstream: Result<()> = Ok(());
    while let Some(chunk) = input.next().await {
        match chunk {
            // A closed channel means the worker failed; its error is returned below.
            Ok(chunk) => {
                if tx.send(Write::Chunk(chunk)).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                upstream = Err(e);
                break;
            }
        }
    }
    if upstream.is_ok() {
        let _ = tx.send(Write::End).await;
    }
    drop(tx);
    let written = worker.await.map_err(Error::from)?;
    upstream.and(written)
}

// *** Query *** //

const MS_PER_DAY: i64 = 86_400_000;

// JSON has no Date, so DATE/TIMESTAMP become epoch milliseconds (sub-ms
// precision is dropped, flooring toward negative infinity).
fn epoch_ms(unit: DuckTimeUnit, value: i64) -> i64 {
    match unit {
        DuckTimeUnit::Second => value.saturating_mul(1000),
        DuckTimeUnit::Millisecond => value,
        DuckTimeUnit::Microsecond => value.div_euclid(1000),
        DuckTimeUnit::Nanosecond => value.div_euclid(1_000_000),
    }
}

fn micros(unit: DuckTimeUnit, value: i64) -> i64 {
    match unit {
        DuckTimeUnit::Second => value.saturating_mul(1_000_000),
        DuckTimeUnit::Millisecond => value.saturating_mul(1000),
        DuckTimeUnit::Microsecond => value,
        DuckTimeUnit::Nanosecond => value.div_euclid(1000),
    }
}

// Non-finite floats have no JSON form.
fn float(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

// Plain JSON for a result cell, following node-api's JS converter where JSON
// allows: DECIMAL -> number, TIME -> microseconds, INTERVAL -> {months, days,
// micros}, BLOB -> byte array, MAP -> [{key, value}]. HUGEINTs outside the
// i64/u64 range become decimal strings.
fn from_duckdb_value(value: &DuckValue) -> Result<Value> {
    Ok(match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(b) => Value::from(*b),
        DuckValue::TinyInt(i) => Value::from(*i),
        DuckValue::SmallInt(i) => Value::from(*i),
        DuckValue::Int(i) => Value::from(*i),
        DuckValue::BigInt(i) => Value::from(*i),
        DuckValue::UTinyInt(u) => Value::from(*u),
        DuckValue::USmallInt(u) => Value::from(*u),
        DuckValue::UInt(u) => Value::from(*u),
        DuckValue::UBigInt(u) => Value::from(*u),
        DuckValue::HugeInt(i) => match (i64::try_from(*i), u64::try_from(*i)) {
            (Ok(i), _) => Value::from(i),
            (_, Ok(u)) => Value::from(u),
            _ => Value::from(i.to_string()),
        },
        DuckValue::UHugeInt(u) => u64::try_from(*u).map_or_else(|_| Value::from(u.to_string()), Value::from),
        DuckValue::Float(f) => float(f64::from(*f)),
        DuckValue::Double(f) => float(*f),
        DuckValue::Decimal(d) => float(d.to_string().parse::<f64>()?),
        DuckValue::Timestamp(unit, v) => Value::from(epoch_ms(*unit, *v)),
        DuckValue::Date32(days) => Value::from(i64::from(*days).saturating_mul(MS_PER_DAY)),
        DuckValue::Time64(unit, v) => Value::from(micros(*unit, *v)),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => serde_json::json!({"months": months, "days": days, "micros": nanos.div_euclid(1000)}),
        DuckValue::Text(s) | DuckValue::Enum(s) => Value::from(s.as_str()),
        DuckValue::Blob(b) | DuckValue::Geometry(b) => Value::from(b.as_slice()),
        DuckValue::List(items) | DuckValue::Array(items) => {
            Value::Array(items.iter().map(from_duckdb_value).collect::<Result<_>>()?)
        }
        DuckValue::Struct(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), from_duckdb_value(v)?)))
                .collect::<Result<Map<_, _>>>()?,
        ),
        DuckValue::Map(entries) => Value::Array(
            entries
                .iter()
                .map(|(k, v)| {
                    Ok(serde_json::json!({"key": from_duckdb_value(k)?, "value": from_duckdb_value(v)?}))
                })
                .collect::<Result<_>>()?,
        ),
        DuckValue::Union(v) => from_duckdb_value(v)?,
        other => return Err(format!("duckdb: unsupported result value {other:?}").into()),
    })
}

// Shared between a query's worker and its stream. `running` is set only while
// the worker is inside a DuckDB call for this query, so a cancel interrupts
// that call and never a paused, finished, or not-yet-started query.
#[derive(Default)]
struct QueryState {
    running: Option<Arc<InterruptHandle>>,
    cancelled: bool,
}

// Marks one DuckDB call as in flight; refuses to start once cancelled.
struct Busy<'a>(&'a Mutex<QueryState>);

impl<'a> Busy<'a> {
    fn start(state: &'a Mutex<QueryState>, handle: &Arc<InterruptHandle>) -> Result<Self> {
        let mut s = lock(state);
        if s.cancelled {
            return Err("Aborted".into());
        }
        s.running = Some(handle.clone());
        Ok(Self(state))
    }
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        lock(self.0).running = None;
    }
}

// Dropping the stream (done, failed, aborted, or abandoned) cancels its query.
// interrupt() is connection-wide; the worker still holds the `Db` lock while
// `running` is set, so it can only stop this stream's own call.
struct CancelOnDrop(Arc<Mutex<QueryState>>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let mut s = lock(&self.0);
        s.cancelled = true;
        if let Some(handle) = s.running.take() {
            handle.interrupt();
        }
    }
}

// Run the query and send each row as a JSON object. Rows are pulled one
// DuckDB chunk (up to 2048 rows) at a time from a streaming result.
fn query_rows(
    db: &Db,
    sql: &str,
    params: &[Value],
    state: &Mutex<QueryState>,
    tx: &mpsc::Sender<Value>,
) -> Result<()> {
    let conn = lock(db);
    let handle = conn.interrupt_handle();
    let busy = || Busy::start(state, &handle);
    let mut stmt = {
        let _busy = busy()?;
        conn.prepare(sql)?
    };
    {
        let _busy = busy()?;
        // Binds and starts the streaming result; raw_query() then fetches from it.
        let params = params.iter().map(|v| to_duckdb_value(Some(v)));
        let _ = stmt.stream_arrow(params_from_iter(params))?;
    }
    let columns = stmt.column_names();
    let mut rows = stmt.raw_query();
    loop {
        let row = {
            let _busy = busy()?;
            rows.next()?
        };
        let Some(row) = row else {
            return Ok(());
        };
        let mut object = Map::with_capacity(columns.len());
        for (i, name) in columns.iter().enumerate() {
            let cell = row.get::<_, DuckValue>(i)?;
            object.insert(name.clone(), from_duckdb_value(&cell)?);
        }
        // A closed channel means the stream was dropped.
        if tx.blocking_send(Value::Object(object)).is_err() {
            return Ok(());
        }
    }
}

/// Stream a query's rows as JSON objects keyed by column name. Nothing runs
/// until the stream is first polled. Dropping the stream, or cancelling
/// `signal`, while a DuckDB call is in flight interrupts it.
pub fn duckdb_query_stream(options: DuckdbQueryOptions) -> DataStream<Value> {
    let DuckdbQueryOptions {
        db,
        sql,
        params,
        signal,
    } = options;
    let signal = signal.unwrap_or_default();
    Box::pin(async_stream::try_stream! {
        let state = Arc::new(Mutex::new(QueryState::default()));
        let _cancel = CancelOnDrop(state.clone());
        let (tx, mut rx) = mpsc::channel(DEFAULT_HIGH_WATER_MARK);
        let worker = tokio::task::spawn_blocking(move || query_rows(&db, &sql, &params, &state, &tx));
        loop {
            let row = tokio::select! {
                biased;
                () = signal.cancelled() => Err(Error::from("Aborted")),
                row = rx.recv() => Ok(row),
            }?;
            match row {
                Some(row) => yield row,
                None => break,
            }
        }
        // Surfaces a query error (or a worker panic) once every row is out.
        worker.await.map_err(Error::from)??;
    })
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
                Value::Object(map) => map.get(&column.name),
                _ => None,
            })
        });
        appender.append_row(appender_params_from_iter(cells))?;
        Ok(())
    })
    .await
}

// DATE and every TIMESTAMP* type (TIMESTAMP WITH TIME ZONE included).
fn is_temporal(sql_type: &str) -> bool {
    sql_type == "DATE" || sql_type.starts_with("TIMESTAMP")
}

// An Arrow Date/Timestamp column bound for a non-temporal table column becomes
// epoch milliseconds (as the JS port stores it: a Timestamp into BIGINT keeps
// the milliseconds, rather than failing for want of a cast). None for any
// other column.
fn epoch_ms_array(array: &ArrayRef) -> Option<ArrayRef> {
    let ms = match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .unary::<_, Int64Type>(|days| i64::from(days).saturating_mul(MS_PER_DAY)),
        DataType::Date64 => array.as_primitive::<Date64Type>().reinterpret_cast(),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .unary(|v| epoch_ms(DuckTimeUnit::Second, v)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .reinterpret_cast(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .unary(|v| epoch_ms(DuckTimeUnit::Microsecond, v)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .unary(|v| epoch_ms(DuckTimeUnit::Nanosecond, v)),
        _ => return None,
    };
    Some(Arc::new(ms))
}

// Lay `batch` out in the table's physical column order, matching each table
// column to a batch field by NAME: the appender is positional, but a batch's
// field order is arbitrary. DuckDB identifiers are case-insensitive (table
// "ID" vs Arrow "id"), so match case-insensitively. Table columns are unique
// ignoring case and the counts match, so a batch with fields differing only by
// case always leaves some table column unmatched and is rejected.
fn batch_in_table_order(
    batch: &RecordBatch,
    columns: &[TableColumn],
    table: &str,
) -> Result<RecordBatch> {
    // Checked first so a mismatch is a clear schema error, not a missing column.
    if batch.num_columns() != columns.len() {
        return Err(format!(
            r#"duckdb: record batch column count ({}) does not match table "{table}" column count ({})"#,
            batch.num_columns(),
            columns.len()
        )
        .into());
    }
    let schema = batch.schema();
    let by_name: std::collections::HashMap<String, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().to_lowercase(), i))
        .collect();
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(&i) = by_name.get(&column.name.to_lowercase()) else {
            return Err(format!(
                r#"duckdb: record batch is missing column "{}" for table "{table}""#,
                column.name
            )
            .into());
        };
        let (field, array) = (schema.field(i), batch.column(i));
        match epoch_ms_array(array).filter(|_| !is_temporal(&column.sql_type)) {
            Some(ms) => {
                fields.push(Field::new(
                    field.name(),
                    DataType::Int64,
                    field.is_nullable(),
                ));
                arrays.push(ms);
            }
            None => {
                fields.push(field.clone());
                arrays.push(array.clone());
            }
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

/// Append Arrow record batches. Batch fields are matched to table columns by
/// name, case-insensitively; Date/Timestamp fields bound for a non-DATE /
/// TIMESTAMP column are written as epoch milliseconds.
pub async fn duckdb_arrow_insert_stream(
    input: DataStream<RecordBatch>,
    options: DuckdbOptions,
) -> Result<()> {
    let table = options.table.clone();
    append_stream(input, options, move |appender, columns, batch| {
        appender.append_record_batch(batch_in_table_order(&batch, columns, &table)?)?;
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use datastream_core::{create_readable_channel, create_readable_stream, stream_to_array};
    use duckdb::arrow::array::{Int32Array, StringArray};
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
            schema: schema.map(Into::into),
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
    async fn appender_resolves_a_lazy_schema_when_data_arrives() {
        let db = duckdb_connect(None, &[]).unwrap();
        let resolved = Arc::new(Mutex::new(false));
        let seen = resolved.clone();
        let lazy = DuckdbSchemaInput::Lazy(Arc::new(move || {
            *seen.lock().unwrap() = true;
            Some(users_schema())
        }));
        let mut opts = options(&db, "users", None);
        opts.schema = Some(lazy.clone());
        let empty = create_readable_stream(Vec::<Value>::new());
        duckdb_appender_stream(empty, opts.clone()).await.unwrap();
        // Nothing written: never resolved.
        assert!(!*resolved.lock().unwrap());
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, opts.clone()).await.unwrap();
        assert!(*resolved.lock().unwrap());
        assert_eq!(select_all(&db), rows(&[(1, "alice")]));
        // A lazy schema that resolves to None creates nothing.
        opts.schema = Some(DuckdbSchemaInput::Lazy(Arc::new(|| None)));
        opts.table = "missing".into();
        let input = create_readable_stream([json!({"id": 1})]);
        let e = duckdb_appender_stream(input, opts).await.unwrap_err();
        assert!(is_missing_table_error(&e.to_string()), "{e}");
        assert_eq!(format!("{lazy:?}"), "Lazy");
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
        // The transaction was rolled back and the lock released.
        assert!(select_all(&db).is_empty());
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

    fn one_row(fields: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let schema = Schema::new(
            fields
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let arrays = fields.into_iter().map(|(_, array)| array).collect();
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    // Every column of the first row (each must be VARCHAR).
    fn select_text(db: &Db, sql: &str) -> Vec<Option<String>> {
        let conn = db.lock().unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let count = row.as_ref().column_count();
        (0..count).map(|i| row.get(i).unwrap()).collect()
    }

    async fn insert(db: &Db, table: &str, batch: RecordBatch, schema: bool) -> Result<()> {
        let schema = schema.then(|| batch.schema().as_ref().clone());
        let input = create_readable_stream([batch]);
        duckdb_arrow_insert_stream(input, options(db, table, schema)).await
    }

    #[tokio::test]
    async fn arrow_maps_batch_columns_to_table_columns_by_name_not_position() {
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch("CREATE TABLE ab (a VARCHAR, b VARCHAR)")
            .unwrap();
        let batch = one_row(vec![
            ("b", Arc::new(StringArray::from(vec!["B1"]))),
            ("a", Arc::new(StringArray::from(vec!["A1"]))),
        ]);
        insert(&db, "ab", batch, false).await.unwrap();
        assert_eq!(
            select_text(&db, "SELECT a, b FROM ab"),
            [Some("A1".into()), Some("B1".into())]
        );
    }

    #[tokio::test]
    async fn arrow_matches_batch_fields_to_table_columns_case_insensitively() {
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch(r#"CREATE TABLE t ("ID" INTEGER, "Name" VARCHAR)"#)
            .unwrap();
        insert(&db, "t", batch(vec![Some(1)], vec![Some("a")]), false)
            .await
            .unwrap();
        assert_eq!(
            select_text(&db, r#"SELECT "ID"::VARCHAR, "Name" FROM t"#),
            [Some("1".into()), Some("a".into())]
        );
    }

    #[tokio::test]
    async fn arrow_rejects_a_batch_missing_a_table_column() {
        for names in [["id", "nickname"], ["ID", "id"]] {
            let db = setup_table();
            let batch = one_row(vec![
                (names[0], Arc::new(Int32Array::from(vec![1]))),
                (names[1], Arc::new(Int32Array::from(vec![2]))),
            ]);
            let e = insert(&db, "users", batch, false).await.unwrap_err();
            assert_eq!(
                e.to_string(),
                r#"duckdb: record batch is missing column "name" for table "users""#,
                "{names:?}"
            );
            assert!(select_all(&db).is_empty());
            assert_no_transaction(&db);
        }
    }

    fn describe(db: &Db, table: &str) -> Vec<(String, String)> {
        let conn = db.lock().unwrap();
        let mut stmt = conn.prepare(&format!("DESCRIBE {table}")).unwrap();
        let columns = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<duckdb::Result<_>>()
            .unwrap();
        columns
    }

    fn pairs(names: &[(&str, &str)]) -> Vec<(String, String)> {
        names
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    fn texts(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some(v.to_string())).collect()
    }

    #[tokio::test]
    async fn arrow_creates_integer_and_float_columns() {
        use duckdb::arrow::array::{
            Float32Array, Float64Array, Int16Array, Int64Array, Int8Array, UInt16Array,
            UInt32Array, UInt64Array, UInt8Array,
        };
        let db = duckdb_connect(None, &[]).unwrap();
        let batch = one_row(vec![
            ("i8", Arc::new(Int8Array::from(vec![-8]))),
            ("i16", Arc::new(Int16Array::from(vec![-16]))),
            ("i32", Arc::new(Int32Array::from(vec![-32]))),
            ("i64", Arc::new(Int64Array::from(vec![-64]))),
            ("u8", Arc::new(UInt8Array::from(vec![8]))),
            ("u16", Arc::new(UInt16Array::from(vec![16]))),
            ("u32", Arc::new(UInt32Array::from(vec![32]))),
            ("u64", Arc::new(UInt64Array::from(vec![64]))),
            ("f32", Arc::new(Float32Array::from(vec![1.5]))),
            ("f64", Arc::new(Float64Array::from(vec![2.25]))),
        ]);
        insert(&db, "nums", batch, true).await.unwrap();
        assert_eq!(
            describe(&db, "nums"),
            pairs(&[
                ("i8", "TINYINT"),
                ("i16", "SMALLINT"),
                ("i32", "INTEGER"),
                ("i64", "BIGINT"),
                ("u8", "UTINYINT"),
                ("u16", "USMALLINT"),
                ("u32", "UINTEGER"),
                ("u64", "UBIGINT"),
                ("f32", "FLOAT"),
                ("f64", "DOUBLE"),
            ])
        );
        let all = ([
            "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64",
        ])
        .map(|c| format!("{c}::VARCHAR"))
        .join(", ");
        assert_eq!(
            select_text(&db, &format!("SELECT {all} FROM nums")),
            texts(&["-8", "-16", "-32", "-64", "8", "16", "32", "64", "1.5", "2.25"])
        );
    }

    // 2024-01-02, as days and as epoch ms.
    const DAY: i32 = 19_724;
    const DAY_MS: i64 = DAY as i64 * MS_PER_DAY;

    #[tokio::test]
    async fn arrow_stores_dates_and_every_timestamp_unit() {
        use duckdb::arrow::array::{
            Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        };
        let db = duckdb_connect(None, &[]).unwrap();
        let at = DAY_MS + 11_045_678; // 03:04:05.678
        let batch = one_row(vec![
            ("d32", Arc::new(Date32Array::from(vec![DAY]))),
            ("d64", Arc::new(Date64Array::from(vec![DAY_MS]))),
            (
                "ts_s",
                Arc::new(TimestampSecondArray::from(vec![at / 1000])),
            ),
            ("ts_ms", Arc::new(TimestampMillisecondArray::from(vec![at]))),
            (
                "ts_us",
                Arc::new(TimestampMicrosecondArray::from(vec![at * 1000])),
            ),
            (
                "ts_ns",
                Arc::new(TimestampNanosecondArray::from(vec![at * 1_000_000])),
            ),
        ]);
        insert(&db, "stamps", batch, true).await.unwrap();
        assert_eq!(
            describe(&db, "stamps"),
            pairs(&[
                ("d32", "DATE"),
                ("d64", "DATE"),
                ("ts_s", "TIMESTAMP_S"),
                ("ts_ms", "TIMESTAMP_MS"),
                ("ts_us", "TIMESTAMP"),
                ("ts_ns", "TIMESTAMP_NS"),
            ])
        );
        assert_eq!(
            select_text(
                &db,
                "SELECT d32::VARCHAR, d64::VARCHAR, ts_s::VARCHAR, ts_ms::VARCHAR, ts_us::VARCHAR, ts_ns::VARCHAR FROM stamps"
            ),
            texts(&[
                "2024-01-02",
                "2024-01-02",
                "2024-01-02 03:04:05",
                "2024-01-02 03:04:05.678",
                "2024-01-02 03:04:05.678",
                "2024-01-02 03:04:05.678",
            ])
        );
    }

    #[tokio::test]
    async fn arrow_converts_dates_and_timestamps_only_for_temporal_target_columns() {
        use duckdb::arrow::array::{
            Date32Array, Date64Array, TimestampMillisecondArray, TimestampNanosecondArray,
        };
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE mixed (ts_big BIGINT, d_text VARCHAR, d64_big BIGINT, ns_big BIGINT, d_ts TIMESTAMP, ts_d DATE)",
            )
            .unwrap();
        let batch = one_row(vec![
            (
                "ts_big",
                Arc::new(TimestampMillisecondArray::from(vec![1000])),
            ),
            ("d_text", Arc::new(Date32Array::from(vec![DAY]))),
            ("d64_big", Arc::new(Date64Array::from(vec![DAY_MS]))),
            ("ns_big", Arc::new(TimestampNanosecondArray::from(vec![-1]))),
            ("d_ts", Arc::new(Date32Array::from(vec![DAY]))),
            (
                "ts_d",
                Arc::new(TimestampMillisecondArray::from(vec![DAY_MS + 1])),
            ),
        ]);
        insert(&db, "mixed", batch, false).await.unwrap();
        let day_ms = DAY_MS.to_string();
        assert_eq!(
            select_text(
                &db,
                "SELECT ts_big::VARCHAR, d_text, d64_big::VARCHAR, ns_big::VARCHAR, d_ts::VARCHAR, ts_d::VARCHAR FROM mixed"
            ),
            texts(&["1000", &day_ms, &day_ms, "-1", "2024-01-02 00:00:00", "2024-01-02"])
        );
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
        assert!(select_all(&db).is_empty());
    }

    // *** write atomicity: one transaction per write stream *** //

    // More rows than DuckDB's 204,800-row appender auto-flush.
    const ROWS: i32 = 300_000;
    const BEGIN_ERROR: &str = "duckdb: could not begin the write stream's transaction; use a connection with no open transaction";

    fn count(db: &Db, table: &str) -> duckdb::Result<i64> {
        db.lock()
            .unwrap()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
    }

    // `chunks`, then an upstream error.
    fn failing<T: Send + 'static>(chunks: Vec<T>) -> DataStream<T> {
        Box::pin(async_stream::stream! {
            for chunk in chunks {
                yield Ok(chunk);
            }
            yield Err("upstream failed".into());
        })
    }

    // The connection has no transaction left open.
    fn assert_no_transaction(db: &Db) {
        let conn = db.lock().unwrap();
        conn.execute_batch("BEGIN TRANSACTION").unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
    }

    #[tokio::test]
    async fn appender_commits_nothing_when_the_pipeline_fails_past_an_auto_flush() {
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch("CREATE TABLE t (x INTEGER)")
            .unwrap();
        let input = failing((0..ROWS).map(|x| json!({ "x": x })).collect());
        let e = duckdb_appender_stream(input, options(&db, "t", None))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "upstream failed");
        assert_eq!(count(&db, "t").unwrap(), 0);
        assert_no_transaction(&db);
    }

    #[tokio::test]
    async fn arrow_commits_nothing_when_the_pipeline_fails_past_an_auto_flush() {
        let db = duckdb_connect(None, &[]).unwrap();
        db.lock()
            .unwrap()
            .execute_batch("CREATE TABLE t (x INTEGER)")
            .unwrap();
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let big = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef],
        )
        .unwrap();
        let e = duckdb_arrow_insert_stream(failing(vec![big]), options(&db, "t", None))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "upstream failed");
        assert_eq!(count(&db, "t").unwrap(), 0);
        assert_no_transaction(&db);
    }

    #[tokio::test]
    async fn appender_rolls_back_the_table_it_created_when_the_pipeline_fails() {
        let db = duckdb_connect(None, &[]).unwrap();
        let input = failing(vec![json!({"id": 1, "name": "alice"})]);
        let e = duckdb_appender_stream(input, options(&db, "users", Some(users_schema())))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "upstream failed");
        let e = count(&db, "users").unwrap_err();
        assert!(
            e.to_string()
                .contains("Table with name users does not exist"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn write_streams_commit_their_transaction_when_the_pipeline_finishes() {
        let db = setup_table();
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        duckdb_appender_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        let input = create_readable_stream([batch(vec![Some(2)], vec![Some("bob")])]);
        duckdb_arrow_insert_stream(input, options(&db, "users", None))
            .await
            .unwrap();
        // Committed, not just visible inside a still-open transaction.
        assert_no_transaction(&db);
        assert_eq!(select_all(&db), rows(&[(1, "alice"), (2, "bob")]));
    }

    #[tokio::test]
    async fn write_streams_refuse_a_connection_with_an_open_transaction() {
        let check = |e: Error| {
            assert_eq!(e.to_string(), BEGIN_ERROR);
            let cause = e.source().unwrap().to_string();
            assert!(
                cause.contains("cannot start a transaction within a transaction"),
                "{cause}"
            );
        };
        // A failed BEGIN also aborts the caller's transaction, so each writer
        // gets a fresh connection.
        let open = || {
            let db = setup_table();
            db.lock()
                .unwrap()
                .execute_batch("BEGIN TRANSACTION")
                .unwrap();
            db
        };
        let input = create_readable_stream([json!({"id": 1, "name": "alice"})]);
        check(
            duckdb_appender_stream(input, options(&open(), "users", None))
                .await
                .unwrap_err(),
        );
        let input = create_readable_stream([batch(vec![Some(1)], vec![Some("alice")])]);
        check(
            duckdb_arrow_insert_stream(input, options(&open(), "users", None))
                .await
                .unwrap_err(),
        );
    }

    #[tokio::test]
    async fn appender_rolls_back_when_its_future_is_dropped() {
        let db = setup_table();
        let (tx, input) = create_readable_channel(None);
        tx.push(json!({"id": 1, "name": "alice"})).unwrap();
        let write = duckdb_appender_stream(input, options(&db, "users", None));
        // The input never ends, so this only stops when the future is dropped.
        assert!(tokio::time::timeout(Duration::from_millis(50), write)
            .await
            .is_err());
        drop(tx);
        // Waits for the worker to roll back and release the lock.
        assert_no_transaction(&db);
        assert!(select_all(&db).is_empty());
    }

    // *** duckdb_query_stream *** //

    async fn collect(stream: DataStream<Value>) -> Result<Vec<Value>> {
        stream_to_array(stream, None).await
    }

    fn query(db: &Db, sql: &str, params: Vec<Value>) -> DataStream<Value> {
        duckdb_query_stream(DuckdbQueryOptions {
            db: db.clone(),
            sql: sql.into(),
            params,
            signal: None,
        })
    }

    // Lock `db` on another thread, failing if a query is still holding it.
    async fn assert_released(db: &Db) {
        let db = db.clone();
        let locked = tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute_batch("SELECT 1").unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), locked)
            .await
            .expect("the query still holds the connection")
            .unwrap();
    }

    // Never yields a row on its own: only an interrupt stops it.
    const ENDLESS: &str =
        "SELECT range FROM range(1000000000000) WHERE hash(range) % 1000000007 = 0";

    #[tokio::test]
    async fn query_streams_rows_as_json_objects() {
        let db = duckdb_connect(None, &[]).unwrap();
        let rows = collect(query(
            &db,
            "SELECT 1::INTEGER AS i, 1.5::DOUBLE AS d, 'a' AS s, true AS b, NULL AS n, 2::BIGINT AS big",
            vec![],
        ))
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![json!({"i": 1, "d": 1.5, "s": "a", "b": true, "n": null, "big": 2})]
        );
        // Column order is kept.
        let keys: Vec<_> = rows[0].as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, ["i", "d", "s", "b", "n", "big"]);
    }

    #[tokio::test]
    async fn query_binds_params() {
        let db = duckdb_connect(None, &[]).unwrap();
        let rows = collect(query(
            &db,
            "SELECT ?::INTEGER AS a, ?::VARCHAR AS b",
            vec![json!(7), json!("x")],
        ))
        .await
        .unwrap();
        assert_eq!(rows, vec![json!({"a": 7, "b": "x"})]);
    }

    #[tokio::test]
    async fn query_yields_every_row_across_result_chunks() {
        let db = duckdb_connect(None, &[]).unwrap();
        // 5000 rows span three 2048-row DuckDB chunks.
        let rows = collect(query(
            &db,
            "SELECT range::INTEGER AS i FROM range(5000) ORDER BY i",
            vec![],
        ))
        .await
        .unwrap();
        assert_eq!(rows.len(), 5000);
        assert_eq!(rows[0], json!({"i": 0}));
        assert_eq!(rows[4999], json!({"i": 4999}));
    }

    #[tokio::test]
    async fn query_surfaces_a_query_error() {
        let db = duckdb_connect(None, &[]).unwrap();
        let e = collect(query(&db, "SELECT * FROM nope", vec![]))
            .await
            .unwrap_err();
        assert!(
            e.to_string()
                .contains("Table with name nope does not exist"),
            "{e}"
        );
        assert_released(&db).await;
    }

    #[tokio::test]
    async fn query_surfaces_a_param_count_mismatch() {
        let db = duckdb_connect(None, &[]).unwrap();
        assert!(collect(query(&db, "SELECT ?::INTEGER AS a", vec![]))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn query_abandoned_mid_result_releases_the_connection() {
        let db = duckdb_connect(None, &[]).unwrap();
        let mut stream = query(
            &db,
            "SELECT range::INTEGER AS i FROM range(1000000)",
            vec![],
        );
        assert_eq!(stream.next().await.unwrap().unwrap(), json!({"i": 0}));
        drop(stream);
        assert_released(&db).await;
        let rows = collect(query(&db, "SELECT 3 AS a", vec![])).await.unwrap();
        assert_eq!(rows, vec![json!({"a": 3})]);
    }

    #[tokio::test]
    async fn query_does_nothing_until_polled() {
        let db = duckdb_connect(None, &[]).unwrap();
        let stream = query(&db, ENDLESS, vec![]);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // A started query would be holding the lock by now.
        assert!(db.try_lock().is_ok());
        drop(stream);
    }

    #[tokio::test]
    async fn query_dropped_while_executing_interrupts_it() {
        let db = duckdb_connect(None, &[]).unwrap();
        let mut stream = query(&db, ENDLESS, vec![]);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err()
        );
        drop(stream);
        // Without interrupt() the worker would hold the lock forever.
        assert_released(&db).await;
    }

    #[tokio::test]
    async fn query_signal_interrupts_a_query_that_is_still_executing() {
        let db = duckdb_connect(None, &[]).unwrap();
        let signal = CancellationToken::new();
        let stream = duckdb_query_stream(DuckdbQueryOptions {
            db: db.clone(),
            sql: ENDLESS.into(),
            params: vec![],
            signal: Some(signal.clone()),
        });
        let abort = signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            abort.cancel();
        });
        let e = collect(stream).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        assert_released(&db).await;
    }

    // The bundled build has no parquet extension (autoloading would download
    // it), so CSV stands in for the JS Parquet round trip.
    #[tokio::test]
    async fn query_reads_back_a_file_written_with_copy() {
        let db = duckdb_connect(None, &[]).unwrap();
        let path = std::env::temp_dir().join(format!(
            "datastream-duckdb-query-{}.csv",
            std::process::id()
        ));
        let file = path.to_str().unwrap().replace('\'', "''");
        db.lock()
            .unwrap()
            .execute_batch(&format!(
                "COPY (SELECT range::INTEGER AS id, 'user' || range AS name, range % 2 = 0 AS even FROM range(3)) TO '{file}' (FORMAT csv, HEADER)"
            ))
            .unwrap();
        let rows = collect(query(
            &db,
            "SELECT * FROM read_csv(?) ORDER BY id",
            vec![json!(path.to_str().unwrap())],
        ))
        .await;
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            rows.unwrap(),
            vec![
                json!({"id": 0, "name": "user0", "even": true}),
                json!({"id": 1, "name": "user1", "even": false}),
                json!({"id": 2, "name": "user2", "even": true}),
            ]
        );
    }

    #[tokio::test]
    async fn query_converts_duckdb_types_to_json() {
        let db = duckdb_connect(None, &[]).unwrap();
        let rows = collect(query(
            &db,
            "SELECT
                DATE '1970-01-02' AS date,
                TIMESTAMP '1970-01-01 00:00:01.5' AS ts,
                TIMESTAMP_S '1970-01-01 00:00:02' AS ts_s,
                TIMESTAMP_NS '1969-12-31 23:59:59.9995' AS ts_ns,
                TIME '00:00:01' AS time,
                INTERVAL '1 month 2 days 3 seconds' AS iv,
                1.25::DECIMAL(5, 2) AS dec,
                170141183460469231731687303715884105727::HUGEINT AS huge,
                18446744073709551615::HUGEINT AS huge_u64,
                'nan'::DOUBLE AS nan,
                '\\x01\\x02'::BLOB AS blob,
                [1, 2] AS list,
                {'a': 1, 'b': 'x'} AS st,
                MAP {'k': 1} AS m",
            vec![],
        ))
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![json!({
                "date": 86_400_000,
                "ts": 1500,
                "ts_s": 2000,
                "ts_ns": -1,
                "time": 1_000_000,
                "iv": {"months": 1, "days": 2, "micros": 3_000_000},
                "dec": 1.25,
                "huge": "170141183460469231731687303715884105727",
                "huge_u64": u64::MAX,
                "nan": null,
                "blob": [1, 2],
                "list": [1, 2],
                "st": {"a": 1, "b": "x"},
                "m": [{"key": "k", "value": 1}],
            })]
        );
    }

    #[test]
    fn time_units_convert_to_epoch_ms_and_micros() {
        assert_eq!(epoch_ms(DuckTimeUnit::Second, 2), 2000);
        assert_eq!(epoch_ms(DuckTimeUnit::Millisecond, -3), -3);
        assert_eq!(epoch_ms(DuckTimeUnit::Microsecond, -1), -1);
        assert_eq!(epoch_ms(DuckTimeUnit::Nanosecond, 1_999_999), 1);
        assert_eq!(epoch_ms(DuckTimeUnit::Second, i64::MAX), i64::MAX);
        assert_eq!(micros(DuckTimeUnit::Second, 1), 1_000_000);
        assert_eq!(micros(DuckTimeUnit::Millisecond, 1), 1000);
        assert_eq!(micros(DuckTimeUnit::Microsecond, 7), 7);
        assert_eq!(micros(DuckTimeUnit::Nanosecond, -1), -1);
        assert_eq!(micros(DuckTimeUnit::Millisecond, i64::MIN), i64::MIN);
    }

    #[test]
    fn duckdb_values_map_to_json() {
        assert_eq!(
            from_duckdb_value(&DuckValue::HugeInt(-1)).unwrap(),
            json!(-1)
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::HugeInt(i128::MIN)).unwrap(),
            json!(i128::MIN.to_string())
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::UHugeInt(5)).unwrap(),
            json!(5)
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::UHugeInt(u128::MAX)).unwrap(),
            json!(u128::MAX.to_string())
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::Float(f32::INFINITY)).unwrap(),
            Value::Null
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::Enum("e".into())).unwrap(),
            json!("e")
        );
        assert_eq!(
            from_duckdb_value(&DuckValue::Union(Box::new(DuckValue::Int(1)))).unwrap(),
            json!(1)
        );
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
