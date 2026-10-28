// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Apache Arrow record batch transform streams, ported from `@datastream/arrow`.
//!
//! Rows are `serde_json::Value` objects or arrays. Supported column types are
//! Boolean, every integer width, Float32/64, Utf8/LargeUtf8, Date32/64 and
//! Timestamp (any unit or timezone). Dates and timestamps are epoch-ms numbers
//! (JSON has no `Date`). Schema detection infers Boolean, Int32, Float64 and
//! Utf8. Batching checks every cell against its column type and errors
//! (naming the stream, row and field) rather than coercing it.

use std::sync::{Arc, Mutex};

pub use arrow_array;
use arrow_array::builder::{
    BooleanBuilder, GenericStringBuilder, LargeStringBuilder, PrimitiveBuilder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
pub use arrow_array::RecordBatch;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait};
pub use arrow_schema;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_stream::try_stream;
use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Map, Result, StreamExt, StreamResult, Value,
};
use serde_json::json;

// *** Schema detection *** //

fn infer_type(value: Option<&Value>) -> Option<DataType> {
    match value? {
        Value::Null => None,
        Value::String(s) if s.is_empty() => None,
        Value::Bool(_) => Some(DataType::Boolean),
        Value::Number(n) => {
            // Integers outside the signed 32-bit range would wrap in an Int32
            // column, so widen them to Float64 (exact up to 2^53).
            let f = n.as_f64().unwrap_or(f64::NAN);
            let int32 = f64::from(i32::MIN)..=f64::from(i32::MAX);
            Some(if f.fract() == 0.0 && int32.contains(&f) {
                DataType::Int32
            } else {
                DataType::Float64
            })
        }
        _ => Some(DataType::Utf8),
    }
}

// Widen across every sampled value rather than trusting the first one, so a
// column that starts with 1 and later holds 2.7 is not rejected by an Int32
// column. Int32 + Float64 widen to Float64; any other mix falls back to Utf8.
fn widen(current: DataType, next: DataType) -> DataType {
    let is_number = |t: &DataType| matches!(t, DataType::Int32 | DataType::Float64);
    if current == next {
        current
    } else if is_number(&current) && is_number(&next) {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

#[derive(Default, Clone, Debug)]
pub struct ArrowDetectSchemaOptions {
    /// Rows sampled before the schema is sealed (default 100).
    pub sample_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Result of [`arrow_detect_schema_stream`]. The [`StreamResult`] value is
/// `{ schema: [{ name, type, nullable }] | null, fields: [name] | null }`.
#[derive(Clone, Debug)]
pub struct ArrowDetectSchemaResult {
    result: StreamResult,
    schema: Arc<Mutex<Option<SchemaRef>>>,
}

impl ArrowDetectSchemaResult {
    pub fn result(&self) -> &StreamResult {
        &self.result
    }
    /// The detected schema, once sealed.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// A lazy schema for the batch streams, resolved when the first row arrives.
    pub fn lazy_schema(&self) -> ArrowSchemaInput {
        let detected = self.clone();
        ArrowSchemaInput::Lazy(Arc::new(move || detected.schema()))
    }

    fn seal(&self, samples: &[Value]) {
        let is_array = samples[0].is_array();
        let names: Vec<String> = if is_array {
            // Column count is the widest array seen across all sampled rows.
            let width = samples
                .iter()
                .map(|s| s.as_array().map_or(0, Vec::len))
                .max()
                .unwrap_or(0);
            (0..width).map(|i| format!("column{i}")).collect()
        } else {
            // Union the keys across every sampled row, in first-seen order.
            let mut names: Vec<String> = Vec::new();
            for key in samples
                .iter()
                .filter_map(Value::as_object)
                .flat_map(|o| o.keys())
            {
                if !names.contains(key) {
                    names.push(key.clone());
                }
            }
            names
        };
        let fields: Vec<Field> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let data_type = samples
                    .iter()
                    .filter_map(|s| {
                        infer_type(if is_array {
                            s.get(i)
                        } else {
                            s.get(name.as_str())
                        })
                    })
                    .reduce(widen)
                    .unwrap_or(DataType::Utf8);
                Field::new(name, data_type, true)
            })
            .collect();
        let described: Vec<Value> = fields
            .iter()
            .map(|f| json!({ "name": f.name(), "type": f.data_type().to_string(), "nullable": f.is_nullable() }))
            .collect();
        *self.schema.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(Arc::new(Schema::new(fields)));
        self.result
            .set(json!({ "schema": described, "fields": names }));
    }
}

/// Buffer the first `sample_size` rows, infer a schema from them, then pass
/// every row through unchanged.
pub fn arrow_detect_schema_stream(
    mut input: DataStream<Value>,
    options: ArrowDetectSchemaOptions,
) -> (DataStream<Value>, ArrowDetectSchemaResult) {
    let sample_size = options.sample_size.unwrap_or(100);
    let key = options
        .result_key
        .unwrap_or_else(|| "arrowDetectSchema".into());
    let detected = ArrowDetectSchemaResult {
        result: StreamResult::new(key, json!({ "schema": null, "fields": null })),
        schema: Arc::new(Mutex::new(None)),
    };
    let d = detected.clone();
    let stream = Box::pin(try_stream! {
        let mut buffered = Vec::new();
        let mut sealed = false;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            if sealed {
                yield chunk;
                continue;
            }
            buffered.push(chunk);
            if buffered.len() >= sample_size {
                d.seal(&buffered);
                sealed = true;
                for chunk in buffered.drain(..) {
                    yield chunk;
                }
            }
        }
        // A short stream that never reached sample_size seals on end.
        if !buffered.is_empty() {
            d.seal(&buffered);
            for chunk in buffered.drain(..) {
                yield chunk;
            }
        }
    });
    (stream, detected)
}

// *** Rows -> RecordBatch *** //

/// A schema, or a function returning one when the first row arrives (e.g.
/// [`ArrowDetectSchemaResult::lazy_schema`]).
#[derive(Clone)]
pub enum ArrowSchemaInput {
    Static(SchemaRef),
    Lazy(Arc<dyn Fn() -> Option<SchemaRef> + Send + Sync>),
}

impl From<SchemaRef> for ArrowSchemaInput {
    fn from(schema: SchemaRef) -> Self {
        Self::Static(schema)
    }
}

impl From<Schema> for ArrowSchemaInput {
    fn from(schema: Schema) -> Self {
        Self::Static(Arc::new(schema))
    }
}

#[derive(Default, Clone)]
pub struct ArrowBatchOptions {
    pub schema: Option<ArrowSchemaInput>,
    /// Rows per record batch (default 10,000).
    pub batch_size: Option<usize>,
}

// A zero-field schema cannot carry rows (a batch's length comes from its
// columns), so reject it instead of silently dropping every row.
fn resolve_schema(name: &str, schema: Option<&ArrowSchemaInput>) -> Result<SchemaRef> {
    let schema = match schema {
        Some(ArrowSchemaInput::Static(schema)) => Some(schema.clone()),
        Some(ArrowSchemaInput::Lazy(f)) => f(),
        None => None,
    };
    let schema = schema.ok_or_else(|| format!("{name}: schema is required"))?;
    if schema.fields().is_empty() {
        return Err(format!("{name}: schema must have at least one field").into());
    }
    Ok(schema)
}

// JS numbers like 3.0 are integers too. Floats outside the i64 range are not
// (an `as` cast would saturate them silently).
fn as_int(value: &Value) -> Option<i64> {
    const I64_BOUND: f64 = 9_223_372_036_854_775_808.0; // 2^63
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|f| f.fract() == 0.0 && (-I64_BOUND..I64_BOUND).contains(f))
            .map(|f| f as i64)
    })
}

// How a rejected value is shown in an error, matching the JS port: strings are
// quoted, objects/arrays are not dumped, and a missing cell is `undefined`.
fn describe_value(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Object(_)) => "[object Object]".into(),
        Some(Value::Array(_)) => "[object Array]".into(),
        Some(other) => other.to_string(),
    }
}

const MS_PER_DAY: i64 = 86_400_000;

fn int<N: TryFrom<i64>>(value: &Value) -> Option<N> {
    as_int(value).and_then(|v| N::try_from(v).ok())
}

// One column's builder. `push` returns `false` (appending nothing) for a value
// the column cannot hold.
trait Cells: Send {
    fn push(&mut self, value: &Value) -> bool;
    fn push_null(&mut self);
    fn build(&mut self) -> ArrayRef;
}

impl Cells for BooleanBuilder {
    fn push(&mut self, value: &Value) -> bool {
        value.as_bool().map(|v| self.append_value(v)).is_some()
    }
    fn push_null(&mut self) {
        self.append_null();
    }
    fn build(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

// Utf8 is unchecked: other values keep their JSON text.
impl<O: OffsetSizeTrait> Cells for GenericStringBuilder<O> {
    fn push(&mut self, value: &Value) -> bool {
        match value {
            Value::String(s) => self.append_value(s),
            other => self.append_value(other.to_string()),
        }
        true
    }
    fn push_null(&mut self) {
        self.append_null();
    }
    fn build(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

// Numbers, dates and timestamps: `convert` is the column's value check.
struct Primitive<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    convert: fn(&Value) -> Option<T::Native>,
}

impl<T: ArrowPrimitiveType> Cells for Primitive<T> {
    fn push(&mut self, value: &Value) -> bool {
        (self.convert)(value)
            .map(|v| self.builder.append_value(v))
            .is_some()
    }
    fn push_null(&mut self) {
        self.builder.append_null();
    }
    fn build(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

fn primitive<T: ArrowPrimitiveType>(
    data_type: &DataType,
    convert: fn(&Value) -> Option<T::Native>,
) -> Box<dyn Cells> {
    Box::new(Primitive::<T> {
        // Keeps a timestamp's timezone.
        builder: PrimitiveBuilder::new().with_data_type(data_type.clone()),
        convert,
    })
}

struct Column {
    cells: Box<dyn Cells>,
    // Only a Utf8 column can hold a real "".
    utf8: bool,
}

impl Column {
    // Dates and timestamps take epoch milliseconds (JSON has no Date), whatever
    // the column's unit.
    fn new(data_type: &DataType) -> Result<Self> {
        let cells: Box<dyn Cells> = match data_type {
            DataType::Boolean => Box::new(BooleanBuilder::new()),
            DataType::Int8 => primitive::<Int8Type>(data_type, int),
            DataType::Int16 => primitive::<Int16Type>(data_type, int),
            DataType::Int32 => primitive::<Int32Type>(data_type, int),
            DataType::Int64 => primitive::<Int64Type>(data_type, as_int),
            DataType::UInt8 => primitive::<UInt8Type>(data_type, int),
            DataType::UInt16 => primitive::<UInt16Type>(data_type, int),
            DataType::UInt32 => primitive::<UInt32Type>(data_type, int),
            DataType::UInt64 => {
                primitive::<UInt64Type>(data_type, |v| v.as_u64().or_else(|| int(v)))
            }
            DataType::Float32 => {
                primitive::<Float32Type>(data_type, |v| v.as_f64().map(|f| f as f32))
            }
            DataType::Float64 => primitive::<Float64Type>(data_type, Value::as_f64),
            DataType::Date32 => primitive::<Date32Type>(data_type, |v| {
                as_int(v).and_then(|ms| i32::try_from(ms.div_euclid(MS_PER_DAY)).ok())
            }),
            DataType::Date64 => primitive::<Date64Type>(data_type, as_int),
            DataType::Timestamp(TimeUnit::Second, _) => {
                primitive::<TimestampSecondType>(data_type, |v| {
                    as_int(v).map(|ms| ms.div_euclid(1000))
                })
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                primitive::<TimestampMillisecondType>(data_type, as_int)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                primitive::<TimestampMicrosecondType>(data_type, |v| as_int(v)?.checked_mul(1000))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                primitive::<TimestampNanosecondType>(data_type, |v| {
                    as_int(v)?.checked_mul(1_000_000)
                })
            }
            DataType::Utf8 => Box::new(StringBuilder::new()),
            DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()),
            other => return Err(format!("arrow: unsupported column type {other}").into()),
        };
        let utf8 = matches!(data_type, DataType::Utf8 | DataType::LargeUtf8);
        Ok(Self { cells, utf8 })
    }

    /// Append one cell, returning `false` (and appending nothing) for a value
    /// the column cannot hold or a null in a non-nullable field. Detection
    /// treats "" as null, so an empty cell is null in every column but Utf8,
    /// the only one that can hold a real "".
    fn append(&mut self, value: Option<&Value>, nullable: bool) -> bool {
        match value {
            None | Some(Value::Null) => {}
            Some(Value::String(s)) if s.is_empty() && !self.utf8 => {}
            Some(value) => return self.cells.push(value),
        }
        if nullable {
            self.cells.push_null();
        }
        nullable
    }

    fn finish(&mut self) -> ArrayRef {
        self.cells.build()
    }
}

struct Batcher {
    name: &'static str,
    schema: SchemaRef,
    columns: Vec<Column>,
    rows: usize,
    // Zero-based index of the row across the whole stream, for error messages.
    row_index: usize,
}

impl Batcher {
    fn new(name: &'static str, schema: SchemaRef) -> Result<Self> {
        let columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|f| Column::new(f.data_type()))
            .collect::<Result<_>>()?;
        Ok(Self {
            name,
            schema,
            columns,
            rows: 0,
            row_index: 0,
        })
    }

    // Each cell is checked against its column type: a schema inferred from the
    // first sample_size rows must reject, not coerce, a later row that does
    // not fit it. The error names the stream, row and field.
    fn push(&mut self, row: &Value, is_array: bool) -> Result<()> {
        let fields = self.schema.fields();
        for (i, (field, column)) in fields.iter().zip(&mut self.columns).enumerate() {
            let value = if is_array {
                row.get(i)
            } else {
                row.get(field.name().as_str())
            };
            if !column.append(value, field.is_nullable()) {
                return Err(format!(
                    "{}: row {}, field \"{}\" expects {}{}, got {}",
                    self.name,
                    self.row_index,
                    field.name(),
                    if field.is_nullable() { "" } else { "non-null " },
                    field.data_type(),
                    describe_value(value)
                )
                .into());
            }
        }
        self.row_index += 1;
        self.rows += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        self.rows = 0;
        let arrays: Vec<ArrayRef> = self.columns.iter_mut().map(Column::finish).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

fn batch_stream(
    mut input: DataStream<Value>,
    options: ArrowBatchOptions,
    name: &'static str,
    is_array: bool,
) -> Result<DataStream<RecordBatch>> {
    let ArrowBatchOptions { schema, batch_size } = options;
    // A concrete schema is validated now; a lazy one when rows (or the end) arrive.
    if !matches!(schema, Some(ArrowSchemaInput::Lazy(_))) {
        resolve_schema(name, schema.as_ref())?;
    }
    let batch_size = batch_size.unwrap_or(10_000);
    Ok(Box::pin(try_stream! {
        let mut batcher: Option<Batcher> = None;
        while let Some(row) = input.next().await {
            let row = row?;
            if batcher.is_none() {
                batcher = Some(Batcher::new(name, resolve_schema(name, schema.as_ref())?)?);
            }
            let b = batcher.as_mut().expect("batcher initialized above");
            b.push(&row, is_array)?;
            if b.rows >= batch_size {
                yield b.finish()?;
            }
        }
        // Resolve even for an empty stream so a missing lazy schema still errors.
        let mut b = match batcher {
            Some(b) => b,
            None => Batcher::new(name, resolve_schema(name, schema.as_ref())?)?,
        };
        if b.rows > 0 {
            yield b.finish()?;
        }
    }))
}

/// Collect array rows (by column position) into record batches of `batch_size`.
pub fn arrow_batch_from_array_stream(
    input: DataStream<Value>,
    options: ArrowBatchOptions,
) -> Result<DataStream<RecordBatch>> {
    batch_stream(input, options, "arrowBatchFromArrayStream", true)
}

/// Collect object rows (by field name) into record batches of `batch_size`.
pub fn arrow_batch_from_object_stream(
    input: DataStream<Value>,
    options: ArrowBatchOptions,
) -> Result<DataStream<RecordBatch>> {
    batch_stream(input, options, "arrowBatchFromObjectStream", false)
}

// *** RecordBatch -> rows *** //

fn cell(column: &ArrayRef, r: usize) -> Result<Value> {
    if column.is_null(r) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Boolean => Value::from(column.as_boolean().value(r)),
        DataType::Int8 => Value::from(column.as_primitive::<Int8Type>().value(r)),
        DataType::Int16 => Value::from(column.as_primitive::<Int16Type>().value(r)),
        DataType::Int32 => Value::from(column.as_primitive::<Int32Type>().value(r)),
        DataType::Int64 => Value::from(column.as_primitive::<Int64Type>().value(r)),
        DataType::UInt8 => Value::from(column.as_primitive::<UInt8Type>().value(r)),
        DataType::UInt16 => Value::from(column.as_primitive::<UInt16Type>().value(r)),
        DataType::UInt32 => Value::from(column.as_primitive::<UInt32Type>().value(r)),
        DataType::UInt64 => Value::from(column.as_primitive::<UInt64Type>().value(r)),
        DataType::Float32 => Value::from(column.as_primitive::<Float32Type>().value(r)),
        DataType::Float64 => Value::from(column.as_primitive::<Float64Type>().value(r)),
        DataType::Utf8 => Value::from(column.as_string::<i32>().value(r)),
        DataType::LargeUtf8 => Value::from(column.as_string::<i64>().value(r)),
        DataType::Date32 => {
            Value::from(i64::from(column.as_primitive::<Date32Type>().value(r)) * MS_PER_DAY)
        }
        DataType::Date64 => Value::from(column.as_primitive::<Date64Type>().value(r)),
        // Epoch milliseconds whatever the unit (sub-ms precision floors).
        DataType::Timestamp(TimeUnit::Second, _) => Value::from(
            column
                .as_primitive::<TimestampSecondType>()
                .value(r)
                .saturating_mul(1000),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Value::from(column.as_primitive::<TimestampMillisecondType>().value(r))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => Value::from(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(r)
                .div_euclid(1000),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Value::from(
            column
                .as_primitive::<TimestampNanosecondType>()
                .value(r)
                .div_euclid(1_000_000),
        ),
        other => return Err(format!("arrow: unsupported column type {other}").into()),
    })
}

/// Emit one array row per record batch row.
pub fn arrow_to_array_stream(input: DataStream<RecordBatch>) -> DataStream<Value> {
    create_transform_stream(
        input,
        |batch: RecordBatch, enqueue: &mut Vec<Value>| {
            for r in 0..batch.num_rows() {
                let row = batch
                    .columns()
                    .iter()
                    .map(|c| cell(c, r))
                    .collect::<Result<_>>()?;
                enqueue.push(Value::Array(row));
            }
            Ok(())
        },
        noop_flush,
    )
}

/// Emit one object row (keyed by field name) per record batch row.
pub fn arrow_to_object_stream(input: DataStream<RecordBatch>) -> DataStream<Value> {
    create_transform_stream(
        input,
        |batch: RecordBatch, enqueue: &mut Vec<Value>| {
            let schema = batch.schema();
            for r in 0..batch.num_rows() {
                let mut row = Map::new();
                for (field, column) in schema.fields().iter().zip(batch.columns()) {
                    row.insert(field.name().clone(), cell(column, r)?);
                }
                enqueue.push(Value::Object(row));
            }
            Ok(())
        },
        noop_flush,
    )
}

#[cfg(test)]
#[allow(clippy::approx_constant)] // 3.14 is test data, not PI.
mod tests {
    use super::*;
    type BatchFn = fn(DataStream<Value>, ArrowBatchOptions) -> Result<DataStream<RecordBatch>>;
    use datastream_core::{create_readable_stream, stream_to_array};

    fn users_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn opts(schema: impl Into<ArrowSchemaInput>, batch_size: usize) -> ArrowBatchOptions {
        ArrowBatchOptions {
            schema: Some(schema.into()),
            batch_size: Some(batch_size),
        }
    }

    async fn detect(rows: Vec<Value>, sample_size: usize) -> (Vec<Value>, ArrowDetectSchemaResult) {
        let options = ArrowDetectSchemaOptions {
            sample_size: Some(sample_size),
            ..Default::default()
        };
        let (stream, detected) = arrow_detect_schema_stream(create_readable_stream(rows), options);
        (stream_to_array(stream, None).await.unwrap(), detected)
    }

    fn types(detected: &ArrowDetectSchemaResult) -> Vec<DataType> {
        let schema = detected.schema().unwrap();
        schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect()
    }

    fn fields(detected: &ArrowDetectSchemaResult) -> Value {
        detected.result().get()["fields"].clone()
    }

    // rows -> detect -> batch (lazy schema) -> object rows
    async fn round_trip(rows: Vec<Value>, sample_size: usize) -> Vec<Value> {
        let options = ArrowDetectSchemaOptions {
            sample_size: Some(sample_size),
            ..Default::default()
        };
        let (stream, detected) = arrow_detect_schema_stream(create_readable_stream(rows), options);
        let batches =
            arrow_batch_from_object_stream(stream, opts(detected.lazy_schema(), 10)).unwrap();
        stream_to_array(arrow_to_object_stream(batches), None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn detect_infers_schema_from_object_rows() {
        let rows = vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
            json!({"id": 3, "name": "carol"}),
        ];
        let (output, detected) = detect(rows, 2).await;
        assert_eq!(output.len(), 3);
        assert_eq!(fields(&detected), json!(["id", "name"]));
        assert_eq!(types(&detected), [DataType::Int32, DataType::Utf8]);
        let schema = detected.result().get()["schema"].clone();
        assert_eq!(schema[0]["name"], "id");
        assert_eq!(schema[1]["nullable"], true);
    }

    #[tokio::test]
    async fn detect_names_array_columns() {
        let (_, detected) = detect(vec![json!([1, "alice"]), json!([2, "bob"])], 1).await;
        assert_eq!(fields(&detected), json!(["column0", "column1"]));
    }

    #[tokio::test]
    async fn detect_int32_boundaries() {
        let rows = vec![json!({
            "big": 3_000_000_000u64, "small": 5, "max": 2147483647, "over": 2147483648u64,
            "min": -2147483648i64, "under": -2147483649i64, "float": 3.14, "whole": 3.0
        })];
        let (_, detected) = detect(rows, 1).await;
        use DataType::{Float64, Int32};
        assert_eq!(
            types(&detected),
            [Float64, Int32, Int32, Float64, Int32, Float64, Float64, Int32]
        );
    }

    #[tokio::test]
    async fn detect_bool_and_null_handling() {
        let rows = vec![
            json!({"flag": false, "a": null, "b": "", "c": "", "d": null}),
            json!({"flag": true, "a": 42, "b": "hello", "c": 42, "d": null}),
        ];
        let (_, detected) = detect(rows, 10).await;
        use DataType::{Boolean, Int32, Utf8};
        // Null and "" are skipped; all-null falls back to Utf8.
        assert_eq!(types(&detected), [Boolean, Int32, Utf8, Int32, Utf8]);
        assert!(detected
            .schema()
            .unwrap()
            .fields()
            .iter()
            .all(|f| f.is_nullable()));
    }

    #[tokio::test]
    async fn detect_seals_on_end_for_short_streams() {
        let (output, detected) = detect(vec![json!({"id": 42, "name": "alice"})], 100).await;
        assert_eq!(output, [json!({"id": 42, "name": "alice"})]);
        assert_eq!(fields(&detected), json!(["id", "name"]));
    }

    #[tokio::test]
    async fn detect_seals_at_sample_size_ignoring_later_rows() {
        let (output, detected) = detect(vec![json!([1]), json!([2]), json!([3, 4, 5])], 2).await;
        assert_eq!(fields(&detected), json!(["column0"]));
        assert_eq!(output.len(), 3);
        assert_eq!(output[2], json!([3, 4, 5]));
        // Schema is frozen after the first sample_size rows.
        let rows = vec![
            json!({"x": 1}),
            json!({"x": 2}),
            json!({"x": 3.14}),
            json!({"x": 2.72}),
        ];
        let (output, detected) = detect(rows, 2).await;
        assert_eq!(output.len(), 4);
        assert_eq!(types(&detected), [DataType::Int32]);
    }

    #[tokio::test]
    async fn detect_unions_keys_and_max_width() {
        let (_, detected) =
            detect(vec![json!({"id": 1}), json!({"id": 2, "name": "bob"})], 10).await;
        assert_eq!(fields(&detected), json!(["id", "name"]));
        assert_eq!(types(&detected)[1], DataType::Utf8);
        let (_, detected) = detect(vec![json!([1]), json!([2, "bob", true])], 10).await;
        assert_eq!(fields(&detected), json!(["column0", "column1", "column2"]));
        let (_, detected) = detect(vec![json!([1, 2, 3]), json!([4])], 10).await;
        assert_eq!(fields(&detected), json!(["column0", "column1", "column2"]));
    }

    // Widening across all samples (order-independent); each column pins one rule.
    #[tokio::test]
    async fn detect_widens_mixed_types_across_samples() {
        let rows = vec![
            json!({"intStr": 42, "strInt": "hello", "floatInt": 2.7, "boolInt": true, "intInt": 1}),
            json!({"intStr": "hello", "strInt": 42, "floatInt": 1, "boolInt": 1, "intInt": null}),
            json!({"intStr": null, "strInt": null, "floatInt": null, "boolInt": null, "intInt": 2}),
        ];
        let (_, detected) = detect(rows, 10).await;
        use DataType::{Float64, Int32, Utf8};
        assert_eq!(types(&detected), [Utf8, Utf8, Float64, Utf8, Int32]);
        // A column that starts with 1 and later holds 2.7 keeps 2.7.
        let output = round_trip(vec![json!({"a": 1}), json!({"a": 2.7})], 10).await;
        let a: Vec<Option<f64>> = output.iter().map(|row| row["a"].as_f64()).collect();
        assert_eq!(a, [Some(1.0), Some(2.7)]);
    }

    #[tokio::test]
    async fn detect_empty_stream_and_result_key() {
        let (output, detected) = detect(vec![], 10).await;
        assert!(output.is_empty());
        assert!(detected.schema().is_none());
        assert_eq!(
            detected.result().get(),
            json!({"schema": null, "fields": null})
        );
        assert_eq!(detected.result().key(), "arrowDetectSchema");
        let options = ArrowDetectSchemaOptions {
            result_key: Some("myKey".into()),
            ..Default::default()
        };
        let (_, detected) =
            arrow_detect_schema_stream(create_readable_stream(Vec::<Value>::new()), options);
        assert_eq!(detected.result().key(), "myKey");
    }

    #[tokio::test]
    async fn batch_from_array_emits_per_batch_size() {
        let rows = vec![json!([1, "a"]), json!([2, "b"]), json!([3, "c"])];
        let stream =
            arrow_batch_from_array_stream(create_readable_stream(rows), opts(users_schema(), 2))
                .unwrap();
        let batches = stream_to_array(stream, None).await.unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        assert_eq!(batches[0].column(0).as_primitive::<Int32Type>().value(0), 1);
        assert_eq!(batches[0].column(1).as_string::<i32>().value(1), "b");
        // Exactly batch_size rows: no trailing empty batch.
        let rows = vec![json!([1, "a"]), json!([2, "b"])];
        let stream =
            arrow_batch_from_array_stream(create_readable_stream(rows), opts(users_schema(), 2))
                .unwrap();
        let batches = stream_to_array(stream, None).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn batch_from_array_resolves_lazy_schema() {
        let lazy = ArrowSchemaInput::Lazy(Arc::new(|| Some(users_schema())));
        let stream = arrow_batch_from_array_stream(
            create_readable_stream([json!([1, "a"])]),
            opts(lazy, 10),
        )
        .unwrap();
        let batches = stream_to_array(stream, None).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn batch_from_object_batches_rows() {
        let rows = vec![
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "b"}),
            json!({"id": 3, "name": "c"}),
        ];
        let stream = arrow_batch_from_object_stream(
            create_readable_stream(rows.clone()),
            opts(users_schema(), 10),
        )
        .unwrap();
        let batches = stream_to_array(stream, None).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].column(0).as_primitive::<Int32Type>().value(1), 2);
        assert_eq!(batches[0].column(1).as_string::<i32>().value(0), "a");
        let stream =
            arrow_batch_from_object_stream(create_readable_stream(rows), opts(users_schema(), 2))
                .unwrap();
        let batches = stream_to_array(stream, None).await.unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 1]
        );
    }

    #[tokio::test]
    async fn batch_rejects_bad_values() {
        let stream = arrow_batch_from_object_stream(
            create_readable_stream([json!({"id": "x"})]),
            opts(users_schema(), 10),
        )
        .unwrap();
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "arrowBatchFromObjectStream: row 0, field \"id\" expects Int32, got \"x\""
        );
    }

    // *** values validated against the sealed schema type *** //
    // A row that does not fit the schema inferred from the first sample_size
    // rows must error (naming stream, row and field), not be coerced.
    #[tokio::test]
    async fn detect_then_batch_rejects_later_int32_overflow() {
        let options = ArrowDetectSchemaOptions {
            sample_size: Some(2),
            ..Default::default()
        };
        let rows = vec![
            json!({"id": 1}),
            json!({"id": 2}),
            json!({"id": 3_000_000_000u64}),
        ];
        let (stream, detected) = arrow_detect_schema_stream(create_readable_stream(rows), options);
        let batches =
            arrow_batch_from_object_stream(stream, opts(detected.lazy_schema(), 10)).unwrap();
        let e = stream_to_array(batches, None).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "arrowBatchFromObjectStream: row 2, field \"id\" expects Int32, got 3000000000"
        );
    }

    #[tokio::test]
    async fn batch_from_array_names_stream_row_and_column_of_bad_value() {
        let schema = Schema::new(vec![Field::new("column0", DataType::Int32, true)]);
        let rows = vec![json!([1]), json!([2]), json!([2.7])];
        let stream =
            arrow_batch_from_array_stream(create_readable_stream(rows), opts(schema, 1)).unwrap();
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "arrowBatchFromArrayStream: row 2, field \"column0\" expects Int32, got 2.7"
        );
    }

    fn timestamp_ms() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }

    // One object row {v: value} through batch -> rows; Ok(v) or the error text.
    async fn batch_one(data_type: DataType, value: Option<Value>, nullable: bool) -> Result<Value> {
        let schema = Schema::new(vec![Field::new("v", data_type, nullable)]);
        let mut row = Map::new();
        if let Some(value) = value {
            row.insert("v".into(), value);
        }
        let batches = arrow_batch_from_object_stream(
            create_readable_stream([Value::Object(row)]),
            opts(schema, 10),
        )?;
        let rows = stream_to_array(arrow_to_object_stream(batches), None).await?;
        Ok(rows[0]["v"].clone())
    }

    const PREFIX: &str = "arrowBatchFromObjectStream: row 0, field \"v\" expects";

    #[tokio::test]
    async fn batch_accepts_values_that_fit_their_column_type() {
        let cases = [
            (DataType::Int8, json!(127), json!(127)),
            (DataType::Int8, json!(-128), json!(-128)),
            (DataType::UInt8, json!(0), json!(0)),
            (DataType::UInt8, json!(255), json!(255)),
            (DataType::Int32, json!(2_147_483_647), json!(2_147_483_647)),
            (
                DataType::Int32,
                json!(-2_147_483_648i64),
                json!(-2_147_483_648i64),
            ),
            (DataType::Int32, json!(3.0), json!(3)),
            (
                DataType::UInt32,
                json!(3_000_000_000u64),
                json!(3_000_000_000u64),
            ),
            (DataType::Int64, json!(5), json!(5)),
            (DataType::UInt64, json!(u64::MAX), json!(u64::MAX)),
            (DataType::Float32, json!(1.5), json!(1.5)),
            (DataType::Float64, json!(2.7), json!(2.7)),
            (DataType::Boolean, json!(false), json!(false)),
            (timestamp_ms(), json!(2_000), json!(2_000)),
            // Utf8 is unchecked: non-strings keep their JSON text.
            (DataType::Utf8, json!(5), json!("5")),
        ];
        for (data_type, value, expected) in cases {
            let label = format!("{data_type} {value}");
            assert_eq!(
                batch_one(data_type, Some(value), true).await.unwrap(),
                expected,
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn batch_rejects_values_that_do_not_fit_their_column_type() {
        let ts = timestamp_ms().to_string();
        let cases = [
            (DataType::Int8, json!(128), "Int8, got 128".to_string()),
            (DataType::Int8, json!(-129), "Int8, got -129".into()),
            (DataType::UInt8, json!(256), "UInt8, got 256".into()),
            (DataType::UInt8, json!(-1), "UInt8, got -1".into()),
            (DataType::Int64, json!(1.5), "Int64, got 1.5".into()),
            (DataType::Float32, json!("1"), "Float32, got \"1\"".into()),
            (
                DataType::Int32,
                json!(2_147_483_648u64),
                "Int32, got 2147483648".into(),
            ),
            (
                DataType::Int32,
                json!(-2_147_483_649i64),
                "Int32, got -2147483649".into(),
            ),
            (DataType::Int32, json!(1e300), "Int32, got 1e+300".into()),
            (DataType::Int32, json!("oops"), "Int32, got \"oops\"".into()),
            (DataType::Int32, json!(true), "Int32, got true".into()),
            (
                DataType::Int32,
                json!({}),
                "Int32, got [object Object]".into(),
            ),
            (
                DataType::Int32,
                json!([1]),
                "Int32, got [object Array]".into(),
            ),
            (DataType::Float64, json!("1"), "Float64, got \"1\"".into()),
            (DataType::Boolean, json!(1), "Boolean, got 1".into()),
            (timestamp_ms(), json!(1.5), format!("{ts}, got 1.5")),
            (timestamp_ms(), json!(1e300), format!("{ts}, got 1e+300")),
        ];
        for (data_type, value, message) in cases {
            let e = batch_one(data_type, Some(value), true).await.unwrap_err();
            assert_eq!(e.to_string(), format!("{PREFIX} {message}"));
        }
    }

    #[tokio::test]
    async fn batch_allows_nulls_only_in_nullable_fields() {
        for value in [Some(Value::Null), None, Some(json!(""))] {
            let v = batch_one(DataType::Int32, value, true).await.unwrap();
            assert_eq!(v, Value::Null);
        }
        assert_eq!(
            batch_one(DataType::Utf8, Some(json!("")), true)
                .await
                .unwrap(),
            ""
        );
        assert_eq!(
            batch_one(DataType::Utf8, Some(json!("")), false)
                .await
                .unwrap(),
            ""
        );
        assert_eq!(
            batch_one(DataType::Int32, Some(json!(1)), false)
                .await
                .unwrap(),
            1
        );
        let cases = [
            (
                DataType::Int32,
                Some(Value::Null),
                "non-null Int32, got null",
            ),
            (DataType::Int32, None, "non-null Int32, got undefined"),
            (DataType::Int32, Some(json!("")), "non-null Int32, got \"\""),
            (DataType::Utf8, Some(Value::Null), "non-null Utf8, got null"),
        ];
        for (data_type, value, message) in cases {
            let e = batch_one(data_type, value, false).await.unwrap_err();
            assert_eq!(e.to_string(), format!("{PREFIX} {message}"));
        }
    }

    #[test]
    fn batch_validates_schema_eagerly() {
        for (build, name) in [
            (
                arrow_batch_from_array_stream as BatchFn,
                "arrowBatchFromArrayStream",
            ),
            (
                arrow_batch_from_object_stream as BatchFn,
                "arrowBatchFromObjectStream",
            ),
        ] {
            let empty = || create_readable_stream(Vec::<Value>::new());
            let e = build(empty(), ArrowBatchOptions::default()).err().unwrap();
            assert_eq!(e.to_string(), format!("{name}: schema is required"));
            let e = build(empty(), opts(Schema::empty(), 10)).err().unwrap();
            assert_eq!(
                e.to_string(),
                format!("{name}: schema must have at least one field")
            );
        }
    }

    #[tokio::test]
    async fn batch_errors_for_missing_lazy_schema_on_empty_input() {
        for (build, name) in [
            (
                arrow_batch_from_array_stream as BatchFn,
                "arrowBatchFromArrayStream",
            ),
            (
                arrow_batch_from_object_stream as BatchFn,
                "arrowBatchFromObjectStream",
            ),
        ] {
            let lazy = ArrowSchemaInput::Lazy(Arc::new(|| None));
            let stream =
                build(create_readable_stream(Vec::<Value>::new()), opts(lazy, 10)).unwrap();
            let e = stream_to_array(stream, None).await.unwrap_err();
            assert_eq!(e.to_string(), format!("{name}: schema is required"));
        }
    }

    #[tokio::test]
    async fn to_array_emits_rows() {
        let rows = vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
        let batches =
            arrow_batch_from_object_stream(create_readable_stream(rows), opts(users_schema(), 10))
                .unwrap();
        let output = stream_to_array(arrow_to_array_stream(batches), None)
            .await
            .unwrap();
        assert_eq!(output, [json!([1, "a"]), json!([2, "b"])]);
    }

    #[tokio::test]
    async fn to_object_round_trips() {
        let rows = vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
            json!({"id": 3, "name": "carol"}),
        ];
        let batches = arrow_batch_from_object_stream(
            create_readable_stream(rows.clone()),
            opts(users_schema(), 2),
        )
        .unwrap();
        let output = stream_to_array(arrow_to_object_stream(batches), None)
            .await
            .unwrap();
        assert_eq!(output, rows);
    }

    #[tokio::test]
    async fn nulls_round_trip() {
        let rows = vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": null}),
        ];
        let batches = arrow_batch_from_object_stream(
            create_readable_stream(rows.clone()),
            opts(users_schema(), 10),
        )
        .unwrap();
        assert_eq!(
            stream_to_array(arrow_to_object_stream(batches), None)
                .await
                .unwrap(),
            rows
        );
        // A missing array cell becomes null.
        let rows = vec![json!([1, "alice"]), json!([2])];
        let batches =
            arrow_batch_from_array_stream(create_readable_stream(rows), opts(users_schema(), 10))
                .unwrap();
        let output = stream_to_array(arrow_to_array_stream(batches), None)
            .await
            .unwrap();
        assert_eq!(output[1], json!([2, null]));
    }

    #[tokio::test]
    async fn detect_to_batch_end_to_end() {
        let rows = vec![
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "b"}),
            json!({"id": 3, "name": "c"}),
        ];
        assert_eq!(round_trip(rows.clone(), 2).await, rows);
        // A large integer is widened, not wrapped.
        let output = round_trip(vec![json!({"id": 3_000_000_000u64, "name": "alice"})], 1).await;
        assert_eq!(output[0]["id"].as_f64(), Some(3e9));
        // A column present only in a later row survives.
        let output = round_trip(vec![json!({"id": 1}), json!({"id": 2, "name": "bob"})], 10).await;
        assert_eq!(output[1]["name"], "bob");
        assert_eq!(output[0]["name"], Value::Null);
    }

    #[tokio::test]
    async fn timestamp_columns_round_trip_epoch_ms() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let rows = vec![json!({"at": 1_590_969_600_000i64}), json!({"at": null})];
        let batches =
            arrow_batch_from_object_stream(create_readable_stream(rows.clone()), opts(schema, 10))
                .unwrap();
        assert_eq!(
            stream_to_array(arrow_to_object_stream(batches), None)
                .await
                .unwrap(),
            rows
        );
    }

    // Dates and timestamps read and write epoch milliseconds in every unit.
    #[tokio::test]
    async fn date_and_timestamp_units_round_trip_epoch_ms() {
        let tz = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let cases = [
            (DataType::Date32, -43_200_000, -86_400_000),
            (DataType::Date64, 1_000, 1_000),
            (DataType::Timestamp(TimeUnit::Second, None), 1_999, 1_000),
            (DataType::Timestamp(TimeUnit::Microsecond, None), -1, -1),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                1_500,
                1_500,
            ),
            (tz, 7, 7),
        ];
        for (data_type, value, expected) in cases {
            let label = format!("{data_type} {value}");
            let v = batch_one(data_type, Some(json!(value)), true).await;
            assert_eq!(v.unwrap(), json!(expected), "{label}");
        }
        // Past the nanosecond range: rejected, not wrapped.
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let e = batch_one(ns.clone(), Some(json!(i64::MAX)), true)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), format!("{PREFIX} {ns}, got {}", i64::MAX));
    }

    #[tokio::test]
    async fn large_utf8_columns_keep_empty_strings() {
        for (value, expected) in [(json!(""), json!("")), (json!(5), json!("5"))] {
            let v = batch_one(DataType::LargeUtf8, Some(value), false).await;
            assert_eq!(v.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn batch_rejects_unsupported_column_types() {
        let e = batch_one(DataType::Binary, Some(json!("x")), true)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "arrow: unsupported column type Binary");
    }

    #[tokio::test]
    async fn bool_and_float_columns_round_trip() {
        let rows = vec![
            json!({"flag": true, "x": 1.5}),
            json!({"flag": false, "x": 2.0}),
        ];
        let (_, detected) = detect(rows.clone(), 10).await;
        let schema = detected.schema().unwrap();
        let batches =
            arrow_batch_from_object_stream(create_readable_stream(rows.clone()), opts(schema, 10))
                .unwrap();
        assert_eq!(
            stream_to_array(arrow_to_object_stream(batches), None)
                .await
                .unwrap(),
            rows
        );
    }
}
