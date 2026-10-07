// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Apache Arrow record batch transform streams, ported from `@datastream/arrow`.
//!
//! Rows are `serde_json::Value` objects or arrays. Supported column types are
//! Boolean, Int32, Float64, Utf8 and Timestamp(Millisecond) (epoch-ms numbers;
//! JSON has no `Date`). Reading also accepts the other integer, float and
//! large-string types.

use std::sync::{Arc, Mutex};

pub use arrow_array;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMillisecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
pub use arrow_array::RecordBatch;
use arrow_array::{Array, ArrayRef};
pub use arrow_schema;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_stream::try_stream;
use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Error, Map, Result, StreamExt, StreamResult,
    Value,
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
                    .find_map(|s| {
                        infer_type(if is_array {
                            s.get(i)
                        } else {
                            s.get(name.as_str())
                        })
                    })
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

fn mismatch(value: &Value) -> Error {
    format!("arrow: cannot append {value} to column").into()
}

// JS numbers like 3.0 are integers too.
fn as_int(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|f| f.fract() == 0.0)
            .map(|f| f as i64)
    })
}

enum Column {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Timestamp(TimestampMillisecondBuilder),
}

impl Column {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean(BooleanBuilder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::Timestamp(
                TimestampMillisecondBuilder::new().with_data_type(data_type.clone()),
            ),
            other => return Err(format!("arrow: unsupported column type {other}").into()),
        })
    }

    fn append(&mut self, value: Option<&Value>) -> Result<()> {
        let Some(value) = value.filter(|v| !v.is_null()) else {
            match self {
                Self::Boolean(b) => b.append_null(),
                Self::Int32(b) => b.append_null(),
                Self::Float64(b) => b.append_null(),
                Self::Utf8(b) => b.append_null(),
                Self::Timestamp(b) => b.append_null(),
            }
            return Ok(());
        };
        match self {
            Self::Boolean(b) => b.append_value(value.as_bool().ok_or_else(|| mismatch(value))?),
            Self::Int32(b) => {
                let int = as_int(value).and_then(|v| i32::try_from(v).ok());
                b.append_value(int.ok_or_else(|| mismatch(value))?);
            }
            Self::Float64(b) => b.append_value(value.as_f64().ok_or_else(|| mismatch(value))?),
            Self::Utf8(b) => match value {
                Value::String(s) => b.append_value(s),
                other => b.append_value(other.to_string()),
            },
            Self::Timestamp(b) => b.append_value(as_int(value).ok_or_else(|| mismatch(value))?),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Timestamp(b) => Arc::new(b.finish()),
        }
    }
}

struct Batcher {
    schema: SchemaRef,
    columns: Vec<Column>,
    rows: usize,
}

impl Batcher {
    fn new(schema: SchemaRef) -> Result<Self> {
        let columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|f| Column::new(f.data_type()))
            .collect::<Result<_>>()?;
        Ok(Self {
            schema,
            columns,
            rows: 0,
        })
    }

    fn push(&mut self, row: &Value, is_array: bool) -> Result<()> {
        let fields = self.schema.fields();
        for (i, (field, column)) in fields.iter().zip(&mut self.columns).enumerate() {
            column.append(if is_array {
                row.get(i)
            } else {
                row.get(field.name().as_str())
            })?;
        }
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
                batcher = Some(Batcher::new(resolve_schema(name, schema.as_ref())?)?);
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
            None => Batcher::new(resolve_schema(name, schema.as_ref())?)?,
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
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Value::from(column.as_primitive::<TimestampMillisecondType>().value(r))
        }
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
            json!({"a": "later", "c": "later"}),
        ];
        let (_, detected) = detect(rows, 10).await;
        use DataType::{Boolean, Int32, Utf8};
        // First non-empty value wins; all-null falls back to Utf8.
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
        assert_eq!(e.to_string(), "arrow: cannot append \"x\" to column");
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
