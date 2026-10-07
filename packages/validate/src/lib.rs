// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! JSON Schema validation stream, ported from `@datastream/validate`.
//!
//! The JS version uses Ajv. Validation here is done by the `jsonschema`
//! crate (draft-07, Ajv's default), with the Ajv behaviour the JS relies on
//! layered on top: `coerceTypes`, `useDefaults: "empty"`, `strict` unknown
//! keywords, Ajv-style messages / `schemaPath`s, and `ajv-errors`'
//! `errorMessage` keyword (string form, keyword form, and `properties`).

use std::fmt;
use std::sync::Arc;

use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Map, Result, StreamResult, Value,
};
use jsonschema::error::ValidationErrorKind;
use serde_json::json;

/// Ajv options. Each defaults to `true` like the JS `ajvDefaults`
/// (`use_defaults` means Ajv's `"empty"` mode).
#[derive(Default, Clone, Debug)]
pub struct TranspileOptions {
    pub strict: Option<bool>,
    pub coerce_types: Option<bool>,
    pub all_errors: Option<bool>,
    pub use_defaults: Option<bool>,
    pub messages: Option<bool>,
}

/// A compiled schema; build once with [`transpile_schema`] and reuse.
#[derive(Clone)]
pub struct CompiledSchema {
    schema: Arc<Value>,
    validator: Arc<jsonschema::Validator>,
    /// `(schema pointer, errorMessage)` for every subschema that has one.
    error_messages: Arc<Vec<(String, Value)>>,
    coerce_types: bool,
    all_errors: bool,
    use_defaults: bool,
    messages: bool,
}

impl fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledSchema")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// A JSON Schema, or one already compiled with [`transpile_schema`].
#[derive(Clone, Debug)]
pub enum Schema {
    Json(Value),
    Compiled(CompiledSchema),
}

impl From<Value> for Schema {
    fn from(schema: Value) -> Self {
        Schema::Json(schema)
    }
}

impl From<CompiledSchema> for Schema {
    fn from(schema: CompiledSchema) -> Self {
        Schema::Compiled(schema)
    }
}

const KEYWORDS: &[&str] = &[
    "$schema",
    "$id",
    "$ref",
    "$comment",
    "$defs",
    "definitions",
    "title",
    "description",
    "default",
    "examples",
    "readOnly",
    "writeOnly",
    "type",
    "enum",
    "const",
    "multipleOf",
    "maximum",
    "exclusiveMaximum",
    "minimum",
    "exclusiveMinimum",
    "maxLength",
    "minLength",
    "pattern",
    "format",
    "contentMediaType",
    "contentEncoding",
    "items",
    "additionalItems",
    "maxItems",
    "minItems",
    "uniqueItems",
    "contains",
    "maxProperties",
    "minProperties",
    "required",
    "properties",
    "patternProperties",
    "additionalProperties",
    "dependencies",
    "propertyNames",
    "if",
    "then",
    "else",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "errorMessage",
];

fn escape(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// Call `f` with every (sub)schema object and its JSON pointer.
fn walk(
    schema: &Value,
    path: &str,
    f: &mut impl FnMut(&str, &Map<String, Value>) -> Result<()>,
) -> Result<()> {
    let Value::Object(map) = schema else {
        return Ok(());
    };
    f(path, map)?;
    for (key, value) in map {
        let path = format!("{path}/{}", escape(key));
        match (key.as_str(), value) {
            (
                "properties" | "patternProperties" | "definitions" | "$defs" | "dependencies",
                Value::Object(m),
            ) => {
                for (name, sub) in m {
                    walk(sub, &format!("{path}/{}", escape(name)), f)?;
                }
            }
            ("items" | "allOf" | "anyOf" | "oneOf", Value::Array(subs)) => {
                for (i, sub) in subs.iter().enumerate() {
                    walk(sub, &format!("{path}/{i}"), f)?;
                }
            }
            (
                "items"
                | "additionalItems"
                | "contains"
                | "additionalProperties"
                | "propertyNames"
                | "if"
                | "then"
                | "else"
                | "not",
                sub,
            ) => walk(sub, &path, f)?,
            _ => {}
        }
    }
    Ok(())
}

/// Compile a schema with Ajv-like defaults. Strict mode rejects unknown keywords.
pub fn transpile_schema(schema: &Value, options: TranspileOptions) -> Result<CompiledSchema> {
    let strict = options.strict.unwrap_or(true);
    let mut error_messages = Vec::new();
    walk(schema, "", &mut |path, map| {
        if strict {
            if let Some(key) = map.keys().find(|k| !KEYWORDS.contains(&k.as_str())) {
                return Err(format!("strict mode: unknown keyword: \"{key}\"").into());
            }
        }
        if let Some(message) = map.get("errorMessage") {
            error_messages.push((path.to_string(), message.clone()));
        }
        Ok(())
    })?;
    let validator = jsonschema::draft7::new(schema).map_err(|e| e.to_string())?;
    Ok(CompiledSchema {
        schema: Arc::new(schema.clone()),
        validator: Arc::new(validator),
        error_messages: Arc::new(error_messages),
        coerce_types: options.coerce_types.unwrap_or(true),
        all_errors: options.all_errors.unwrap_or(true),
        use_defaults: options.use_defaults.unwrap_or(true),
        messages: options.messages.unwrap_or(true),
    })
}

/// An error in Ajv's shape.
#[derive(Debug)]
struct AjvError {
    keyword: String,
    schema_path: String,
    instance_path: String,
    message: String,
    missing_property: Option<String>,
    additional_property: Option<String>,
    /// Errors replaced by an `errorMessage` error.
    errors: Vec<AjvError>,
}

impl AjvError {
    fn new(keyword: &str, schema_path: &str, instance_path: &str, message: String) -> Self {
        AjvError {
            keyword: keyword.into(),
            schema_path: schema_path.into(),
            instance_path: instance_path.into(),
            message,
            missing_property: None,
            additional_property: None,
            errors: Vec::new(),
        }
    }
}

fn matches_type(t: &str, data: &Value) -> bool {
    match t {
        "string" => data.is_string(),
        "number" => data.is_number(),
        "integer" => data.as_f64().is_some_and(|n| n.fract() == 0.0),
        "boolean" => data.is_boolean(),
        "null" => data.is_null(),
        "object" => data.is_object(),
        "array" => data.is_array(),
        _ => false,
    }
}

fn number(n: f64) -> Value {
    // Keep integral values as integers so `"1"` coerces to `1`, not `1.0`.
    if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 {
        json!(n as i64)
    } else {
        json!(n)
    }
}

/// Ajv's `coerceTypes: true` scalar rules.
fn coerce_to(t: &str, data: &Value) -> Option<Value> {
    match (t, data) {
        ("string", Value::Number(n)) => Some(Value::String(n.to_string())),
        ("string", Value::Bool(b)) => Some(Value::String(b.to_string())),
        ("string", Value::Null) => Some(Value::String(String::new())),
        ("number" | "integer", Value::Bool(b)) => Some(json!(u8::from(*b))),
        ("number" | "integer", Value::Null) => Some(json!(0)),
        ("number" | "integer", Value::String(s)) => {
            let n = s.trim().parse::<f64>().ok().filter(|n| n.is_finite())?;
            (t == "number" || n.fract() == 0.0).then(|| number(n))
        }
        ("boolean", Value::String(s)) if s == "true" || s == "false" => {
            Some(Value::Bool(s == "true"))
        }
        ("boolean", Value::Number(n)) if matches!(n.as_f64(), Some(v) if v == 0.0 || v == 1.0) => {
            Some(Value::Bool(n.as_f64() == Some(1.0)))
        }
        ("boolean", Value::Null) => Some(Value::Bool(false)),
        ("null", Value::String(s)) if s.is_empty() => Some(Value::Null),
        ("null", Value::Number(n)) if n.as_f64() == Some(0.0) => Some(Value::Null),
        ("null", Value::Bool(false)) => Some(Value::Null),
        _ => None,
    }
}

fn ajv_message(keyword: &str, value: Option<&Value>) -> Option<String> {
    let v = value?;
    Some(match keyword {
        "type" => {
            let types = match v {
                Value::Array(a) => a
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
                v => v.as_str()?.to_string(),
            };
            format!("must be {types}")
        }
        "minimum" => format!("must be >= {v}"),
        "maximum" => format!("must be <= {v}"),
        "exclusiveMinimum" => format!("must be > {v}"),
        "exclusiveMaximum" => format!("must be < {v}"),
        "minLength" => format!("must NOT have fewer than {v} characters"),
        "maxLength" => format!("must NOT have more than {v} characters"),
        "minItems" => format!("must NOT have fewer than {v} items"),
        "maxItems" => format!("must NOT have more than {v} items"),
        "minProperties" => format!("must NOT have fewer than {v} properties"),
        "maxProperties" => format!("must NOT have more than {v} properties"),
        "multipleOf" => format!("must be multiple of {v}"),
        "pattern" => format!("must match pattern \"{}\"", v.as_str()?),
        "format" => format!("must match format \"{}\"", v.as_str()?),
        "enum" => "must be equal to one of the allowed values".into(),
        "const" => "must be equal to constant".into(),
        "anyOf" => "must match a schema in anyOf".into(),
        "oneOf" => "must match exactly one schema in oneOf".into(),
        "not" => "must NOT be valid".into(),
        _ => return None,
    })
}

/// Replace the errors matching `matches` with one `errorMessage` error.
fn group(
    errors: Vec<AjvError>,
    matches: impl Fn(&AjvError) -> bool,
    message: &str,
    schema_path: &str,
) -> Vec<AjvError> {
    let Some(first) = errors.iter().position(&matches) else {
        return errors;
    };
    let (matched, mut rest): (Vec<_>, Vec<_>) = errors.into_iter().partition(&matches);
    let instance_path = matched
        .iter()
        .map(|e| e.instance_path.as_str())
        .min_by_key(|p| p.len())
        .unwrap_or_default()
        .to_string();
    let mut error = AjvError::new("errorMessage", schema_path, &instance_path, message.into());
    error.errors = matched;
    rest.insert(first, error);
    rest
}

impl CompiledSchema {
    /// Coercion and defaults, applied before validation like Ajv does.
    // ponytail: only follows `properties` and `items`; Ajv also coerces
    // inside allOf/anyOf/oneOf/$ref branches.
    fn prepare(&self, schema: &Value, data: &mut Value) {
        let Value::Object(s) = schema else {
            return;
        };
        if self.coerce_types {
            let types: Vec<&str> = match s.get("type") {
                Some(Value::String(t)) => vec![t.as_str()],
                Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
                _ => Vec::new(),
            };
            if !types.iter().any(|t| matches_type(t, data)) {
                if let Some(value) = types.iter().find_map(|t| coerce_to(t, data)) {
                    *data = value;
                }
            }
        }
        match data {
            Value::Object(obj) => {
                let Some(Value::Object(props)) = s.get("properties") else {
                    return;
                };
                for (name, sub) in props {
                    if let (true, Some(default)) = (self.use_defaults, sub.get("default")) {
                        let empty = match obj.get(name) {
                            None | Some(Value::Null) => true,
                            Some(Value::String(v)) => v.is_empty(),
                            _ => false,
                        };
                        if empty {
                            obj.insert(name.clone(), default.clone());
                        }
                    }
                    if let Some(value) = obj.get_mut(name) {
                        self.prepare(sub, value);
                    }
                }
            }
            Value::Array(items) => match s.get("items") {
                Some(Value::Array(subs)) => {
                    for (sub, value) in subs.iter().zip(items.iter_mut()) {
                        self.prepare(sub, value);
                    }
                }
                Some(sub) => {
                    for value in items.iter_mut() {
                        self.prepare(sub, value);
                    }
                }
                None => {}
            },
            _ => {}
        }
    }

    /// Coerce `data` in place and return its errors (empty when valid).
    fn errors(&self, data: &mut Value) -> Vec<AjvError> {
        self.prepare(&self.schema, data);
        let mut errors = Vec::new();
        for e in self.validator.iter_errors(data) {
            let schema_path = format!("#{}", e.schema_path().as_str());
            let instance_path = e.instance_path().as_str();
            let keyword = e
                .schema_path()
                .as_str()
                .rsplit('/')
                .next()
                .unwrap_or_default();
            let message = |m: String| if self.messages { m } else { String::new() };
            match e.kind() {
                ValidationErrorKind::Required { property, .. } => {
                    let property = property
                        .as_str()
                        .map(String::from)
                        .unwrap_or_else(|| property.to_string());
                    let text = message(format!("must have required property '{property}'"));
                    let mut error = AjvError::new("required", &schema_path, instance_path, text);
                    error.missing_property = Some(property);
                    errors.push(error);
                }
                ValidationErrorKind::AdditionalProperties { unexpected, .. } => {
                    // Ajv reports each additional property separately.
                    for property in unexpected {
                        let text = message("must NOT have additional properties".into());
                        let mut error = AjvError::new(
                            "additionalProperties",
                            &schema_path,
                            instance_path,
                            text,
                        );
                        error.additional_property = Some(property.clone());
                        errors.push(error);
                    }
                }
                _ => {
                    let text = ajv_message(keyword, self.schema.pointer(e.schema_path().as_str()))
                        .unwrap_or_else(|| e.to_string());
                    errors.push(AjvError::new(
                        keyword,
                        &schema_path,
                        instance_path,
                        message(text),
                    ));
                }
            }
        }
        if !self.all_errors {
            errors.truncate(1);
        }
        // ajv-errors: innermost schemas first.
        // ponytail: `items` and `_` forms of errorMessage are not supported.
        for (path, error_message) in self.error_messages.iter().rev() {
            let target = format!("#{path}/errorMessage");
            match error_message {
                Value::String(msg) => {
                    let prefix = format!("#{path}/");
                    errors = group(errors, |e| e.schema_path.starts_with(&prefix), msg, &target);
                }
                Value::Object(map) => {
                    for (key, msg) in map {
                        match (key.as_str(), msg) {
                            ("properties", Value::Object(props)) => {
                                for (name, msg) in
                                    props.iter().filter_map(|(n, m)| Some((n, m.as_str()?)))
                                {
                                    let prefix = format!("#{path}/properties/{}/", escape(name));
                                    errors = group(
                                        errors,
                                        |e| e.schema_path.starts_with(&prefix),
                                        msg,
                                        &target,
                                    );
                                }
                            }
                            (keyword, Value::String(msg)) => {
                                let at = format!("#{path}/{}", escape(keyword));
                                errors = group(errors, |e| e.schema_path == at, msg, &target);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        errors
    }
}

fn make_key(error: &AjvError) -> String {
    error
        .missing_property
        .clone()
        .or_else(|| error.additional_property.clone())
        .unwrap_or_else(|| error.instance_path.replacen('/', "", 1))
}

/// The result entry id and keys for an error, as in the JS `processError`.
fn process_error(error: &AjvError) -> (String, Vec<String>) {
    let mut keys: Vec<String> = if error.keyword == "errorMessage" {
        let mut keys: Vec<String> = error.errors.iter().map(make_key).collect();
        keys.sort();
        keys.dedup();
        keys
    } else {
        vec![make_key(error)]
    };
    keys.retain(|k| !k.is_empty());
    let mut id = error.schema_path.clone();
    if error.instance_path.is_empty() && !keys.is_empty() {
        id = format!("{id}/{}", keys.join("|"));
    }
    (id, keys)
}

#[derive(Default, Clone, Debug)]
pub struct ValidateOptions {
    pub schema: Option<Schema>,
    pub idx_start: Option<i64>,
    /// Also emit rows that failed validation.
    pub on_error_enqueue: Option<bool>,
    /// `Some(false)` emits the original rows instead of the coerced ones.
    pub allow_coerce_types: Option<bool>,
    pub result_key: Option<String>,
    pub max_error_rows: Option<usize>,
    pub max_error_keys: Option<usize>,
}

/// Validate each row, emitting valid (coerced) rows. Errors are collected in
/// the result (default key `validate`) as `{id: {id, keys, message, idx}}`.
pub fn validate_stream(
    input: DataStream<Value>,
    options: ValidateOptions,
) -> Result<(DataStream<Value>, StreamResult)> {
    let schema =
        match options.schema {
            None => return Err(
                "validateStream requires a schema (JSON Schema object or compiled AJV function)"
                    .into(),
            ),
            Some(Schema::Json(json)) => transpile_schema(&json, TranspileOptions::default())?,
            Some(Schema::Compiled(compiled)) => compiled,
        };
    let max_error_rows = options.max_error_rows.unwrap_or(usize::MAX);
    let max_error_keys = options.max_error_keys.unwrap_or(1000);
    let on_error_enqueue = options.on_error_enqueue.unwrap_or(false);
    let emit_original = options.allow_coerce_types == Some(false);
    let key = options.result_key.unwrap_or_else(|| "validate".into());
    let result = StreamResult::new(key, json!({}));
    let value = result.clone();
    let mut idx = options.idx_start.unwrap_or(0) - 1;
    let stream = create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            idx += 1;
            let (mut target, original) = if emit_original {
                (chunk.clone(), Some(chunk))
            } else {
                (chunk, None)
            };
            let errors = schema.errors(&mut target);
            if !errors.is_empty() {
                value.update(|value| {
                    let Some(map) = value.as_object_mut() else {
                        return;
                    };
                    for error in &errors {
                        let (id, keys) = process_error(error);
                        if !map.contains_key(&id) {
                            // Stop creating new entries once maxErrorKeys is reached.
                            if map.len() >= max_error_keys {
                                continue;
                            }
                            let entry = json!({ "id": id, "keys": keys, "message": error.message, "idx": [] });
                            map.insert(id.clone(), entry);
                        }
                        if let Some(list) = map.get_mut(&id).and_then(|e| e["idx"].as_array_mut()) {
                            if list.len() < max_error_rows {
                                list.push(idx.into());
                            }
                        }
                    }
                });
            }
            if errors.is_empty() || on_error_enqueue {
                enqueue.push(original.unwrap_or(target));
            }
            Ok(())
        },
        noop_flush,
    );
    Ok((stream, result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array};

    fn number_schema() -> Value {
        json!({
            "type": "object",
            "properties": { "a": { "type": "number" } },
            "required": ["a"]
        })
    }

    fn options(schema: Value) -> ValidateOptions {
        ValidateOptions {
            schema: Some(schema.into()),
            ..Default::default()
        }
    }

    async fn output(input: Vec<Value>, options: ValidateOptions) -> Vec<Value> {
        let (stream, _) = validate_stream(create_readable_stream(input), options).unwrap();
        stream_to_array(stream, None).await.unwrap()
    }

    async fn errors(input: Vec<Value>, options: ValidateOptions) -> Value {
        let (stream, result) = validate_stream(create_readable_stream(input), options).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        output[result.key()].clone()
    }

    fn first(errors: &Value) -> &Value {
        errors.as_object().unwrap().values().next().unwrap()
    }

    #[tokio::test]
    async fn coerces_types() {
        let input = vec![json!({"a": "1"}), json!({"a": "2"}), json!({"a": "3"})];
        let out = output(input, options(number_schema())).await;
        assert_eq!(out, [json!({"a": 1}), json!({"a": 2}), json!({"a": 3})]);
    }

    #[tokio::test]
    async fn accepts_compiled_schema() {
        let schema =
            json!({"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]});
        let compiled = transpile_schema(&schema, Default::default()).unwrap();
        let input = vec![json!({"a": "1"}), json!({"a": "2"})];
        let out = output(
            input.clone(),
            ValidateOptions {
                schema: Some(compiled.into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn collects_errors() {
        let input = vec![json!({"a": "1"}), json!({"a": "a"}), json!({"a": "3"})];
        let expected = json!({
            "#/properties/a/type": { "id": "#/properties/a/type", "idx": [1], "keys": ["a"], "message": "must be number" }
        });
        assert_eq!(
            errors(input.clone(), options(number_schema())).await,
            expected
        );
        // Invalid rows are dropped unless onErrorEnqueue.
        assert_eq!(
            output(input.clone(), options(number_schema())).await.len(),
            2
        );
        let opts = ValidateOptions {
            on_error_enqueue: Some(true),
            ..options(number_schema())
        };
        assert_eq!(
            output(input, opts).await,
            [json!({"a": 1}), json!({"a": "a"}), json!({"a": 3})]
        );
    }

    #[tokio::test]
    async fn allow_coerce_types_false_emits_originals() {
        let input = vec![json!({"a": "1"}), json!({"a": "2"})];
        let opts = ValidateOptions {
            allow_coerce_types: Some(false),
            ..options(number_schema())
        };
        assert_eq!(output(input.clone(), opts).await, input);
    }

    #[tokio::test]
    async fn idx_start_and_accumulation() {
        let input = vec![json!({"a": "1"}), json!({"a": "a"}), json!({"a": "3"})];
        let opts = ValidateOptions {
            idx_start: Some(10),
            ..options(number_schema())
        };
        assert_eq!(
            errors(input, opts).await["#/properties/a/type"]["idx"],
            json!([11])
        );
        let input = vec![json!({"a": "bad"}); 3];
        assert_eq!(
            first(&errors(input, options(number_schema())).await)["idx"],
            json!([0, 1, 2])
        );
    }

    #[tokio::test]
    async fn required_and_additional_properties() {
        let result = errors(vec![json!({"b": 1})], options(number_schema())).await;
        assert_eq!(result["#/required/a"]["keys"], json!(["a"]));
        assert_eq!(
            result["#/required/a"]["message"],
            "must have required property 'a'"
        );
        let schema = json!({"type": "object", "properties": {"a": {"type": "number"}}, "additionalProperties": false});
        let result = errors(vec![json!({"a": 1, "extra": 2})], options(schema)).await;
        assert_eq!(
            result["#/additionalProperties/extra"]["keys"],
            json!(["extra"])
        );
        assert_eq!(
            result["#/additionalProperties/extra"]["message"],
            "must NOT have additional properties"
        );
    }

    #[tokio::test]
    async fn result_key() {
        let opts = ValidateOptions {
            result_key: Some("custom".into()),
            ..options(number_schema())
        };
        let (stream, result) =
            validate_stream(create_readable_stream(vec![json!({"a": 1})]), opts).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(output["custom"], json!({}));
    }

    #[tokio::test]
    async fn nested_and_root_errors() {
        let schema = json!({
            "type": "object",
            "properties": { "nested": { "type": "object", "properties": { "value": { "type": "number" } } } }
        });
        let result = errors(vec![json!({"nested": {"value": "wrong"}})], options(schema)).await;
        let entry = &result["#/properties/nested/properties/value/type"];
        assert_eq!(entry["keys"], json!(["nested/value"]));
        // Root-level errors have no keys and no trailing `/` in the id.
        let result = errors(
            vec![json!("not-an-object")],
            options(json!({"type": "object"})),
        )
        .await;
        assert_eq!(
            result,
            json!({"#/type": {"id": "#/type", "keys": [], "message": "must be object", "idx": [0]}})
        );
    }

    #[tokio::test]
    async fn limit_messages() {
        let schema =
            json!({"type": "object", "properties": {"a": {"type": "integer", "maximum": 0}}});
        let result = errors(vec![json!({"a": 1})], options(schema)).await;
        assert_eq!(result["#/properties/a/maximum"]["message"], "must be <= 0");
    }

    #[tokio::test]
    async fn error_message_properties() {
        let schema = json!({
            "type": "object",
            "properties": { "a": { "type": "number" } },
            "required": ["a"],
            "errorMessage": { "properties": { "a": "Property a must be a number" } }
        });
        let result = errors(vec![json!({"a": "string"})], options(schema)).await;
        assert_eq!(
            result["#/errorMessage"]["message"],
            "Property a must be a number"
        );
        assert_eq!(result["#/errorMessage"]["keys"], json!(["a"]));
    }

    #[tokio::test]
    async fn error_message_required_sorted() {
        for required in [json!(["z_field", "a_field"]), json!(["a_field", "z_field"])] {
            let schema = json!({
                "type": "object",
                "properties": { "z_field": { "type": "string" }, "a_field": { "type": "string" } },
                "required": required,
                "errorMessage": { "required": "Both fields are required" }
            });
            let result = errors(vec![json!({})], options(schema)).await;
            let entry = &result["#/errorMessage/a_field|z_field"];
            assert_eq!(entry["keys"], json!(["a_field", "z_field"]));
            assert_eq!(entry["message"], "Both fields are required");
        }
    }

    #[tokio::test]
    async fn error_message_type_and_string() {
        let schema = json!({"type": "object", "errorMessage": {"type": "Must be an object"}});
        let result = errors(vec![json!("not-an-object")], options(schema)).await;
        assert_eq!(result["#/errorMessage"]["message"], "Must be an object");
        assert_eq!(result["#/errorMessage"]["keys"], json!([]));
        let schema = json!({"type": "object", "errorMessage": "Must be an object"});
        let result = errors(vec![json!("not-an-object")], options(schema)).await;
        assert_eq!(result["#/errorMessage"]["message"], "Must be an object");
    }

    #[tokio::test]
    async fn messages_false() {
        let schema = json!({"type": "object", "properties": {"a": {"type": "number"}}});
        let options = TranspileOptions {
            messages: Some(false),
            ..Default::default()
        };
        let compiled = transpile_schema(&schema, options).unwrap();
        let opts = ValidateOptions {
            schema: Some(compiled.into()),
            ..Default::default()
        };
        let result = errors(vec![json!({"a": "not-a-number"})], opts).await;
        assert_eq!(first(&result)["message"], "");
    }

    #[tokio::test]
    async fn max_error_rows_and_keys() {
        let schema = json!({"type": "object", "properties": {"a": {"type": "number"}}});
        let input = (0..100)
            .map(|i| json!({ "a": format!("bad{i}") }))
            .collect::<Vec<_>>();
        let opts = ValidateOptions {
            max_error_rows: Some(10),
            ..options(schema)
        };
        assert_eq!(
            first(&errors(input, opts).await)["idx"]
                .as_array()
                .unwrap()
                .len(),
            10
        );

        let schema = json!({"type": "object", "additionalProperties": false, "properties": {"a": {"type": "number"}}});
        let rows = |n: usize| {
            (0..n)
                .map(|i| json!({ "a": 1, format!("extra_{i}"): i }))
                .collect::<Vec<_>>()
        };
        let opts = ValidateOptions {
            max_error_keys: Some(50),
            ..options(schema.clone())
        };
        assert_eq!(errors(rows(500), opts).await.as_object().unwrap().len(), 50);
        assert_eq!(
            errors(rows(1500), options(schema))
                .await
                .as_object()
                .unwrap()
                .len(),
            1000
        );
    }

    #[tokio::test]
    async fn use_defaults_empty() {
        let schema = json!({"type": "object", "properties": {"name": {"type": "string", "default": "fallback"}}});
        let input = vec![
            json!({"name": ""}),
            json!({}),
            json!({"name": null}),
            json!({"name": "x"}),
        ];
        let out = output(input, options(schema)).await;
        let names: Vec<&Value> = out.iter().map(|row| &row["name"]).collect();
        assert_eq!(
            names,
            [
                &json!("fallback"),
                &json!("fallback"),
                &json!("fallback"),
                &json!("x")
            ]
        );
    }

    #[test]
    fn coercion_rules() {
        assert_eq!(coerce_to("number", &json!("1.5")), Some(json!(1.5)));
        assert_eq!(coerce_to("number", &json!("abc")), None);
        assert_eq!(coerce_to("number", &json!("")), None);
        assert_eq!(coerce_to("number", &json!("inf")), None);
        assert_eq!(coerce_to("integer", &json!("1.5")), None);
        assert_eq!(coerce_to("integer", &json!(" 2 ")), Some(json!(2)));
        assert_eq!(coerce_to("number", &json!(true)), Some(json!(1)));
        assert_eq!(coerce_to("number", &Value::Null), Some(json!(0)));
        assert_eq!(coerce_to("string", &json!(1)), Some(json!("1")));
        assert_eq!(coerce_to("string", &json!(false)), Some(json!("false")));
        assert_eq!(coerce_to("string", &Value::Null), Some(json!("")));
        assert_eq!(coerce_to("boolean", &json!("true")), Some(json!(true)));
        assert_eq!(coerce_to("boolean", &json!(0)), Some(json!(false)));
        assert_eq!(coerce_to("boolean", &json!(2)), None);
        assert_eq!(coerce_to("null", &json!("")), Some(Value::Null));
        assert_eq!(coerce_to("null", &json!(false)), Some(Value::Null));
        assert_eq!(coerce_to("object", &json!("x")), None);
    }

    #[test]
    fn requires_schema() {
        let e = validate_stream(
            create_readable_stream(Vec::<Value>::new()),
            Default::default(),
        )
        .err()
        .unwrap();
        assert_eq!(
            e.to_string(),
            "validateStream requires a schema (JSON Schema object or compiled AJV function)"
        );
    }

    #[test]
    fn strict_rejects_unknown_keywords() {
        let schema = json!({"type": "object", "unknownKeyword": true});
        let e = transpile_schema(&schema, Default::default()).err().unwrap();
        assert_eq!(
            e.to_string(),
            "strict mode: unknown keyword: \"unknownKeyword\""
        );
        let nested = json!({"properties": {"a": {"typo": 1}}});
        assert!(transpile_schema(&nested, Default::default()).is_err());
        let lax = TranspileOptions {
            strict: Some(false),
            ..Default::default()
        };
        assert!(transpile_schema(&schema, lax).is_ok());
    }
}
