// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Apache Avro streams, ported from `@datastream/avro`.
//!
//! One whole Avro record per chunk (no container file, no framing): unframe
//! upstream, e.g. with `datastream-schema-registry`. Records are
//! `serde_json::Value`s. Logical types are read and written as their
//! underlying type (as avsc does by default), and `bytes`/`fixed` values are
//! arrays of byte numbers.
//!
//! The codec is self-contained rather than built on a general-purpose Avro
//! library: decoding has to guard array/map block counts (see
//! [`AvroType::decode`]) and encoding has to name every offending field,
//! neither of which such a library has a hook for.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use async_stream::try_stream;
use datastream_core::{BoxFuture, DataStream, Error, Map, Result, StreamExt, Value};

/// Per-message ceiling, matching `@datastream/protobuf`.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
// Compiled types kept per schema id by avro_decode_envelope_stream; the
// resolver caches the definitions, this only saves recompiling them.
const MAX_CACHED_TYPES: usize = 1000;
// Deepest record/array/map nesting a decoded record may have, matching
// serde_json's recursion limit. Also stops a schema that recurses without
// consuming bytes, e.g. a record whose only field is the record itself.
const MAX_DEPTH: usize = 128;
const TRUNCATED: &str = "truncated buffer";

// *** Schema *** //

#[derive(Debug)]
enum Node {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(usize),
    Map(usize),
    Record(Vec<Field>),
    Enum(Vec<String>),
    Fixed(usize),
    Union(Vec<usize>),
}

#[derive(Debug)]
struct Field {
    name: String,
    node: usize,
    default: Option<Value>,
}

/// A compiled Avro schema. Cheap to clone.
#[derive(Clone, Debug)]
pub struct AvroType {
    // Every type in the schema; named references (and so recursion) are
    // indices into it.
    nodes: Arc<[Node]>,
    root: usize,
}

fn invalid(schema: &Value) -> Error {
    format!("invalid schema: {schema}").into()
}

struct Parser {
    nodes: Vec<Node>,
    // Full name -> index of every named type (record, enum, fixed).
    names: HashMap<String, usize>,
}

impl Parser {
    fn push(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn parse(&mut self, schema: &Value, namespace: &str) -> Result<usize> {
        match schema {
            Value::String(name) => self.reference(name, namespace),
            Value::Array(branches) => {
                let mut ids = Vec::with_capacity(branches.len());
                for branch in branches {
                    let id = self.parse(branch, namespace)?;
                    if matches!(self.nodes[id], Node::Union(_)) {
                        return Err("unions may not immediately contain other unions".into());
                    }
                    ids.push(id);
                }
                Ok(self.push(Node::Union(ids)))
            }
            Value::Object(object) => {
                let kind = object.get("type").and_then(Value::as_str);
                match kind.ok_or_else(|| invalid(schema))? {
                    "record" | "error" => self.record(object, schema, namespace),
                    "enum" => {
                        let (id, _) = self.define(object, schema, namespace)?;
                        let symbols = object
                            .get("symbols")
                            .and_then(Value::as_array)
                            .and_then(|symbols| {
                                symbols
                                    .iter()
                                    .map(|s| s.as_str().map(str::to_owned))
                                    .collect()
                            })
                            .ok_or_else(|| invalid(schema))?;
                        self.nodes[id] = Node::Enum(symbols);
                        Ok(id)
                    }
                    "fixed" => {
                        let (id, _) = self.define(object, schema, namespace)?;
                        let size = object
                            .get("size")
                            .and_then(Value::as_u64)
                            .and_then(|size| usize::try_from(size).ok())
                            .ok_or_else(|| invalid(schema))?;
                        self.nodes[id] = Node::Fixed(size);
                        Ok(id)
                    }
                    "array" => {
                        let items = object.get("items").ok_or_else(|| invalid(schema))?;
                        let items = self.parse(items, namespace)?;
                        Ok(self.push(Node::Array(items)))
                    }
                    "map" => {
                        let values = object.get("values").ok_or_else(|| invalid(schema))?;
                        let values = self.parse(values, namespace)?;
                        Ok(self.push(Node::Map(values)))
                    }
                    // A primitive (possibly with a logicalType) or a reference.
                    name => self.reference(name, namespace),
                }
            }
            _ => Err(invalid(schema)),
        }
    }

    fn reference(&mut self, name: &str, namespace: &str) -> Result<usize> {
        let node = match name {
            "null" => Node::Null,
            "boolean" => Node::Boolean,
            "int" => Node::Int,
            "long" => Node::Long,
            "float" => Node::Float,
            "double" => Node::Double,
            "bytes" => Node::Bytes,
            "string" => Node::String,
            _ => {
                // A short name resolves in the enclosing namespace first.
                let found = if name.contains('.') || namespace.is_empty() {
                    self.names.get(name)
                } else {
                    let full = format!("{namespace}.{name}");
                    self.names.get(&full).or_else(|| self.names.get(name))
                };
                return found
                    .copied()
                    .ok_or_else(|| format!("unknown type: {name:?}").into());
            }
        };
        Ok(self.push(node))
    }

    // Registers a named type before its body is parsed, so the body can refer
    // to it (recursive records). Returns its index and its namespace.
    fn define(
        &mut self,
        object: &Map<String, Value>,
        schema: &Value,
        namespace: &str,
    ) -> Result<(usize, String)> {
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(schema))?;
        let full = match object.get("namespace").and_then(Value::as_str) {
            _ if name.contains('.') => name.to_owned(),
            Some(ns) if !ns.is_empty() => format!("{ns}.{name}"),
            // An explicit "" is the null namespace.
            Some(_) => name.to_owned(),
            None if !namespace.is_empty() => format!("{namespace}.{name}"),
            None => name.to_owned(),
        };
        if self.names.contains_key(&full) {
            return Err(format!("duplicate type name: {full:?}").into());
        }
        // Placeholder until the body is parsed.
        let id = self.push(Node::Null);
        let ns = full.rsplit_once('.').map_or("", |(ns, _)| ns).to_owned();
        self.names.insert(full, id);
        Ok((id, ns))
    }

    fn record(
        &mut self,
        object: &Map<String, Value>,
        schema: &Value,
        namespace: &str,
    ) -> Result<usize> {
        let (id, ns) = self.define(object, schema, namespace)?;
        let raw = object
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid(schema))?;
        let mut fields = Vec::with_capacity(raw.len());
        for field in raw {
            let name = field
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid(field))?;
            let kind = field.get("type").ok_or_else(|| invalid(field))?;
            fields.push(Field {
                name: name.to_owned(),
                node: self.parse(kind, &ns)?,
                default: field.get("default").cloned(),
            });
        }
        self.nodes[id] = Node::Record(fields);
        Ok(id)
    }
}

impl AvroType {
    /// Compile a schema given as JSON: an object, a union array, or a type
    /// name string.
    pub fn parse(schema: &Value) -> Result<Self> {
        let mut parser = Parser {
            nodes: Vec::new(),
            names: HashMap::new(),
        };
        let root = parser.parse(schema, "")?;
        let ty = Self {
            nodes: parser.nodes.into(),
            root,
        };
        ty.check_defaults()?;
        Ok(ty)
    }

    /// Compile a schema's JSON text, e.g. a schema registry's definition.
    pub fn parse_str(text: &str) -> Result<Self> {
        Self::parse(&serde_json::from_str(text)?)
    }

    // A field default must be valid for its type (for a union, its first
    // branch), so encoding a record that omits the field cannot fail.
    fn check_defaults(&self) -> Result<()> {
        for node in self.nodes.iter() {
            let Node::Record(fields) = node else { continue };
            for field in fields {
                let Some(default) = &field.default else {
                    continue;
                };
                let node = match &self.nodes[field.node] {
                    Node::Union(branches) => branches.first().copied(),
                    _ => Some(field.node),
                };
                let mut encoder = Encoder::new(&self.nodes, true);
                if !node.is_some_and(|node| encoder.write(node, default)) {
                    return Err(format!("invalid default for field {:?}", field.name).into());
                }
            }
        }
        Ok(())
    }

    /// Encode one record. The error names the `$`-rooted path of every value
    /// that does not match the schema; the values themselves are left out, as
    /// records may carry personal data.
    pub fn encode(&self, value: &Value) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new(&self.nodes, false);
        if encoder.write(self.root, value) {
            return Ok(encoder.out);
        }
        let paths = encoder.errors.join(", ");
        Err(format!("record does not match schema at {paths}").into())
    }

    /// Decode one record, which must span all of `bytes`.
    ///
    /// An array/map block claiming more items than there are bytes left is
    /// rejected: every map entry (its key) and every array item takes at least
    /// one byte, so a 5-byte message cannot claim 50M items and allocate for
    /// them. Zero-width items (null, a record with no fields, fixed of size 0)
    /// are the exception, so an array of more of them than the bytes that
    /// follow is rejected too.
    pub fn decode(&self, bytes: &[u8]) -> Result<Value> {
        let mut decoder = Decoder {
            nodes: &self.nodes,
            buf: bytes,
            pos: 0,
            depth: 0,
        };
        let value = decoder.read(self.root)?;
        if decoder.pos < bytes.len() {
            return Err("trailing data".into());
        }
        Ok(value)
    }
}

// *** Encoding *** //

fn write_varint(out: &mut Vec<u8>, mut n: u64) {
    while n > 0x7f {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

// zigzag varint, as Avro encodes int and long.
fn write_long(out: &mut Vec<u8>, n: i64) {
    write_varint(out, ((n << 1) ^ (n >> 63)).cast_unsigned());
}

// A length or index: never negative, so its zigzag form is just n * 2.
fn write_len(out: &mut Vec<u8>, n: usize) {
    // usize is at most 64 bits wide.
    write_varint(out, (n as u64) << 1);
}

// JSON integers, and floats without a fractional part (avsc accepts 1.0).
fn integer(value: &Value) -> Option<i64> {
    let n = value.as_number()?;
    n.as_i64().or_else(|| {
        let x = n.as_f64()?;
        let in_range = (i64::MIN as f64..i64::MAX as f64).contains(&x);
        // In range and integral, so the cast is exact.
        (in_range && x.fract() == 0.0).then_some(x as i64)
    })
}

enum Segment<'a> {
    Key(&'a str),
    Index(usize),
}

struct Encoder<'a> {
    nodes: &'a [Node],
    out: Vec<u8>,
    path: Vec<Segment<'a>>,
    // `$`-rooted paths of the values that do not match.
    errors: Vec<String>,
    // Above 0 while a union tries its branches: a mismatch only rejects the
    // branch.
    trials: usize,
    // Writing a field default: `bytes`/`fixed` also take the spec's form, a
    // string of code points 0-255.
    defaults: bool,
}

impl<'a> Encoder<'a> {
    fn new(nodes: &'a [Node], defaults: bool) -> Self {
        Self {
            nodes,
            out: Vec::new(),
            path: Vec::new(),
            errors: Vec::new(),
            trials: 0,
            defaults,
        }
    }

    fn mismatch(&mut self) -> bool {
        if self.trials == 0 {
            let mut path = String::from("$");
            for segment in &self.path {
                let _ = match segment {
                    Segment::Key(key) => write!(path, ".{key}"),
                    Segment::Index(i) => write!(path, ".{i}"),
                };
            }
            self.errors.push(path);
        }
        false
    }

    fn bytes(&self, value: &Value) -> Option<Vec<u8>> {
        match value {
            Value::Array(items) => items
                .iter()
                .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                .collect(),
            Value::String(s) if self.defaults => s.chars().map(|c| u8::try_from(c).ok()).collect(),
            _ => None,
        }
    }

    // Writes `value` as `node`, returning whether it matched. Keeps going
    // after a mismatch (outside a union trial) to report every one.
    fn write(&mut self, node: usize, value: &'a Value) -> bool {
        let nodes = self.nodes;
        match (&nodes[node], value) {
            (Node::Null, Value::Null) => true,
            (Node::Boolean, Value::Bool(b)) => {
                self.out.push(u8::from(*b));
                true
            }
            (Node::Int, _) => match integer(value).and_then(|n| i32::try_from(n).ok()) {
                Some(n) => {
                    write_long(&mut self.out, n.into());
                    true
                }
                None => self.mismatch(),
            },
            (Node::Long, _) => match integer(value) {
                Some(n) => {
                    write_long(&mut self.out, n);
                    true
                }
                None => self.mismatch(),
            },
            (Node::Float, Value::Number(n)) => match n.as_f64() {
                Some(x) => {
                    // Narrowing to single precision is what a float field is.
                    self.out.extend_from_slice(&(x as f32).to_le_bytes());
                    true
                }
                None => self.mismatch(),
            },
            (Node::Double, Value::Number(n)) => match n.as_f64() {
                Some(x) => {
                    self.out.extend_from_slice(&x.to_le_bytes());
                    true
                }
                None => self.mismatch(),
            },
            (Node::Bytes, _) => match self.bytes(value) {
                Some(bytes) => {
                    write_len(&mut self.out, bytes.len());
                    self.out.extend_from_slice(&bytes);
                    true
                }
                None => self.mismatch(),
            },
            (Node::Fixed(size), _) => match self.bytes(value) {
                Some(bytes) if bytes.len() == *size => {
                    self.out.extend_from_slice(&bytes);
                    true
                }
                _ => self.mismatch(),
            },
            (Node::String, Value::String(s)) => {
                write_len(&mut self.out, s.len());
                self.out.extend_from_slice(s.as_bytes());
                true
            }
            (Node::Enum(symbols), Value::String(s)) => match symbols.iter().position(|x| x == s) {
                Some(i) => {
                    write_len(&mut self.out, i);
                    true
                }
                None => self.mismatch(),
            },
            (Node::Array(items), Value::Array(values)) => {
                let mut ok = true;
                if !values.is_empty() {
                    write_len(&mut self.out, values.len());
                }
                for (i, item) in values.iter().enumerate() {
                    self.path.push(Segment::Index(i));
                    ok &= self.write(*items, item);
                    self.path.pop();
                    if !ok && self.trials > 0 {
                        return false;
                    }
                }
                self.out.push(0);
                ok
            }
            (Node::Map(values), Value::Object(map)) => {
                let mut ok = true;
                if !map.is_empty() {
                    write_len(&mut self.out, map.len());
                }
                for (key, item) in map {
                    write_len(&mut self.out, key.len());
                    self.out.extend_from_slice(key.as_bytes());
                    self.path.push(Segment::Key(key));
                    ok &= self.write(*values, item);
                    self.path.pop();
                    if !ok && self.trials > 0 {
                        return false;
                    }
                }
                self.out.push(0);
                ok
            }
            (Node::Record(fields), Value::Object(map)) => {
                let mut ok = true;
                for field in fields {
                    self.path.push(Segment::Key(&field.name));
                    ok &= match (map.get(&field.name), &field.default) {
                        (Some(item), _) => self.write(field.node, item),
                        // A missing field takes its default, which
                        // check_defaults made valid for the first branch.
                        (None, Some(default)) => {
                            let defaults = std::mem::replace(&mut self.defaults, true);
                            let written = self.write(field.node, default);
                            self.defaults = defaults;
                            written
                        }
                        (None, None) => self.mismatch(),
                    };
                    self.path.pop();
                    if !ok && self.trials > 0 {
                        return false;
                    }
                }
                ok
            }
            // Like avsc's unwrapped unions: the first branch that matches.
            (Node::Union(branches), _) => {
                let start = self.out.len();
                self.trials += 1;
                let mut matched = false;
                for (i, &branch) in branches.iter().enumerate() {
                    self.out.truncate(start);
                    write_len(&mut self.out, i);
                    if self.write(branch, value) {
                        matched = true;
                        break;
                    }
                }
                self.trials -= 1;
                if !matched {
                    self.out.truncate(start);
                    return self.mismatch();
                }
                true
            }
            _ => self.mismatch(),
        }
    }
}

// *** Decoding *** //

struct Decoder<'a> {
    nodes: &'a [Node],
    buf: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Decoder<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.buf.len())
            .ok_or(TRUNCATED)?;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn long(&mut self) -> Result<i64> {
        let mut n = 0u64;
        for shift in (0..64).step_by(7) {
            let &byte = self.buf.get(self.pos).ok_or(TRUNCATED)?;
            self.pos += 1;
            n |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok((n >> 1).cast_signed() ^ -(n & 1).cast_signed());
            }
        }
        Err("invalid long".into())
    }

    fn index(&mut self) -> Result<Option<usize>> {
        Ok(usize::try_from(self.long()?).ok())
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.index()?.ok_or("invalid length")?;
        self.take(len)
    }

    fn string(&mut self) -> Result<&'a str> {
        Ok(std::str::from_utf8(self.bytes()?).map_err(|_| "invalid utf-8 string")?)
    }

    // A block's item count, rejecting one that claims more items than there
    // are bytes left (see AvroType::decode).
    fn block_count(&mut self) -> Result<usize> {
        let n = self.long()?;
        // A negative count is followed by the block's size in bytes.
        if n < 0 {
            self.long()?;
        }
        let n = n.unsigned_abs();
        let left = self.buf.len() - self.pos;
        match usize::try_from(n) {
            Ok(n) if n <= left => Ok(n),
            _ => Err(format!("block count {n} exceeds the {left} bytes left").into()),
        }
    }

    fn nested(&mut self, read: impl FnOnce(&mut Self) -> Result<Value>) -> Result<Value> {
        if self.depth == MAX_DEPTH {
            return Err(format!("nesting exceeds {MAX_DEPTH} levels").into());
        }
        self.depth += 1;
        let value = read(self);
        self.depth -= 1;
        value
    }

    fn read(&mut self, node: usize) -> Result<Value> {
        let nodes = self.nodes;
        Ok(match &nodes[node] {
            Node::Null => Value::Null,
            Node::Boolean => Value::Bool(self.take(1)?[0] != 0),
            Node::Int => Value::from(i32::try_from(self.long()?).map_err(|_| "invalid int")?),
            Node::Long => Value::from(self.long()?),
            // Non-finite floats become null, as serde_json serializes them.
            Node::Float => Value::from(f64::from(f32::from_le_bytes(self.array()?))),
            Node::Double => Value::from(f64::from_le_bytes(self.array()?)),
            Node::Bytes => Value::from(self.bytes()?.to_vec()),
            Node::Fixed(size) => Value::from(self.take(*size)?.to_vec()),
            Node::String => Value::from(self.string()?),
            Node::Enum(symbols) => {
                let symbol = self.index()?.and_then(|i| symbols.get(i));
                Value::from(symbol.ok_or("invalid enum index")?.as_str())
            }
            Node::Union(branches) => {
                let branch = self.index()?.and_then(|i| branches.get(i));
                return self.read(*branch.ok_or("invalid union index")?);
            }
            Node::Array(items) => self.nested(|d| {
                let mut values = Vec::new();
                loop {
                    let n = d.block_count()?;
                    if n == 0 {
                        break;
                    }
                    values.reserve(n);
                    for _ in 0..n {
                        values.push(d.read(*items)?);
                    }
                }
                Ok(Value::Array(values))
            })?,
            Node::Map(values) => self.nested(|d| {
                let mut map = Map::new();
                loop {
                    let n = d.block_count()?;
                    if n == 0 {
                        break;
                    }
                    for _ in 0..n {
                        let key = d.string()?.to_owned();
                        map.insert(key, d.read(*values)?);
                    }
                }
                Ok(Value::Object(map))
            })?,
            Node::Record(fields) => self.nested(|d| {
                let mut map = Map::with_capacity(fields.len());
                for field in fields {
                    map.insert(field.name.clone(), d.read(field.node)?);
                }
                Ok(Value::Object(map))
            })?,
        })
    }
}

// *** Schema inputs *** //

/// An Avro schema: JSON (an object, a union array, or a type name string),
/// its JSON text (e.g. a schema registry's schema definition), or compiled.
#[derive(Clone, Debug)]
pub enum AvroSchema {
    Json(Value),
    Text(String),
    Compiled(AvroType),
}

impl AvroSchema {
    pub fn compile(self) -> Result<AvroType> {
        match self {
            Self::Json(schema) => AvroType::parse(&schema),
            Self::Text(text) => AvroType::parse_str(&text),
            Self::Compiled(ty) => Ok(ty),
        }
    }
}

impl From<Value> for AvroSchema {
    fn from(schema: Value) -> Self {
        Self::Json(schema)
    }
}

impl From<String> for AvroSchema {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for AvroSchema {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<AvroType> for AvroSchema {
    fn from(ty: AvroType) -> Self {
        Self::Compiled(ty)
    }
}

pub type LazySchema = Box<dyn FnOnce() -> BoxFuture<Result<AvroSchema>> + Send>;

/// A schema, or a function producing one. The function is called once, when
/// the first chunk arrives, so upstream can produce the schema first (e.g.
/// from a schema registry lookup).
pub enum AvroSchemaInput {
    Schema(AvroSchema),
    Lazy(LazySchema),
}

impl AvroSchemaInput {
    pub fn lazy<F, Fut>(f: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<AvroSchema>> + Send + 'static,
    {
        Self::Lazy(Box::new(move || -> BoxFuture<Result<AvroSchema>> {
            Box::pin(f())
        }))
    }
}

impl From<AvroSchema> for AvroSchemaInput {
    fn from(schema: AvroSchema) -> Self {
        Self::Schema(schema)
    }
}

impl From<Value> for AvroSchemaInput {
    fn from(schema: Value) -> Self {
        Self::Schema(schema.into())
    }
}

impl From<String> for AvroSchemaInput {
    fn from(text: String) -> Self {
        Self::Schema(text.into())
    }
}

impl From<&str> for AvroSchemaInput {
    fn from(text: &str) -> Self {
        Self::Schema(text.into())
    }
}

impl From<AvroType> for AvroSchemaInput {
    fn from(ty: AvroType) -> Self {
        Self::Schema(ty.into())
    }
}

// A concrete schema is compiled at construction, so a bad one errors at the
// call site; a lazy one on the first chunk.
struct TypeSource {
    ty: Option<AvroType>,
    lazy: Option<LazySchema>,
}

impl TypeSource {
    fn new(schema: AvroSchemaInput) -> Result<Self> {
        Ok(match schema {
            AvroSchemaInput::Schema(schema) => Self {
                ty: Some(schema.compile()?),
                lazy: None,
            },
            AvroSchemaInput::Lazy(lazy) => Self {
                ty: None,
                lazy: Some(lazy),
            },
        })
    }

    async fn get(&mut self) -> Result<&AvroType> {
        if let Some(lazy) = self.lazy.take() {
            self.ty = Some(lazy().await?.compile()?);
        }
        // Only empty after the lazy schema failed, which ended the stream.
        self.ty.as_ref().ok_or_else(|| "schema required".into())
    }
}

// *** Streams *** //

pub struct AvroEncodeOptions {
    pub schema: AvroSchemaInput,
}

/// Encode each record to Avro binary.
pub fn avro_encode_stream(
    mut input: DataStream<Value>,
    options: AvroEncodeOptions,
) -> Result<DataStream<Vec<u8>>> {
    let mut source = TypeSource::new(options.schema)?;
    Ok(Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let bytes = source
                .get()
                .await?
                .encode(&chunk)
                .map_err(|e| Error::from(format!("avroEncodeStream: {e}")))?;
            yield bytes;
        }
    }))
}

// Checked per message, not cumulatively, as in @datastream/protobuf.
fn check_size(len: usize, max: usize) -> Result<()> {
    if len > max {
        return Err(
            format!("avroDecodeStream: message exceeds maxMessageSize ({max} bytes)").into(),
        );
    }
    Ok(())
}

fn decode(ty: &AvroType, bytes: &[u8]) -> Result<Value> {
    ty.decode(bytes)
        .map_err(|e| format!("avroDecodeStream: {e}").into())
}

pub struct AvroDecodeOptions {
    pub schema: AvroSchemaInput,
    /// Largest accepted message in bytes (default 64MiB; `usize::MAX` for
    /// none).
    pub max_message_size: Option<usize>,
}

/// Decode each chunk, one whole record, to its value.
pub fn avro_decode_stream<I>(
    mut input: DataStream<I>,
    options: AvroDecodeOptions,
) -> Result<DataStream<Value>>
where
    I: AsRef<[u8]> + Send + 'static,
{
    let mut source = TypeSource::new(options.schema)?;
    let max = options.max_message_size.unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
    Ok(Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            check_size(chunk.as_ref().len(), max)?;
            yield decode(source.get().await?, chunk.as_ref())?;
        }
    }))
}

/// A chunk carrying one Avro record's bytes and the id of the schema it was
/// written with, e.g. a schema registry unframe stream's envelope.
pub trait AvroEnvelope: Send + 'static {
    type SchemaId: Clone + Eq + Hash + Send + Sync + 'static;
    fn schema_id(&self) -> Self::SchemaId;
    /// Move the record's bytes out; the envelope is emitted without them.
    fn take_payload(&mut self) -> Vec<u8>;
}

#[cfg(feature = "schema-registry")]
impl<M: Send + 'static> AvroEnvelope for datastream_schema_registry::ConfluentEnvelope<M> {
    type SchemaId = u32;
    fn schema_id(&self) -> u32 {
        self.schema_id
    }
    fn take_payload(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.payload)
    }
}

#[cfg(feature = "schema-registry")]
impl<M: Send + 'static> AvroEnvelope for datastream_schema_registry::GlueEnvelope<M> {
    type SchemaId = String;
    fn schema_id(&self) -> String {
        self.schema_version_id.clone()
    }
    fn take_payload(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.payload)
    }
}

/// A decoded envelope: the input envelope, its payload taken, and its record.
/// Message metadata on the envelope (e.g. Kafka's topic/partition/offset)
/// rides along for a downstream commit.
#[derive(Clone, Debug, PartialEq)]
pub struct AvroDecoded<E> {
    pub envelope: E,
    pub value: Value,
}

pub type ResolveSchema<K> = Box<dyn Fn(K) -> BoxFuture<Result<AvroSchema>> + Send + Sync>;

pub struct AvroDecodeEnvelopeOptions<K> {
    /// Looks up a writer schema by id, e.g. via a schema registry resolver
    /// (map its result's schema definition to [`AvroSchema::Text`]).
    pub resolve_schema: ResolveSchema<K>,
    /// Largest accepted message in bytes (default 64MiB; `usize::MAX` for
    /// none).
    pub max_message_size: Option<usize>,
}

impl<K> AvroDecodeEnvelopeOptions<K> {
    pub fn new<F, Fut>(resolve_schema: F) -> Self
    where
        F: Fn(K) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroSchema>> + Send + 'static,
    {
        Self {
            resolve_schema: Box::new(move |id| -> BoxFuture<Result<AvroSchema>> {
                Box::pin(resolve_schema(id))
            }),
            max_message_size: None,
        }
    }
}

/// Decode each envelope's payload with the schema its id resolves to.
/// Compiled types are cached per id (the 1000 most recently added).
pub fn avro_decode_envelope_stream<E: AvroEnvelope>(
    mut input: DataStream<E>,
    options: AvroDecodeEnvelopeOptions<E::SchemaId>,
) -> DataStream<AvroDecoded<E>> {
    let resolve = options.resolve_schema;
    let max = options.max_message_size.unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
    Box::pin(try_stream! {
        let mut types = HashMap::new();
        // Ids in insertion order, for FIFO eviction.
        let mut order = VecDeque::new();
        while let Some(chunk) = input.next().await {
            let mut envelope = chunk?;
            let payload = envelope.take_payload();
            check_size(payload.len(), max)?;
            let id = envelope.schema_id();
            if !types.contains_key(&id) {
                let ty = resolve(id.clone()).await?.compile()?;
                if types.len() >= MAX_CACHED_TYPES {
                    if let Some(oldest) = order.pop_front() {
                        types.remove(&oldest);
                    }
                }
                order.push_back(id.clone());
                types.insert(id.clone(), ty);
            }
            let value = decode(&types[&id], &payload)?;
            yield AvroDecoded { envelope, value };
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, stream_to_array};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn schema() -> Value {
        json!({
            "type": "record",
            "name": "Msg",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        })
    }

    fn msg_type() -> AvroType {
        AvroType::parse(&schema()).unwrap()
    }

    fn messages() -> Vec<Value> {
        vec![
            json!({"id": 1, "name": "alpha"}),
            json!({"id": 2, "name": "béta"}),
            json!({"id": 3, "name": "gamma"}),
        ]
    }

    fn encoded() -> Vec<Vec<u8>> {
        let ty = msg_type();
        messages().iter().map(|m| ty.encode(m).unwrap()).collect()
    }

    async fn encode_all(
        input: Vec<Value>,
        schema: impl Into<AvroSchemaInput>,
    ) -> Result<Vec<Vec<u8>>> {
        let options = AvroEncodeOptions {
            schema: schema.into(),
        };
        stream_to_array(
            avro_encode_stream(create_readable_stream(input), options)?,
            None,
        )
        .await
    }

    async fn decode_all(
        input: Vec<Vec<u8>>,
        schema: impl Into<AvroSchemaInput>,
        max_message_size: Option<usize>,
    ) -> Result<Vec<Value>> {
        let options = AvroDecodeOptions {
            schema: schema.into(),
            max_message_size,
        };
        stream_to_array(
            avro_decode_stream(create_readable_stream(input), options)?,
            None,
        )
        .await
    }

    fn decode_err(bytes: Vec<u8>, schema: Value) -> String {
        let result = block_on(decode_all(vec![bytes], schema, None));
        result.unwrap_err().to_string()
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    // zigzag varint, as Avro encodes longs (block counts and sizes).
    fn long(n: i64) -> Vec<u8> {
        let mut out = Vec::new();
        write_long(&mut out, n);
        out
    }

    // A 5-byte message claiming 50M items.
    fn bomb() -> Vec<u8> {
        [long(50_000_000), vec![0]].concat()
    }
    const BOMB_MESSAGE: &str = "avroDecodeStream: block count 50000000 exceeds the 1 bytes left";

    // *** avro_encode_stream *** //

    #[tokio::test]
    async fn encode_objects_to_avro_binary() {
        let encoded = encode_all(messages(), schema()).await.unwrap();
        assert_eq!(encoded.len(), 3);
        // id 1 -> zigzag 0x02; "alpha" -> length 5 zigzag 0x0a + utf8
        assert_eq!(encoded[0], [0x02, 0x0a, 97, 108, 112, 104, 97]);
        let ty = msg_type();
        let decoded: Vec<Value> = encoded.iter().map(|b| ty.decode(b).unwrap()).collect();
        assert_eq!(decoded, messages());
    }

    #[tokio::test]
    async fn encode_accepts_json_text_and_compiled_type() {
        let text = encode_all(messages(), schema().to_string()).await.unwrap();
        let compiled = encode_all(messages(), msg_type()).await.unwrap();
        assert_eq!(text, encoded());
        assert_eq!(compiled, encoded());
    }

    #[tokio::test]
    async fn encode_resolves_a_lazy_schema_once_on_the_first_chunk() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let options = AvroEncodeOptions {
            schema: AvroSchemaInput::lazy(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(schema().into())
            }),
        };
        let stream = avro_encode_stream(create_readable_stream(messages()), options).unwrap();
        // Not resolved at construction: upstream may not have produced it yet.
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(stream_to_array(stream, None).await.unwrap(), encoded());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn encode_compiles_a_concrete_schema_at_construction() {
        let options = AvroEncodeOptions {
            schema: json!({"type": "nope"}).into(),
        };
        let e = avro_encode_stream(create_readable_stream(Vec::<Value>::new()), options)
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "unknown type: \"nope\"");
    }

    #[tokio::test]
    async fn encode_errors_when_a_lazy_schema_fails() {
        let options = AvroEncodeOptions {
            schema: AvroSchemaInput::lazy(|| async {
                Err::<AvroSchema, _>("lookup failed".into())
            }),
        };
        let stream = avro_encode_stream(create_readable_stream(messages()), options).unwrap();
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "lookup failed");
    }

    #[tokio::test]
    async fn encode_names_each_bad_field() {
        let input = vec![messages()[0].clone(), json!({"id": "x", "name": 1})];
        let e = encode_all(input, schema()).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroEncodeStream: record does not match schema at $.id, $.name"
        );
        let e = encode_all(vec![json!({"name": "a"})], schema())
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroEncodeStream: record does not match schema at $.id"
        );
    }

    #[tokio::test]
    async fn encode_reports_the_root_path_for_a_non_record_value() {
        let e = encode_all(vec![json!(1)], json!({"type": "string"}))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroEncodeStream: record does not match schema at $"
        );
    }

    #[tokio::test]
    async fn encode_reports_nested_paths() {
        let schema = json!({
            "type": "record",
            "name": "Outer",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "meta", "type": {"type": "map", "values": "long"}},
                {"name": "opt", "type": ["null", "int"]},
            ],
        });
        let input = json!({"tags": ["a", 2], "meta": {"k": "v"}, "opt": "x"});
        let e = encode_all(vec![input], schema).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroEncodeStream: record does not match schema at $.tags.1, $.meta.k, $.opt"
        );
    }

    #[test]
    fn every_type_round_trips() {
        let schema = json!({
            "type": "record",
            "name": "All",
            "namespace": "test",
            "fields": [
                {"name": "n", "type": "null"},
                {"name": "b", "type": "boolean"},
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "by", "type": "bytes"},
                {"name": "s", "type": "string"},
                {"name": "a", "type": {"type": "array", "items": "int"}},
                {"name": "m", "type": {"type": "map", "values": "string"}},
                {"name": "e", "type": {"type": "enum", "name": "Suit", "symbols": ["A", "B"]}},
                {"name": "fx", "type": {"type": "fixed", "name": "Two", "size": 2}},
                {"name": "u", "type": ["null", "string", "Suit"]},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "ref", "type": "test.Two"},
            ],
        });
        let ty = AvroType::parse(&schema).unwrap();
        let value = json!({
            "n": null, "b": true, "i": -5, "l": 1_i64 << 40, "f": 1.5, "d": -0.25,
            "by": [0, 255], "s": "é", "a": [1, 2, 3], "m": {"x": "y"},
            "e": "B", "fx": [7, 8], "u": "A", "ts": 1_700_000_000_000_i64, "ref": [1, 2],
        });
        assert_eq!(ty.decode(&ty.encode(&value).unwrap()).unwrap(), value);
        // "A" matches the union's string branch first, as in avsc.
        let union = AvroType::parse(
            &json!(["null", "string", {"type": "enum", "name": "E", "symbols": ["A"]}]),
        )
        .unwrap();
        assert_eq!(union.encode(&json!("A")).unwrap(), [0x02, 0x02, b'A']);
        assert_eq!(union.encode(&json!(null)).unwrap(), [0x00]);
    }

    #[test]
    fn encode_writes_defaults_for_missing_fields() {
        let ty = AvroType::parse(&json!({
            "type": "record",
            "name": "D",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "tag", "type": ["null", "string"], "default": null},
                {"name": "raw", "type": "bytes", "default": "\u{00ff}"},
                {"name": "n", "type": "long", "default": 7},
            ],
        }))
        .unwrap();
        let bytes = ty.encode(&json!({"id": 1})).unwrap();
        assert_eq!(
            ty.decode(&bytes).unwrap(),
            json!({"id": 1, "tag": null, "raw": [255], "n": 7})
        );
        // The spec's string form of bytes is only for defaults.
        assert!(ty.encode(&json!({"id": 1, "raw": "a"})).is_err());
    }

    #[test]
    fn parse_rejects_bad_schemas() {
        let cases = [
            (
                json!({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int", "default": "x"}]}),
                "invalid default for field \"a\"",
            ),
            (
                json!({"type": "record", "name": "R", "fields": [{"name": "a", "type": ["null", "int"], "default": 1}]}),
                "invalid default for field \"a\"",
            ),
            (
                json!(["null", ["int"]]),
                "unions may not immediately contain other unions",
            ),
            (
                json!({"type": "record", "name": "R", "fields": [{"name": "a", "type": {"type": "record", "name": "R", "fields": []}}]}),
                "duplicate type name: \"R\"",
            ),
            (json!("Missing"), "unknown type: \"Missing\""),
            (
                json!({"type": "fixed", "name": "F"}),
                "invalid schema: {\"type\":\"fixed\",\"name\":\"F\"}",
            ),
            (json!(1), "invalid schema: 1"),
        ];
        for (schema, message) in cases {
            assert_eq!(AvroType::parse(&schema).unwrap_err().to_string(), message);
        }
        assert!(AvroType::parse_str("{").is_err());
    }

    #[test]
    fn parse_resolves_names_by_namespace() {
        let ty = AvroType::parse(&json!({
            "type": "record",
            "name": "Outer",
            "namespace": "a.b",
            "fields": [
                {"name": "x", "type": {"type": "fixed", "name": "F", "size": 1}},
                {"name": "y", "type": "F"},
                {"name": "z", "type": "a.b.F"},
                {"name": "w", "type": {"type": "enum", "name": "c.E", "symbols": ["S"]}},
                {"name": "v", "type": "c.E"},
            ],
        }))
        .unwrap();
        let value = json!({"x": [1], "y": [2], "z": [3], "w": "S", "v": "S"});
        assert_eq!(ty.decode(&ty.encode(&value).unwrap()).unwrap(), value);
        // Fully-qualified elsewhere, a short name does not resolve.
        assert!(AvroType::parse(&json!({
            "type": "record",
            "name": "R",
            "namespace": "n",
            "fields": [{"name": "a", "type": {"type": "fixed", "name": "F", "size": 1}}, {"name": "b", "type": {"type": "record", "name": "x.S", "fields": [{"name": "c", "type": "F"}]}}],
        }))
        .is_err());
    }

    // *** avro_decode_stream *** //

    #[tokio::test]
    async fn decode_one_record_per_chunk() {
        let decoded = decode_all(encoded(), schema(), None).await.unwrap();
        assert_eq!(decoded, messages());
    }

    #[tokio::test]
    async fn decode_resolves_a_lazy_schema_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let schema = AvroSchemaInput::lazy(move || async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(schema().to_string().into())
        });
        let decoded = decode_all(encoded(), schema, None).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decoded, messages());
    }

    #[test]
    fn decode_errors_on_truncated_and_trailing_bytes() {
        assert_eq!(
            decode_err(vec![0x02, 0x0a, 0x41], schema()),
            "avroDecodeStream: truncated buffer"
        );
        assert_eq!(
            decode_err(vec![0x02, 0x00, 0xff], schema()),
            "avroDecodeStream: trailing data"
        );
    }

    #[test]
    fn decode_errors_on_invalid_values() {
        let cases = [
            (vec![0x04], json!(["null", "int"]), "invalid union index"),
            (vec![0x01], json!(["null", "int"]), "invalid union index"),
            (
                vec![0x02],
                json!({"type": "enum", "name": "E", "symbols": ["A"]}),
                "invalid enum index",
            ),
            (vec![0x02, 0xff], json!("string"), "invalid utf-8 string"),
            (vec![0x01], json!("string"), "invalid length"),
            (long(1 << 40), json!("int"), "invalid int"),
            (vec![0xff; 11], json!("long"), "invalid long"),
        ];
        for (bytes, schema, message) in cases {
            assert_eq!(
                decode_err(bytes, schema),
                format!("avroDecodeStream: {message}")
            );
        }
    }

    #[test]
    fn decode_non_finite_floats_as_null() {
        let ty = AvroType::parse(&json!("double")).unwrap();
        assert_eq!(ty.decode(&f64::NAN.to_le_bytes()).unwrap(), Value::Null);
    }

    // *** Limits *** //

    #[tokio::test]
    async fn decode_rejects_a_chunk_over_the_64mib_default_max_message_size() {
        let limit = DEFAULT_MAX_MESSAGE_SIZE;
        // At the limit the size check passes and decoding proceeds.
        let e = decode_all(vec![vec![0; limit]], json!("null"), None)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "avroDecodeStream: trailing data");
        let e = decode_all(vec![vec![0; limit + 1]], json!("null"), None)
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroDecodeStream: message exceeds maxMessageSize (67108864 bytes)"
        );
        // usize::MAX imposes no ceiling.
        let e = decode_all(vec![vec![0; limit + 1]], json!("null"), Some(usize::MAX))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "avroDecodeStream: trailing data");
    }

    #[tokio::test]
    async fn decode_honors_max_message_size() {
        let fixed = |size| json!({"type": "fixed", "name": "Blob", "size": size});
        let ok = decode_all(vec![vec![0; 3]], fixed(3), Some(3))
            .await
            .unwrap();
        assert_eq!(ok, [json!([0, 0, 0])]);
        let e = decode_all(vec![vec![0; 4]], fixed(4), Some(3))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroDecodeStream: message exceeds maxMessageSize (3 bytes)"
        );
    }

    #[test]
    fn decode_rejects_array_and_map_block_counts_larger_than_the_bytes_left() {
        assert_eq!(
            decode_err(bomb(), json!({"type": "array", "items": "null"})),
            BOMB_MESSAGE
        );
        assert_eq!(
            decode_err(bomb(), json!({"type": "map", "values": "null"})),
            BOMB_MESSAGE
        );
    }

    #[test]
    fn decode_guards_nested_arrays_and_maps() {
        let nested = [
            // record field
            (
                json!({"type": "record", "name": "R", "fields": [{"name": "a", "type": {"type": "array", "items": "null"}}]}),
                vec![],
            ),
            // union branch 1
            (json!(["null", {"type": "map", "values": "null"}]), long(1)),
            // array of arrays: one outer item, then the inner bomb
            (
                json!({"type": "array", "items": {"type": "array", "items": "null"}}),
                long(1),
            ),
            // map of arrays: one entry with key "k"
            (
                json!({"type": "map", "values": {"type": "array", "items": "null"}}),
                [long(1), long(1), vec![b'k']].concat(),
            ),
        ];
        for (schema, prefix) in nested {
            assert_eq!(decode_err([prefix, bomb()].concat(), schema), BOMB_MESSAGE);
        }
    }

    fn tree() -> Value {
        json!({
            "type": "record",
            "name": "Tree",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "children", "type": {"type": "array", "items": "Tree"}},
                {"name": "labels", "type": {"type": "map", "values": "Tree"}},
            ],
        })
    }

    #[test]
    fn decode_guards_recursive_schemas() {
        let ty = AvroType::parse(&tree()).unwrap();
        let value = json!({
            "id": 1,
            "children": [
                {"id": 2, "children": [], "labels": {}},
                {"id": 3, "children": [], "labels": {"x": {"id": 4, "children": [], "labels": {}}}},
            ],
            "labels": {},
        });
        assert_eq!(ty.decode(&ty.encode(&value).unwrap()).unwrap(), value);
        let bytes = [long(1), long(1), long(2), bomb()].concat();
        assert_eq!(decode_err(bytes, tree()), BOMB_MESSAGE);
    }

    #[test]
    fn decode_limits_nesting_depth() {
        // Each level is id 0 then a one-item children block.
        let deep = [0x00u8, 0x02].repeat(MAX_DEPTH);
        assert_eq!(
            decode_err(deep, tree()),
            "avroDecodeStream: nesting exceeds 128 levels"
        );
        // Recursion that consumes no bytes at all.
        let endless =
            json!({"type": "record", "name": "R", "fields": [{"name": "r", "type": "R"}]});
        assert_eq!(
            decode_err(vec![], endless),
            "avroDecodeStream: nesting exceeds 128 levels"
        );
    }

    #[test]
    fn decode_reads_negative_sized_blocks() {
        // [1, 2] as one block of -2 items, 2 bytes long
        let array = [long(-2), long(2), long(1), long(2), vec![0]].concat();
        let ty = AvroType::parse(&json!({"type": "array", "items": "int"})).unwrap();
        assert_eq!(ty.decode(&array).unwrap(), json!([1, 2]));
        // { a: 1, b: 2 } as one block of -2 entries, 6 bytes long, then { c: 3 }
        let map = [
            long(-2),
            long(6),
            long(1),
            vec![b'a'],
            long(1),
            long(1),
            vec![b'b'],
            long(2),
            long(1),
            long(1),
            vec![b'c'],
            long(3),
            vec![0],
        ]
        .concat();
        let ty = AvroType::parse(&json!({"type": "map", "values": "int"})).unwrap();
        assert_eq!(ty.decode(&map).unwrap(), json!({"a": 1, "b": 2, "c": 3}));
    }

    #[test]
    fn decode_reports_an_array_missing_its_closing_block_as_truncated() {
        let bytes = [long(1), vec![1]].concat();
        assert_eq!(
            decode_err(bytes, json!({"type": "array", "items": "boolean"})),
            "avroDecodeStream: truncated buffer"
        );
    }

    #[test]
    fn decode_rejects_arrays_of_zero_width_items_longer_than_the_bytes_left() {
        // [null] is count 1 with 1 byte left (the closing 0): allowed. [null,
        // null] encodes as 2 bytes (count 2, then the closing 0): rejected, a
        // documented limitation of the block-count guard.
        let nulls = json!({"type": "array", "items": "null"});
        let ty = AvroType::parse(&nulls).unwrap();
        assert_eq!(
            ty.decode(&ty.encode(&json!([null])).unwrap()).unwrap(),
            json!([null])
        );
        assert_eq!(
            decode_err(ty.encode(&json!([null, null])).unwrap(), nulls),
            "avroDecodeStream: block count 2 exceeds the 1 bytes left"
        );
    }

    // *** avro_decode_envelope_stream *** //

    #[derive(Clone, Debug, PartialEq)]
    struct Envelope {
        schema_id: u32,
        payload: Vec<u8>,
        offset: i64,
    }

    impl AvroEnvelope for Envelope {
        type SchemaId = u32;
        fn schema_id(&self) -> u32 {
            self.schema_id
        }
        fn take_payload(&mut self) -> Vec<u8> {
            std::mem::take(&mut self.payload)
        }
    }

    fn v2() -> Value {
        json!({
            "type": "record",
            "name": "Msg",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
            ],
        })
    }

    async fn decode_envelopes(
        input: Vec<Envelope>,
        options: AvroDecodeEnvelopeOptions<u32>,
    ) -> Result<Vec<AvroDecoded<Envelope>>> {
        let stream = avro_decode_envelope_stream(create_readable_stream(input), options);
        stream_to_array(stream, None).await
    }

    #[tokio::test]
    async fn decode_envelopes_per_schema_id_and_keeps_the_metadata() {
        let ty = msg_type();
        let ty2 = AvroType::parse(&v2()).unwrap();
        let values = [
            (1, messages()[0].clone()),
            (2, json!({"id": 2, "name": "béta", "tags": ["x"]})),
            (1, messages()[2].clone()),
            (2, json!({"id": 1, "name": "alpha", "tags": []})),
        ];
        let input = values
            .iter()
            .zip(0..)
            .map(|((schema_id, value), offset)| Envelope {
                schema_id: *schema_id,
                payload: (if *schema_id == 1 { &ty } else { &ty2 })
                    .encode(value)
                    .unwrap(),
                offset,
            })
            .collect();
        let lookups = Arc::new(Mutex::new(Vec::new()));
        let seen = lookups.clone();
        let options = AvroDecodeEnvelopeOptions::new(move |id: u32| {
            seen.lock().unwrap().push(id);
            // Shaped like a registry result's schema definition.
            let definition = if id == 1 { schema() } else { v2() }.to_string();
            async move { Ok(AvroSchema::Text(definition)) }
        });
        let out = decode_envelopes(input, options).await.unwrap();
        let expected: Vec<AvroDecoded<Envelope>> = values
            .into_iter()
            .zip(0..)
            .map(|((schema_id, value), offset)| AvroDecoded {
                envelope: Envelope {
                    schema_id,
                    payload: vec![],
                    offset,
                },
                value,
            })
            .collect();
        assert_eq!(out, expected);
        // Compiled types are cached per id: one lookup each.
        assert_eq!(*lookups.lock().unwrap(), [1, 2]);
    }

    #[tokio::test]
    async fn decode_envelopes_with_json_or_compiled_schemas() {
        let input = encoded()
            .into_iter()
            .take(2)
            .zip(1..)
            .map(|(payload, schema_id)| Envelope {
                schema_id,
                payload,
                offset: 0,
            })
            .collect();
        let options = AvroDecodeEnvelopeOptions::new(|id: u32| async move {
            Ok(if id == 1 {
                schema().into()
            } else {
                msg_type().into()
            })
        });
        let out = decode_envelopes(input, options).await.unwrap();
        let values: Vec<Value> = out.into_iter().map(|d| d.value).collect();
        assert_eq!(values, messages()[..2]);
    }

    #[tokio::test]
    async fn decode_envelopes_propagates_resolver_errors() {
        let input = vec![Envelope {
            schema_id: 1,
            payload: vec![2],
            offset: 0,
        }];
        let options = AvroDecodeEnvelopeOptions::new(|_: u32| async {
            Err::<AvroSchema, _>("not found".into())
        });
        let e = decode_envelopes(input, options).await.unwrap_err();
        assert_eq!(e.to_string(), "not found");
    }

    #[tokio::test]
    async fn decode_envelopes_applies_max_message_size_and_the_block_count_guard() {
        let input = vec![Envelope {
            schema_id: 1,
            payload: vec![0; 4],
            offset: 0,
        }];
        let mut options = AvroDecodeEnvelopeOptions::new(|_: u32| async {
            Ok(json!({"type": "fixed", "name": "Blob", "size": 4}).into())
        });
        options.max_message_size = Some(3);
        let e = decode_envelopes(input, options).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "avroDecodeStream: message exceeds maxMessageSize (3 bytes)"
        );
        let input = vec![Envelope {
            schema_id: 1,
            payload: bomb(),
            offset: 0,
        }];
        let options = AvroDecodeEnvelopeOptions::new(|_: u32| async {
            Ok(json!({"type": "array", "items": "null"}).into())
        });
        let e = decode_envelopes(input, options).await.unwrap_err();
        assert_eq!(e.to_string(), BOMB_MESSAGE);
    }

    #[tokio::test]
    async fn decode_envelopes_caches_at_most_1000_types_evicting_the_oldest() {
        let lookups = Arc::new(Mutex::new(Vec::new()));
        let seen = lookups.clone();
        let options = AvroDecodeEnvelopeOptions::new(move |id: u32| {
            seen.lock().unwrap().push(id);
            async { Ok("\"int\"".into()) }
        });
        let ids: Vec<u32> = (0..1000).chain([0, 1000, 1, 0]).collect();
        let input = ids
            .into_iter()
            .map(|schema_id| Envelope {
                schema_id,
                payload: vec![0],
                offset: 0,
            })
            .collect();
        decode_envelopes(input, options).await.unwrap();
        let lookups = lookups.lock().unwrap();
        assert_eq!(lookups.len(), 1002);
        assert_eq!(lookups[1000..], [1000, 0]);
    }

    #[tokio::test]
    async fn encode_then_decode_round_trips() {
        let options = AvroEncodeOptions {
            schema: schema().into(),
        };
        let stream = avro_encode_stream(create_readable_stream(messages()), options).unwrap();
        let options = AvroDecodeOptions {
            schema: schema().into(),
            max_message_size: None,
        };
        let stream = avro_decode_stream(stream, options).unwrap();
        assert_eq!(stream_to_array(stream, None).await.unwrap(), messages());
    }

    #[cfg(feature = "schema-registry")]
    mod schema_registry {
        use super::*;
        use datastream_schema_registry::{
            confluent_unframe_stream, glue_unframe_stream, ConfluentUnframeOptions,
            GlueUnframeOptions,
        };

        #[tokio::test]
        async fn confluent_unframe_then_decode_per_schema_id() {
            let frames: Vec<Vec<u8>> = encoded()
                .into_iter()
                .zip([1u32, 2, 1])
                .map(|(payload, id)| [vec![0x00], id.to_be_bytes().to_vec(), payload].concat())
                .collect();
            let (stream, _) = confluent_unframe_stream(
                create_readable_stream(frames),
                ConfluentUnframeOptions::default(),
            );
            let options = AvroDecodeEnvelopeOptions::new(|_: u32| async { Ok(schema().into()) });
            let out = stream_to_array(avro_decode_envelope_stream(stream, options), None)
                .await
                .unwrap();
            let ids: Vec<u32> = out.iter().map(|d| d.envelope.schema_id).collect();
            assert_eq!(ids, [1, 2, 1]);
            assert!(out.iter().all(|d| d.envelope.payload.is_empty()));
            let values: Vec<Value> = out.into_iter().map(|d| d.value).collect();
            assert_eq!(values, messages());
        }

        #[tokio::test]
        async fn glue_unframe_then_decode_by_schema_version_id() {
            let uuid = "12345678-1234-1234-1234-1234567890ab";
            let uuid_bytes: Vec<u8> = (0..16)
                .map(|i| {
                    let hex = uuid.replace('-', "");
                    u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap()
                })
                .collect();
            // 0x03 magic, 0x00 = uncompressed, the 16 UUID bytes, then the record.
            let frame = [vec![0x03, 0x00], uuid_bytes, encoded()[0].clone()].concat();
            let (stream, _) = glue_unframe_stream(
                create_readable_stream(vec![frame]),
                GlueUnframeOptions::default(),
            );
            let lookups = Arc::new(Mutex::new(Vec::new()));
            let seen = lookups.clone();
            let options = AvroDecodeEnvelopeOptions::new(move |id: String| {
                seen.lock().unwrap().push(id);
                async { Ok(schema().into()) }
            });
            let out = stream_to_array(avro_decode_envelope_stream(stream, options), None)
                .await
                .unwrap();
            assert_eq!(*lookups.lock().unwrap(), [uuid]);
            assert_eq!(out[0].envelope.compression, "none");
            assert_eq!(out[0].value, messages()[0]);
        }
    }
}
