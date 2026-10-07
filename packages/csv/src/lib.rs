// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! CSV parsing and formatting streams, ported from `@datastream/csv`.
//!
//! Text goes in as `String` chunks; parsed rows come out as `Value::Array`s of
//! strings. Lengths and `field_max_size` are measured in bytes.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Error, Map, Result, StreamExt, StreamResult,
    Value,
};
pub use datastream_object::Keys;
use datastream_object::{object_to_entries_stream, ObjectEntriesOptions};
use serde_json::json;

const DEFAULT_DELIMITER: &str = ",";
const DEFAULT_NEWLINE: &str = "\r\n";
const DEFAULT_QUOTE: &str = "\"";
const DEFAULT_FIELD_MAX_SIZE: usize = 16_777_216; // 16MB

// *** Options *** //

/// A string option known up front, or resolved when the stream first needs it
/// (JS `delimiterChar: () => detect.result().value.delimiterChar`).
#[derive(Clone)]
pub enum LazyString {
    Value(String),
    Fn(Arc<dyn Fn() -> Option<String> + Send + Sync>),
}

impl LazyString {
    pub fn lazy(f: impl Fn() -> Option<String> + Send + Sync + 'static) -> Self {
        Self::Fn(Arc::new(f))
    }
    /// Read the string at JSON `pointer` from `result` when first needed.
    pub fn from_result(result: &StreamResult, pointer: &str) -> Self {
        let (result, pointer) = (result.clone(), pointer.to_string());
        Self::lazy(move || result.get().pointer(&pointer)?.as_str().map(String::from))
    }
    fn resolve(&self) -> Option<String> {
        match self {
            Self::Value(v) => Some(v.clone()),
            Self::Fn(f) => f(),
        }
    }
}

impl fmt::Debug for LazyString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => f.debug_tuple("Value").field(v).finish(),
            Self::Fn(_) => f.write_str("Fn(..)"),
        }
    }
}

impl From<&str> for LazyString {
    fn from(v: &str) -> Self {
        Self::Value(v.to_string())
    }
}

impl From<String> for LazyString {
    fn from(v: String) -> Self {
        Self::Value(v)
    }
}

/// Characters and running state handed to a parser.
#[derive(Clone, Debug)]
pub struct ParserOptions {
    pub delimiter_char: String,
    pub newline_char: String,
    pub quote_char: String,
    /// Defaults to `quote_char` (doubled-quote escaping).
    pub escape_char: Option<String>,
    pub field_max_size: usize,
    pub num_cols: usize,
    pub idx: usize,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            delimiter_char: DEFAULT_DELIMITER.into(),
            newline_char: DEFAULT_NEWLINE.into(),
            quote_char: DEFAULT_QUOTE.into(),
            escape_char: None,
            field_max_size: usize::MAX,
            num_cols: 0,
            idx: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CsvError {
    pub id: String,
    pub message: String,
    pub idx: Vec<usize>,
}

pub type CsvErrors = BTreeMap<String, CsvError>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParseResult {
    pub rows: Vec<Vec<String>>,
    pub tail: String,
    pub num_cols: usize,
    pub idx: usize,
    pub errors: CsvErrors,
}

type ParserFn = dyn Fn(&str, &ParserOptions, bool) -> Result<ParseResult> + Send + Sync;

/// A parser function: `(text, options, is_flushing) -> ParseResult`.
/// Defaults to [`csv_quoted_parser`].
#[derive(Clone)]
pub struct CsvParser(pub Arc<ParserFn>);

impl CsvParser {
    pub fn new(
        f: impl Fn(&str, &ParserOptions, bool) -> Result<ParseResult> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(f))
    }
}

impl Default for CsvParser {
    fn default() -> Self {
        Self::new(csv_quoted_parser)
    }
}

impl fmt::Debug for CsvParser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CsvParser(..)")
    }
}

fn resolve_parser_options(
    delimiter_char: &Option<LazyString>,
    newline_char: &Option<LazyString>,
    quote_char: &Option<LazyString>,
    escape_char: &Option<LazyString>,
    field_max_size: usize,
) -> ParserOptions {
    let resolve = |o: &Option<LazyString>, default: &str| {
        o.as_ref()
            .and_then(LazyString::resolve)
            .unwrap_or_else(|| default.to_string())
    };
    let quote = resolve(quote_char, DEFAULT_QUOTE);
    ParserOptions {
        delimiter_char: resolve(delimiter_char, DEFAULT_DELIMITER),
        newline_char: resolve(newline_char, DEFAULT_NEWLINE),
        escape_char: Some(resolve(escape_char, quote.as_str())),
        quote_char: quote,
        field_max_size,
        num_cols: 0,
        idx: 0,
    }
}

// *** Helpers *** //

fn find(text: &str, pat: &str, from: usize) -> Option<usize> {
    text.get(from..)?.find(pat).map(|i| i + from)
}

fn strip_bom(text: &str) -> &str {
    text.strip_prefix('\u{feff}').unwrap_or(text)
}

fn row_value(row: Vec<String>) -> Value {
    Value::Array(row.into_iter().map(Value::String).collect())
}

fn errors_value(errors: &CsvErrors) -> Value {
    Value::Object(
        errors
            .iter()
            .map(|(k, e)| {
                (
                    k.clone(),
                    json!({"id": e.id, "message": e.message, "idx": e.idx}),
                )
            })
            .collect(),
    )
}

fn track_error(result: &StreamResult, id: &str, message: &str, idx: usize) {
    result.update(|v| {
        if v.get(id).is_none() {
            v[id] = json!({"id": id, "message": message, "idx": []});
        }
        if let Some(list) = v[id]["idx"].as_array_mut() {
            list.push(json!(idx));
        }
    });
}

// True when the quote at `idx` is preceded by an ODD run of `escape`
// (scanning no further back than `lower`).
fn quote_is_escaped(text: &str, idx: usize, lower: usize, escape: &str) -> bool {
    if escape.is_empty() {
        return false;
    }
    let mut escaped = false;
    let mut k = idx;
    while k >= lower + escape.len() && text[..k].ends_with(escape) {
        escaped = !escaped;
        k -= escape.len();
    }
    escaped
}

// Index of the newline ending row 0 (quote/escape aware), or None when no
// complete row is buffered.
fn find_row_end(text: &str, options: &ParserOptions) -> Option<usize> {
    let delimiter = options.delimiter_char.as_str();
    let newline = options.newline_char.as_str();
    let quote = options.quote_char.as_str();
    let escape = options.escape_char.as_deref().unwrap_or(quote);
    let mut pos = 0;
    let mut next_nl = find(text, newline, 0);
    loop {
        // `pos` is always a field start here, so a quote opens a quoted field.
        if !quote.is_empty() && text[pos..].starts_with(quote) {
            let content_start = pos + quote.len();
            let mut close = find(text, quote, content_start);
            while let Some(i) = close.filter(|&i| quote_is_escaped(text, i, content_start, escape))
            {
                close = find(text, quote, i + quote.len());
            }
            pos = close? + quote.len();
            continue;
        }
        if next_nl.is_some_and(|n| n < pos) {
            next_nl = find(text, newline, pos);
        }
        // A delimiter that is a prefix of the newline wins the tie.
        match (find(text, delimiter, pos), next_nl) {
            (Some(d), Some(n)) if d <= n => pos = d + delimiter.len(),
            _ => return next_nl,
        }
    }
}

// Inverse of the formatter's custom escaping: each escape consumes the next
// char literally. A trailing escape with nothing after it is kept.
fn unescape_custom(text: &str, escape: &str) -> String {
    if escape.is_empty() {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut start = 0;
    while let Some(i) = find(text, escape, start) {
        let after = i + escape.len();
        let Some(c) = text[after..].chars().next() else {
            break;
        };
        out.push_str(&text[start..i]);
        out.push(c);
        start = after + c.len_utf8();
    }
    out.push_str(&text[start..]);
    out
}

// *** Detect *** //

#[derive(Clone, Debug, Default)]
pub struct CsvDetectDelimitersOptions {
    pub result_key: Option<String>,
}

fn detect_delimiters(text: &str) -> Option<[&'static str; 3]> {
    let text = strip_bom(text);
    let end = text.find(['\r', '\n'])?;
    let newline = if text[end..].starts_with("\r\n") {
        "\r\n"
    } else if text[end..].starts_with('\r') {
        "\r"
    } else {
        "\n"
    };
    let header = &text[..end];
    let delimiter = ["\t", "|", ";", ","]
        .into_iter()
        .find(|d| header.contains(*d))
        .unwrap_or(DEFAULT_DELIMITER);

    // A char is the quote char only when it BRACKETS a field: it opens at a
    // field start and a matching quote closes right before a field end.
    let bytes = text.as_bytes();
    let field_start = |i: usize| {
        i == 0 || text[..i].ends_with(delimiter) || matches!(bytes[i - 1], b'\r' | b'\n')
    };
    let field_end = |i: usize| {
        i >= text.len() || text[i..].starts_with(delimiter) || matches!(bytes[i], b'\r' | b'\n')
    };
    let brackets = |q: &str| {
        let mut open = find(text, q, 0);
        while let Some(i) = open {
            if field_start(i) {
                let mut close = find(text, q, i + 1);
                while let Some(c) = close {
                    if field_end(c + 1) {
                        return true;
                    }
                    close = find(text, q, c + 1);
                }
            }
            open = find(text, q, i + 1);
        }
        false
    };
    let quote = ["\"", "'"]
        .into_iter()
        .find(|q| brackets(q))
        .unwrap_or(DEFAULT_QUOTE);
    Some([delimiter, newline, quote])
}

/// Pass text through unchanged while detecting the delimiter, newline and
/// quote characters from the first line. Result key defaults to
/// "csvDetectDelimiters"; value is `{delimiterChar, newlineChar, quoteChar, escapeChar}`.
pub fn csv_detect_delimiters_stream(
    input: DataStream<String>,
    options: CsvDetectDelimitersOptions,
) -> (DataStream<String>, StreamResult) {
    let key = options
        .result_key
        .unwrap_or_else(|| "csvDetectDelimiters".into());
    let result = StreamResult::new(
        key,
        json!({"delimiterChar": null, "newlineChar": null, "quoteChar": null, "escapeChar": null}),
    );
    let r = result.clone();
    let detect = move |text: &str| match detect_delimiters(text) {
        Some([delimiter, newline, quote]) => {
            r.set(json!({
                "delimiterChar": delimiter,
                "newlineChar": newline,
                "quoteChar": quote,
                "escapeChar": quote,
            }));
            true
        }
        None => false,
    };
    let stream: DataStream<String> = Box::pin(try_stream! {
        let mut input = input;
        let mut buffer = String::new();
        let mut detected = false;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            if detected {
                yield chunk;
                continue;
            }
            // Detection succeeds once a complete first line is buffered.
            buffer.push_str(&chunk);
            if detect(buffer.as_str()) {
                detected = true;
                yield std::mem::take(&mut buffer);
            }
        }
        if !detected && !buffer.is_empty() {
            // Detect from whatever was buffered (may be a partial line).
            detect(buffer.as_str());
            yield buffer;
        }
    });
    (stream, result)
}

#[derive(Clone, Debug, Default)]
pub struct CsvDetectHeaderOptions {
    pub parser: CsvParser,
    pub delimiter_char: Option<LazyString>,
    pub newline_char: Option<LazyString>,
    pub quote_char: Option<LazyString>,
    pub escape_char: Option<LazyString>,
    pub result_key: Option<String>,
}

// Parse the header row into `result` and return the data after it, if any.
fn process_header(
    text: &str,
    row_end: Option<usize>,
    options: &ParserOptions,
    parser: &CsvParser,
    result: &StreamResult,
) -> Result<Option<String>> {
    let header_chunk = row_end.map_or(text, |end| &text[..end]);
    let parsed = (parser.0)(header_chunk, options, true)?;
    let header = parsed.rows.into_iter().next().unwrap_or_default();
    result.update(|v| v["header"] = json!(header));
    Ok(row_end
        .map(|end| text[end + options.newline_char.len()..].to_string())
        .filter(|rest| !rest.is_empty()))
}

/// Remove the header row from the text, storing it as the result. Result key
/// defaults to "csvDetectHeader"; value is `{header: [...]}`.
pub fn csv_detect_header_stream(
    input: DataStream<String>,
    options: CsvDetectHeaderOptions,
) -> (DataStream<String>, StreamResult) {
    let key = options
        .result_key
        .clone()
        .unwrap_or_else(|| "csvDetectHeader".into());
    let result = StreamResult::new(key, json!({}));
    let r = result.clone();
    let stream: DataStream<String> = Box::pin(try_stream! {
        let mut input = input;
        let mut buffer = String::new();
        let mut detected = false;
        let mut resolved: Option<ParserOptions> = None;
        let resolve = || resolve_parser_options(
            &options.delimiter_char,
            &options.newline_char,
            &options.quote_char,
            &options.escape_char,
            usize::MAX,
        );
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            if detected {
                yield chunk;
                continue;
            }
            buffer.push_str(&chunk);
            let opts = resolved.get_or_insert_with(resolve);
            let text = strip_bom(&buffer);
            // Process as soon as a complete header row (quote aware) is buffered.
            if let Some(end) = find_row_end(text, opts) {
                detected = true;
                if let Some(rest) = process_header(text, Some(end), opts, &options.parser, &r)? {
                    yield rest;
                }
            }
        }
        if !detected {
            // A partial header row (or nothing) is finalized here.
            let opts = resolved.get_or_insert_with(resolve);
            let text = strip_bom(&buffer);
            let end = find_row_end(text, opts);
            if let Some(rest) = process_header(text, end, opts, &options.parser, &r)? {
                yield rest;
            }
        }
    });
    (stream, result)
}

// *** Parsers *** //

fn field_too_big(len: usize, max: usize) -> Error {
    format!("CSV field size ({len}) exceeds fieldMaxSize ({max} bytes)").into()
}

/// Quote-aware parser. Returns complete rows and the unparsed `tail`; on
/// `is_flushing` the tail is parsed too and an unterminated quote is recorded
/// as an `UnterminatedQuote` error.
pub fn csv_quoted_parser(
    text: &str,
    options: &ParserOptions,
    is_flushing: bool,
) -> Result<ParseResult> {
    let delimiter = options.delimiter_char.as_str();
    let newline = options.newline_char.as_str();
    let quote = options.quote_char.as_str();
    let escape = options.escape_char.as_deref().unwrap_or(quote);
    let escape_is_quote = escape == quote;
    let escaped_quote = format!("{escape}{quote}");
    let unescape = |s: &str| {
        if escape_is_quote {
            s.replace(&escaped_quote, quote)
        } else {
            unescape_custom(s, escape)
        }
    };

    let len = text.len();
    let mut out = ParseResult {
        num_cols: options.num_cols,
        idx: options.idx,
        ..Default::default()
    };
    let mut row_start = 0;
    let mut field_start = 0;
    let mut fields: Vec<String> = Vec::new();
    let mut pos = 0;
    let mut last_was_delimiter = false;
    // First delimiter/newline at or after pos, cached so each is found once.
    let mut next_nl = find(text, newline, 0);
    let mut next_delim = find(text, delimiter, 0);

    while pos < len {
        // The loop is only entered at a field start, so a quote here opens a
        // quoted field (mid-field quotes are literal).
        if !quote.is_empty() && text[pos..].starts_with(quote) {
            last_was_delimiter = false;
            pos += quote.len();
            let content_start = pos;
            let mut close = find(text, quote, pos);
            if escape_is_quote {
                // Skip escaped "" pairs.
                while let Some(i) = close.filter(|&i| text[i + quote.len()..].starts_with(quote)) {
                    close = find(text, quote, i + 2 * quote.len());
                }
            } else {
                // A quote is escaped only after an odd run of escape chars.
                while let Some(i) =
                    close.filter(|&i| quote_is_escaped(text, i, content_start, escape))
                {
                    close = find(text, quote, i + quote.len());
                }
            }

            let Some(close) = close else {
                // Unterminated quote
                if is_flushing {
                    out.errors.insert(
                        "UnterminatedQuote".into(),
                        CsvError {
                            id: "UnterminatedQuote".into(),
                            message: "Unterminated quoted field".into(),
                            idx: vec![out.idx],
                        },
                    );
                    fields.push(unescape(&text[content_start..]));
                    if out.num_cols == 0 {
                        out.num_cols = fields.len();
                    }
                    out.rows.push(fields);
                    out.idx += 1;
                } else {
                    out.tail = text[row_start..].to_string();
                }
                return Ok(out);
            };

            let field = unescape(&text[content_start..close]);
            if field.len() > options.field_max_size {
                return Err(field_too_big(field.len(), options.field_max_size));
            }
            pos = close + quote.len();
            fields.push(field);

            // Post-quote dispatch: delimiter, newline, or garbage / end of input.
            if text[pos..].starts_with(delimiter) {
                pos += delimiter.len();
                field_start = pos;
                last_was_delimiter = true;
            } else if text[pos..].starts_with(newline) {
                if out.num_cols == 0 {
                    out.num_cols = fields.len();
                }
                out.rows.push(std::mem::take(&mut fields));
                out.idx += 1;
                pos += newline.len();
                row_start = pos;
                field_start = pos;
                last_was_delimiter = false;
            } else {
                field_start = pos;
            }
            continue;
        }

        // Unquoted field
        last_was_delimiter = false;
        if next_nl.is_some_and(|n| n < pos) {
            next_nl = find(text, newline, pos);
        }
        if next_delim.is_some_and(|d| d < pos) {
            next_delim = find(text, delimiter, pos);
        }
        if let Some(d) = next_delim.filter(|&d| !matches!(next_nl, Some(n) if n < d)) {
            // Terminated by a delimiter (which wins a tie with the newline).
            fields.push(text[field_start..d].to_string());
            pos = d + delimiter.len();
            field_start = pos;
            last_was_delimiter = true;
            continue;
        }
        if let Some(n) = next_nl {
            fields.push(text[field_start..n].to_string());
            if out.num_cols == 0 {
                out.num_cols = fields.len();
            }
            out.rows.push(std::mem::take(&mut fields));
            out.idx += 1;
            pos = n + newline.len();
            row_start = pos;
            field_start = pos;
            continue;
        }
        break;
    }

    if !is_flushing {
        out.tail = text[row_start..].to_string();
        return Ok(out);
    }
    // Flushing: emit any trailing field or the empty field of a dangling delimiter.
    if field_start < len {
        fields.push(text[field_start..].to_string());
    } else if last_was_delimiter {
        fields.push(String::new());
    }
    if !fields.is_empty() {
        if out.num_cols == 0 {
            out.num_cols = fields.len();
        }
        out.rows.push(fields);
        out.idx += 1;
    }
    Ok(out)
}

/// Fast parser for input with no quoted fields: splits on newline then delimiter.
pub fn csv_unquoted_parser(
    text: &str,
    options: &ParserOptions,
    is_flushing: bool,
) -> Result<ParseResult> {
    let delimiter = options.delimiter_char.as_str();
    let newline = options.newline_char.as_str();
    let mut out = ParseResult {
        num_cols: options.num_cols,
        idx: options.idx,
        ..Default::default()
    };
    let push = |out: &mut ParseResult, line: &str| {
        let fields: Vec<String> = line.split(delimiter).map(String::from).collect();
        if out.num_cols == 0 {
            out.num_cols = fields.len();
        }
        out.rows.push(fields);
        out.idx += 1;
    };
    let mut pos = 0;
    while let Some(n) = find(text, newline, pos) {
        push(&mut out, &text[pos..n]);
        pos = n + newline.len();
    }
    if pos < text.len() {
        if is_flushing {
            push(&mut out, &text[pos..]);
        } else {
            out.tail = text[pos..].to_string();
        }
    }
    Ok(out)
}

// *** Parse *** //

#[derive(Clone, Debug, Default)]
pub struct CsvParseOptions {
    pub parser: CsvParser,
    /// Defaults to 16MB. Text buffered beyond twice this is an error.
    pub field_max_size: Option<usize>,
    pub delimiter_char: Option<LazyString>,
    pub newline_char: Option<LazyString>,
    pub quote_char: Option<LazyString>,
    pub escape_char: Option<LazyString>,
    pub result_key: Option<String>,
}

fn merge_errors(errors: &mut CsvErrors, incoming: CsvErrors) {
    for (id, error) in incoming {
        match errors.get_mut(&id) {
            Some(existing) => existing.idx.extend(error.idx),
            None => {
                errors.insert(id, error);
            }
        }
    }
}

/// Parse CSV text into rows (`Value::Array` of strings), buffering partial rows
/// across chunks. Result key defaults to "csvErrors"; value maps error id to
/// `{id, message, idx}`.
pub fn csv_parse_stream(
    input: DataStream<String>,
    options: CsvParseOptions,
) -> (DataStream<Value>, StreamResult) {
    let key = options
        .result_key
        .clone()
        .unwrap_or_else(|| "csvErrors".into());
    let result = StreamResult::new(key, json!({}));
    let r = result.clone();
    let field_max_size = options.field_max_size.unwrap_or(DEFAULT_FIELD_MAX_SIZE);
    let stream: DataStream<Value> = Box::pin(try_stream! {
        let mut input = input;
        let parser = options.parser.0.clone();
        let mut resolved: Option<ParserOptions> = None;
        let mut buffer = String::new();
        let mut errors = CsvErrors::new();
        // Lazy options are resolved once the first chunk arrives (upstream has run).
        let resolve = || resolve_parser_options(
            &options.delimiter_char,
            &options.newline_char,
            &options.quote_char,
            &options.escape_char,
            field_max_size,
        );
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let ctx = resolved.get_or_insert_with(resolve);
            buffer.push_str(&chunk);
            if buffer.len() > ctx.field_max_size.saturating_mul(2) {
                Err::<(), Error>(format!(
                    "CSV buffer size ({}) exceeds safety limit, likely unterminated quoted field",
                    buffer.len()
                ).into())?;
            }
            let parsed = parser(buffer.as_str(), &*ctx, false)?;
            ctx.num_cols = parsed.num_cols;
            ctx.idx = parsed.idx;
            buffer = parsed.tail;
            if !parsed.errors.is_empty() {
                merge_errors(&mut errors, parsed.errors);
                r.set(errors_value(&errors));
            }
            for row in parsed.rows {
                yield row_value(row);
            }
        }
        let ctx = resolved.get_or_insert_with(resolve);
        if !buffer.is_empty() {
            let parsed = parser(buffer.as_str(), &*ctx, true)?;
            if !parsed.errors.is_empty() {
                merge_errors(&mut errors, parsed.errors);
                r.set(errors_value(&errors));
            }
            for row in parsed.rows {
                yield row_value(row);
            }
        }
    });
    (stream, result)
}

// *** Row filters *** //

fn row_len(row: &Value) -> usize {
    row.as_array().map_or(0, Vec::len)
}

#[derive(Clone, Debug, Default)]
pub struct CsvRemoveMalformedRowsOptions {
    /// Expected columns; defaults to the first row's field count.
    pub headers: Option<Keys>,
    pub on_error_enqueue: bool,
    pub result_key: Option<String>,
}

/// Drop rows whose field count differs from `headers` (or the first row).
/// Result key defaults to "csvRemoveMalformedRows".
pub fn csv_remove_malformed_rows_stream(
    input: DataStream<Value>,
    options: CsvRemoveMalformedRowsOptions,
) -> (DataStream<Value>, StreamResult) {
    let key = options
        .result_key
        .unwrap_or_else(|| "csvRemoveMalformedRows".into());
    let result = StreamResult::new(key, json!({}));
    let r = result.clone();
    let headers = options.headers;
    let on_error_enqueue = options.on_error_enqueue;
    let mut expected: Option<usize> = None;
    let mut idx = 0;
    let stream = create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            let len = row_len(&chunk);
            let expected = *expected
                .get_or_insert_with(|| headers.as_ref().map_or(len, |h| object_headers(h).len()));
            if len != expected {
                track_error(
                    &r,
                    "MalformedRow",
                    "Row has incorrect number of fields",
                    idx,
                );
                if on_error_enqueue {
                    enqueue.push(chunk);
                }
            } else {
                enqueue.push(chunk);
            }
            idx += 1;
            Ok(())
        },
        noop_flush,
    );
    (stream, result)
}

#[derive(Clone, Debug, Default)]
pub struct CsvRemoveEmptyRowsOptions {
    pub on_error_enqueue: bool,
    pub result_key: Option<String>,
}

/// Drop rows where every field is "" (including zero-length rows).
/// Result key defaults to "csvRemoveEmptyRows".
pub fn csv_remove_empty_rows_stream(
    input: DataStream<Value>,
    options: CsvRemoveEmptyRowsOptions,
) -> (DataStream<Value>, StreamResult) {
    let key = options
        .result_key
        .unwrap_or_else(|| "csvRemoveEmptyRows".into());
    let result = StreamResult::new(key, json!({}));
    let r = result.clone();
    let on_error_enqueue = options.on_error_enqueue;
    let mut idx = 0;
    let stream = create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            let empty = match chunk.as_array() {
                Some(fields) => fields.iter().all(|f| f.as_str() == Some("")),
                None => true,
            };
            if empty {
                track_error(&r, "EmptyRow", "Row is empty", idx);
                if on_error_enqueue {
                    enqueue.push(chunk);
                }
            } else {
                enqueue.push(chunk);
            }
            idx += 1;
            Ok(())
        },
        noop_flush,
    );
    (stream, result)
}

// *** Coerce *** //

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CsvCoerceType {
    Number,
    Boolean,
    Null,
    /// ISO 8601 string, normalised to `toISOString()` form (UTC).
    Date,
    Json,
    /// Leave the value unchanged.
    String,
}

#[derive(Clone, Debug, Default)]
pub struct CsvCoerceValuesOptions {
    /// Explicit column types; other columns are auto-coerced.
    pub columns: HashMap<String, CsvCoerceType>,
    pub result_key: Option<String>,
}

// `^-?\d+(\.\d+)?([eE][+-]?\d+)?$`
fn is_number_like(s: &str) -> bool {
    let b = s.as_bytes();
    let digits = |start: usize| {
        let mut i = start;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        (i > start).then_some(i)
    };
    let Some(mut i) = digits(usize::from(b.first() == Some(&b'-'))) else {
        return false;
    };
    if b.get(i) == Some(&b'.') {
        match digits(i + 1) {
            Some(j) => i = j,
            None => return false,
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(b.get(i + 1), Some(b'+' | b'-')));
        match digits(i + 1 + sign) {
            Some(j) => i = j,
            None => return false,
        }
    }
    i == b.len()
}

// JS `Number(string)`: trimmed decimal literal, 0x/0o/0b integers, Infinity,
// and "" -> 0. Anything else is NaN.
fn js_string_to_number(s: &str) -> f64 {
    let t = s.trim_matches(|c: char| c.is_whitespace() || c == '\u{feff}');
    if t.is_empty() {
        return 0.0;
    }
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0o", 8),
        ("0O", 8),
        ("0b", 2),
        ("0B", 2),
    ] {
        if let Some(digits) = t.strip_prefix(prefix) {
            if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
                return f64::NAN;
            }
            return digits.chars().fold(0.0, |n, c| {
                n * f64::from(radix) + f64::from(c.to_digit(radix).unwrap_or(0))
            });
        }
    }
    match t {
        "Infinity" | "+Infinity" => return f64::INFINITY,
        "-Infinity" => return f64::NEG_INFINITY,
        _ => {}
    }
    let b = t.as_bytes();
    let mut i = usize::from(matches!(b[0], b'+' | b'-'));
    let mut mantissa = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        mantissa += 1;
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            mantissa += 1;
        }
    }
    if mantissa == 0 {
        return f64::NAN;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return f64::NAN;
        }
    }
    if i != b.len() {
        return f64::NAN;
    }
    t.parse().unwrap_or(f64::NAN)
}

// Integral values become JSON integers so they compare equal to `json!(1)`.
// JSON cannot hold Infinity/NaN, so those give None.
fn number_value(n: f64) -> Option<Value> {
    if !n.is_finite() {
        return None;
    }
    if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 && !(n == 0.0 && n.is_sign_negative())
    {
        return Some(json!(n as i64));
    }
    serde_json::Number::from_f64(n).map(Value::Number)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

// JS `Date.prototype.toISOString` for epoch milliseconds.
fn iso_string(ms: i64) -> String {
    let (y, mo, d) = civil_from_days(ms.div_euclid(86_400_000));
    let t = ms.rem_euclid(86_400_000);
    let year = match y {
        0..=9999 => format!("{y:04}"),
        y if y < 0 => format!("-{:06}", -y),
        y => format!("+{y:06}"),
    };
    format!(
        "{year}-{mo:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        t / 3_600_000,
        t / 60_000 % 60,
        t / 1000 % 60,
        t % 1000
    )
}

// `^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$`,
// validated like V8's Date parser. A missing offset is read as UTC.
fn parse_iso_date(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let num = |from: usize, n: usize| -> Option<i64> {
        let part = b.get(from..from + n)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| part.iter().fold(0, |a, d| a * 10 + i64::from(d - b'0')))
    };
    let at = |i: usize, c: u8| b.get(i) == Some(&c);
    let year = num(0, 4)?;
    let month = num(5, 2).filter(|_| at(4, b'-'))?;
    let day = num(8, 2).filter(|_| at(7, b'-'))?;
    let (mut hour, mut minute, mut second, mut ms, mut offset) = (0, 0, 0, 0, 0);
    let mut i = 10;
    if i < b.len() {
        if !at(i, b'T') && !at(i, b' ') {
            return None;
        }
        hour = num(i + 1, 2)?;
        minute = num(i + 4, 2).filter(|_| at(i + 3, b':'))?;
        i += 6;
        if at(i, b':') {
            second = num(i + 1, 2)?;
            i += 3;
            if at(i, b'.') {
                let start = i + 1;
                i = start;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                if i == start {
                    return None;
                }
                let frac = &s[start..i.min(start + 3)];
                ms = format!("{frac:0<3}").parse().ok()?;
            }
        }
        match b.get(i) {
            None => {}
            Some(b'Z') => i += 1,
            Some(&sign @ (b'+' | b'-')) => {
                let oh = num(i + 1, 2)?;
                i += 3;
                if at(i, b':') {
                    i += 1;
                }
                let om = num(i, 2)?;
                i += 2;
                if oh > 23 || om > 59 {
                    return None;
                }
                offset = (oh * 60 + om) * if sign == b'-' { -1 } else { 1 };
            }
            _ => return None,
        }
        if i != b.len() {
            return None;
        }
    }
    let valid = (1..=12).contains(&month)
        && (1..=31).contains(&day)
        && minute <= 59
        && second <= 59
        && (hour < 24 || (hour == 24 && minute == 0 && second == 0 && ms == 0));
    if !valid {
        return None;
    }
    // Days past the end of the month roll over, as in V8 ("2024-02-30" -> Mar 1).
    let days = days_from_civil(year, month, 1) + day - 1;
    let total = days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1000 + ms
        - offset * 60_000;
    Some(iso_string(total))
}

fn auto_coerce(val: Value) -> Value {
    let Value::String(s) = val else { return val };
    if s.is_empty() {
        return Value::Null;
    }
    let lower = s.to_lowercase();
    if lower == "true" {
        return Value::Bool(true);
    }
    if lower == "false" {
        return Value::Bool(false);
    }
    if is_number_like(&s) {
        if let Some(n) = number_value(js_string_to_number(&s)) {
            return n;
        }
    }
    if let Some(date) = parse_iso_date(&s) {
        return Value::String(date);
    }
    // JSON only for '{' or '[' so values like "null" are not parsed.
    if s.starts_with('{') || s.starts_with('[') {
        if let Ok(parsed) = serde_json::from_str(&s) {
            return parsed;
        }
    }
    Value::String(s)
}

fn coerce_to_type(val: Value, kind: CsvCoerceType) -> Value {
    match kind {
        CsvCoerceType::Number => {
            let n = match &val {
                Value::String(s) if s.is_empty() => None,
                Value::String(s) => Some(js_string_to_number(s)),
                Value::Bool(b) => Some(f64::from(u8::from(*b))),
                Value::Null => Some(0.0),
                // Numbers stay as they are; arrays/objects are NaN.
                _ => Some(f64::NAN),
            };
            match n {
                None => Value::Null,
                Some(n) => number_value(n).unwrap_or(val),
            }
        }
        CsvCoerceType::Boolean => Value::Bool(match &val {
            Value::String(s) => s.to_lowercase() == "true",
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0 && !f.is_nan()),
            Value::Null => false,
            _ => true,
        }),
        CsvCoerceType::Null => Value::Null,
        CsvCoerceType::Date => match val.as_str().and_then(parse_iso_date) {
            Some(date) => Value::String(date),
            None => val,
        },
        CsvCoerceType::Json => match val.as_str().and_then(|s| serde_json::from_str(s).ok()) {
            Some(parsed) => parsed,
            None => val,
        },
        CsvCoerceType::String => val,
    }
}

/// Convert string values to numbers, booleans, null, dates and JSON, either
/// automatically or per `columns`. Result key defaults to "csvCoerceValues".
pub fn csv_coerce_values_stream(
    input: DataStream<Value>,
    options: CsvCoerceValuesOptions,
) -> (DataStream<Value>, StreamResult) {
    let key = options
        .result_key
        .unwrap_or_else(|| "csvCoerceValues".into());
    let result = StreamResult::new(key, json!({}));
    let columns = options.columns;
    let stream = create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            let Value::Object(map) = chunk else {
                enqueue.push(chunk);
                return Ok(());
            };
            let coerced: Map<String, Value> = map
                .into_iter()
                .map(|(k, v)| {
                    let v = match columns.get(&k) {
                        Some(&kind) => coerce_to_type(v, kind),
                        None => auto_coerce(v),
                    };
                    (k, v)
                })
                .collect();
            enqueue.push(Value::Object(coerced));
            Ok(())
        },
        noop_flush,
    );
    (stream, result)
}

// *** Formatting *** //

#[derive(Clone, Debug, Default)]
pub struct CsvInjectHeaderOptions {
    pub header: Vec<String>,
}

/// Emit `header` before the first row (nothing for empty input).
pub fn csv_inject_header_stream(
    input: DataStream<Value>,
    options: CsvInjectHeaderOptions,
) -> DataStream<Value> {
    let mut header = Some(json!(options.header));
    create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            if let Some(header) = header.take() {
                enqueue.push(header);
            }
            enqueue.push(chunk);
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Clone, Debug, Default)]
pub struct CsvFormatOptions {
    pub delimiter_char: Option<String>,
    pub newline_char: Option<String>,
    pub quote_char: Option<String>,
    /// Defaults to `quote_char`.
    pub escape_char: Option<String>,
}

struct Formatter {
    delimiter: String,
    quote: String,
    escape: String,
    escaped_quote: String,
    escaped_escape: String,
}

// JS `String(number)`.
fn js_number_string(n: &serde_json::Number) -> String {
    let Some(f) = n.as_f64().filter(|_| n.is_f64()) else {
        return n.to_string();
    };
    let a = f.abs();
    if a == 0.0 {
        return "0".into();
    }
    if a >= 1e21 || a < 1e-6 {
        let s = format!("{f:e}");
        return match s.split_once('e') {
            Some((m, e)) if !e.starts_with('-') => format!("{m}e+{e}"),
            _ => s,
        };
    }
    format!("{f}")
}

impl Formatter {
    // Quote fields starting with a formula/whitespace/BOM trigger, ending with
    // a space, or containing the delimiter, the quote char, CR or LF.
    fn needs_quote(&self, v: &str) -> bool {
        matches!(
            v.chars().next(),
            Some('=' | '+' | '-' | '@' | ' ' | '\u{feff}')
        ) || v.ends_with(' ')
            || v.contains(self.delimiter.as_str())
            || v.contains(self.quote.as_str())
            || v.contains('\r')
            || v.contains('\n')
    }

    fn field(&self, raw: &Value) -> String {
        let val = match raw {
            Value::Null => return String::new(),
            Value::String(s) => s.clone(),
            Value::Number(n) => js_number_string(n),
            Value::Bool(b) => b.to_string(),
            other => other.to_string(),
        };
        if !self.needs_quote(&val) {
            return val;
        }
        let val = if self.escape == self.quote {
            val
        } else {
            val.replace(&self.escape, &self.escaped_escape)
        };
        format!(
            "{q}{}{q}",
            val.replace(&self.quote, &self.escaped_quote),
            q = self.quote
        )
    }

    fn row(&self, row: &Value) -> String {
        let fields: Vec<String> = row
            .as_array()
            .map(|fields| fields.iter().map(|f| self.field(f)).collect())
            .unwrap_or_default();
        fields.join(self.delimiter.as_str())
    }
}

/// Format rows (arrays of values) as CSV text, 64 rows per output chunk.
pub fn csv_format_stream(
    input: DataStream<Value>,
    options: CsvFormatOptions,
) -> DataStream<String> {
    let quote = options.quote_char.unwrap_or_else(|| DEFAULT_QUOTE.into());
    let escape = options.escape_char.unwrap_or_else(|| quote.clone());
    let newline = options
        .newline_char
        .unwrap_or_else(|| DEFAULT_NEWLINE.into());
    let format = Formatter {
        delimiter: options
            .delimiter_char
            .unwrap_or_else(|| DEFAULT_DELIMITER.into()),
        escaped_quote: format!("{escape}{quote}"),
        escaped_escape: format!("{escape}{escape}"),
        quote,
        escape,
    };
    Box::pin(try_stream! {
        let mut input = input;
        let mut batch: Vec<String> = Vec::new();
        while let Some(chunk) = input.next().await {
            batch.push(format.row(&chunk?));
            if batch.len() >= 64 {
                yield batch.join(newline.as_str()) + &newline;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            yield batch.join(newline.as_str()) + &newline;
        }
    })
}

#[derive(Clone, Debug, Default)]
pub struct CsvHeadersOptions {
    pub headers: Keys,
}

fn object_headers(headers: &Keys) -> Vec<String> {
    match headers {
        Keys::List(keys) => keys.clone(),
        Keys::Lazy(f) => f(),
    }
}

/// Row array -> object keyed by `headers` (resolved on the first row).
pub fn csv_array_to_object(
    input: DataStream<Value>,
    options: CsvHeadersOptions,
) -> DataStream<Value> {
    let source = options.headers;
    let mut keys: Option<Vec<String>> = None;
    create_transform_stream(
        input,
        move |chunk: Value, enqueue: &mut Vec<Value>| {
            let keys = keys.get_or_insert_with(|| object_headers(&source));
            let value: Map<String, Value> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.clone(), chunk.get(i).cloned().unwrap_or(Value::Null)))
                .collect();
            enqueue.push(Value::Object(value));
            Ok(())
        },
        noop_flush,
    )
}

/// Object -> row array ordered by `headers`.
pub fn csv_object_to_array(
    input: DataStream<Value>,
    options: CsvHeadersOptions,
) -> DataStream<Value> {
    object_to_entries_stream(
        input,
        ObjectEntriesOptions {
            keys: options.headers,
        },
    )
}

#[cfg(test)]
mod tests;
