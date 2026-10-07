// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Commonly used stream patterns for async Rust, ported from `@datastream/core`.
//!
//! A stream is a [`DataStream<T>`]: a boxed `futures::Stream` of `Result<T>`.
//! Readables create one, transforms take one and return another, and
//! writables consume one. Pass-through streams that compute something (counts,
//! digests, error lists) also return a [`StreamResult`] that holds the value.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::try_stream;
pub use futures::{Stream, StreamExt, TryStreamExt};
pub use serde_json::{Map, Value};
use tokio::sync::mpsc;
pub use tokio_util::sync::CancellationToken;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type DataStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub const DEFAULT_CHUNK_SIZE: usize = 16_384;
pub const DEFAULT_HIGH_WATER_MARK: usize = 1024;

/// Chain stream functions left to right: `readable.pipe(transform).pipe(other)`.
pub trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}
impl<T> Pipe for T {}

// *** Results *** //

/// Shared, keyed value produced by a stream (the JS `stream.result()`).
#[derive(Clone, Debug)]
pub struct StreamResult {
    key: String,
    value: Arc<Mutex<Value>>,
}

impl StreamResult {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Arc::new(Mutex::new(value)),
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn get(&self) -> Value {
        self.lock().clone()
    }
    pub fn set(&self, value: Value) {
        *self.lock() = value;
    }
    pub fn update<R>(&self, f: impl FnOnce(&mut Value) -> R) -> R {
        f(&mut self.lock())
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Value> {
        // A panic while holding the lock leaves a value that is still usable.
        self.value.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Collect results into a map keyed by each result's key. Empty keys are skipped.
pub fn result(results: &[&StreamResult]) -> Map<String, Value> {
    let mut output = Map::new();
    for r in results {
        if !r.key.is_empty() {
            output.insert(r.key.clone(), r.get());
        }
    }
    output
}

/// Drain a stream to completion, then collect `results`.
pub async fn pipeline<T>(
    mut stream: DataStream<T>,
    results: &[&StreamResult],
) -> Result<Map<String, Value>> {
    while let Some(chunk) = stream.next().await {
        chunk?;
    }
    Ok(result(results))
}

// *** Collectors *** //

fn exceeds(name: &str, max: usize) -> Error {
    format!("{name} buffer exceeds maxBufferSize ({max})").into()
}

/// Collect every chunk. `max_buffer_size` limits the number of chunks.
pub async fn stream_to_array<T>(
    mut stream: DataStream<T>,
    max_buffer_size: Option<usize>,
) -> Result<Vec<T>> {
    let max = max_buffer_size.unwrap_or(usize::MAX);
    let mut value = Vec::new();
    while let Some(chunk) = stream.next().await {
        if value.len() >= max {
            return Err(exceeds("streamToArray", max));
        }
        value.push(chunk?);
    }
    Ok(value)
}

/// Concatenate string chunks. `max_buffer_size` limits the total bytes.
pub async fn stream_to_string<T: AsRef<str>>(
    mut stream: DataStream<T>,
    max_buffer_size: Option<usize>,
) -> Result<String> {
    let max = max_buffer_size.unwrap_or(usize::MAX);
    let mut value = String::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if value.len() + chunk.as_ref().len() > max {
            return Err(exceeds("streamToString", max));
        }
        value.push_str(chunk.as_ref());
    }
    Ok(value)
}

/// Concatenate byte chunks. `max_buffer_size` limits the total bytes.
pub async fn stream_to_buffer<T: AsRef<[u8]>>(
    mut stream: DataStream<T>,
    max_buffer_size: Option<usize>,
) -> Result<Vec<u8>> {
    let max = max_buffer_size.unwrap_or(usize::MAX);
    let mut value = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if value.len() + chunk.as_ref().len() > max {
            return Err(exceeds("streamToBuffer", max));
        }
        value.extend_from_slice(chunk.as_ref());
    }
    Ok(value)
}

/// Merge object chunks into one object (later keys win; non-objects are
/// ignored). `max_buffer_size` limits the number of chunks.
pub async fn stream_to_object(
    mut stream: DataStream<Value>,
    max_buffer_size: Option<usize>,
) -> Result<Map<String, Value>> {
    let max = max_buffer_size.unwrap_or(usize::MAX);
    let mut value = Map::new();
    let mut count = 0;
    while let Some(chunk) = stream.next().await {
        count += 1;
        if count > max {
            return Err(exceeds("streamToObject", max));
        }
        if let Value::Object(map) = chunk? {
            value.extend(map);
        }
    }
    Ok(value)
}

// *** Readable *** //

/// Readable from any iterator (arrays, vectors, ranges, ...).
pub fn create_readable_stream<I>(input: I) -> DataStream<I::Item>
where
    I: IntoIterator,
    I::IntoIter: Send + 'static,
    I::Item: Send + 'static,
{
    Box::pin(futures::stream::iter(input.into_iter().map(Ok)))
}

/// Readable from an existing infallible stream.
pub fn create_readable_stream_from_stream<S>(input: S) -> DataStream<S::Item>
where
    S: Stream + Send + 'static,
    S::Item: Send + 'static,
{
    Box::pin(input.map(Ok))
}

fn check_chunk_size(chunk_size: Option<usize>) -> Result<usize> {
    match chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE) {
        0 => Err("chunkSize must be a positive number".into()),
        size => Ok(size),
    }
}

/// Readable that emits `input` in chunks of `chunk_size` chars (default 16KB).
pub fn create_readable_stream_from_string(
    input: impl Into<String>,
    chunk_size: Option<usize>,
) -> Result<DataStream<String>> {
    let size = check_chunk_size(chunk_size)?;
    let input = input.into();
    let mut chunks = Vec::new();
    let mut chars = input.chars().peekable();
    while chars.peek().is_some() {
        chunks.push(chars.by_ref().take(size).collect::<String>());
    }
    Ok(create_readable_stream(chunks))
}

/// Readable that emits `input` in chunks of `chunk_size` bytes (default 16KB).
pub fn create_readable_stream_from_bytes(
    input: impl Into<Vec<u8>>,
    chunk_size: Option<usize>,
) -> Result<DataStream<Vec<u8>>> {
    let size = check_chunk_size(chunk_size)?;
    let chunks: Vec<Vec<u8>> = input.into().chunks(size).map(<[u8]>::to_vec).collect();
    Ok(create_readable_stream(chunks))
}

/// Push side of [`create_readable_channel`]. Dropping it ends the stream.
#[derive(Clone, Debug)]
pub struct ReadableSender<T> {
    tx: mpsc::Sender<Result<T>>,
    max_queue_size: usize,
}

impl<T: Send + 'static> ReadableSender<T> {
    /// Push without waiting. Errors when the queue is full.
    pub fn push(&self, chunk: T) -> Result<()> {
        self.tx.try_send(Ok(chunk)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => format!(
                "createReadableStream queue size ({}) exceeds limit ({})",
                self.max_queue_size, self.max_queue_size
            )
            .into(),
            mpsc::error::TrySendError::Closed(_) => "createReadableStream is closed".into(),
        })
    }
    /// Push, waiting for room in the queue (backpressure).
    pub async fn send(&self, chunk: T) -> Result<()> {
        self.tx
            .send(Ok(chunk))
            .await
            .map_err(|_| "createReadableStream is closed".into())
    }
    /// Fail the stream with `error`.
    pub async fn error(&self, error: Error) {
        let _ = self.tx.send(Err(error)).await;
    }
}

/// Readable you push values onto (JS `createReadableStream()` with no input).
pub fn create_readable_channel<T: Send + 'static>(
    high_water_mark: Option<usize>,
) -> (ReadableSender<T>, DataStream<T>) {
    let max_queue_size = high_water_mark.unwrap_or(DEFAULT_HIGH_WATER_MARK).max(1);
    let (tx, mut rx) = mpsc::channel(max_queue_size);
    let stream = Box::pin(async_stream::stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    });
    (ReadableSender { tx, max_queue_size }, stream)
}

// *** PassThrough *** //

/// Observe each chunk without changing it. `flush` runs once at the end.
pub fn create_pass_through_stream<T, F, G>(
    mut input: DataStream<T>,
    mut pass_through: F,
    mut flush: G,
) -> DataStream<T>
where
    T: Send + 'static,
    F: FnMut(&T) -> Result<()> + Send + 'static,
    G: FnMut() -> Result<()> + Send + 'static,
{
    Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            pass_through(&chunk)?;
            yield chunk;
        }
        flush()?;
    })
}

/// Async version of [`create_pass_through_stream`].
pub fn create_pass_through_stream_async<T, F, Fut, G, GFut>(
    mut input: DataStream<T>,
    mut pass_through: F,
    mut flush: G,
) -> DataStream<T>
where
    T: Send + 'static,
    F: FnMut(&T) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
    G: FnMut() -> GFut + Send + 'static,
    GFut: Future<Output = Result<()>> + Send,
{
    Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            pass_through(&chunk).await?;
            yield chunk;
        }
        flush().await?;
    })
}

/// No-op flush / final for the `create_*_stream` helpers.
pub fn noop() -> Result<()> {
    Ok(())
}

/// No-op flush for [`create_transform_stream`].
pub fn noop_flush<O>(_enqueue: &mut Vec<O>) -> Result<()> {
    Ok(())
}

// *** Transform *** //

/// Map each chunk to zero or more outputs by pushing onto `enqueue`.
/// `flush` can push trailing outputs once the input ends.
pub fn create_transform_stream<I, O, F, G>(
    mut input: DataStream<I>,
    mut transform: F,
    mut flush: G,
) -> DataStream<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: FnMut(I, &mut Vec<O>) -> Result<()> + Send + 'static,
    G: FnMut(&mut Vec<O>) -> Result<()> + Send + 'static,
{
    Box::pin(try_stream! {
        let mut enqueue = Vec::new();
        while let Some(chunk) = input.next().await {
            transform(chunk?, &mut enqueue)?;
            for out in enqueue.drain(..) {
                yield out;
            }
        }
        flush(&mut enqueue)?;
        for out in enqueue.drain(..) {
            yield out;
        }
    })
}

/// Async version of [`create_transform_stream`]: each call returns its outputs.
pub fn create_transform_stream_async<I, O, F, Fut, G, GFut>(
    mut input: DataStream<I>,
    mut transform: F,
    mut flush: G,
) -> DataStream<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: FnMut(I) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Vec<O>>> + Send,
    G: FnMut() -> GFut + Send + 'static,
    GFut: Future<Output = Result<Vec<O>>> + Send,
{
    Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            for out in transform(chunk?).await? {
                yield out;
            }
        }
        for out in flush().await? {
            yield out;
        }
    })
}

// *** Writable *** //

/// Consume every chunk with `write`, then call `final_`.
pub async fn create_writable_stream<T, F, G>(
    mut input: DataStream<T>,
    mut write: F,
    mut final_: G,
) -> Result<()>
where
    F: FnMut(T) -> Result<()>,
    G: FnMut() -> Result<()>,
{
    while let Some(chunk) = input.next().await {
        write(chunk?)?;
    }
    final_()
}

/// Async version of [`create_writable_stream`].
pub async fn create_writable_stream_async<T, F, Fut, G, GFut>(
    mut input: DataStream<T>,
    mut write: F,
    mut final_: G,
) -> Result<()>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Result<()>>,
    G: FnMut() -> GFut,
    GFut: Future<Output = Result<()>>,
{
    while let Some(chunk) = input.next().await {
        write(chunk?).await?;
    }
    final_().await
}

// *** Shared helpers *** //

/// Equality used by the "skip consecutive duplicates" streams. `serde_json`
/// values have no identity, so shallow and deep comparison are the same.
pub fn deep_equal(a: &Value, b: &Value) -> bool {
    a == b
}

/// Sleep for `duration`; errors with "Aborted" if `signal` is cancelled first.
pub async fn timeout(duration: Duration, signal: Option<&CancellationToken>) -> Result<()> {
    let aborted = || Error::from("Aborted");
    match signal {
        Some(signal) if signal.is_cancelled() => Err(aborted()),
        Some(signal) => tokio::select! {
            _ = signal.cancelled() => Err(aborted()),
            _ = tokio::time::sleep(duration) => Ok(()),
        },
        None => {
            tokio::time::sleep(duration).await;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn err<T>(msg: &str) -> Result<T> {
        Err(msg.into())
    }

    #[tokio::test]
    async fn stream_to_array_from_readable() {
        let input = vec![
            json!(true),
            json!(1),
            json!("a"),
            json!([1, 2]),
            json!({"a": "1"}),
        ];
        let output = stream_to_array(create_readable_stream(input.clone()), None)
            .await
            .unwrap();
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn stream_to_array_through_identity_transform() {
        let stream = create_transform_stream(
            create_readable_stream(["a", "b", "c"]),
            |chunk, enqueue| {
                enqueue.push(chunk);
                Ok(())
            },
            noop_flush,
        );
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            ["a", "b", "c"]
        );
    }

    #[tokio::test]
    async fn stream_to_array_preserves_null() {
        let input = vec![json!(1), Value::Null, json!(2)];
        let output = stream_to_array(create_readable_stream(input.clone()), None)
            .await
            .unwrap();
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn stream_to_array_max_buffer_size() {
        let at_limit = stream_to_array(create_readable_stream([1, 2, 3]), Some(3)).await;
        assert_eq!(at_limit.unwrap(), [1, 2, 3]);
        let over = stream_to_array(create_readable_stream([1, 2, 3]), Some(2)).await;
        assert_eq!(
            over.unwrap_err().to_string(),
            "streamToArray buffer exceeds maxBufferSize (2)"
        );
    }

    #[tokio::test]
    async fn stream_to_string_joins_and_limits() {
        let s = || create_readable_stream(["ab", "cd"]);
        assert_eq!(stream_to_string(s(), None).await.unwrap(), "abcd");
        assert_eq!(stream_to_string(s(), Some(4)).await.unwrap(), "abcd");
        let over = stream_to_string(s(), Some(3)).await.unwrap_err();
        assert_eq!(
            over.to_string(),
            "streamToString buffer exceeds maxBufferSize (3)"
        );
    }

    #[tokio::test]
    async fn stream_to_buffer_concats_and_limits() {
        let s = || create_readable_stream([b"a".to_vec(), b"bc".to_vec()]);
        assert_eq!(stream_to_buffer(s(), None).await.unwrap(), b"abc");
        assert_eq!(stream_to_buffer(s(), Some(3)).await.unwrap(), b"abc");
        let over = stream_to_buffer(s(), Some(2)).await.unwrap_err();
        assert_eq!(
            over.to_string(),
            "streamToBuffer buffer exceeds maxBufferSize (2)"
        );
    }

    #[tokio::test]
    async fn stream_to_object_merges_and_limits() {
        let s = || create_readable_stream([json!({"a": 1}), Value::Null, json!({"b": 2, "a": 3})]);
        let output = stream_to_object(s(), None).await.unwrap();
        assert_eq!(Value::Object(output), json!({"a": 3, "b": 2}));
        assert!(stream_to_object(s(), Some(3)).await.is_ok());
        let over = stream_to_object(s(), Some(2)).await.unwrap_err();
        assert_eq!(
            over.to_string(),
            "streamToObject buffer exceeds maxBufferSize (2)"
        );
    }

    #[tokio::test]
    async fn readable_from_string_chunks() {
        let s = create_readable_stream_from_string("abcde", Some(2)).unwrap();
        assert_eq!(stream_to_array(s, None).await.unwrap(), ["ab", "cd", "e"]);
        // Multi-byte chars are never split.
        let s = create_readable_stream_from_string("é€😀", Some(1)).unwrap();
        assert_eq!(stream_to_array(s, None).await.unwrap(), ["é", "€", "😀"]);
        let s = create_readable_stream_from_string("", None).unwrap();
        assert!(stream_to_array(s, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn readable_default_chunk_size() {
        let s =
            create_readable_stream_from_string("a".repeat(DEFAULT_CHUNK_SIZE + 1), None).unwrap();
        let chunks = stream_to_array(s, None).await.unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1], "a");
    }

    #[tokio::test]
    async fn readable_from_bytes_chunks() {
        let s = create_readable_stream_from_bytes(vec![1, 2, 3, 4, 5], Some(2)).unwrap();
        assert_eq!(
            stream_to_array(s, None).await.unwrap(),
            [vec![1, 2], vec![3, 4], vec![5]]
        );
    }

    #[test]
    fn readable_rejects_zero_chunk_size() {
        let e = create_readable_stream_from_string("a", Some(0))
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "chunkSize must be a positive number");
        assert!(create_readable_stream_from_bytes(vec![1], Some(0)).is_err());
    }

    #[tokio::test]
    async fn readable_from_stream() {
        let s = create_readable_stream_from_stream(futures::stream::iter([1, 2]));
        assert_eq!(stream_to_array(s, None).await.unwrap(), [1, 2]);
    }

    #[tokio::test]
    async fn readable_channel_push_and_limit() {
        let (tx, stream) = create_readable_channel(Some(2));
        tx.push("a").unwrap();
        tx.push("b").unwrap();
        let e = tx.push("c").unwrap_err();
        assert_eq!(
            e.to_string(),
            "createReadableStream queue size (2) exceeds limit (2)"
        );
        drop(tx);
        assert_eq!(stream_to_array(stream, None).await.unwrap(), ["a", "b"]);
    }

    #[tokio::test]
    async fn readable_channel_send_with_backpressure_and_error() {
        let (tx, stream) = create_readable_channel(Some(1));
        let producer = tokio::spawn(async move {
            for i in 0..5 {
                tx.send(i).await.unwrap();
            }
            tx.error("boom".into()).await;
        });
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "boom");
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn pass_through_observes_and_flushes() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let flushed = Arc::new(Mutex::new(false));
        let (s, f) = (seen.clone(), flushed.clone());
        let stream = create_pass_through_stream(
            create_readable_stream([1, 2, 3]),
            move |c| {
                s.lock().unwrap().push(*c);
                Ok(())
            },
            move || {
                *f.lock().unwrap() = true;
                Ok(())
            },
        );
        assert_eq!(stream_to_array(stream, None).await.unwrap(), [1, 2, 3]);
        assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
        assert!(*flushed.lock().unwrap());
    }

    #[tokio::test]
    async fn pass_through_errors() {
        let s = create_pass_through_stream(create_readable_stream([1]), |_| err("t"), noop);
        assert_eq!(stream_to_array(s, None).await.unwrap_err().to_string(), "t");
        let s = create_pass_through_stream(create_readable_stream([1]), |_| Ok(()), || err("f"));
        assert_eq!(stream_to_array(s, None).await.unwrap_err().to_string(), "f");
    }

    #[tokio::test]
    async fn pass_through_async() {
        let count = StreamResult::new("count", json!(0));
        let c = count.clone();
        let s = create_pass_through_stream_async(
            create_readable_stream([1, 2]),
            move |_| {
                let c = c.clone();
                async move {
                    tokio::task::yield_now().await;
                    c.update(|v| *v = json!(v.as_i64().unwrap() + 1));
                    Ok(())
                }
            },
            || async { err("flushed") },
        );
        assert_eq!(
            stream_to_array(s, None).await.unwrap_err().to_string(),
            "flushed"
        );
        assert_eq!(count.get(), json!(2));
    }

    #[tokio::test]
    async fn transform_maps_and_flushes() {
        let stream = create_transform_stream(
            create_readable_stream([1, 2]),
            |c, enqueue| {
                enqueue.push(c * 10);
                enqueue.push(c * 10 + 1);
                Ok(())
            },
            |enqueue| {
                enqueue.push(99);
                Ok(())
            },
        );
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            [10, 11, 20, 21, 99]
        );
    }

    #[tokio::test]
    async fn transform_errors() {
        let s = create_transform_stream::<_, i32, _, _>(
            create_readable_stream([1]),
            |_, _| err("t"),
            noop_flush,
        );
        assert_eq!(stream_to_array(s, None).await.unwrap_err().to_string(), "t");
        let s = create_transform_stream::<_, i32, _, _>(
            create_readable_stream([1]),
            |_, _| Ok(()),
            |_| err("f"),
        );
        assert_eq!(stream_to_array(s, None).await.unwrap_err().to_string(), "f");
    }

    #[tokio::test]
    async fn upstream_error_propagates() {
        let (tx, readable) = create_readable_channel::<i32>(None);
        tx.error("upstream".into()).await;
        let s = create_transform_stream(
            readable,
            |c, e| {
                e.push(c);
                Ok(())
            },
            noop_flush,
        );
        assert_eq!(
            stream_to_array(s, None).await.unwrap_err().to_string(),
            "upstream"
        );
    }

    #[tokio::test]
    async fn transform_async() {
        let s = create_transform_stream_async(
            create_readable_stream([1, 2]),
            |c| async move {
                tokio::task::yield_now().await;
                Ok(vec![c, c])
            },
            || async { Ok(vec![0]) },
        );
        assert_eq!(stream_to_array(s, None).await.unwrap(), [1, 1, 2, 2, 0]);
    }

    #[tokio::test]
    async fn writable_consumes_then_finalizes() {
        let mut seen = Vec::new();
        let mut finalized = false;
        create_writable_stream(
            create_readable_stream([1, 2]),
            |c| {
                seen.push(c);
                Ok(())
            },
            || {
                finalized = true;
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(seen, [1, 2]);
        assert!(finalized);
    }

    #[tokio::test]
    async fn writable_errors() {
        let e = create_writable_stream(create_readable_stream([1]), |_| err("w"), noop).await;
        assert_eq!(e.unwrap_err().to_string(), "w");
        let e = create_writable_stream(create_readable_stream([1]), |_| Ok(()), || err("f")).await;
        assert_eq!(e.unwrap_err().to_string(), "f");
        let e = create_writable_stream_async(
            create_readable_stream([1]),
            |_| async { Ok(()) },
            || async { err("af") },
        )
        .await;
        assert_eq!(e.unwrap_err().to_string(), "af");
    }

    #[tokio::test]
    async fn pipeline_drains_and_collects_results() {
        let count = StreamResult::new("count", json!(0));
        let c = count.clone();
        let stream = create_readable_stream([1, 2, 3]).pipe(|s| {
            create_pass_through_stream(
                s,
                move |_| {
                    c.update(|v| *v = json!(v.as_i64().unwrap() + 1));
                    Ok(())
                },
                noop,
            )
        });
        let output = pipeline(stream, &[&count]).await.unwrap();
        assert_eq!(Value::Object(output), json!({"count": 3}));
    }

    #[tokio::test]
    async fn pipeline_propagates_errors() {
        let s = create_transform_stream::<_, i32, _, _>(
            create_readable_stream([1]),
            |_, _| err("boom"),
            noop_flush,
        );
        assert_eq!(pipeline(s, &[]).await.unwrap_err().to_string(), "boom");
    }

    #[test]
    fn result_skips_empty_keys() {
        let a = StreamResult::new("a", json!(1));
        let empty = StreamResult::new("", json!(2));
        assert_eq!(Value::Object(result(&[&a, &empty])), json!({"a": 1}));
        a.set(json!(5));
        assert_eq!(a.key(), "a");
        assert_eq!(a.get(), json!(5));
    }

    #[test]
    fn deep_equal_compares_structurally() {
        assert!(deep_equal(
            &json!({"a": [1, {"b": 2}]}),
            &json!({"a": [1, {"b": 2}]})
        ));
        assert!(!deep_equal(&json!({"a": 1}), &json!({"a": 2})));
    }

    #[tokio::test]
    async fn timeout_resolves() {
        let start = std::time::Instant::now();
        timeout(Duration::from_millis(10), None).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn timeout_aborts() {
        let signal = CancellationToken::new();
        let s = signal.clone();
        tokio::spawn(async move { s.cancel() });
        let e = timeout(Duration::from_secs(10), Some(&signal))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        // Already aborted rejects immediately.
        let e = timeout(Duration::from_secs(10), Some(&signal))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        // Not aborted resolves normally.
        let fresh = CancellationToken::new();
        timeout(Duration::from_millis(1), Some(&fresh))
            .await
            .unwrap();
    }
}
