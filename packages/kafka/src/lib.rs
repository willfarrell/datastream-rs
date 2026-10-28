// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Kafka producer and consumer streams, ported from `@datastream/kafka`.
//!
//! The client is passed in through the [`Kafka`], [`KafkaProducer`] and
//! [`KafkaConsumer`] traits. The `rdkafka` feature provides [`RdKafka`].
//! Client settings (brokers, client id, TLS/SASL, acks, compression,
//! auto commit, offset reset) belong to the client, not to these streams.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use datastream_core::{CancellationToken, DataStream, Error, Result, StreamExt};
use futures::future::{join_all, BoxFuture};
use tokio::sync::mpsc;

#[cfg(feature = "rdkafka")]
mod rdkafka_client;
#[cfg(feature = "rdkafka")]
pub use rdkafka_client::{GroupContext, RdKafka, RdKafkaConsumer, RdKafkaProducer};

const DEFAULT_BATCH_SIZE: usize = 100;
const HIGH_WATER_MARK: usize = 100;

/// A message to produce. Strings and bytes become `{ value }`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProducerMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl From<Vec<u8>> for ProducerMessage {
    fn from(value: Vec<u8>) -> Self {
        Self {
            value: Some(value),
            ..Self::default()
        }
    }
}

impl From<String> for ProducerMessage {
    fn from(value: String) -> Self {
        value.into_bytes().into()
    }
}

impl From<&str> for ProducerMessage {
    fn from(value: &str) -> Self {
        value.as_bytes().to_vec().into()
    }
}

/// A consumed message.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: Option<i64>,
}

/// The offset to commit for a topic-partition: the next one to read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// A consumed message's position (`offset` is the message's own offset).
impl From<ConsumerMessage> for TopicPartitionOffset {
    fn from(message: ConsumerMessage) -> Self {
        Self {
            topic: message.topic,
            partition: message.partition,
            offset: message.offset,
        }
    }
}

impl From<&ConsumerMessage> for TopicPartitionOffset {
    fn from(message: &ConsumerMessage) -> Self {
        Self {
            topic: message.topic.clone(),
            partition: message.partition,
            offset: message.offset,
        }
    }
}

/// What [`KafkaConsumer::commit_offsets`] returns while the client has no
/// consumer group (after a stop, or between a crash and its restart).
/// [`kafka_commit_stream`] keeps those offsets for its next commit.
#[derive(Debug)]
pub struct KafkaGroupNotJoined;

impl std::fmt::Display for KafkaGroupNotJoined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Consumer group was not initialized, consumer#run must be called first")
    }
}

impl std::error::Error for KafkaGroupNotJoined {}

/// Message handler given to [`KafkaConsumer::run`].
pub type EachMessage = Arc<dyn Fn(ConsumerMessage) -> BoxFuture<'static, ()> + Send + Sync>;

/// Creates producers and consumers (the JS `Kafka` constructor).
pub trait Kafka: Send + Sync {
    fn producer(&self) -> Result<Arc<dyn KafkaProducer>>;
    fn consumer(&self, group_id: &str) -> Result<Arc<dyn KafkaConsumer>>;
}

pub trait KafkaProducer: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<()>>;
    fn disconnect(&self) -> BoxFuture<'_, Result<()>>;
    fn send<'a>(
        &'a self,
        topic: &'a str,
        messages: &'a [ProducerMessage],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<()>>;
}

pub trait KafkaConsumer: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<()>>;
    fn disconnect(&self) -> BoxFuture<'_, Result<()>>;
    fn subscribe<'a>(&'a self, topics: &'a [String]) -> BoxFuture<'a, Result<()>>;
    /// Await `each_message` for every message until [`stop`](Self::stop).
    fn run(&self, each_message: EachMessage) -> BoxFuture<'_, Result<()>>;
    fn stop(&self) -> BoxFuture<'_, Result<()>>;
    /// Commit `offsets` (each the next offset to read). Return
    /// [`KafkaGroupNotJoined`] while the client has no consumer group.
    fn commit_offsets<'a>(
        &'a self,
        offsets: &'a [TopicPartitionOffset],
    ) -> BoxFuture<'a, Result<()>> {
        let _ = offsets;
        Box::pin(async { Err("kafkaCommitStream: consumer does not support commitOffsets".into()) })
    }
    /// Bumped on every consumer-group (re)join. A rejoin refetches from the
    /// committed offsets and may move partitions to another member, so
    /// [`kafka_commit_stream`] drops what it had pending.
    fn group_generation(&self) -> u64 {
        0
    }
}

// *** Connect *** //

#[derive(Clone, Debug, Default)]
pub struct KafkaConnectOptions {
    /// A consumer is opened only when set.
    pub group_id: Option<String>,
    /// `Some(false)` skips the producer.
    pub producer: Option<bool>,
}

pub struct KafkaConnection {
    pub producer: Option<Arc<dyn KafkaProducer>>,
    pub consumer: Option<Arc<dyn KafkaConsumer>>,
}

impl KafkaConnection {
    /// Disconnect both, even if one fails, returning the first failure.
    pub async fn disconnect(&self) -> Result<()> {
        // Sequential awaits would skip the consumer disconnect (leaking its
        // socket/heartbeat) when the producer disconnect fails.
        let (producer, consumer) = tokio::join!(
            async {
                match &self.producer {
                    Some(producer) => producer.disconnect().await,
                    None => Ok(()),
                }
            },
            async {
                match &self.consumer {
                    Some(consumer) => consumer.disconnect().await,
                    None => Ok(()),
                }
            },
        );
        producer.and(consumer)
    }
}

/// Open and connect a producer and/or consumer.
pub async fn kafka_connect(
    kafka: &dyn Kafka,
    options: KafkaConnectOptions,
) -> Result<KafkaConnection> {
    let producer = match options.producer {
        Some(false) => None,
        _ => Some(kafka.producer()?),
    };
    let consumer = match options.group_id.as_deref() {
        Some(group_id) if !group_id.is_empty() => Some(kafka.consumer(group_id)?),
        _ => None,
    };
    let connected: Result<()> = async {
        if let Some(producer) = &producer {
            producer.connect().await?;
        }
        if let Some(consumer) = &consumer {
            consumer.connect().await?;
        }
        Ok::<(), Error>(())
    }
    .await;
    if let Err(e) = connected {
        // Tear down whatever connected so the error path leaks nothing.
        if let Some(producer) = &producer {
            let _ = producer.disconnect().await;
        }
        if let Some(consumer) = &consumer {
            let _ = consumer.disconnect().await;
        }
        return Err(e);
    }
    Ok(KafkaConnection { producer, consumer })
}

// *** Produce *** //

#[derive(Clone, Default)]
pub struct KafkaProduceOptions {
    pub producer: Option<Arc<dyn KafkaProducer>>,
    pub topic: Option<String>,
    /// Messages per send (default 100).
    pub batch_size: Option<usize>,
    pub timeout: Option<Duration>,
}

/// A send (or the input) failed; `failed_messages` were not sent.
#[derive(Debug)]
pub struct KafkaProduceError {
    pub error: Error,
    pub failed_messages: Vec<ProducerMessage>,
}

impl std::fmt::Display for KafkaProduceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for KafkaProduceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
}

/// Send every chunk to `topic`, in batches of `batch_size`.
pub async fn kafka_produce_stream<T>(
    mut input: DataStream<T>,
    options: KafkaProduceOptions,
) -> Result<()>
where
    T: Into<ProducerMessage> + Send + 'static,
{
    let producer = options
        .producer
        .ok_or("kafkaProduceStream: producer required")?;
    let topic = options
        .topic
        .filter(|topic| !topic.is_empty())
        .ok_or("kafkaProduceStream: topic required")?;
    let batch_size = options.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    if batch_size < 1 {
        return Err("kafkaProduceStream: batchSize must be >= 1".into());
    }

    let mut batch = Vec::new();
    while let Some(chunk) = input.next().await {
        match chunk {
            Ok(chunk) => batch.push(chunk.into()),
            // Surface the buffered batch so callers can retry it.
            Err(error) if !batch.is_empty() => {
                return Err(KafkaProduceError {
                    error,
                    failed_messages: batch,
                }
                .into());
            }
            Err(error) => return Err(error),
        }
        if batch.len() >= batch_size {
            send(
                &*producer,
                &topic,
                std::mem::take(&mut batch),
                options.timeout,
            )
            .await?;
        }
    }
    if !batch.is_empty() {
        send(&*producer, &topic, batch, options.timeout).await?;
    }
    Ok(())
}

async fn send(
    producer: &dyn KafkaProducer,
    topic: &str,
    messages: Vec<ProducerMessage>,
    timeout: Option<Duration>,
) -> Result<()> {
    let sent = producer.send(topic, &messages, timeout).await;
    sent.map_err(|error| {
        Box::new(KafkaProduceError {
            error,
            failed_messages: messages,
        }) as Error
    })
}

// *** Consume *** //

// kafka_commit_streams still open, per consumer, keyed by its address (each
// open commit stream holds the consumer, so the address stays live while
// keyed). A graceful kafka_consume_stream stop waits for them before
// consumer.stop(): a client that has left the group refuses commits, so
// stopping first would drop every final commit.
static OPEN_COMMIT_STREAMS: LazyLock<Mutex<HashMap<usize, Vec<CancellationToken>>>> =
    LazyLock::new(Mutex::default);

fn consumer_key(consumer: &Arc<dyn KafkaConsumer>) -> usize {
    Arc::as_ptr(consumer).cast::<()>().addr()
}

fn open_commit_streams() -> std::sync::MutexGuard<'static, HashMap<usize, Vec<CancellationToken>>> {
    OPEN_COMMIT_STREAMS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Registers a commit stream as open until dropped.
struct OpenCommitStream {
    key: usize,
    closed: CancellationToken,
}

impl OpenCommitStream {
    fn new(consumer: &Arc<dyn KafkaConsumer>) -> Self {
        let (key, closed) = (consumer_key(consumer), CancellationToken::new());
        open_commit_streams()
            .entry(key)
            .or_default()
            .push(closed.clone());
        Self { key, closed }
    }
}

impl Drop for OpenCommitStream {
    fn drop(&mut self) {
        self.closed.cancel();
        let mut open = open_commit_streams();
        if let Some(streams) = open.get_mut(&self.key) {
            streams.retain(|closed| !closed.is_cancelled());
            if streams.is_empty() {
                open.remove(&self.key);
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct KafkaConsumeOptions {
    pub consumer: Option<Arc<dyn KafkaConsumer>>,
    pub topics: Vec<String>,
    /// Cancelling it aborts the stream: the consumer stops first, so a
    /// downstream [`kafka_commit_stream`]'s final commit is refused and
    /// nothing still in flight is committed (at-least-once).
    pub signal: Option<CancellationToken>,
}

/// Stops a [`kafka_consume_stream`].
#[derive(Clone)]
pub struct KafkaConsumeStop {
    requested: CancellationToken,
    stopped: CancellationToken,
}

impl KafkaConsumeStop {
    /// Graceful shutdown: end the stream (already buffered messages are still
    /// delivered), wait until every open [`kafka_commit_stream`] on the same
    /// consumer has finished (so its final commit runs while the group is
    /// still joined), then stop the consumer. Resolves once it has stopped.
    pub async fn stop(&self) {
        self.cancel();
        self.stopped.cancelled().await;
    }
    /// Request [`stop`](Self::stop) without waiting for it.
    pub fn cancel(&self) {
        self.requested.cancel();
    }
}

/// Subscribe to `topics` and stream their messages. Stop gracefully with the
/// returned [`KafkaConsumeStop`], abort with the `signal`; dropping the stream
/// before it ends stops the consumer without waiting for commit streams.
pub async fn kafka_consume_stream(
    options: KafkaConsumeOptions,
) -> Result<(DataStream<ConsumerMessage>, KafkaConsumeStop)> {
    let consumer = options
        .consumer
        .ok_or("kafkaConsumeStream: consumer required")?;
    if options.topics.is_empty() {
        return Err("kafkaConsumeStream: topics required".into());
    }
    consumer.subscribe(&options.topics).await?;

    // Never cancelled here, so stopping this stream never cancels a shared
    // signal.
    let abort = options.signal.unwrap_or_default();
    let requested = CancellationToken::new();
    let stopped = CancellationToken::new();
    // Any stop: new messages hold instead of being delivered.
    let halted = CancellationToken::new();
    // Held messages are released (undelivered) once the group is being left.
    let leaving = CancellationToken::new();
    // Ends the stream after what is already buffered.
    let ended = CancellationToken::new();
    // The stream was dropped before it ended.
    let destroyed = CancellationToken::new();
    if abort.is_cancelled() {
        halted.cancel();
    }
    let (tx, mut rx) = mpsc::channel::<Result<ConsumerMessage>>(HIGH_WATER_MARK);

    // Holds a weak sender so the stream ends when the run task drops `tx`.
    let weak = tx.downgrade();
    let (halt, leave) = (halted.clone(), leaving.clone());
    let each_message: EachMessage =
        Arc::new(move |message: ConsumerMessage| -> BoxFuture<'static, ()> {
            let (tx, halted, leaving) = (weak.upgrade(), halt.clone(), leave.clone());
            Box::pin(async move {
                // Never deliver after a stop, and hold until the group is
                // being left: returning sooner tells the client the message
                // was handled while the group is still live.
                if halted.is_cancelled() {
                    return leaving.cancelled().await;
                }
                let Some(tx) = tx else { return };
                // Waits while the buffer is full (backpressure); a stop
                // releases it. consumer.stop() waits for in-flight callbacks,
                // so a callback parked here must not outlive `leaving`.
                tokio::select! {
                    biased;
                    _ = halted.cancelled() => leaving.cancelled().await,
                    _ = tx.send(Ok(message)) => {}
                }
            })
        });

    let key = consumer_key(&consumer);
    let handle = KafkaConsumeStop {
        requested: requested.clone(),
        stopped: stopped.clone(),
    };
    let (stream_ended, guard) = (ended.clone(), destroyed.clone().drop_guard());
    tokio::spawn(async move {
        let runner = consumer.clone();
        let mut run = tokio::spawn(async move { runner.run(each_message).await });
        let graceful = tokio::select! {
            biased;
            _ = abort.cancelled() => false,
            _ = destroyed.cancelled() => false,
            _ = requested.cancelled() => true,
            result = &mut run => {
                let result = result.map_err(Error::from).and_then(|result| result);
                if let Err(e) = result {
                    let _ = tx.send(Err(e)).await;
                }
                stopped.cancel();
                return;
            }
        };
        halted.cancel();
        drop(tx);
        if graceful {
            // End the stream, let the pipeline drain and every open commit
            // stream on this consumer commit, then leave the group.
            ended.cancel();
            let open = open_commit_streams().get(&key).cloned().unwrap_or_default();
            tokio::select! {
                _ = join_all(open.iter().map(CancellationToken::cancelled)) => {}
                // Dropped mid-drain: a hard stop.
                _ = destroyed.cancelled() => {}
            }
        }
        leaving.cancel();
        // Best effort, like the JS: errors while stopping are swallowed.
        let _ = consumer.stop().await;
        // An abort ends the stream only once the group is left.
        ended.cancel();
        let _ = run.await;
        stopped.cancel();
    });

    let stream = async_stream::stream! {
        // Dropping the stream before it ends stops the consumer.
        let guard = guard;
        let mut closed = false;
        loop {
            let item = tokio::select! {
                biased;
                item = rx.recv() => item,
                _ = stream_ended.cancelled(), if !closed => {
                    // Buffered messages are still received, then None.
                    rx.close();
                    closed = true;
                    continue;
                }
            };
            match item {
                Some(item) => yield item,
                None => break,
            }
        }
        // Ended: a later drop (e.g. the pipeline finishing) must not hard-stop
        // the consumer before a graceful stop's commit streams are done.
        guard.disarm();
    };
    let stream: DataStream<ConsumerMessage> = Box::pin(stream);
    Ok((stream, handle))
}

// *** Commit *** //

#[derive(Clone, Default)]
pub struct KafkaCommitOptions {
    pub consumer: Option<Arc<dyn KafkaConsumer>>,
    /// Commit every N messages (and always at the end; default 100).
    pub batch_size: Option<usize>,
}

type PartitionKey = (String, i32);

// Next offset to commit (`offset + 1`) per topic-partition. Only ever raised
// within a group generation, so a redelivered or out-of-order message never
// moves a commit backwards.
#[derive(Default)]
struct Offsets {
    generation: u64,
    highest: HashMap<PartitionKey, i64>,
    // Insertion order, like the JS Map, with an index per topic-partition.
    pending: Vec<TopicPartitionOffset>,
    index: HashMap<PartitionKey, usize>,
}

impl Offsets {
    // A (re)join starts a new generation: the client refetches from the
    // committed offsets (which an operator may have reset backwards), and
    // partitions may now belong to another member, so committing what was
    // pending could move its offsets. Start over; the dropped tail is
    // redelivered (at-least-once).
    fn sync(&mut self, generation: u64) {
        if generation != self.generation {
            self.generation = generation;
            self.highest.clear();
            self.clear_pending();
        }
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.index.clear();
    }

    fn record(&mut self, position: TopicPartitionOffset) -> Result<()> {
        let next = position
            .offset
            .checked_add(1)
            .ok_or("kafkaCommitStream: offset out of range")?;
        let key = (position.topic, position.partition);
        match self.highest.get_mut(&key) {
            Some(highest) if next <= *highest => return Ok(()),
            Some(highest) => *highest = next,
            // Offsets start at 0, so a negative one (none) is never committed.
            None if next <= 0 => return Ok(()),
            None => {
                self.highest.insert(key.clone(), next);
            }
        }
        match self.index.get(&key) {
            Some(&i) => self.pending[i].offset = next,
            None => {
                self.index.insert(key.clone(), self.pending.len());
                let (topic, partition) = key;
                self.pending.push(TopicPartitionOffset {
                    topic,
                    partition,
                    offset: next,
                });
            }
        }
        Ok(())
    }

    async fn commit(&mut self, consumer: &dyn KafkaConsumer) -> Result<()> {
        self.sync(consumer.group_generation());
        if self.pending.is_empty() {
            return Ok(());
        }
        match consumer.commit_offsets(&self.pending).await {
            Ok(()) => self.clear_pending(),
            // Refused while the client has no group (between a crash and its
            // restart, or after a stop): keep the offsets for the next attempt.
            Err(e) if e.is::<KafkaGroupNotJoined>() => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

/// Commit `offset + 1` per topic-partition for every message read (e.g. from
/// [`kafka_consume_stream`], after processing): every `batch_size` messages
/// and once more when the input ends. An input error commits nothing more:
/// those messages may not have been processed, so they are redelivered.
///
/// Registers with the consumer when called, not when first polled, so a
/// graceful stop requested before then still waits for its final commit.
pub fn kafka_commit_stream<T>(
    mut input: DataStream<T>,
    options: KafkaCommitOptions,
) -> impl Future<Output = Result<()>> + Send + 'static
where
    T: Into<TopicPartitionOffset> + Send + 'static,
{
    let batch_size = options.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let checked = match options.consumer {
        None => Err("kafkaCommitStream: consumer required"),
        Some(_) if batch_size < 1 => Err("kafkaCommitStream: batchSize must be >= 1"),
        Some(consumer) => Ok((OpenCommitStream::new(&consumer), consumer)),
    };
    async move {
        let (_open, consumer) = checked?;
        let mut offsets = Offsets {
            generation: consumer.group_generation(),
            ..Offsets::default()
        };
        let mut count = 0;
        while let Some(chunk) = input.next().await {
            offsets.sync(consumer.group_generation());
            offsets.record(chunk?.into())?;
            count += 1;
            if count >= batch_size {
                count = 0;
                offsets.commit(&*consumer).await?;
            }
        }
        offsets.commit(&*consumer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, stream_to_array};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn ready(fail: Option<&'static str>) -> BoxFuture<'static, Result<()>> {
        let result: Result<()> = match fail {
            Some(message) => Err(message.into()),
            None => Ok(()),
        };
        Box::pin(async move { result })
    }

    type Sent = (String, Vec<ProducerMessage>, Option<Duration>);

    #[derive(Default)]
    struct FakeProducer {
        calls: Mutex<Vec<&'static str>>,
        sent: Mutex<Vec<Sent>>,
        fail_connect: Option<&'static str>,
        fail_disconnect: Option<&'static str>,
        fail_send: Option<&'static str>,
    }

    impl FakeProducer {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
        fn sent(&self) -> Vec<Sent> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl KafkaProducer for FakeProducer {
        fn connect(&self) -> BoxFuture<'_, Result<()>> {
            self.calls.lock().unwrap().push("connect");
            ready(self.fail_connect)
        }
        fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
            self.calls.lock().unwrap().push("disconnect");
            ready(self.fail_disconnect)
        }
        fn send<'a>(
            &'a self,
            topic: &'a str,
            messages: &'a [ProducerMessage],
            timeout: Option<Duration>,
        ) -> BoxFuture<'a, Result<()>> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), messages.to_vec(), timeout));
            ready(self.fail_send)
        }
    }

    #[derive(Default)]
    struct FakeConsumer {
        calls: Mutex<Vec<String>>,
        messages: Vec<ConsumerMessage>,
        delivered: AtomicUsize,
        stopped: CancellationToken,
        // run() returns after delivering instead of waiting for stop().
        run_returns: bool,
        fail_connect: Option<&'static str>,
        fail_disconnect: Option<&'static str>,
        fail_run: Option<&'static str>,
        fail_stop: Option<&'static str>,
    }

    impl FakeConsumer {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn count(&self, call: &str) -> usize {
            self.calls().iter().filter(|c| *c == call).count()
        }
        fn call(&self, call: impl Into<String>) {
            self.calls.lock().unwrap().push(call.into());
        }
    }

    impl KafkaConsumer for FakeConsumer {
        fn connect(&self) -> BoxFuture<'_, Result<()>> {
            self.call("connect");
            ready(self.fail_connect)
        }
        fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
            self.call("disconnect");
            ready(self.fail_disconnect)
        }
        fn subscribe<'a>(&'a self, topics: &'a [String]) -> BoxFuture<'a, Result<()>> {
            self.call(format!("subscribe:{}", topics.join(",")));
            ready(None)
        }
        fn run(&self, each_message: EachMessage) -> BoxFuture<'_, Result<()>> {
            self.call("run");
            Box::pin(async move {
                for message in &self.messages {
                    each_message(message.clone()).await;
                    self.delivered.fetch_add(1, Ordering::SeqCst);
                }
                if !self.run_returns {
                    self.stopped.cancelled().await;
                }
                ready(self.fail_run).await
            })
        }
        fn stop(&self) -> BoxFuture<'_, Result<()>> {
            self.call("stop");
            self.stopped.cancel();
            ready(self.fail_stop)
        }
    }

    #[derive(Default)]
    struct FakeKafka {
        producer: Arc<FakeProducer>,
        consumer: Arc<FakeConsumer>,
        group_ids: Mutex<Vec<String>>,
    }

    impl Kafka for FakeKafka {
        fn producer(&self) -> Result<Arc<dyn KafkaProducer>> {
            let producer: Arc<dyn KafkaProducer> = self.producer.clone();
            Ok(producer)
        }
        fn consumer(&self, group_id: &str) -> Result<Arc<dyn KafkaConsumer>> {
            self.group_ids.lock().unwrap().push(group_id.to_string());
            let consumer: Arc<dyn KafkaConsumer> = self.consumer.clone();
            Ok(consumer)
        }
    }

    fn group(id: &str) -> KafkaConnectOptions {
        KafkaConnectOptions {
            group_id: Some(id.to_string()),
            ..Default::default()
        }
    }

    fn message(offset: i64) -> ConsumerMessage {
        ConsumerMessage {
            topic: "t".to_string(),
            offset,
            value: Some(format!("v{offset}").into_bytes()),
            timestamp: Some(1_700_000_000_000),
            ..Default::default()
        }
    }

    async fn wait_for(condition: impl Fn() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("condition not met");
    }

    // *** kafka_connect *** //

    #[tokio::test]
    async fn connect_opens_producer_and_consumer() {
        let kafka = FakeKafka::default();
        let conn = kafka_connect(&kafka, group("g1")).await.unwrap();
        assert!(conn.producer.is_some());
        assert!(conn.consumer.is_some());
        assert_eq!(*kafka.group_ids.lock().unwrap(), ["g1"]);
        assert_eq!(kafka.producer.calls(), ["connect"]);
        assert_eq!(kafka.consumer.calls(), ["connect"]);

        conn.disconnect().await.unwrap();
        assert_eq!(kafka.producer.calls(), ["connect", "disconnect"]);
        assert_eq!(kafka.consumer.calls(), ["connect", "disconnect"]);
    }

    #[tokio::test]
    async fn connect_skips_producer_when_false() {
        let kafka = FakeKafka::default();
        let options = KafkaConnectOptions {
            producer: Some(false),
            ..group("g1")
        };
        let conn = kafka_connect(&kafka, options).await.unwrap();
        assert!(conn.producer.is_none());
        conn.disconnect().await.unwrap();
        assert!(kafka.producer.calls().is_empty());
    }

    #[tokio::test]
    async fn connect_skips_consumer_without_group_id() {
        let kafka = FakeKafka::default();
        for options in [KafkaConnectOptions::default(), group("")] {
            let conn = kafka_connect(&kafka, options).await.unwrap();
            assert!(conn.consumer.is_none());
            conn.disconnect().await.unwrap();
        }
        assert!(kafka.consumer.calls().is_empty());
        assert!(kafka.group_ids.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn connect_producer_error_tears_down() {
        let kafka = FakeKafka {
            producer: Arc::new(FakeProducer {
                fail_connect: Some("producer connect failed"),
                // Teardown errors are swallowed.
                fail_disconnect: Some("disconnect failed"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let e = kafka_connect(&kafka, group("g1")).await.err().unwrap();
        assert_eq!(e.to_string(), "producer connect failed");
        assert_eq!(kafka.producer.calls(), ["connect", "disconnect"]);
        assert_eq!(kafka.consumer.calls(), ["disconnect"]);
    }

    #[tokio::test]
    async fn connect_consumer_error_tears_down() {
        let kafka = FakeKafka {
            consumer: Arc::new(FakeConsumer {
                fail_connect: Some("consumer connect failed"),
                fail_disconnect: Some("disconnect failed"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let options = KafkaConnectOptions {
            producer: Some(false),
            ..group("g1")
        };
        let e = kafka_connect(&kafka, options).await.err().unwrap();
        assert_eq!(e.to_string(), "consumer connect failed");
        assert_eq!(kafka.consumer.calls(), ["connect", "disconnect"]);
        assert!(kafka.producer.calls().is_empty());
    }

    #[tokio::test]
    async fn disconnect_still_disconnects_consumer_when_producer_fails() {
        let kafka = FakeKafka {
            producer: Arc::new(FakeProducer {
                fail_disconnect: Some("producer disconnect failed"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn = kafka_connect(&kafka, group("g1")).await.unwrap();
        let e = conn.disconnect().await.unwrap_err();
        assert_eq!(e.to_string(), "producer disconnect failed");
        assert_eq!(kafka.producer.calls(), ["connect", "disconnect"]);
        assert_eq!(kafka.consumer.calls(), ["connect", "disconnect"]);
    }

    #[tokio::test]
    async fn disconnect_returns_the_first_failure() {
        let kafka = FakeKafka {
            producer: Arc::new(FakeProducer {
                fail_disconnect: Some("producer disconnect failed"),
                ..Default::default()
            }),
            consumer: Arc::new(FakeConsumer {
                fail_disconnect: Some("consumer disconnect failed"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn = kafka_connect(&kafka, group("g1")).await.unwrap();
        let e = conn.disconnect().await.unwrap_err();
        assert_eq!(e.to_string(), "producer disconnect failed");

        let kafka = FakeKafka {
            consumer: Arc::new(FakeConsumer {
                fail_disconnect: Some("consumer disconnect failed"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn = kafka_connect(&kafka, group("g1")).await.unwrap();
        let e = conn.disconnect().await.unwrap_err();
        assert_eq!(e.to_string(), "consumer disconnect failed");
        assert_eq!(kafka.producer.calls(), ["connect", "disconnect"]);
    }

    // *** kafka_produce_stream *** //

    fn produce_options(producer: &Arc<FakeProducer>) -> KafkaProduceOptions {
        let producer: Arc<dyn KafkaProducer> = producer.clone();
        KafkaProduceOptions {
            producer: Some(producer),
            topic: Some("t".to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn produce_validates_options() {
        let producer = Arc::new(FakeProducer::default());
        let cases = [
            (
                KafkaProduceOptions {
                    producer: None,
                    ..produce_options(&producer)
                },
                "kafkaProduceStream: producer required",
            ),
            (
                KafkaProduceOptions {
                    topic: None,
                    ..produce_options(&producer)
                },
                "kafkaProduceStream: topic required",
            ),
            (
                KafkaProduceOptions {
                    topic: Some(String::new()),
                    ..produce_options(&producer)
                },
                "kafkaProduceStream: topic required",
            ),
            (
                KafkaProduceOptions {
                    batch_size: Some(0),
                    ..produce_options(&producer)
                },
                "kafkaProduceStream: batchSize must be >= 1",
            ),
        ];
        for (options, expected) in cases {
            let e = kafka_produce_stream(create_readable_stream(["a"]), options)
                .await
                .unwrap_err();
            assert_eq!(e.to_string(), expected);
        }
        assert!(producer.sent().is_empty());
    }

    #[tokio::test]
    async fn produce_wraps_strings_and_bytes() {
        let producer = Arc::new(FakeProducer::default());
        let options = KafkaProduceOptions {
            timeout: Some(Duration::from_secs(5)),
            ..produce_options(&producer)
        };
        kafka_produce_stream(
            create_readable_stream(["hello".to_string()]),
            options.clone(),
        )
        .await
        .unwrap();
        kafka_produce_stream(create_readable_stream([vec![1u8, 2]]), options)
            .await
            .unwrap();
        let sent = producer.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, "t");
        assert_eq!(sent[0].1, [ProducerMessage::from("hello")]);
        assert_eq!(sent[0].1[0].value.as_deref(), Some(&b"hello"[..]));
        assert_eq!(sent[0].2, Some(Duration::from_secs(5)));
        assert_eq!(sent[1].1[0].value, Some(vec![1, 2]));
    }

    #[tokio::test]
    async fn produce_passes_messages_through() {
        let producer = Arc::new(FakeProducer::default());
        let message = ProducerMessage {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![("h".to_string(), b"1".to_vec())],
        };
        kafka_produce_stream(
            create_readable_stream([message.clone()]),
            produce_options(&producer),
        )
        .await
        .unwrap();
        assert_eq!(producer.sent()[0].1, [message]);
    }

    #[tokio::test]
    async fn produce_batches_and_flushes_remainder() {
        let producer = Arc::new(FakeProducer::default());
        let options = KafkaProduceOptions {
            batch_size: Some(2),
            ..produce_options(&producer)
        };
        kafka_produce_stream(create_readable_stream(["a", "b", "c", "d", "e"]), options)
            .await
            .unwrap();
        let sizes: Vec<usize> = producer.sent().iter().map(|s| s.1.len()).collect();
        assert_eq!(sizes, [2, 2, 1]);
        assert_eq!(
            producer.sent()[1].1,
            [ProducerMessage::from("c"), ProducerMessage::from("d")]
        );
    }

    #[tokio::test]
    async fn produce_empty_stream_sends_nothing() {
        let producer = Arc::new(FakeProducer::default());
        kafka_produce_stream(
            create_readable_stream(Vec::<String>::new()),
            produce_options(&producer),
        )
        .await
        .unwrap();
        assert!(producer.sent().is_empty());
    }

    #[tokio::test]
    async fn produce_send_error_attaches_failed_messages() {
        let producer = Arc::new(FakeProducer {
            fail_send: Some("send failed"),
            ..Default::default()
        });
        let e = kafka_produce_stream(
            create_readable_stream(["a", "b"]),
            produce_options(&producer),
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "send failed");
        let e = e.downcast_ref::<KafkaProduceError>().unwrap();
        assert_eq!(
            e.failed_messages,
            [ProducerMessage::from("a"), ProducerMessage::from("b")]
        );
    }

    #[tokio::test]
    async fn produce_input_error_attaches_buffered_batch() {
        let producer = Arc::new(FakeProducer::default());
        let items: Vec<Result<&str>> = vec![Ok("a"), Err("boom".into())];
        let input: DataStream<&str> = Box::pin(futures::stream::iter(items));
        let e = kafka_produce_stream(input, produce_options(&producer))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        let e = e.downcast_ref::<KafkaProduceError>().unwrap();
        assert_eq!(e.failed_messages, [ProducerMessage::from("a")]);
        assert!(producer.sent().is_empty());
    }

    #[tokio::test]
    async fn produce_input_error_with_empty_batch_is_unchanged() {
        let producer = Arc::new(FakeProducer::default());
        let items: Vec<Result<&str>> = vec![Err("boom".into())];
        let input: DataStream<&str> = Box::pin(futures::stream::iter(items));
        let e = kafka_produce_stream(input, produce_options(&producer))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        assert!(e.downcast_ref::<KafkaProduceError>().is_none());
    }

    // *** kafka_consume_stream *** //

    fn consume_options(consumer: &Arc<FakeConsumer>, topics: &[&str]) -> KafkaConsumeOptions {
        let consumer: Arc<dyn KafkaConsumer> = consumer.clone();
        KafkaConsumeOptions {
            consumer: Some(consumer),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            signal: None,
        }
    }

    #[tokio::test]
    async fn consume_validates_options() {
        let consumer = Arc::new(FakeConsumer::default());
        let options = KafkaConsumeOptions {
            consumer: None,
            ..consume_options(&consumer, &["t"])
        };
        let e = kafka_consume_stream(options).await.err().unwrap();
        assert_eq!(e.to_string(), "kafkaConsumeStream: consumer required");
        let e = kafka_consume_stream(consume_options(&consumer, &[]))
            .await
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "kafkaConsumeStream: topics required");
        assert!(consumer.calls().is_empty());
    }

    #[tokio::test]
    async fn consume_subscribes_and_delivers_messages() {
        let consumer = Arc::new(FakeConsumer {
            messages: vec![message(0), message(1)],
            run_returns: true,
            ..Default::default()
        });
        let (stream, _stop) = kafka_consume_stream(consume_options(&consumer, &["a", "b"]))
            .await
            .unwrap();
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            [message(0), message(1)]
        );
        assert_eq!(consumer.calls()[..2], ["subscribe:a,b", "run"]);
    }

    #[tokio::test]
    async fn consume_stop_ends_stream_once() {
        let consumer = Arc::new(FakeConsumer {
            messages: vec![message(0)],
            ..Default::default()
        });
        let (mut stream, stop) = kafka_consume_stream(consume_options(&consumer, &["t"]))
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), message(0));
        stop.cancel();
        stop.cancel();
        assert!(stream.next().await.is_none());
        stop.stop().await;
        stop.stop().await;
        assert_eq!(consumer.count("stop"), 1);
    }

    #[tokio::test]
    async fn consume_drop_stops_consumer() {
        let consumer = Arc::new(FakeConsumer::default());
        let (stream, _stop) = kafka_consume_stream(consume_options(&consumer, &["t"]))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        drop(stream);
        wait_for(|| consumer.count("stop") == 1).await;
    }

    #[tokio::test]
    async fn consume_signal_stops_without_cancelling_signal_owner() {
        let consumer = Arc::new(FakeConsumer::default());
        let signal = CancellationToken::new();
        let options = KafkaConsumeOptions {
            signal: Some(signal.clone()),
            ..consume_options(&consumer, &["t"])
        };
        let (stream, stop) = kafka_consume_stream(options).await.unwrap();
        stop.cancel();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert!(!signal.is_cancelled());

        let options = KafkaConsumeOptions {
            signal: Some(signal.clone()),
            ..consume_options(&consumer, &["t"])
        };
        let (stream, _stop) = kafka_consume_stream(options).await.unwrap();
        signal.cancel();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn consume_already_cancelled_signal_stops_immediately() {
        let consumer = Arc::new(FakeConsumer {
            messages: vec![message(0)],
            ..Default::default()
        });
        let signal = CancellationToken::new();
        signal.cancel();
        let options = KafkaConsumeOptions {
            signal: Some(signal),
            ..consume_options(&consumer, &["t"])
        };
        let (stream, _stop) = kafka_consume_stream(options).await.unwrap();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(consumer.count("stop"), 1);
    }

    #[tokio::test]
    async fn consume_swallows_errors_while_stopping() {
        let consumer = Arc::new(FakeConsumer {
            fail_stop: Some("stop failed"),
            fail_run: Some("run failed"),
            ..Default::default()
        });
        let (stream, stop) = kafka_consume_stream(consume_options(&consumer, &["t"]))
            .await
            .unwrap();
        stop.cancel();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn consume_surfaces_run_error() {
        let consumer = Arc::new(FakeConsumer {
            messages: vec![message(0)],
            run_returns: true,
            fail_run: Some("run failed"),
            ..Default::default()
        });
        let (mut stream, _stop) = kafka_consume_stream(consume_options(&consumer, &["t"]))
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), message(0));
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "run failed"
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn consume_backpressure_suspends_each_message() {
        let consumer = Arc::new(FakeConsumer {
            messages: (0..150).map(message).collect(),
            ..Default::default()
        });
        let (mut stream, stop) = kafka_consume_stream(consume_options(&consumer, &["t"]))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(consumer.delivered.load(Ordering::SeqCst), HIGH_WATER_MARK);

        assert_eq!(stream.next().await.unwrap().unwrap(), message(0));
        wait_for(|| consumer.delivered.load(Ordering::SeqCst) == HIGH_WATER_MARK + 1).await;

        // Stopping releases the parked callback, so the consumer can stop.
        stop.cancel();
        let rest = stream_to_array(stream, None).await.unwrap();
        assert_eq!(rest.first(), Some(&message(1)));
        stop.stop().await;
        assert_eq!(consumer.count("stop"), 1);
    }

    // *** kafka_commit_stream *** //

    #[derive(Clone, Debug, PartialEq)]
    enum Entry {
        Stop,
        Commit(Vec<TopicPartitionOffset>),
        Refused(Vec<TopicPartitionOffset>),
    }

    /// Mirrors a kafkajs consumer's group lifecycle: run() joins the group,
    /// stop() leaves it (waiting for in-flight messages first), and
    /// commit_offsets() is refused while no group exists.
    #[derive(Default)]
    struct GroupConsumer {
        log: Mutex<Vec<Entry>>,
        group: std::sync::atomic::AtomicBool,
        generation: std::sync::atomic::AtomicU64,
        each_message: Mutex<Option<EachMessage>>,
        in_flight: AtomicUsize,
        stopped: CancellationToken,
        fail_commit: Option<&'static str>,
    }

    impl GroupConsumer {
        fn joined() -> Arc<Self> {
            let consumer = Arc::new(Self::default());
            consumer.join();
            consumer
        }
        fn join(&self) {
            self.group.store(true, Ordering::SeqCst);
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
        fn leave(&self) {
            self.group.store(false, Ordering::SeqCst);
        }
        fn log(&self) -> Vec<Entry> {
            self.log.lock().unwrap().clone()
        }
        fn commits(&self) -> Vec<Vec<TopicPartitionOffset>> {
            let log = self.log();
            log.into_iter()
                .filter_map(|entry| match entry {
                    Entry::Commit(offsets) => Some(offsets),
                    _ => None,
                })
                .collect()
        }
        async fn running(&self) -> EachMessage {
            for _ in 0..200 {
                if let Some(each_message) = self.each_message.lock().unwrap().clone() {
                    return each_message;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("consumer not running");
        }
        async fn deliver(&self, offset: i64) {
            let each_message = self.running().await;
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            each_message(message(offset)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl KafkaConsumer for GroupConsumer {
        fn connect(&self) -> BoxFuture<'_, Result<()>> {
            ready(None)
        }
        fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
            ready(None)
        }
        fn subscribe<'a>(&'a self, _topics: &'a [String]) -> BoxFuture<'a, Result<()>> {
            ready(None)
        }
        fn run(&self, each_message: EachMessage) -> BoxFuture<'_, Result<()>> {
            *self.each_message.lock().unwrap() = Some(each_message);
            self.join();
            Box::pin(async move {
                self.stopped.cancelled().await;
                Ok(())
            })
        }
        fn stop(&self) -> BoxFuture<'_, Result<()>> {
            self.log.lock().unwrap().push(Entry::Stop);
            self.leave();
            self.stopped.cancel();
            Box::pin(async move {
                // Like kafkajs runner.stop(): wait for in-flight messages.
                while self.in_flight.load(Ordering::SeqCst) > 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok(())
            })
        }
        fn commit_offsets<'a>(
            &'a self,
            offsets: &'a [TopicPartitionOffset],
        ) -> BoxFuture<'a, Result<()>> {
            let offsets = offsets.to_vec();
            if let Some(message) = self.fail_commit {
                return ready(Some(message));
            }
            let result: Result<()> = if self.group.load(Ordering::SeqCst) {
                self.log.lock().unwrap().push(Entry::Commit(offsets));
                Ok(())
            } else {
                self.log.lock().unwrap().push(Entry::Refused(offsets));
                Err(KafkaGroupNotJoined.into())
            };
            Box::pin(async move { result })
        }
        fn group_generation(&self) -> u64 {
            self.generation.load(Ordering::SeqCst)
        }
    }

    fn tpo(topic: &str, partition: i32, offset: i64) -> TopicPartitionOffset {
        TopicPartitionOffset {
            topic: topic.to_string(),
            partition,
            offset,
        }
    }

    fn commit_options(
        consumer: &Arc<GroupConsumer>,
        batch_size: Option<usize>,
    ) -> KafkaCommitOptions {
        let consumer: Arc<dyn KafkaConsumer> = consumer.clone();
        KafkaCommitOptions {
            consumer: Some(consumer),
            batch_size,
        }
    }

    async fn commit_all(
        consumer: &Arc<GroupConsumer>,
        batch_size: Option<usize>,
        positions: Vec<TopicPartitionOffset>,
    ) -> Result<()> {
        kafka_commit_stream(
            create_readable_stream(positions),
            commit_options(consumer, batch_size),
        )
        .await
    }

    /// An input fed one message at a time.
    fn channel_input() -> (
        mpsc::UnboundedSender<TopicPartitionOffset>,
        DataStream<TopicPartitionOffset>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let input = async_stream::stream! {
            while let Some(position) = rx.recv().await {
                yield Ok(position);
            }
        };
        (tx, Box::pin(input))
    }

    /// Never yields: an open commit stream that is not finishing.
    fn pending_input() -> DataStream<TopicPartitionOffset> {
        Box::pin(futures::stream::pending())
    }

    #[tokio::test]
    async fn commit_validates_options() {
        let e = kafka_commit_stream(
            create_readable_stream(Vec::<TopicPartitionOffset>::new()),
            KafkaCommitOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "kafkaCommitStream: consumer required");
        let consumer = GroupConsumer::joined();
        let e = commit_all(&consumer, Some(0), vec![]).await.unwrap_err();
        assert_eq!(e.to_string(), "kafkaCommitStream: batchSize must be >= 1");
        let dyn_consumer: Arc<dyn KafkaConsumer> = consumer;
        assert!(open_commit_streams()
            .get(&consumer_key(&dyn_consumer))
            .is_none());
    }

    #[tokio::test]
    async fn commit_highest_offset_plus_one_per_partition_at_the_end() {
        let consumer = GroupConsumer::joined();
        let positions = vec![
            tpo("a", 0, 4),
            tpo("a", 1, 7),
            tpo("a", 0, 5),
            tpo("b", 0, 0),
            tpo("a", 1, 8),
        ];
        commit_all(&consumer, None, positions).await.unwrap();
        assert_eq!(
            consumer.commits(),
            [vec![tpo("a", 0, 6), tpo("a", 1, 9), tpo("b", 0, 1)]]
        );
    }

    #[tokio::test]
    async fn commit_every_batch_size_messages_and_the_rest_at_the_end() {
        let consumer = GroupConsumer::joined();
        let positions = (0..5).map(|offset| tpo("t", 0, offset)).collect();
        commit_all(&consumer, Some(2), positions).await.unwrap();
        assert_eq!(
            consumer.commits(),
            [
                vec![tpo("t", 0, 2)],
                vec![tpo("t", 0, 4)],
                vec![tpo("t", 0, 5)]
            ]
        );

        // batch_size defaults to 100.
        let consumer = GroupConsumer::joined();
        let positions = (0..101).map(|offset| tpo("t", 0, offset)).collect();
        commit_all(&consumer, None, positions).await.unwrap();
        assert_eq!(
            consumer.commits(),
            [vec![tpo("t", 0, 100)], vec![tpo("t", 0, 101)]]
        );
    }

    #[tokio::test]
    async fn commit_never_moves_an_offset_backwards() {
        let consumer = GroupConsumer::joined();
        // Redelivered / out-of-order: lower and equal offsets are ignored.
        let positions = vec![tpo("t", 0, 10), tpo("t", 0, 5), tpo("t", 0, 10)];
        commit_all(&consumer, Some(1), positions).await.unwrap();
        assert_eq!(consumer.commits(), [vec![tpo("t", 0, 11)]]);
    }

    #[tokio::test]
    async fn commit_offsets_beyond_f64_precision() {
        let consumer = GroupConsumer::joined();
        commit_all(&consumer, None, vec![tpo("t", 0, 9_007_199_254_740_993)])
            .await
            .unwrap();
        assert_eq!(
            consumer.commits(),
            [vec![tpo("t", 0, 9_007_199_254_740_994)]]
        );

        let e = commit_all(&consumer, None, vec![tpo("t", 0, i64::MAX)])
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "kafkaCommitStream: offset out of range");
    }

    #[tokio::test]
    async fn commit_skips_empty_input_and_negative_offsets() {
        let consumer = GroupConsumer::joined();
        commit_all(&consumer, None, vec![]).await.unwrap();
        commit_all(&consumer, None, vec![tpo("t", 0, -1)])
            .await
            .unwrap();
        assert!(consumer.log().is_empty());
    }

    #[tokio::test]
    async fn commit_accepts_consumer_messages() {
        let consumer = GroupConsumer::joined();
        let messages = vec![
            ConsumerMessage {
                partition: 3,
                ..message(41)
            },
            message(1),
        ];
        kafka_commit_stream(
            create_readable_stream(messages),
            commit_options(&consumer, None),
        )
        .await
        .unwrap();
        assert_eq!(consumer.commits(), [vec![tpo("t", 3, 42), tpo("t", 0, 2)]]);
    }

    #[tokio::test]
    async fn commit_input_error_commits_nothing_more() {
        let consumer = GroupConsumer::joined();
        let items: Vec<Result<TopicPartitionOffset>> = vec![Ok(tpo("t", 0, 0)), Err("boom".into())];
        let e = kafka_commit_stream(
            Box::pin(futures::stream::iter(items)),
            commit_options(&consumer, None),
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        assert!(consumer.log().is_empty());
    }

    #[tokio::test]
    async fn commit_drop_commits_nothing_and_deregisters() {
        let consumer = GroupConsumer::joined();
        let dyn_consumer: Arc<dyn KafkaConsumer> = consumer.clone();
        let (tx, input) = channel_input();
        let mut commit = Box::pin(kafka_commit_stream(input, commit_options(&consumer, None)));
        assert_eq!(open_commit_streams()[&consumer_key(&dyn_consumer)].len(), 1);
        tx.send(tpo("t", 0, 0)).unwrap();
        assert!(futures::poll!(commit.as_mut()).is_pending());
        drop(commit);
        assert!(consumer.log().is_empty());
        assert!(open_commit_streams()
            .get(&consumer_key(&dyn_consumer))
            .is_none());
    }

    #[tokio::test]
    async fn commit_error_errors_the_stream() {
        let consumer = Arc::new(GroupConsumer {
            fail_commit: Some("commit failed"),
            ..Default::default()
        });
        let e = commit_all(&consumer, None, vec![tpo("t", 0, 0)])
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "commit failed");

        // Only KafkaGroupNotJoined is kept for later; the same message as a
        // plain error still fails.
        let consumer = Arc::new(GroupConsumer {
            fail_commit: Some(
                "Consumer group was not initialized, consumer#run must be called first",
            ),
            ..Default::default()
        });
        assert!(commit_all(&consumer, None, vec![tpo("t", 0, 0)])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn commit_keeps_committing_after_a_crash_restart() {
        let consumer = GroupConsumer::joined();
        let (tx, input) = channel_input();
        let commit = tokio::spawn(kafka_commit_stream(
            input,
            commit_options(&consumer, Some(1)),
        ));
        tx.send(tpo("t", 0, 0)).unwrap();
        wait_for(|| consumer.log().len() == 1).await;
        // A commit while the group is gone is refused and must not error.
        consumer.leave();
        tx.send(tpo("t", 0, 1)).unwrap();
        wait_for(|| consumer.log().len() == 2).await;
        consumer.join();
        tx.send(tpo("t", 0, 2)).unwrap();
        tx.send(tpo("t", 0, 3)).unwrap();
        drop(tx);
        commit.await.unwrap().unwrap();
        assert_eq!(
            consumer.log(),
            [
                Entry::Commit(vec![tpo("t", 0, 1)]),
                Entry::Refused(vec![tpo("t", 0, 2)]),
                Entry::Commit(vec![tpo("t", 0, 3)]),
                Entry::Commit(vec![tpo("t", 0, 4)]),
            ]
        );
    }

    #[tokio::test]
    async fn commit_keeps_refused_offsets_pending() {
        let consumer = Arc::new(GroupConsumer::default());
        let (tx, input) = channel_input();
        let commit = tokio::spawn(kafka_commit_stream(
            input,
            commit_options(&consumer, Some(2)),
        ));
        tx.send(tpo("t", 0, 0)).unwrap();
        tx.send(tpo("t", 1, 0)).unwrap();
        wait_for(|| consumer.log().len() == 1).await;
        // The group comes back without a new generation: the refused offsets
        // are retried, not dropped.
        consumer.group.store(true, Ordering::SeqCst);
        tx.send(tpo("t", 0, 1)).unwrap();
        drop(tx);
        commit.await.unwrap().unwrap();
        assert_eq!(
            consumer.log(),
            [
                Entry::Refused(vec![tpo("t", 0, 1), tpo("t", 1, 1)]),
                Entry::Commit(vec![tpo("t", 0, 2), tpo("t", 1, 1)]),
            ]
        );
    }

    #[tokio::test]
    async fn commit_rejoin_resets_high_water_marks() {
        let consumer = GroupConsumer::joined();
        let (tx, input) = channel_input();
        let commit = tokio::spawn(kafka_commit_stream(
            input,
            commit_options(&consumer, Some(1)),
        ));
        tx.send(tpo("t", 0, 10)).unwrap();
        wait_for(|| consumer.log().len() == 1).await;
        // Offsets reset backwards; after the rejoin the replay from 5 commits.
        consumer.join();
        tx.send(tpo("t", 0, 5)).unwrap();
        tx.send(tpo("t", 0, 6)).unwrap();
        drop(tx);
        commit.await.unwrap().unwrap();
        assert_eq!(
            consumer.commits(),
            [
                vec![tpo("t", 0, 11)],
                vec![tpo("t", 0, 6)],
                vec![tpo("t", 0, 7)]
            ]
        );
    }

    #[tokio::test]
    async fn commit_rejoin_drops_offsets_pending_from_the_previous_generation() {
        let consumer = GroupConsumer::joined();
        let (tx, input) = channel_input();
        let commit = tokio::spawn(kafka_commit_stream(input, commit_options(&consumer, None)));
        tx.send(tpo("t", 0, 0)).unwrap();
        tx.send(tpo("t", 1, 0)).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        // The partitions may now belong to another member: leave them
        // uncommitted (redelivered, at-least-once).
        consumer.join();
        tx.send(tpo("t", 1, 1)).unwrap();
        drop(tx);
        commit.await.unwrap().unwrap();
        assert_eq!(consumer.commits(), [vec![tpo("t", 1, 2)]]);
    }

    // *** kafka_consume_stream + kafka_commit_stream lifecycle *** //

    async fn consume(
        consumer: &Arc<GroupConsumer>,
        signal: Option<CancellationToken>,
    ) -> (DataStream<ConsumerMessage>, KafkaConsumeStop) {
        let dyn_consumer: Arc<dyn KafkaConsumer> = consumer.clone();
        kafka_consume_stream(KafkaConsumeOptions {
            consumer: Some(dyn_consumer),
            topics: vec!["t".to_string()],
            signal,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn consume_commit_commits_what_was_consumed() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let sink = tokio::spawn(kafka_commit_stream(
            source,
            commit_options(&consumer, Some(1)),
        ));
        consumer.deliver(41).await;
        stop.stop().await;
        sink.await.unwrap().unwrap();
        assert_eq!(
            consumer.log(),
            [Entry::Commit(vec![tpo("t", 0, 42)]), Entry::Stop]
        );
    }

    #[tokio::test]
    async fn consume_graceful_stop_commits_the_tail_before_consumer_stop() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let sink = tokio::spawn(kafka_commit_stream(source, commit_options(&consumer, None)));
        // Fewer than batch_size: only the final commit covers them.
        for offset in 0..50 {
            consumer.deliver(offset).await;
        }
        stop.stop().await;
        sink.await.unwrap().unwrap();
        assert_eq!(
            consumer.log(),
            [Entry::Commit(vec![tpo("t", 0, 50)]), Entry::Stop]
        );
    }

    #[tokio::test]
    async fn consume_stop_waits_for_every_open_commit_stream() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let sink = tokio::spawn(kafka_commit_stream(source, commit_options(&consumer, None)));
        let other = kafka_commit_stream(pending_input(), commit_options(&consumer, None));
        consumer.deliver(0).await;
        let stopping = tokio::spawn({
            let stop = stop.clone();
            async move { stop.stop().await }
        });
        sink.await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !stopping.is_finished(),
            "still waiting for the other commit stream"
        );
        assert_eq!(consumer.log(), [Entry::Commit(vec![tpo("t", 0, 1)])]);
        drop(other);
        stopping.await.unwrap();
        assert_eq!(consumer.log().last(), Some(&Entry::Stop));
    }

    #[tokio::test]
    async fn consume_stop_ignores_commit_streams_of_another_consumer() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let other = kafka_commit_stream(
            pending_input(),
            commit_options(&GroupConsumer::joined(), None),
        );
        let reader = tokio::spawn(stream_to_array(source, None));
        stop.stop().await;
        assert!(reader.await.unwrap().unwrap().is_empty());
        assert_eq!(consumer.log(), [Entry::Stop]);
        drop(other);
    }

    #[tokio::test]
    async fn consume_stop_ignores_an_already_finished_commit_stream() {
        let consumer = Arc::new(GroupConsumer::default());
        drop(kafka_commit_stream(
            pending_input(),
            commit_options(&consumer, None),
        ));
        let (source, stop) = consume(&consumer, None).await;
        let reader = tokio::spawn(stream_to_array(source, None));
        stop.stop().await;
        reader.await.unwrap().unwrap();
        assert_eq!(consumer.log(), [Entry::Stop]);
    }

    #[tokio::test]
    async fn consume_holds_a_message_arriving_while_draining() {
        let consumer = Arc::new(GroupConsumer::default());
        let (mut source, stop) = consume(&consumer, None).await;
        let sink = kafka_commit_stream(pending_input(), commit_options(&consumer, None));
        consumer.deliver(0).await;
        assert_eq!(source.next().await.unwrap().unwrap(), message(0));
        let stopping = tokio::spawn({
            let stop = stop.clone();
            async move { stop.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Returning now would tell the client it was handled mid-drain.
        let late = tokio::spawn({
            let consumer = consumer.clone();
            async move { consumer.deliver(1).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!late.is_finished(), "held while draining");
        assert!(source.next().await.is_none());
        drop(sink);
        stopping.await.unwrap();
        late.await.unwrap();
        assert_eq!(consumer.log(), [Entry::Stop]);
    }

    #[tokio::test]
    async fn consume_abort_leaves_the_group_first() {
        let consumer = Arc::new(GroupConsumer::default());
        let signal = CancellationToken::new();
        let (source, stop) = consume(&consumer, Some(signal.clone())).await;
        let sink = tokio::spawn(kafka_commit_stream(source, commit_options(&consumer, None)));
        consumer.deliver(0).await;
        consumer.deliver(1).await;
        signal.cancel();
        // Refused, so nothing in flight is committed (at-least-once).
        sink.await.unwrap().unwrap();
        stop.stop().await;
        assert_eq!(
            consumer.log(),
            [Entry::Stop, Entry::Refused(vec![tpo("t", 0, 2)])]
        );
    }

    #[tokio::test]
    async fn consume_drop_mid_drain_does_not_wait_for_commit_streams() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let sink = kafka_commit_stream(pending_input(), commit_options(&consumer, None));
        consumer.deliver(0).await;
        // Not read: the stream has not ended, so dropping it is a hard stop.
        let stopping = tokio::spawn({
            let stop = stop.clone();
            async move { stop.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        drop(source);
        stopping.await.unwrap();
        assert_eq!(consumer.log(), [Entry::Stop]);
        drop(sink);
    }

    // consumer.stop() waits for in-flight messages; one parked on a full
    // buffer must be released first or the two wait on each other.
    async fn park_one(consumer: &Arc<GroupConsumer>) -> tokio::task::JoinHandle<()> {
        for offset in (0..).take(HIGH_WATER_MARK) {
            consumer.deliver(offset).await;
        }
        let parked = tokio::spawn({
            let consumer = consumer.clone();
            async move { consumer.deliver(100).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!parked.is_finished(), "parked on backpressure");
        parked
    }

    async fn within_a_second<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .expect("deadlocked")
    }

    #[tokio::test]
    async fn consume_stop_does_not_deadlock_on_a_parked_message() {
        let consumer = Arc::new(GroupConsumer::default());
        let (_source, stop) = consume(&consumer, None).await;
        let parked = park_one(&consumer).await;
        within_a_second(stop.stop()).await;
        within_a_second(parked).await.unwrap();
        assert_eq!(consumer.log(), [Entry::Stop]);
    }

    #[tokio::test]
    async fn consume_drop_does_not_deadlock_on_a_parked_message() {
        let consumer = Arc::new(GroupConsumer::default());
        let (source, stop) = consume(&consumer, None).await;
        let parked = park_one(&consumer).await;
        drop(source);
        within_a_second(stop.stopped.cancelled()).await;
        within_a_second(parked).await.unwrap();
        assert_eq!(consumer.log(), [Entry::Stop]);
    }

    #[tokio::test]
    async fn consume_abort_does_not_deadlock_on_a_parked_message() {
        let consumer = Arc::new(GroupConsumer::default());
        let signal = CancellationToken::new();
        let (_source, stop) = consume(&consumer, Some(signal.clone())).await;
        let parked = park_one(&consumer).await;
        signal.cancel();
        within_a_second(parked).await.unwrap();
        within_a_second(stop.stopped.cancelled()).await;
        assert_eq!(consumer.log(), [Entry::Stop]);
    }
}
