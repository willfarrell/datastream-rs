// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Kafka producer and consumer streams, ported from `@datastream/kafka`.
//!
//! The client is passed in through the [`Kafka`], [`KafkaProducer`] and
//! [`KafkaConsumer`] traits. The `rdkafka` feature provides [`RdKafka`].
//! Client settings (brokers, client id, TLS/SASL, acks, compression,
//! auto commit, offset reset) belong to the client, not to these streams.

use std::sync::Arc;
use std::time::Duration;

use datastream_core::{CancellationToken, DataStream, Error, Result, StreamExt};
use futures::future::BoxFuture;
use tokio::sync::mpsc;

#[cfg(feature = "rdkafka")]
mod rdkafka_client;
#[cfg(feature = "rdkafka")]
pub use rdkafka_client::{RdKafka, RdKafkaConsumer, RdKafkaProducer};

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
    pub async fn disconnect(&self) -> Result<()> {
        if let Some(producer) = &self.producer {
            producer.disconnect().await?;
        }
        if let Some(consumer) = &self.consumer {
            consumer.disconnect().await?;
        }
        Ok(())
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

#[derive(Clone, Default)]
pub struct KafkaConsumeOptions {
    pub consumer: Option<Arc<dyn KafkaConsumer>>,
    pub topics: Vec<String>,
    /// Cancelling it stops the stream.
    pub signal: Option<CancellationToken>,
}

/// Subscribe to `topics` and stream their messages. Cancel the returned token
/// (or drop the stream) to stop the consumer; the stream ends once it stops.
pub async fn kafka_consume_stream(
    options: KafkaConsumeOptions,
) -> Result<(DataStream<ConsumerMessage>, CancellationToken)> {
    let consumer = options
        .consumer
        .ok_or("kafkaConsumeStream: consumer required")?;
    if options.topics.is_empty() {
        return Err("kafkaConsumeStream: topics required".into());
    }
    consumer.subscribe(&options.topics).await?;

    // A child token, so stopping this stream never cancels a shared signal.
    let stop = options
        .signal
        .map_or_else(CancellationToken::new, |signal| signal.child_token());
    let (tx, mut rx) = mpsc::channel::<Result<ConsumerMessage>>(HIGH_WATER_MARK);

    // Holds a weak sender so the stream ends when the run task drops `tx`.
    let weak = tx.downgrade();
    let token = stop.clone();
    let each_message: EachMessage =
        Arc::new(move |message: ConsumerMessage| -> BoxFuture<'static, ()> {
            let (tx, stop) = (weak.upgrade(), token.clone());
            Box::pin(async move {
                let Some(tx) = tx else { return };
                // Waits while the buffer is full (backpressure); a stop releases it.
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => {}
                    _ = tx.send(Ok(message)) => {}
                }
            })
        });

    let token = stop.clone();
    tokio::spawn(async move {
        let mut run = consumer.run(each_message);
        let finished = tokio::select! {
            result = &mut run => Some(result),
            _ = token.cancelled() => None,
        };
        let result = match finished {
            Some(result) => result,
            None => {
                // Best effort, like the JS: errors while stopping are swallowed.
                let _ = consumer.stop().await;
                let _ = run.await;
                Ok(())
            }
        };
        if let Err(e) = result {
            let _ = tx.send(Err(e)).await;
        }
    });

    let guard = stop.clone().drop_guard();
    let stream = async_stream::stream! {
        // Dropping the stream stops the consumer.
        let _guard = guard;
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };
    let stream: DataStream<ConsumerMessage> = Box::pin(stream);
    Ok((stream, stop))
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
        assert_eq!(consumer.count("stop"), 1);
    }
}
