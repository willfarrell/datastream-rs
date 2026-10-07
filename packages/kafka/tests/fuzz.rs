// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datastream_core::{create_readable_stream, StreamExt};
use datastream_kafka::{
    kafka_connect, kafka_consume_stream, kafka_produce_stream, ConsumerMessage, EachMessage, Kafka,
    KafkaConnectOptions, KafkaConsumeOptions, KafkaConsumer, KafkaProduceOptions, KafkaProducer,
    ProducerMessage,
};
use futures::future::BoxFuture;
use proptest::prelude::*;
use tokio::sync::Notify;

type Result<T> = datastream_core::Result<T>;

/// Records every batch it is asked to send.
#[derive(Default)]
struct Producer {
    batches: Mutex<Vec<Vec<ProducerMessage>>>,
}

impl KafkaProducer for Producer {
    fn connect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn send<'a>(
        &'a self,
        _topic: &'a str,
        messages: &'a [ProducerMessage],
        _timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<()>> {
        self.batches.lock().unwrap().push(messages.to_vec());
        Box::pin(async { Ok(()) })
    }
}

/// Delivers `messages` from `run`, then blocks until `stop`.
#[derive(Default)]
struct Consumer {
    messages: Vec<ConsumerMessage>,
    stopped: Notify,
}

impl KafkaConsumer for Consumer {
    fn connect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn subscribe<'a>(&'a self, _topics: &'a [String]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn run(&self, each_message: EachMessage) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            for message in &self.messages {
                each_message(message.clone()).await;
            }
            self.stopped.notified().await;
            Ok(())
        })
    }
    fn stop(&self) -> BoxFuture<'_, Result<()>> {
        self.stopped.notify_one();
        Box::pin(async { Ok(()) })
    }
}

struct FakeKafka;

impl Kafka for FakeKafka {
    fn producer(&self) -> Result<Arc<dyn KafkaProducer>> {
        Ok(Arc::new(Producer::default()))
    }
    fn consumer(&self, _group_id: &str) -> Result<Arc<dyn KafkaConsumer>> {
        Ok(Arc::new(Consumer::default()))
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn consumer_message() -> impl Strategy<Value = ConsumerMessage> {
    (
        ".{1,50}",
        0..10i32,
        any::<i64>(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..50)),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..200)),
    )
        .prop_map(|(topic, partition, offset, key, value)| ConsumerMessage {
            topic,
            partition,
            offset,
            key,
            value,
            ..Default::default()
        })
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_kafka_connect_options(group_id in prop::option::of(".{0,50}"), producer in prop::option::of(any::<bool>())) {
        let want_consumer = group_id.as_deref().is_some_and(|id| !id.is_empty());
        let connection = runtime().block_on(async {
            let options = KafkaConnectOptions { group_id, producer };
            let connection = kafka_connect(&FakeKafka, options).await.unwrap();
            connection.disconnect().await.unwrap();
            connection
        });
        prop_assert_eq!(connection.producer.is_some(), producer != Some(false));
        prop_assert_eq!(connection.consumer.is_some(), want_consumer);
    }

    #[test]
    fn fuzz_kafka_produce_stream_options(
        topic in prop::option::of(".{0,50}"),
        batch_size in prop::option::of(0..200usize),
        timeout in prop::option::of(0..30_000u64),
    ) {
        let result = runtime().block_on(kafka_produce_stream(
            create_readable_stream(Vec::<String>::new()),
            KafkaProduceOptions {
                producer: Some(Arc::new(Producer::default())),
                topic: topic.clone(),
                batch_size,
                timeout: timeout.map(Duration::from_millis),
            },
        ));
        match result {
            Ok(()) => {
                prop_assert!(topic.is_some_and(|t| !t.is_empty()));
                prop_assert_ne!(batch_size, Some(0));
            }
            Err(e) => prop_assert!([
                "kafkaProduceStream: topic required",
                "kafkaProduceStream: batchSize must be >= 1",
            ]
            .contains(&e.to_string().as_str())),
        }
    }

    // Every message is sent once, in order, in full batches but the last.
    #[test]
    fn fuzz_kafka_produce_stream_messages(
        messages in prop::collection::vec(".{0,100}", 0..300),
        batch_size in 1..150usize,
    ) {
        let producer = Arc::new(Producer::default());
        runtime()
            .block_on(kafka_produce_stream(
                create_readable_stream(messages.clone()),
                KafkaProduceOptions {
                    producer: Some(producer.clone()),
                    topic: Some("topic".into()),
                    batch_size: Some(batch_size),
                    timeout: None,
                },
            ))
            .unwrap();
        let batches = producer.batches.lock().unwrap();
        let sent: Vec<ProducerMessage> = batches.concat();
        let expected: Vec<ProducerMessage> = messages.into_iter().map(Into::into).collect();
        prop_assert_eq!(sent, expected);
        if let Some((last, full)) = batches.split_last() {
            prop_assert!(full.iter().all(|b| b.len() == batch_size));
            prop_assert!(!last.is_empty() && last.len() <= batch_size);
        }
    }

    #[test]
    fn fuzz_kafka_consume_stream_options(topics in prop::collection::vec(".{1,50}", 0..8)) {
        let result = runtime().block_on(async {
            let options = KafkaConsumeOptions {
                consumer: Some(Arc::new(Consumer::default())),
                topics: topics.clone(),
                signal: None,
            };
            let (_stream, stop) = kafka_consume_stream(options).await?;
            stop.cancel();
            Ok::<_, datastream_core::Error>(())
        });
        match result {
            Ok(()) => prop_assert!(!topics.is_empty()),
            Err(e) => prop_assert_eq!(e.to_string(), "kafkaConsumeStream: topics required"),
        }
    }

    #[test]
    fn fuzz_kafka_consume_stream_messages(messages in prop::collection::vec(consumer_message(), 0..50)) {
        let received = runtime().block_on(async {
            let options = KafkaConsumeOptions {
                consumer: Some(Arc::new(Consumer { messages: messages.clone(), ..Default::default() })),
                topics: vec!["topic".into()],
                signal: None,
            };
            let (stream, stop) = kafka_consume_stream(options).await.unwrap();
            let mut stream = stream.take(messages.len());
            let mut received = Vec::new();
            while let Some(message) = stream.next().await {
                received.push(message.unwrap());
            }
            stop.cancel();
            received
        });
        prop_assert_eq!(received, messages);
    }
}
