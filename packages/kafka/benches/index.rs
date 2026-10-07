// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, Result, StreamExt};
use datastream_kafka::{
    kafka_consume_stream, kafka_produce_stream, ConsumerMessage, EachMessage, KafkaConsumeOptions,
    KafkaConsumer, KafkaProduceOptions, KafkaProducer, ProducerMessage,
};
use futures::future::BoxFuture;
use tokio::sync::Notify;

const MSG_COUNT: usize = 10_000;

struct Producer;

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
        _messages: &'a [ProducerMessage],
        _timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Delivers `MSG_COUNT` messages from `run`, then blocks until `stop`.
#[derive(Default)]
struct Consumer {
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
            for i in 0..MSG_COUNT {
                each_message(ConsumerMessage {
                    topic: "perf-topic".into(),
                    offset: i as i64,
                    key: Some(format!("key-{i}").into_bytes()),
                    value: Some(format!("value-{i}").into_bytes()),
                    ..Default::default()
                })
                .await;
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

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let messages: Vec<ProducerMessage> = (0..MSG_COUNT)
        .map(|i| ProducerMessage {
            key: Some(format!("key-{i}").into_bytes()),
            value: Some(format!("message-{i}").into_bytes()),
            headers: Vec::new(),
        })
        .collect();
    let strings: Vec<String> = (0..MSG_COUNT).map(|i| format!("message-{i}")).collect();
    let produce = |batch_size| KafkaProduceOptions {
        producer: Some(Arc::new(Producer)),
        topic: Some("perf-topic".into()),
        batch_size,
        timeout: None,
    };

    let mut group = c.benchmark_group("kafkaProduceStream");
    for batch_size in [None, Some(1_000)] {
        let name = format!(
            "produce {MSG_COUNT} messages (batchSize {})",
            batch_size.unwrap_or(100)
        );
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                let input = create_readable_stream(messages.clone());
                kafka_produce_stream(input, produce(batch_size))
                    .await
                    .unwrap()
            })
        });
    }
    group.bench_function(format!("produce {MSG_COUNT} string messages"), |b| {
        b.to_async(&rt).iter(|| async {
            let input = create_readable_stream(strings.clone());
            kafka_produce_stream(input, produce(None)).await.unwrap()
        })
    });
    group.finish();

    c.bench_function(
        &format!("kafkaConsumeStream/consume {MSG_COUNT} messages"),
        |b| {
            b.to_async(&rt).iter(|| async {
                let options = KafkaConsumeOptions {
                    consumer: Some(Arc::new(Consumer::default())),
                    topics: vec!["perf-topic".into()],
                    signal: None,
                };
                let (stream, stop) = kafka_consume_stream(options).await.unwrap();
                let mut stream = stream.take(MSG_COUNT);
                while let Some(message) = stream.next().await {
                    message.unwrap();
                }
                stop.cancel();
            })
        },
    );
}

criterion_group!(index, benches);
criterion_main!(index);
