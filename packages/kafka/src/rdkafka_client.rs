// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! [`Kafka`] backed by `rdkafka`.

use std::sync::Arc;
use std::time::Duration;

use datastream_core::{CancellationToken, Result};
use futures::future::{try_join_all, BoxFuture};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use crate::{ConsumerMessage, EachMessage, Kafka, KafkaConsumer, KafkaProducer, ProducerMessage};

const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Creates producers and consumers from a `ClientConfig`
/// (`bootstrap.servers`, `client.id`, `security.protocol`, `acks`, ...).
pub struct RdKafka(pub ClientConfig);

impl Kafka for RdKafka {
    fn producer(&self) -> Result<Arc<dyn KafkaProducer>> {
        let producer: Arc<dyn KafkaProducer> = Arc::new(RdKafkaProducer(self.0.create()?));
        Ok(producer)
    }
    fn consumer(&self, group_id: &str) -> Result<Arc<dyn KafkaConsumer>> {
        let consumer: StreamConsumer = self.0.clone().set("group.id", group_id).create()?;
        let consumer: Arc<dyn KafkaConsumer> = Arc::new(RdKafkaConsumer {
            consumer,
            stop: CancellationToken::new(),
        });
        Ok(consumer)
    }
}

pub struct RdKafkaProducer(pub FutureProducer);

impl RdKafkaProducer {
    async fn flush(&self) -> Result<()> {
        let producer = self.0.clone();
        tokio::task::spawn_blocking(move || producer.flush(FLUSH_TIMEOUT)).await??;
        Ok(())
    }

    async fn send_all(
        &self,
        topic: &str,
        messages: &[ProducerMessage],
        timeout: Option<Duration>,
    ) -> Result<()> {
        try_join_all(messages.iter().map(|message| {
            let mut headers = OwnedHeaders::new();
            for (key, value) in &message.headers {
                headers = headers.insert(Header {
                    key: key.as_str(),
                    value: Some(value.as_slice()),
                });
            }
            let mut record = FutureRecord::<[u8], [u8]>::to(topic).headers(headers);
            if let Some(key) = &message.key {
                record = record.key(key.as_slice());
            }
            if let Some(value) = &message.value {
                record = record.payload(value.as_slice());
            }
            self.0
                .send(record, timeout.map_or(Timeout::Never, Timeout::After))
        }))
        .await
        .map_err(|(e, _)| e)?;
        Ok(())
    }
}

impl KafkaProducer for RdKafkaProducer {
    // librdkafka connects lazily.
    fn connect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.flush())
    }
    fn send<'a>(
        &'a self,
        topic: &'a str,
        messages: &'a [ProducerMessage],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.send_all(topic, messages, timeout))
    }
}

pub struct RdKafkaConsumer {
    pub consumer: StreamConsumer,
    stop: CancellationToken,
}

impl RdKafkaConsumer {
    async fn subscribe_all(&self, topics: &[String]) -> Result<()> {
        let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.consumer.subscribe(&topics)?;
        Ok(())
    }

    async fn run_until_stopped(&self, each_message: EachMessage) -> Result<()> {
        loop {
            let message = tokio::select! {
                _ = self.stop.cancelled() => return Ok(()),
                message = self.consumer.recv() => to_message(&message?),
            };
            each_message(message).await;
        }
    }
}

impl KafkaConsumer for RdKafkaConsumer {
    fn connect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<()>> {
        self.consumer.unsubscribe();
        Box::pin(async { Ok(()) })
    }
    fn subscribe<'a>(&'a self, topics: &'a [String]) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.subscribe_all(topics))
    }
    fn run(&self, each_message: EachMessage) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.run_until_stopped(each_message))
    }
    fn stop(&self) -> BoxFuture<'_, Result<()>> {
        self.stop.cancel();
        Box::pin(async { Ok(()) })
    }
}

fn to_message(message: &BorrowedMessage<'_>) -> ConsumerMessage {
    ConsumerMessage {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        key: message.key().map(<[u8]>::to_vec),
        value: message.payload().map(<[u8]>::to_vec),
        headers: message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|h| {
                        (
                            h.key.to_string(),
                            h.value.map(<[u8]>::to_vec).unwrap_or_default(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
        timestamp: message.timestamp().to_millis(),
    }
}
