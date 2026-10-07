// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Event Hubs receive and send streams.

use async_stream::try_stream;
use azure_messaging_eventhubs::models::{EventData, ReceivedEventData};
use azure_messaging_eventhubs::{EventDataBatchOptions, EventReceiver, ProducerClient};
use datastream_core::{CancellationToken, DataStream, Result, StreamExt};

use crate::client::send;

/// Stream events from a partition receiver (`ConsumerClient::open_receiver_on_partition`).
/// Runs until `signal` aborts, or until the receiver's `receive_timeout` elapses
/// with no event.
pub fn azure_event_hubs_receive_events_stream(
    receiver: EventReceiver,
    signal: Option<CancellationToken>,
) -> DataStream<ReceivedEventData> {
    Box::pin(try_stream! {
        let mut events = receiver.stream_events();
        while let Some(event) = send(async { events.next().await.transpose() }, signal.as_ref()).await? {
            yield event;
        }
    })
}

#[derive(Default, Clone, Debug)]
pub struct AzureEventHubsSendOptions {
    pub partition_id: Option<String>,
    pub partition_key: Option<String>,
    /// Batch size limit (default: the link's max message size).
    pub max_size_in_bytes: Option<u64>,
    pub signal: Option<CancellationToken>,
}

/// Send events in batches as large as the producer link allows.
pub async fn azure_event_hubs_send_events_stream(
    mut input: DataStream<EventData>,
    producer: &ProducerClient,
    options: AzureEventHubsSendOptions,
) -> Result<()> {
    let signal = options.signal.as_ref();
    let mut batch = send(producer.create_batch(Some(batch_options(&options))), signal).await?;
    while let Some(event) = input.next().await {
        let event = event?;
        if batch.try_add_event_data(event.clone(), None)? {
            continue;
        }
        if batch.is_empty() {
            return Err("azureEventHubsSendBatch event exceeds batch size limit".into());
        }
        send(producer.send_batch(batch, None), signal).await?;
        batch = send(producer.create_batch(Some(batch_options(&options))), signal).await?;
        if !batch.try_add_event_data(event, None)? {
            return Err("azureEventHubsSendBatch event exceeds batch size limit".into());
        }
    }
    if !batch.is_empty() {
        send(producer.send_batch(batch, None), signal).await?;
    }
    Ok(())
}

fn batch_options(options: &AzureEventHubsSendOptions) -> EventDataBatchOptions {
    EventDataBatchOptions {
        max_size_in_bytes: options.max_size_in_bytes,
        partition_key: options.partition_key.clone(),
        partition_id: options.partition_id.clone(),
    }
}
