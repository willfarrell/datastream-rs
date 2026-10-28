// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Event Hubs receive and send streams.

use async_stream::try_stream;
use azure_messaging_eventhubs::models::{EventData, ReceivedEventData};
use azure_messaging_eventhubs::{
    EventDataBatch, EventDataBatchOptions, EventReceiver, ProducerClient,
};
use datastream_core::{CancellationToken, DataStream, Result, StreamExt};

use crate::client::{send, with_failed_messages};

/// Stream events from a partition receiver (`ConsumerClient::open_receiver_on_partition`).
/// Ends once the receiver's `receive_timeout` elapses with no event (the JS
/// stream's empty page without `pollingActive`); leave `receive_timeout` unset
/// to keep receiving until `signal` aborts.
pub fn azure_event_hubs_receive_events_stream(
    receiver: EventReceiver,
    signal: Option<CancellationToken>,
) -> DataStream<ReceivedEventData> {
    Box::pin(try_stream! {
        let mut events = receiver.stream_events();
        while let Some(event) = next_event(&mut events, signal.as_ref()).await? {
            yield event;
        }
    })
}

// The SDK reports an elapsed `receive_timeout` as an error, not the end of
// the stream: treat it as the end, as the JS SDK's empty page is.
async fn next_event<T, E>(
    events: &mut (impl futures::Stream<Item = std::result::Result<T, E>> + Unpin),
    signal: Option<&CancellationToken>,
) -> Result<Option<T>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match send(async { events.next().await.transpose() }, signal).await {
        Err(error) if is_receive_timeout(&*error) => Ok(None),
        result => result,
    }
}

// The SDK's timeout is a bare `io::Error` of kind `TimedOut` (no OS code or
// payload), so a real socket timeout still fails the stream.
fn is_receive_timeout(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        let io = error
            .downcast_ref::<std::io::Error>()
            .or_else(|| error.downcast_ref::<Box<std::io::Error>>().map(|e| &**e));
        if let Some(io) = io {
            return io.kind() == std::io::ErrorKind::TimedOut
                && io.raw_os_error().is_none()
                && io.get_ref().is_none();
        }
        current = error.source();
    }
    false
}

#[derive(Default, Clone, Debug)]
pub struct AzureEventHubsSendOptions {
    pub partition_id: Option<String>,
    pub partition_key: Option<String>,
    /// Batch size limit (default: the link's max message size).
    pub max_size_in_bytes: Option<u64>,
    pub signal: Option<CancellationToken>,
}

/// Send events in batches as large as the producer link allows. An input
/// error or abort fails with [`crate::AzureFailedMessagesError`] carrying the events
/// buffered but not yet sent (in-flight batches are not included).
pub async fn azure_event_hubs_send_events_stream(
    input: DataStream<EventData>,
    producer: &ProducerClient,
    options: AzureEventHubsSendOptions,
) -> Result<()> {
    let signal = options.signal.clone();
    let batcher = Producer {
        producer,
        options: &options,
    };
    batch_send(input, &batcher, signal.as_ref()).await
}

const OVERSIZE: &str = "azureEventHubsSendBatch event exceeds batch size limit";

// The SDK batch calls the send loop needs, so it can be tested without AMQP.
trait Batcher {
    type Item;
    type Batch;
    async fn create_batch(&self) -> Result<Self::Batch>;
    fn try_add(&self, batch: &Self::Batch, item: Self::Item) -> Result<bool>;
    fn is_empty(batch: &Self::Batch) -> bool;
    async fn send_batch(&self, batch: Self::Batch) -> Result<()>;
}

struct Producer<'a> {
    producer: &'a ProducerClient,
    options: &'a AzureEventHubsSendOptions,
}

impl<'a> Batcher for Producer<'a> {
    type Item = EventData;
    type Batch = EventDataBatch<'a>;

    async fn create_batch(&self) -> Result<EventDataBatch<'a>> {
        let options = EventDataBatchOptions {
            max_size_in_bytes: self.options.max_size_in_bytes,
            partition_key: self.options.partition_key.clone(),
            partition_id: self.options.partition_id.clone(),
        };
        Ok(self.producer.create_batch(Some(options)).await?)
    }

    fn try_add(&self, batch: &EventDataBatch<'a>, item: EventData) -> Result<bool> {
        Ok(batch.try_add_event_data(item, None)?)
    }

    fn is_empty(batch: &EventDataBatch<'a>) -> bool {
        batch.is_empty()
    }

    async fn send_batch(&self, batch: EventDataBatch<'a>) -> Result<()> {
        Ok(self.producer.send_batch(batch, None).await?)
    }
}

// Flush when the batch is full and fail fast on an event that cannot fit an
// empty batch. The batch is created on the first event, so empty input opens
// no link.
async fn batch_send<B>(
    mut input: DataStream<B::Item>,
    batcher: &B,
    signal: Option<&CancellationToken>,
) -> Result<()>
where
    B: Batcher,
    B::Item: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let mut batch = None;
    // The SDK batch does not expose its events, so they are tracked here too.
    let mut pending = Vec::new();
    loop {
        // An input error or abort leaves the batch unsent: hand its events back.
        let item = match send(async { input.next().await.transpose() }, signal).await {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(error) => return Err(with_failed_messages(error, pending)),
        };
        let current = match batch.take() {
            Some(current) => current,
            None => send(batcher.create_batch(), signal).await?,
        };
        if batcher.try_add(&current, item.clone())? {
            pending.push(item);
            batch = Some(current);
            continue;
        }
        if B::is_empty(&current) {
            return Err(OVERSIZE.into());
        }
        // Cleared before the send: in-flight events are no longer buffered.
        pending.clear();
        send(batcher.send_batch(current), signal).await?;
        let current = send(batcher.create_batch(), signal).await?;
        if !batcher.try_add(&current, item.clone())? {
            return Err(OVERSIZE.into());
        }
        pending.push(item);
        batch = Some(current);
    }
    if let Some(current) = batch.filter(|current| !B::is_empty(current)) {
        send(batcher.send_batch(current), signal).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AzureFailedMessagesError;
    use datastream_core::create_readable_stream;
    use std::sync::Mutex;

    // Fake SDK batch: holds at most `max` entries; "big" never fits.
    #[derive(Default)]
    struct FakeBatcher {
        max: usize,
        created: Mutex<usize>,
        sent: Mutex<Vec<Vec<&'static str>>>,
    }

    impl Batcher for FakeBatcher {
        type Item = &'static str;
        type Batch = Mutex<Vec<&'static str>>;

        async fn create_batch(&self) -> Result<Self::Batch> {
            *self.created.lock().unwrap() += 1;
            Ok(Mutex::default())
        }

        fn try_add(&self, batch: &Self::Batch, item: &'static str) -> Result<bool> {
            let mut batch = batch.lock().unwrap();
            if item == "big" || batch.len() == self.max {
                return Ok(false);
            }
            batch.push(item);
            Ok(true)
        }

        fn is_empty(batch: &Self::Batch) -> bool {
            batch.lock().unwrap().is_empty()
        }

        async fn send_batch(&self, batch: Self::Batch) -> Result<()> {
            self.sent.lock().unwrap().push(batch.into_inner().unwrap());
            Ok(())
        }
    }

    fn batcher(max: usize) -> FakeBatcher {
        FakeBatcher {
            max,
            ..Default::default()
        }
    }

    // Built the way the SDK reports an elapsed `receive_timeout`.
    fn receive_timeout() -> azure_messaging_eventhubs::EventHubsError {
        azure_core::Error::new(
            azure_core::error::ErrorKind::Io,
            Box::new(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        )
        .into()
    }

    type Received = std::result::Result<&'static str, azure_messaging_eventhubs::EventHubsError>;

    async fn receive_all(items: Vec<Received>) -> Result<Vec<&'static str>> {
        let mut events = futures::stream::iter(items);
        let mut out = Vec::new();
        while let Some(event) = next_event(&mut events, None).await? {
            out.push(event);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn receive_ends_on_receive_timeout() {
        let items = vec![Ok("a"), Ok("b"), Err(receive_timeout()), Ok("c")];
        assert_eq!(receive_all(items).await.unwrap(), ["a", "b"]);
    }

    #[tokio::test]
    async fn receive_fails_on_other_errors() {
        let os_timeout = azure_core::Error::new(
            azure_core::error::ErrorKind::Io,
            std::io::Error::from_raw_os_error(60),
        );
        for error in [
            azure_messaging_eventhubs::EventHubsError::from(os_timeout),
            azure_core::Error::with_message(azure_core::error::ErrorKind::Other, "boom").into(),
        ] {
            let e = receive_all(vec![Ok("a"), Err(error)]).await.unwrap_err();
            assert!(!e.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn receive_aborts_on_signal() {
        let signal = CancellationToken::new();
        signal.cancel();
        let mut events = futures::stream::pending::<Received>();
        let e = next_event(&mut events, Some(&signal)).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[tokio::test]
    async fn batch_send_flushes_full_batches_and_the_remainder() {
        let batcher = batcher(2);
        let input = create_readable_stream(["a", "b", "c", "d", "e"]);
        batch_send(input, &batcher, None).await.unwrap();
        assert_eq!(
            *batcher.sent.lock().unwrap(),
            [vec!["a", "b"], vec!["c", "d"], vec!["e"]]
        );
    }

    #[tokio::test]
    async fn batch_send_creates_nothing_for_empty_input() {
        let batcher = batcher(2);
        let input = create_readable_stream(Vec::<&str>::new());
        batch_send(input, &batcher, None).await.unwrap();
        assert_eq!(*batcher.created.lock().unwrap(), 0);
        assert!(batcher.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn batch_send_rejects_an_event_too_big_for_an_empty_batch() {
        let batcher = batcher(2);
        let e = batch_send(create_readable_stream(["big"]), &batcher, None)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), OVERSIZE);
        assert!(batcher.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn batch_send_rejects_oversize_event_after_flushing() {
        let batcher = batcher(2);
        let e = batch_send(create_readable_stream(["a", "big"]), &batcher, None)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), OVERSIZE);
        assert_eq!(*batcher.sent.lock().unwrap(), [vec!["a"]]);
    }

    #[tokio::test]
    async fn batch_send_input_error_attaches_buffered_events() {
        let batcher = batcher(2);
        let items: Vec<Result<&str>> = vec![Ok("a"), Ok("b"), Ok("c"), Err("boom".into())];
        let input: DataStream<&str> = Box::pin(futures::stream::iter(items));
        let e = batch_send(input, &batcher, None).await.unwrap_err();
        assert_eq!(e.to_string(), "boom");
        let e = e.downcast_ref::<AzureFailedMessagesError<&str>>().unwrap();
        // Only the event not yet sent; nothing is sent on failure.
        assert_eq!(e.failed_messages, ["c"]);
        assert_eq!(*batcher.sent.lock().unwrap(), [vec!["a", "b"]]);
    }

    #[tokio::test]
    async fn batch_send_abort_attaches_buffered_events_without_sending() {
        let batcher = batcher(2);
        let signal = CancellationToken::new();
        let (tx, rx) = futures::channel::mpsc::unbounded::<Result<&str>>();
        tx.unbounded_send(Ok("a")).unwrap();
        tx.unbounded_send(Ok("b")).unwrap();
        let input: DataStream<&str> = Box::pin(rx);
        let abort = signal.clone();
        let (result, ()) = tokio::join!(batch_send(input, &batcher, Some(&signal)), async {
            tokio::task::yield_now().await;
            abort.cancel();
        });
        let e = result.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        let e = e.downcast_ref::<AzureFailedMessagesError<&str>>().unwrap();
        assert_eq!(e.failed_messages, ["a", "b"]);
        assert!(batcher.sent.lock().unwrap().is_empty());
        drop(tx);
    }

    #[tokio::test]
    async fn batch_send_input_error_with_empty_batch_is_unchanged() {
        let batcher = batcher(2);
        let items: Vec<Result<&str>> = vec![Err("boom".into())];
        let input: DataStream<&str> = Box::pin(futures::stream::iter(items));
        let e = batch_send(input, &batcher, None).await.unwrap_err();
        assert_eq!(e.to_string(), "boom");
        assert!(e.downcast_ref::<AzureFailedMessagesError<&str>>().is_none());
    }
}
