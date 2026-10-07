// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Queue Storage receive, send and delete streams.

use async_stream::try_stream;
use azure_storage_queue::models::{
    QueueClientReceiveMessagesOptions, QueueClientSendMessageOptions, QueueMessage, ReceivedMessage,
};
use azure_storage_queue::QueueClient;
use datastream_core::{CancellationToken, DataStream, Result, StreamExt};

use crate::client::{send, AzurePollingOptions};

/// Receive messages until the queue is empty (or forever with `polling_active`).
/// Set `number_of_messages` (max 32) in `request` to receive more than one per call.
pub fn azure_queue_receive_messages_stream(
    client: QueueClient,
    request: Option<QueueClientReceiveMessagesOptions<'static>>,
    options: AzurePollingOptions,
) -> DataStream<ReceivedMessage> {
    Box::pin(try_stream! {
        loop {
            let response = send(client.receive_messages(request.clone()), options.signal.as_ref()).await?;
            let messages = response.into_model()?.items.unwrap_or_default();
            let empty = messages.is_empty();
            for message in messages {
                yield message;
            }
            if empty {
                if !options.polling_active {
                    break;
                }
                options.idle().await?;
            }
        }
    })
}

/// Send each chunk as a message's text. Queue Storage has no batch send, so
/// messages go one request at a time.
pub async fn azure_queue_send_message_stream(
    mut input: DataStream<String>,
    client: QueueClient,
    request: Option<QueueClientSendMessageOptions<'static>>,
    signal: Option<CancellationToken>,
) -> Result<()> {
    while let Some(text) = input.next().await {
        let message = QueueMessage {
            message_text: Some(text?),
        };
        send(
            client.send_message(message.try_into()?, request.clone()),
            signal.as_ref(),
        )
        .await?;
    }
    Ok(())
}

/// Delete received messages (by `message_id` + `pop_receipt`).
pub async fn azure_queue_delete_message_stream(
    mut input: DataStream<ReceivedMessage>,
    client: QueueClient,
    signal: Option<CancellationToken>,
) -> Result<()> {
    while let Some(message) = input.next().await {
        let message = message?;
        let (Some(id), Some(pop_receipt)) = (&message.message_id, &message.pop_receipt) else {
            return Err("azureQueueDeleteMessage requires message_id and pop_receipt".into());
        };
        send(
            client.delete_message(id, pop_receipt, None),
            signal.as_ref(),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock::MockHttpClient;
    use azure_core::http::Url;
    use azure_storage_queue::QueueClientOptions;
    use datastream_core::{create_readable_stream, stream_to_array};
    use std::sync::Arc;
    use std::time::Duration;

    fn client(mock: &Arc<MockHttpClient>) -> QueueClient {
        let options = QueueClientOptions {
            client_options: mock.client_options(),
            ..Default::default()
        };
        let url = Url::parse("https://account.queue.core.windows.net/queue").unwrap();
        QueueClient::new(url, None, Some(options)).unwrap()
    }

    fn page(ids: &[&str]) -> String {
        let messages: String = ids
            .iter()
            .map(|id| {
                format!(
                    "<QueueMessage><MessageId>{id}</MessageId><PopReceipt>r{id}</PopReceipt>\
                     <MessageText>text {id}</MessageText><DequeueCount>1</DequeueCount></QueueMessage>"
                )
            })
            .collect();
        format!("<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList>{messages}</QueueMessagesList>")
    }

    #[tokio::test]
    async fn receive_stream_reads_until_empty() {
        let mock = MockHttpClient::new(&[(200, &page(&["1", "2"])), (200, &page(&[]))]);
        let stream = azure_queue_receive_messages_stream(client(&mock), None, Default::default());
        let messages = stream_to_array(stream, None).await.unwrap();
        let ids: Vec<_> = messages
            .iter()
            .map(|m| m.message_id.clone().unwrap())
            .collect();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(mock.requests().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn receive_stream_polls_after_empty_page() {
        let mock = MockHttpClient::new(&[(200, &page(&[])), (200, &page(&["1"]))]);
        let options = AzurePollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_millis(10)),
            ..Default::default()
        };
        let mut stream = azure_queue_receive_messages_stream(client(&mock), None, options);
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.message_id.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn receive_stream_aborts_on_signal() {
        let signal = CancellationToken::new();
        signal.cancel();
        let mock = MockHttpClient::new(&[]);
        let options = AzurePollingOptions {
            signal: Some(signal),
            ..Default::default()
        };
        let stream = azure_queue_receive_messages_stream(client(&mock), None, options);
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[tokio::test]
    async fn send_stream_posts_each_message() {
        let sent = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList><QueueMessage>\
                    <MessageId>1</MessageId><PopReceipt>r</PopReceipt></QueueMessage></QueueMessagesList>";
        let mock = MockHttpClient::new(&[(201, sent), (201, sent)]);
        let input = create_readable_stream(["a".to_string(), "b".to_string()]);
        azure_queue_send_message_stream(input, client(&mock), None, None)
            .await
            .unwrap();
        let bodies = mock.bodies.lock().unwrap().clone();
        assert_eq!(bodies.len(), 2);
        assert!(String::from_utf8_lossy(&bodies[1]).contains("<MessageText>b</MessageText>"));
    }

    #[tokio::test]
    async fn delete_stream_deletes_received_messages() {
        // Receive is lazy, so each delete runs before the next receive.
        let mock = MockHttpClient::new(&[
            (200, &page(&["1", "2"])),
            (204, ""),
            (204, ""),
            (200, &page(&[])),
        ]);
        let received = azure_queue_receive_messages_stream(client(&mock), None, Default::default());
        azure_queue_delete_message_stream(received, client(&mock), None)
            .await
            .unwrap();
        let requests = mock.requests();
        assert!(
            requests[1].starts_with("DELETE") && requests[1].contains("/messages/1?popreceipt=r1"),
            "{requests:?}"
        );
        assert!(
            requests[2].contains("/messages/2?popreceipt=r2"),
            "{requests:?}"
        );
    }

    #[tokio::test]
    async fn delete_stream_rejects_message_without_receipt() {
        let mock = MockHttpClient::new(&[(200, "<QueueMessagesList><QueueMessage><MessageId>1</MessageId></QueueMessage></QueueMessagesList>"), (200, &page(&[]))]);
        let received = azure_queue_receive_messages_stream(client(&mock), None, Default::default());
        let e = azure_queue_delete_message_stream(received, client(&mock), None)
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "azureQueueDeleteMessage requires message_id and pop_receipt"
        );
    }
}
