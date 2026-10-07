// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! SQS receive, send and delete streams.

use std::collections::{HashMap, HashSet};

use async_stream::try_stream;
use aws_sdk_sqs::operation::delete_message_batch::builders::DeleteMessageBatchFluentBuilder;
use aws_sdk_sqs::operation::receive_message::builders::ReceiveMessageFluentBuilder;
use aws_sdk_sqs::operation::send_message_batch::builders::SendMessageBatchFluentBuilder;
use aws_sdk_sqs::types::{
    DeleteMessageBatchRequestEntry, Message, MessageAttributeValue, SendMessageBatchRequestEntry,
};
use datastream_core::{DataStream, Error, Result};

use crate::client::{
    batch_write, cause_error, send, AwsPollingOptions, AwsWriteOptions, Batcher, IntoVec, Retry,
};

// SendMessageBatch/DeleteMessageBatch: <=10 entries, <=256KiB aggregate payload.
const SQS_MAX_ENTRIES: usize = 10;
const SQS_MAX_BATCH_BYTES: usize = 256 * 1024;

/// Receive messages until the queue is empty (or forever with `polling_active`).
pub fn aws_sqs_receive_message_stream(
    request: ReceiveMessageFluentBuilder,
    options: AwsPollingOptions,
) -> DataStream<Message> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), options.signal.as_ref()).await?;
            let messages = output.messages.into_vec();
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

/// Send entries with SendMessageBatch, retrying the `Failed` subset.
pub async fn aws_sqs_send_message_stream(
    input: DataStream<SendMessageBatchRequestEntry>,
    request: SendMessageBatchFluentBuilder,
    options: AwsWriteOptions,
) -> Result<()> {
    let signal = options.signal.clone();
    let batcher = Batcher::<SendMessageBatchRequestEntry> {
        max_entries: SQS_MAX_ENTRIES,
        max_entry_bytes: SQS_MAX_BATCH_BYTES,
        max_batch_bytes: SQS_MAX_BATCH_BYTES,
        size: |entry| entry.message_body().len() + attributes_size(entry.message_attributes()),
        oversize: |entry, bytes| {
            oversize(
                "awsSQSSendMessageBatch entry exceeds 256KiB limit",
                entry.id(),
                bytes,
            )
        },
    };
    let retry = retry(options, "awsSQSSendMessageBatch has failed entries");
    batch_write(input, batcher, retry, move |entries| {
        let request = request.clone().set_entries(Some(entries.clone()));
        let signal = signal.clone();
        async move {
            let output = send(request.send(), signal.as_ref()).await?;
            let failed: HashSet<&str> = output.failed().iter().map(|e| e.id()).collect();
            let cause = format!("{:?}", output.failed());
            let entries = entries
                .into_iter()
                .filter(|e| failed.contains(e.id()))
                .collect();
            Ok::<_, Error>((entries, cause))
        }
    })
    .await
}

/// Delete entries with DeleteMessageBatch, retrying the `Failed` subset.
pub async fn aws_sqs_delete_message_stream(
    input: DataStream<DeleteMessageBatchRequestEntry>,
    request: DeleteMessageBatchFluentBuilder,
    options: AwsWriteOptions,
) -> Result<()> {
    let signal = options.signal.clone();
    let batcher = Batcher::<DeleteMessageBatchRequestEntry> {
        max_entries: SQS_MAX_ENTRIES,
        max_entry_bytes: SQS_MAX_BATCH_BYTES,
        max_batch_bytes: SQS_MAX_BATCH_BYTES,
        size: |entry| entry.id().len() + entry.receipt_handle().len(),
        oversize: |entry, bytes| {
            oversize(
                "awsSQSDeleteMessageBatch entry exceeds 256KiB limit",
                entry.id(),
                bytes,
            )
        },
    };
    let retry = retry(options, "awsSQSDeleteMessageBatch has failed entries");
    batch_write(input, batcher, retry, move |entries| {
        let request = request.clone().set_entries(Some(entries.clone()));
        let signal = signal.clone();
        async move {
            let output = send(request.send(), signal.as_ref()).await?;
            let failed: HashSet<&str> = output.failed().iter().map(|e| e.id()).collect();
            let cause = format!("{:?}", output.failed());
            let entries = entries
                .into_iter()
                .filter(|e| failed.contains(e.id()))
                .collect();
            Ok::<_, Error>((entries, cause))
        }
    })
    .await
}

fn retry(options: AwsWriteOptions, message: &'static str) -> Retry {
    Retry {
        max_count: options.retry_max_count.unwrap_or(10),
        signal: options.signal,
        message,
    }
}

fn oversize(message: &str, id: &str, bytes: usize) -> Error {
    let cause = format!("Id: {id}, bytes: {bytes}, limit: {SQS_MAX_BATCH_BYTES}");
    cause_error(message, cause)
}

// The SQS size limit counts the body plus each attribute's name, type and value.
fn attributes_size(attributes: Option<&HashMap<String, MessageAttributeValue>>) -> usize {
    attributes
        .into_iter()
        .flatten()
        .fold(0, |size, (name, value)| {
            size + name.len()
                + value.data_type().len()
                + value.string_value().map_or(0, str::len)
                + value.binary_value().map_or(0, |b| b.as_ref().len())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput;
    use aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput;
    use aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput;
    use aws_sdk_sqs::types::BatchResultErrorEntry;
    use aws_sdk_sqs::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, stream_to_array, CancellationToken, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn message(id: &str) -> Message {
        Message::builder().message_id(id).build()
    }

    fn entry(id: &str, body: &str) -> SendMessageBatchRequestEntry {
        SendMessageBatchRequestEntry::builder()
            .id(id)
            .message_body(body)
            .build()
            .unwrap()
    }

    fn send_output(failed: &[&str]) -> SendMessageBatchOutput {
        let failed = failed
            .iter()
            .map(|id| {
                BatchResultErrorEntry::builder()
                    .id(*id)
                    .code("Throttled")
                    .sender_fault(false)
                    .build()
                    .unwrap()
            })
            .collect();
        SendMessageBatchOutput::builder()
            .set_successful(Some(Vec::new()))
            .set_failed(Some(failed))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn receive_message_stream_reads_until_empty() {
        let rule = mock!(Client::receive_message)
            .sequence()
            .output(|| {
                ReceiveMessageOutput::builder()
                    .messages(message("1"))
                    .messages(message("2"))
                    .build()
            })
            .output(|| ReceiveMessageOutput::builder().build())
            .build();
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let stream = aws_sqs_receive_message_stream(
            client.receive_message().queue_url("q"),
            AwsPollingOptions::default(),
        );
        let output = stream_to_array(stream, None).await.unwrap();
        let ids: Vec<_> = output.iter().map(|m| m.message_id().unwrap()).collect();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(rule.num_calls(), 2);
    }

    #[tokio::test]
    async fn receive_message_stream_handles_empty_queue() {
        let rule =
            mock!(Client::receive_message).then_output(|| ReceiveMessageOutput::builder().build());
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let stream =
            aws_sqs_receive_message_stream(client.receive_message(), AwsPollingOptions::default());
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn receive_message_stream_keeps_polling_with_delay() {
        let rule = mock!(Client::receive_message)
            .sequence()
            .output(|| ReceiveMessageOutput::builder().build())
            .output(|| {
                ReceiveMessageOutput::builder()
                    .messages(message("1"))
                    .build()
            })
            .repeatedly()
            .build();
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        let mut stream = aws_sqs_receive_message_stream(client.receive_message(), options);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.message_id(), Some("1"));
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert_eq!(rule.num_calls(), 2);
    }

    #[tokio::test]
    async fn receive_message_stream_aborts_idle_poll() {
        let rule =
            mock!(Client::receive_message).then_output(|| ReceiveMessageOutput::builder().build());
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_secs(60)),
            signal: Some(signal.clone()),
        };
        let stream = aws_sqs_receive_message_stream(client.receive_message(), options);
        let task = tokio::spawn(stream_to_array(stream, None));
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        let e = task.await.unwrap().unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[tokio::test]
    async fn send_message_stream_batches_by_ten() {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let seen = sizes.clone();
        let rule = mock!(Client::send_message_batch).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.entries().len());
            send_output(&[])
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let input = (0..11)
            .map(|i| entry(&i.to_string(), "x"))
            .collect::<Vec<_>>();
        aws_sqs_send_message_stream(
            create_readable_stream(input),
            client.send_message_batch().queue_url("q"),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(*sizes.lock().unwrap(), [10, 1]);
    }

    #[tokio::test]
    async fn send_message_stream_handles_empty_input() {
        let rule = mock!(Client::send_message_batch).then_output(|| send_output(&[]));
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        aws_sqs_send_message_stream(
            create_readable_stream(Vec::new()),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(rule.num_calls(), 0);
    }

    #[tokio::test]
    async fn send_message_stream_flushes_before_256kib() {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let seen = sizes.clone();
        let rule = mock!(Client::send_message_batch).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.entries().len());
            send_output(&[])
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let body = "x".repeat(100 * 1024);
        let input = vec![entry("1", &body), entry("2", &body), entry("3", &body)];
        aws_sqs_send_message_stream(
            create_readable_stream(input),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(*sizes.lock().unwrap(), [2, 1]);
    }

    #[tokio::test]
    async fn send_message_stream_keeps_exactly_256kib_in_one_batch() {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let seen = sizes.clone();
        let rule = mock!(Client::send_message_batch).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.entries().len());
            send_output(&[])
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let half = "x".repeat(128 * 1024);
        let input = vec![entry("1", &half), entry("2", &half)];
        aws_sqs_send_message_stream(
            create_readable_stream(input),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(*sizes.lock().unwrap(), [2]);
    }

    #[tokio::test]
    async fn send_message_stream_rejects_oversize_entry() {
        let rule = mock!(Client::send_message_batch).then_output(|| send_output(&[]));
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        // Exactly 256KiB is allowed, one byte more is rejected.
        let ok = vec![entry("1", &"x".repeat(SQS_MAX_BATCH_BYTES))];
        aws_sqs_send_message_stream(
            create_readable_stream(ok),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        let big = vec![entry("big", &"x".repeat(SQS_MAX_BATCH_BYTES + 1))];
        let e = aws_sqs_send_message_stream(
            create_readable_stream(big),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsSQSSendMessageBatch entry exceeds 256KiB limit"
        );
        assert_eq!(
            e.downcast_ref::<crate::CauseError>().unwrap().cause,
            "Id: big, bytes: 262145, limit: 262144"
        );
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn send_message_stream_retries_failed_entries() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::send_message_batch).then_compute_output(move |req| {
            let ids: Vec<String> = req.entries().iter().map(|e| e.id().to_string()).collect();
            let mut calls = seen.lock().unwrap();
            calls.push(ids);
            if calls.len() == 1 {
                send_output(&["2"])
            } else {
                send_output(&[])
            }
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        aws_sqs_send_message_stream(
            create_readable_stream(vec![entry("1", "a"), entry("2", "b")]),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec!["1", "2"], vec!["2"]]);
    }

    #[tokio::test(start_paused = true)]
    async fn send_message_stream_throws_after_retry_max_count() {
        let rule = mock!(Client::send_message_batch).then_output(|| send_output(&["1"]));
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let e = aws_sqs_send_message_stream(
            create_readable_stream(vec![entry("1", "a")]),
            client.send_message_batch(),
            AwsWriteOptions {
                retry_max_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "awsSQSSendMessageBatch has failed entries");
        let cause = &e.downcast_ref::<crate::CauseError>().unwrap().cause;
        assert!(cause.contains("Throttled"), "{cause}");
        assert_eq!(rule.num_calls(), 2);

        let rule = mock!(Client::send_message_batch).then_output(|| send_output(&["1"]));
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        aws_sqs_send_message_stream(
            create_readable_stream(vec![entry("1", "a")]),
            client.send_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(rule.num_calls(), 11);
    }

    #[tokio::test]
    async fn send_message_stream_aborts() {
        let rule = mock!(Client::send_message_batch).then_output(|| send_output(&["1"]));
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        signal.cancel();
        let e = aws_sqs_send_message_stream(
            create_readable_stream(vec![entry("1", "a")]),
            client.send_message_batch(),
            AwsWriteOptions {
                signal: Some(signal),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[tokio::test(start_paused = true)]
    async fn delete_message_stream_deletes_and_throws_on_failure() {
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let rule = mock!(Client::delete_message_batch).then_compute_output(move |req| {
            seen.fetch_add(req.entries().len(), Ordering::SeqCst);
            DeleteMessageBatchOutput::builder()
                .set_successful(Some(Vec::new()))
                .set_failed(Some(Vec::new()))
                .build()
                .unwrap()
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let entry = |id: &str| {
            DeleteMessageBatchRequestEntry::builder()
                .id(id)
                .receipt_handle("r")
                .build()
                .unwrap()
        };
        aws_sqs_delete_message_stream(
            create_readable_stream(vec![entry("1"), entry("2")]),
            client.delete_message_batch().queue_url("q"),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let rule = mock!(Client::delete_message_batch).then_output(|| {
            let failed = BatchResultErrorEntry::builder()
                .id("1")
                .code("X")
                .sender_fault(true)
                .build()
                .unwrap();
            DeleteMessageBatchOutput::builder()
                .set_successful(Some(Vec::new()))
                .failed(failed)
                .build()
                .unwrap()
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let e = aws_sqs_delete_message_stream(
            create_readable_stream(vec![entry("1")]),
            client.delete_message_batch(),
            AwsWriteOptions {
                retry_max_count: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "awsSQSDeleteMessageBatch has failed entries");
    }

    #[tokio::test]
    async fn delete_message_stream_rejects_oversize_entry() {
        let rule = mock!(Client::delete_message_batch).then_output(|| {
            DeleteMessageBatchOutput::builder()
                .set_successful(Some(Vec::new()))
                .set_failed(Some(Vec::new()))
                .build()
                .unwrap()
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let big = DeleteMessageBatchRequestEntry::builder()
            .id("1")
            .receipt_handle("r".repeat(SQS_MAX_BATCH_BYTES))
            .build()
            .unwrap();
        let e = aws_sqs_delete_message_stream(
            create_readable_stream(vec![big]),
            client.delete_message_batch(),
            AwsWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsSQSDeleteMessageBatch entry exceeds 256KiB limit"
        );
    }

    #[test]
    fn attributes_count_toward_size() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("value")
                .build()
                .unwrap(),
        );
        assert_eq!(attributes_size(Some(&attributes)), 4 + 6 + 5);
        assert_eq!(attributes_size(None), 0);
    }
}
