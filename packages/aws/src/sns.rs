// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! SNS publish stream.

use std::collections::HashSet;

use aws_sdk_sns::operation::publish_batch::builders::PublishBatchFluentBuilder;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use datastream_core::{DataStream, Error, Result};

use crate::client::{batch_write, cause_error, send, AwsWriteOptions, Batcher, Retry};

// PublishBatch: <=10 entries, <=256KiB aggregate payload.
const SNS_MAX_ENTRIES: usize = 10;
const SNS_MAX_BATCH_BYTES: usize = 256 * 1024;

/// Publish entries with PublishBatch, retrying the `Failed` subset.
pub async fn aws_sns_publish_message_stream(
    input: DataStream<PublishBatchRequestEntry>,
    request: PublishBatchFluentBuilder,
    options: AwsWriteOptions,
) -> Result<()> {
    let signal = options.signal.clone();
    let batcher = Batcher::<PublishBatchRequestEntry> {
        max_entries: SNS_MAX_ENTRIES,
        max_entry_bytes: SNS_MAX_BATCH_BYTES,
        max_batch_bytes: SNS_MAX_BATCH_BYTES,
        size,
        oversize: |entry, bytes| {
            let cause = format!(
                "Id: {}, bytes: {bytes}, limit: {SNS_MAX_BATCH_BYTES}",
                entry.id()
            );
            cause_error("awsSNSPublishBatch entry exceeds 256KiB limit", cause)
        },
    };
    let retry = Retry {
        max_count: options.retry_max_count.unwrap_or(10),
        signal: options.signal,
        message: "awsSNSPublishBatch has failed entries",
    };
    batch_write(input, batcher, retry, move |entries| {
        let request = request
            .clone()
            .set_publish_batch_request_entries(Some(entries.clone()));
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

// The SNS size limit counts the message plus each attribute's name, type and value.
fn size(entry: &PublishBatchRequestEntry) -> usize {
    let attributes = entry.message_attributes().into_iter().flatten();
    entry.message().len()
        + entry.subject().map_or(0, str::len)
        + attributes.fold(0, |size, (name, value)| {
            size + name.len()
                + value.data_type().len()
                + value.string_value().map_or(0, str::len)
                + value.binary_value().map_or(0, |b| b.as_ref().len())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sns::operation::publish_batch::PublishBatchOutput;
    use aws_sdk_sns::types::{BatchResultErrorEntry, MessageAttributeValue};
    use aws_sdk_sns::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, CancellationToken};
    use std::sync::{Arc, Mutex};

    fn entry(id: &str, message: &str) -> PublishBatchRequestEntry {
        PublishBatchRequestEntry::builder()
            .id(id)
            .message(message)
            .build()
            .unwrap()
    }

    fn output(failed: &[&str]) -> PublishBatchOutput {
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
        PublishBatchOutput::builder()
            .set_failed(Some(failed))
            .build()
    }

    fn recording_client(
        failures: Vec<Vec<&'static str>>,
    ) -> (Client, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::publish_batch).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            let ids = req
                .publish_batch_request_entries()
                .iter()
                .map(|e| e.id().to_string())
                .collect();
            calls.push(ids);
            output(failures.get(calls.len() - 1).map_or(&[][..], |f| &f[..]))
        });
        (
            mock_client!(aws_sdk_sns, RuleMode::MatchAny, [&rule]),
            calls,
        )
    }

    async fn publish(
        client: &Client,
        input: Vec<PublishBatchRequestEntry>,
        options: AwsWriteOptions,
    ) -> Result<()> {
        let request = client
            .publish_batch()
            .topic_arn("arn:aws:sns:us-east-1:000000000000:test");
        aws_sns_publish_message_stream(create_readable_stream(input), request, options).await
    }

    #[tokio::test]
    async fn publishes_in_batches_of_ten() {
        let (client, calls) = recording_client(Vec::new());
        let input = "abcdefghijk"
            .chars()
            .map(|c| entry(&c.to_string(), "m"))
            .collect();
        publish(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].len(), 10);
        assert_eq!(calls[1], ["k"]);
    }

    #[tokio::test]
    async fn handles_empty_input() {
        let (client, calls) = recording_client(Vec::new());
        publish(&client, Vec::new(), AwsWriteOptions::default())
            .await
            .unwrap();
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_failed_entries() {
        let (client, calls) = recording_client(vec![vec!["b"]]);
        let input = vec![entry("a", "1"), entry("b", "2")];
        publish(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec!["a", "b"], vec!["b"]]);
    }

    #[tokio::test(start_paused = true)]
    async fn throws_after_retry_max_count() {
        let (client, calls) = recording_client(vec![vec!["a"]; 20]);
        let options = AwsWriteOptions {
            retry_max_count: Some(1),
            ..Default::default()
        };
        let e = publish(&client, vec![entry("a", "1")], options)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "awsSNSPublishBatch has failed entries");
        let cause = &e.downcast_ref::<crate::CauseError>().unwrap().cause;
        assert!(cause.contains("Throttled"), "{cause}");
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn flushes_before_256kib_and_keeps_exact_fit() {
        let (client, calls) = recording_client(Vec::new());
        let body = "x".repeat(100 * 1024);
        let input = vec![entry("1", &body), entry("2", &body), entry("3", &body)];
        publish(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        let sizes: Vec<_> = calls.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, [2, 1]);

        let (client, calls) = recording_client(Vec::new());
        let half = "x".repeat(128 * 1024);
        publish(
            &client,
            vec![entry("1", &half), entry("2", &half)],
            AwsWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rejects_entry_over_256kib() {
        let (client, calls) = recording_client(Vec::new());
        let exact = vec![entry("ok", &"x".repeat(SNS_MAX_BATCH_BYTES))];
        publish(&client, exact, AwsWriteOptions::default())
            .await
            .unwrap();
        let big = vec![entry("big", &"x".repeat(SNS_MAX_BATCH_BYTES + 1))];
        let e = publish(&client, big, AwsWriteOptions::default())
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsSNSPublishBatch entry exceeds 256KiB limit"
        );
        assert_eq!(
            e.downcast_ref::<crate::CauseError>().unwrap().cause,
            "Id: big, bytes: 262145, limit: 262144"
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn aborts_via_signal() {
        let (client, _) = recording_client(vec![vec!["a"]; 20]);
        let signal = CancellationToken::new();
        signal.cancel();
        let options = AwsWriteOptions {
            signal: Some(signal),
            ..Default::default()
        };
        let e = publish(&client, vec![entry("a", "1")], options)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[test]
    fn size_counts_subject_and_attributes() {
        let attribute = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("v")
            .build()
            .unwrap();
        let entry = PublishBatchRequestEntry::builder()
            .id("1")
            .message("abc")
            .subject("s")
            .message_attributes("k", attribute)
            .build()
            .unwrap();
        assert_eq!(size(&entry), 3 + 1 + 1 + 6 + 1);
    }
}
