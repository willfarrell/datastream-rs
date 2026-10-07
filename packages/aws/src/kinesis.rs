// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Kinesis get and put records streams.

use async_stream::try_stream;
use aws_sdk_kinesis::operation::get_records::builders::GetRecordsFluentBuilder;
use aws_sdk_kinesis::operation::put_records::builders::PutRecordsFluentBuilder;
use aws_sdk_kinesis::types::{PutRecordsRequestEntry, Record};
use datastream_core::{DataStream, Error, Result};

use crate::client::{
    batch_write, cause_error, send, AwsPollingOptions, AwsWriteOptions, Batcher, IntoVec, Retry,
};

// PutRecords limits: <=500 records, <=5 MiB aggregate, <=1 MiB per record.
const KINESIS_MAX_RECORDS: usize = 500;
const KINESIS_MAX_RECORD_BYTES: usize = 1024 * 1024;
// 5 MiB aggregate with headroom for request framing.
const KINESIS_MAX_BATCH_BYTES: usize = 5 * 1024 * 1024 - 64 * 1024;

/// Read records from the request's shard iterator until no more arrive (or
/// forever with `polling_active`), stopping when the shard closes.
pub fn aws_kinesis_get_records_stream(
    mut request: GetRecordsFluentBuilder,
    options: AwsPollingOptions,
) -> DataStream<Record> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), options.signal.as_ref()).await?;
            let records = output.records.into_vec();
            let empty = records.is_empty();
            for record in records {
                yield record;
            }
            let Some(next) = output.next_shard_iterator else {
                break;
            };
            request = request.shard_iterator(next);
            if empty {
                if !options.polling_active {
                    break;
                }
                options.idle().await?;
            }
        }
    })
}

/// Put records with PutRecords, retrying the records whose result has an `ErrorCode`.
pub async fn aws_kinesis_put_records_stream(
    input: DataStream<PutRecordsRequestEntry>,
    request: PutRecordsFluentBuilder,
    options: AwsWriteOptions,
) -> Result<()> {
    let signal = options.signal.clone();
    let batcher = Batcher::<PutRecordsRequestEntry> {
        max_entries: KINESIS_MAX_RECORDS,
        max_entry_bytes: KINESIS_MAX_RECORD_BYTES,
        max_batch_bytes: KINESIS_MAX_BATCH_BYTES,
        size: record_size,
        oversize: |_, bytes| {
            let cause = format!("bytes: {bytes}, limit: {KINESIS_MAX_RECORD_BYTES}");
            cause_error("awsKinesisPutRecords record exceeds 1MiB limit", cause)
        },
    };
    let retry = Retry {
        max_count: options.retry_max_count.unwrap_or(10),
        signal: options.signal,
        message: "awsKinesisPutRecords has failed records",
    };
    batch_write(input, batcher, retry, move |records| {
        let request = request.clone().set_records(Some(records.clone()));
        let signal = signal.clone();
        async move {
            let output = send(request.send(), signal.as_ref()).await?;
            if output.failed_record_count().unwrap_or(0) == 0 {
                return Ok((Vec::new(), String::new()));
            }
            // Results line up with the sent records by index.
            let results = output.records();
            let failed = records
                .into_iter()
                .zip(results)
                .filter(|(_, result)| result.error_code().is_some())
                .map(|(record, _)| record)
                .collect();
            let errors: Vec<_> = results
                .iter()
                .filter(|r| r.error_code().is_some())
                .collect();
            Ok::<_, Error>((failed, format!("{errors:?}")))
        }
    })
    .await
}

fn record_size(record: &PutRecordsRequestEntry) -> usize {
    record.data().as_ref().len()
        + record.partition_key().map_or(0, str::len)
        + record.explicit_hash_key().map_or(0, str::len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
    use aws_sdk_kinesis::operation::put_records::PutRecordsOutput;
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::PutRecordsResultEntry;
    use aws_sdk_kinesis::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, stream_to_array, CancellationToken, StreamExt};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn record(data: &str) -> Record {
        Record::builder()
            .sequence_number(data)
            .data(Blob::new(data))
            .partition_key("pk")
            .build()
            .unwrap()
    }

    fn get_output(records: &[&str], next: Option<&str>) -> GetRecordsOutput {
        GetRecordsOutput::builder()
            .set_records(Some(records.iter().map(|r| record(r)).collect()))
            .set_next_shard_iterator(next.map(String::from))
            .build()
            .unwrap()
    }

    fn entry(data: &str, partition_key: &str) -> PutRecordsRequestEntry {
        PutRecordsRequestEntry::builder()
            .data(Blob::new(data))
            .partition_key(partition_key)
            .build()
            .unwrap()
    }

    // `failed` lists, per sent record, whether its result carries an ErrorCode.
    fn put_output(failed: &[bool]) -> PutRecordsOutput {
        let results = failed
            .iter()
            .map(|f| {
                let result = PutRecordsResultEntry::builder();
                if *f {
                    result
                        .error_code("ProvisionedThroughputExceededException")
                        .build()
                } else {
                    result.sequence_number("1").build()
                }
            })
            .collect();
        PutRecordsOutput::builder()
            .failed_record_count(failed.iter().filter(|f| **f).count() as i32)
            .set_records(Some(results))
            .build()
            .unwrap()
    }

    fn data(records: &[Record]) -> Vec<String> {
        records
            .iter()
            .map(|r| String::from_utf8(r.data().as_ref().to_vec()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn get_records_stream_tracks_shard_iterator() {
        let iterators = Arc::new(Mutex::new(Vec::new()));
        let seen = iterators.clone();
        let rule = mock!(Client::get_records).then_compute_output(move |req| {
            let mut seen = seen.lock().unwrap();
            seen.push(req.shard_iterator().unwrap_or_default().to_string());
            match seen.len() {
                1 => get_output(&["a"], Some("it-2")),
                2 => get_output(&["b"], Some("it-3")),
                _ => get_output(&[], Some("it-4")),
            }
        });
        let client = mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]);
        let stream = aws_kinesis_get_records_stream(
            client.get_records().shard_iterator("it-1"),
            AwsPollingOptions::default(),
        );
        let output = stream_to_array(stream, None).await.unwrap();
        assert_eq!(data(&output), ["a", "b"]);
        assert_eq!(*iterators.lock().unwrap(), ["it-1", "it-2", "it-3"]);
    }

    #[tokio::test]
    async fn get_records_stream_stops_on_closed_shard_even_when_polling() {
        let rule = mock!(Client::get_records).then_output(|| get_output(&["a"], None));
        let client = mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            ..Default::default()
        };
        let stream = aws_kinesis_get_records_stream(client.get_records(), options);
        assert_eq!(data(&stream_to_array(stream, None).await.unwrap()), ["a"]);
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn get_records_stream_polls_with_delay() {
        let rule = mock!(Client::get_records)
            .sequence()
            .output(|| get_output(&[], Some("it")))
            .output(|| get_output(&["a"], Some("it")))
            .repeatedly()
            .build();
        let client = mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        let mut stream = aws_kinesis_get_records_stream(client.get_records(), options);
        stream.next().await.unwrap().unwrap();
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert_eq!(rule.num_calls(), 2);
    }

    #[tokio::test]
    async fn get_records_stream_aborts_idle_poll() {
        let rule = mock!(Client::get_records).then_output(|| get_output(&[], Some("it")));
        let client = mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_secs(60)),
            signal: Some(signal.clone()),
        };
        let stream = aws_kinesis_get_records_stream(client.get_records(), options);
        let task = tokio::spawn(stream_to_array(stream, None));
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        assert_eq!(task.await.unwrap().unwrap_err().to_string(), "Aborted");
    }

    fn recording_client(responses: Vec<Vec<bool>>) -> (Client, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::put_records).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            let sent: Vec<String> = req
                .records()
                .iter()
                .map(|r| String::from_utf8(r.data().as_ref().to_vec()).unwrap())
                .collect();
            let failed = responses
                .get(calls.len())
                .cloned()
                .unwrap_or_else(|| vec![false; sent.len()]);
            calls.push(sent);
            put_output(&failed)
        });
        (
            mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]),
            calls,
        )
    }

    async fn put(
        client: &Client,
        input: Vec<PutRecordsRequestEntry>,
        options: AwsWriteOptions,
    ) -> Result<()> {
        let request = client.put_records().stream_name("s");
        aws_kinesis_put_records_stream(create_readable_stream(input), request, options).await
    }

    #[tokio::test]
    async fn put_records_stream_batches_by_500() {
        let (client, calls) = recording_client(Vec::new());
        let input = (0..501).map(|i| entry(&i.to_string(), "pk")).collect();
        put(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        let sizes: Vec<_> = calls.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, [500, 1]);
    }

    #[tokio::test]
    async fn put_records_stream_handles_empty_input() {
        let (client, calls) = recording_client(Vec::new());
        put(&client, Vec::new(), AwsWriteOptions::default())
            .await
            .unwrap();
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn put_records_stream_retries_only_failed_records() {
        let (client, calls) = recording_client(vec![vec![false, true, false]]);
        let input = vec![entry("a", "pk"), entry("b", "pk"), entry("c", "pk")];
        put(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec!["a", "b", "c"], vec!["b"]]);
    }

    #[tokio::test(start_paused = true)]
    async fn put_records_stream_throws_after_retry_max_count() {
        let (client, calls) = recording_client(vec![vec![true, false]; 20]);
        let options = AwsWriteOptions {
            retry_max_count: Some(2),
            ..Default::default()
        };
        let input = vec![entry("a", "pk"), entry("b", "pk")];
        let e = put(&client, input, options).await.unwrap_err();
        assert_eq!(e.to_string(), "awsKinesisPutRecords has failed records");
        let cause = &e.downcast_ref::<crate::CauseError>().unwrap().cause;
        assert!(
            cause.contains("ProvisionedThroughputExceededException"),
            "{cause}"
        );
        assert!(!cause.contains("sequence_number: Some"), "{cause}");
        assert_eq!(calls.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn put_records_stream_does_not_retry_without_error_codes() {
        let rule = mock!(Client::put_records).then_output(|| {
            // FailedRecordCount says 1, but no result carries an ErrorCode.
            PutRecordsOutput::builder()
                .failed_record_count(1)
                .set_records(Some(Vec::new()))
                .build()
                .unwrap()
        });
        let client = mock_client!(aws_sdk_kinesis, RuleMode::MatchAny, [&rule]);
        put(&client, vec![entry("a", "pk")], AwsWriteOptions::default())
            .await
            .unwrap();
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_records_stream_splits_at_aggregate_cap() {
        let (client, calls) = recording_client(Vec::new());
        // Five 1MiB records exceed 5MiB-64KiB, so the fifth starts a new batch.
        let big = "x".repeat(KINESIS_MAX_RECORD_BYTES - 2);
        let input = (0..5).map(|_| entry(&big, "pk")).collect();
        put(&client, input, AwsWriteOptions::default())
            .await
            .unwrap();
        let sizes: Vec<_> = calls.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, [4, 1]);
    }

    #[tokio::test]
    async fn put_records_stream_record_size_limit() {
        let (client, _) = recording_client(Vec::new());
        // Exactly 1MiB including the partition key is accepted.
        let exact = entry(&"x".repeat(KINESIS_MAX_RECORD_BYTES - 2), "pk");
        put(&client, vec![exact], AwsWriteOptions::default())
            .await
            .unwrap();

        let over = entry(&"x".repeat(KINESIS_MAX_RECORD_BYTES - 1), "pk");
        let e = put(&client, vec![over], AwsWriteOptions::default())
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsKinesisPutRecords record exceeds 1MiB limit"
        );
        assert_eq!(
            e.downcast_ref::<crate::CauseError>().unwrap().cause,
            "bytes: 1048577, limit: 1048576"
        );

        let hashed = PutRecordsRequestEntry::builder()
            .data(Blob::new("x".repeat(KINESIS_MAX_RECORD_BYTES - 2)))
            .partition_key("pk")
            .explicit_hash_key("1")
            .build()
            .unwrap();
        assert!(put(&client, vec![hashed], AwsWriteOptions::default())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn put_records_stream_aborts() {
        let (client, _) = recording_client(Vec::new());
        let signal = CancellationToken::new();
        signal.cancel();
        let options = AwsWriteOptions {
            signal: Some(signal),
            ..Default::default()
        };
        let e = put(&client, vec![entry("a", "pk")], options)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
