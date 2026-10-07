// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! DynamoDB Streams get records stream.

use async_stream::try_stream;
use aws_sdk_dynamodbstreams::operation::get_records::builders::GetRecordsFluentBuilder;
use aws_sdk_dynamodbstreams::types::Record;
use datastream_core::DataStream;

use crate::client::{send, AwsPollingOptions, IntoVec};

/// Read records from the request's shard iterator until no more arrive (or
/// forever with `polling_active`), stopping when the shard closes.
pub fn aws_dynamodb_streams_get_records_stream(
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodbstreams::operation::get_records::GetRecordsOutput;
    use aws_sdk_dynamodbstreams::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{stream_to_array, CancellationToken, StreamExt};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn output(ids: &[&str], next: Option<&str>) -> GetRecordsOutput {
        let records = ids.iter().map(|id| Record::builder().event_id(*id).build());
        GetRecordsOutput::builder()
            .set_records(Some(records.collect()))
            .set_next_shard_iterator(next.map(String::from))
            .build()
    }

    fn ids(records: &[Record]) -> Vec<&str> {
        records.iter().map(|r| r.event_id().unwrap()).collect()
    }

    #[tokio::test]
    async fn tracks_shard_iterator_and_passes_limit() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::get_records).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            calls.push((
                req.shard_iterator().unwrap_or_default().to_string(),
                req.limit(),
            ));
            match calls.len() {
                1 => output(&["1"], Some("it-2")),
                _ => output(&[], Some("it-3")),
            }
        });
        let client = mock_client!(aws_sdk_dynamodbstreams, RuleMode::MatchAny, [&rule]);
        let request = client.get_records().shard_iterator("it-1").limit(5);
        let stream = aws_dynamodb_streams_get_records_stream(request, AwsPollingOptions::default());
        let records = stream_to_array(stream, None).await.unwrap();
        assert_eq!(ids(&records), ["1"]);
        assert_eq!(
            *calls.lock().unwrap(),
            [("it-1".to_string(), Some(5)), ("it-2".to_string(), Some(5))]
        );
    }

    #[tokio::test]
    async fn handles_empty_records_and_closed_shard() {
        let rule = mock!(Client::get_records).then_output(|| GetRecordsOutput::builder().build());
        let client = mock_client!(aws_sdk_dynamodbstreams, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            ..Default::default()
        };
        let stream = aws_dynamodb_streams_get_records_stream(client.get_records(), options);
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn polls_with_delay_only_when_empty() {
        let rule = mock!(Client::get_records)
            .sequence()
            .output(|| output(&["1"], Some("it")))
            .output(|| output(&[], Some("it")))
            .output(|| output(&["2"], Some("it")))
            .repeatedly()
            .build();
        let client = mock_client!(aws_sdk_dynamodbstreams, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        let mut stream = aws_dynamodb_streams_get_records_stream(client.get_records(), options);
        stream.next().await.unwrap().unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));
        stream.next().await.unwrap().unwrap();
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn aborts_idle_poll() {
        let rule = mock!(Client::get_records).then_output(|| output(&[], Some("it")));
        let client = mock_client!(aws_sdk_dynamodbstreams, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_secs(60)),
            signal: Some(signal.clone()),
        };
        let stream = aws_dynamodb_streams_get_records_stream(client.get_records(), options);
        let task = tokio::spawn(stream_to_array(stream, None));
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        assert_eq!(task.await.unwrap().unwrap_err().to_string(), "Aborted");
    }

    #[tokio::test]
    async fn non_polling_empty_page_ends() {
        let rule = mock!(Client::get_records).then_output(|| output(&[], Some("it")));
        let client = mock_client!(aws_sdk_dynamodbstreams, RuleMode::MatchAny, [&rule]);
        let stream = aws_dynamodb_streams_get_records_stream(
            client.get_records(),
            AwsPollingOptions::default(),
        );
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(rule.num_calls(), 1);
    }
}
