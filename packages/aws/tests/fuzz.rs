// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::types::{AttributeValue, WriteRequest};
use aws_sdk_sns::operation::publish_batch::PublishBatchOutput;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use datastream_aws::dynamodb::{
    aws_dynamodb_delete_item_stream, aws_dynamodb_put_item_stream, aws_dynamodb_query_stream,
    AwsDynamoDBOptions, Item,
};
use datastream_aws::s3::{aws_s3_checksum_stream, AwsS3ChecksumOptions};
use datastream_aws::sns::aws_sns_publish_message_stream;
use datastream_aws::sqs::aws_sqs_send_message_stream;
use datastream_aws::AwsWriteOptions;
use datastream_core::{create_readable_stream, result, stream_to_array};
use proptest::prelude::*;

// SNS PublishBatch and SQS SendMessageBatch limits.
const MAX_ENTRIES: usize = 10;
const MAX_BATCH_BYTES: usize = 256 * 1024;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn item(id: usize) -> Item {
    HashMap::from([("id".to_string(), AttributeValue::N(id.to_string()))])
}

fn dynamodb_options() -> AwsDynamoDBOptions {
    AwsDynamoDBOptions {
        table_name: "FuzzTable".into(),
        ..Default::default()
    }
}

/// A DynamoDB client that accepts every BatchWriteItem and records the batches.
fn dynamodb_write_client() -> (aws_sdk_dynamodb::Client, Arc<Mutex<Vec<Vec<WriteRequest>>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let rule = mock!(aws_sdk_dynamodb::Client::batch_write_item).then_compute_output(move |req| {
        let batch = req.request_items().unwrap()["FuzzTable"].clone();
        seen.lock().unwrap().push(batch);
        BatchWriteItemOutput::builder().build()
    });
    (
        mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]),
        calls,
    )
}

/// Every write reaches DynamoDB once, in order, 25 per batch.
fn check_dynamodb_batches(
    calls: &[Vec<WriteRequest>],
    count: usize,
    key: impl Fn(&WriteRequest) -> &Item,
) -> std::result::Result<(), TestCaseError> {
    prop_assert_eq!(calls.len(), count.div_ceil(25));
    prop_assert!(calls
        .iter()
        .all(|batch| !batch.is_empty() && batch.len() <= 25));
    let written: Vec<Item> = calls.iter().flatten().map(|r| key(r).clone()).collect();
    prop_assert_eq!(written, (0..count).map(item).collect::<Vec<_>>());
    Ok(())
}

fn sqs_entry(id: usize, bytes: usize) -> SendMessageBatchRequestEntry {
    SendMessageBatchRequestEntry::builder()
        .id(id.to_string())
        .message_body("x".repeat(bytes))
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_aws_dynamodb_put_item_stream_count(count in 0..100usize) {
        let (client, calls) = dynamodb_write_client();
        let input = create_readable_stream((0..count).map(item).collect::<Vec<_>>());
        runtime()
            .block_on(aws_dynamodb_put_item_stream(client, input, dynamodb_options()))
            .unwrap();
        check_dynamodb_batches(&calls.lock().unwrap(), count, |r| r.put_request().unwrap().item())?;
    }

    #[test]
    fn fuzz_aws_dynamodb_delete_item_stream_count(count in 0..100usize) {
        let (client, calls) = dynamodb_write_client();
        let input = create_readable_stream((0..count).map(item).collect::<Vec<_>>());
        runtime()
            .block_on(aws_dynamodb_delete_item_stream(client, input, dynamodb_options()))
            .unwrap();
        check_dynamodb_batches(&calls.lock().unwrap(), count, |r| r.delete_request().unwrap().key())?;
    }

    // Every item of every page comes out once, in order, including empty pages.
    #[test]
    fn fuzz_aws_dynamodb_query_stream_page_sizes(pages in prop::collection::vec(0..20usize, 1..10)) {
        let total: usize = pages.iter().sum();
        let state = Arc::new(Mutex::new((0usize, 0usize))); // (page, next id)
        let page_sizes = pages.clone();
        let rule = mock!(aws_sdk_dynamodb::Client::query).then_compute_output(move |_| {
            let mut state = state.lock().unwrap();
            let (page, next) = *state;
            let items: Vec<Item> = (next..next + page_sizes[page]).map(item).collect();
            *state = (page + 1, next + page_sizes[page]);
            let more = page + 1 < page_sizes.len();
            QueryOutput::builder()
                .set_items(Some(items))
                .set_last_evaluated_key(more.then(|| item(next)))
                .build()
        });
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream = aws_dynamodb_query_stream(client.query().table_name("FuzzTable"), None);
        let items = runtime().block_on(stream_to_array(stream, None)).unwrap();
        prop_assert_eq!(items, (0..total).map(item).collect::<Vec<_>>());
        prop_assert_eq!(rule.num_calls(), pages.len());
    }

    #[test]
    fn fuzz_aws_sns_publish_message_stream_count(count in 0..100usize) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(aws_sdk_sns::Client::publish_batch).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.publish_batch_request_entries().to_vec());
            PublishBatchOutput::builder().build()
        });
        let client = mock_client!(aws_sdk_sns, RuleMode::MatchAny, [&rule]);
        let input: Vec<PublishBatchRequestEntry> = (0..count)
            .map(|i| PublishBatchRequestEntry::builder().id(i.to_string()).message("m").build().unwrap())
            .collect();
        runtime()
            .block_on(aws_sns_publish_message_stream(
                create_readable_stream(input.clone()),
                client.publish_batch().topic_arn("arn:aws:sns:us-east-1:000000000000:fuzz"),
                AwsWriteOptions::default(),
            ))
            .unwrap();
        let calls = calls.lock().unwrap();
        prop_assert_eq!(calls.len(), count.div_ceil(MAX_ENTRIES));
        prop_assert_eq!(calls.concat(), input);
    }

    // Batches respect both the entry count and the aggregate byte limit.
    #[test]
    fn fuzz_aws_sqs_send_message_stream_sizes(sizes in prop::collection::vec(0..60_000usize, 0..60)) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(aws_sdk_sqs::Client::send_message_batch).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.entries().to_vec());
            SendMessageBatchOutput::builder()
                .set_successful(Some(Vec::new()))
                .set_failed(Some(Vec::new()))
                .build()
                .unwrap()
        });
        let client = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&rule]);
        let input: Vec<_> = sizes.iter().enumerate().map(|(i, n)| sqs_entry(i, *n)).collect();
        runtime()
            .block_on(aws_sqs_send_message_stream(
                create_readable_stream(input.clone()),
                client.send_message_batch().queue_url("https://sqs.us-east-1.amazonaws.com/0/fuzz"),
                AwsWriteOptions::default(),
            ))
            .unwrap();
        let calls = calls.lock().unwrap();
        for batch in calls.iter() {
            prop_assert!(!batch.is_empty() && batch.len() <= MAX_ENTRIES);
            prop_assert!(batch.iter().map(|e| e.message_body().len()).sum::<usize>() <= MAX_BATCH_BYTES);
        }
        prop_assert_eq!(calls.concat(), input);
    }

    // The checksum depends only on the bytes, not on how they are chunked.
    #[test]
    fn fuzz_aws_s3_checksum_stream_chunking(
        data in prop::collection::vec(any::<u8>(), 0..2_000),
        cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..10),
        part_size in 1..500usize,
        algorithm in prop::sample::select(vec!["SHA1", "SHA256"]),
    ) {
        let checksum = |chunks: Vec<Vec<u8>>| {
            let options = AwsS3ChecksumOptions {
                checksum_algorithm: Some(algorithm.into()),
                part_size: Some(part_size),
                result_key: None,
            };
            let (stream, checksum) = aws_s3_checksum_stream(create_readable_stream(chunks), options).unwrap();
            let output = runtime().block_on(stream_to_array(stream, None)).unwrap();
            (output.concat(), result(&[&checksum]))
        };
        let mut cuts: Vec<usize> = cuts.iter().map(|i| i.index(data.len() + 1)).collect();
        cuts.sort_unstable();
        let mut chunks = Vec::new();
        let mut start = 0;
        for cut in cuts.into_iter().chain([data.len()]) {
            chunks.push(data[start..cut].to_vec());
            start = cut;
        }
        let (chunked_bytes, chunked) = checksum(chunks);
        let (whole_bytes, whole) = checksum(vec![data.clone()]);
        prop_assert_eq!(&chunked_bytes, &data);
        prop_assert_eq!(&whole_bytes, &data);
        prop_assert_eq!(&chunked, &whole);
        let parts = whole["s3"]["checksums"].as_array().unwrap().len();
        prop_assert_eq!(parts, data.len().div_ceil(part_size));
    }
}
