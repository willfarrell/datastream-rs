// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::operation::scan::ScanOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sns::operation::publish_batch::PublishBatchOutput;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput;
use aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput;
use aws_sdk_sqs::types::{DeleteMessageBatchRequestEntry, Message, SendMessageBatchRequestEntry};
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use criterion::{criterion_group, criterion_main, Criterion};
use datastream_aws::dynamodb::{
    aws_dynamodb_delete_item_stream, aws_dynamodb_put_item_stream, aws_dynamodb_query_stream,
    aws_dynamodb_scan_stream, AwsDynamoDBOptions, Item,
};
use datastream_aws::s3::{aws_s3_checksum_stream, aws_s3_get_object_stream, AwsS3ChecksumOptions};
use datastream_aws::sns::aws_sns_publish_message_stream;
use datastream_aws::sqs::{
    aws_sqs_delete_message_stream, aws_sqs_receive_message_stream, aws_sqs_send_message_stream,
};
use datastream_aws::{AwsPollingOptions, AwsWriteOptions};
use datastream_core::{create_readable_stream, DataStream, Result, StreamExt};

const ITEMS: usize = 1_000;
const PAGE_SIZE: usize = 100;
const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/000000000000/perf";

fn item(id: usize) -> Item {
    HashMap::from([
        ("id".to_string(), AttributeValue::N(id.to_string())),
        ("name".to_string(), AttributeValue::S(format!("item_{id}"))),
    ])
}

async fn drain<T>(mut stream: DataStream<T>) {
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
}

/// `(offset, more)` for the next page of `ITEMS`, advancing `cursor`; wraps
/// back to the start after the last page so every iteration reads them all.
fn next_page(cursor: &AtomicUsize, size: usize) -> (usize, bool) {
    let offset = cursor.fetch_add(size, Ordering::SeqCst);
    let more = offset + size < ITEMS;
    if !more {
        cursor.store(0, Ordering::SeqCst);
    }
    (offset, more)
}

fn sqs_sns(c: &mut Criterion, rt: &tokio::runtime::Runtime) {
    let rule = mock!(aws_sdk_sns::Client::publish_batch)
        .then_output(|| PublishBatchOutput::builder().build());
    let sns = mock_client!(aws_sdk_sns, RuleMode::MatchAny, [&rule]);
    let entries: Vec<_> = (0..ITEMS)
        .map(|i| {
            PublishBatchRequestEntry::builder()
                .id(i.to_string())
                .message(format!("message {i}"))
                .build()
                .unwrap()
        })
        .collect();
    c.bench_function(
        &format!("awsSNSPublishMessageStream/{ITEMS} messages"),
        |b| {
            b.to_async(rt).iter(|| async {
                let request = sns
                    .publish_batch()
                    .topic_arn("arn:aws:sns:us-east-1:0:perf");
                let input = create_readable_stream(entries.clone());
                aws_sns_publish_message_stream(input, request, AwsWriteOptions::default())
                    .await
                    .unwrap()
            })
        },
    );

    let send = mock!(aws_sdk_sqs::Client::send_message_batch).then_output(|| {
        SendMessageBatchOutput::builder()
            .set_successful(Some(Vec::new()))
            .set_failed(Some(Vec::new()))
            .build()
            .unwrap()
    });
    let delete = mock!(aws_sdk_sqs::Client::delete_message_batch).then_output(|| {
        DeleteMessageBatchOutput::builder()
            .set_successful(Some(Vec::new()))
            .set_failed(Some(Vec::new()))
            .build()
            .unwrap()
    });
    let cursor = Arc::new(AtomicUsize::new(0));
    let page = cursor.clone();
    let receive = mock!(aws_sdk_sqs::Client::receive_message).then_compute_output(move |_| {
        // 10 per page, then an empty page (which ends the non-polling stream)
        // that rewinds for the next iteration.
        let offset = page.fetch_add(10, Ordering::SeqCst);
        if offset >= ITEMS {
            page.store(0, Ordering::SeqCst);
            return ReceiveMessageOutput::builder().build();
        }
        let messages = (offset..offset + 10).map(|i| {
            Message::builder()
                .message_id(i.to_string())
                .receipt_handle(format!("rh-{i}"))
                .body(format!("message {i}"))
                .build()
        });
        ReceiveMessageOutput::builder()
            .set_messages(Some(messages.collect()))
            .build()
    });
    let sqs = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&send, &delete, &receive]);
    let sends: Vec<_> = (0..ITEMS)
        .map(|i| {
            SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(format!("message {i}"))
                .build()
                .unwrap()
        })
        .collect();
    let deletes: Vec<_> = (0..ITEMS)
        .map(|i| {
            DeleteMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .receipt_handle(format!("rh-{i}"))
                .build()
                .unwrap()
        })
        .collect();
    c.bench_function(&format!("awsSQSSendMessageStream/{ITEMS} messages"), |b| {
        b.to_async(rt).iter(|| async {
            let request = sqs.send_message_batch().queue_url(QUEUE_URL);
            let input = create_readable_stream(sends.clone());
            aws_sqs_send_message_stream(input, request, AwsWriteOptions::default())
                .await
                .unwrap()
        })
    });
    c.bench_function(
        &format!("awsSQSDeleteMessageStream/{ITEMS} messages"),
        |b| {
            b.to_async(rt).iter(|| async {
                let request = sqs.delete_message_batch().queue_url(QUEUE_URL);
                let input = create_readable_stream(deletes.clone());
                aws_sqs_delete_message_stream(input, request, AwsWriteOptions::default())
                    .await
                    .unwrap()
            })
        },
    );
    c.bench_function(
        &format!("awsSQSReceiveMessageStream/{ITEMS} messages, 10/batch"),
        |b| {
            b.to_async(rt).iter(|| async {
                let request = sqs.receive_message().queue_url(QUEUE_URL);
                drain(aws_sqs_receive_message_stream(
                    request,
                    AwsPollingOptions::default(),
                ))
                .await
            })
        },
    );
}

fn dynamodb(c: &mut Criterion, rt: &tokio::runtime::Runtime) {
    let write = mock!(aws_sdk_dynamodb::Client::batch_write_item)
        .then_output(|| BatchWriteItemOutput::builder().build());
    let query_cursor = Arc::new(AtomicUsize::new(0));
    let cursor = query_cursor.clone();
    let query = mock!(aws_sdk_dynamodb::Client::query).then_compute_output(move |_| {
        let (offset, more) = next_page(&cursor, PAGE_SIZE);
        QueryOutput::builder()
            .set_items(Some((offset..offset + PAGE_SIZE).map(item).collect()))
            .set_last_evaluated_key(more.then(|| item(offset)))
            .build()
    });
    let scan_cursor = Arc::new(AtomicUsize::new(0));
    let cursor = scan_cursor.clone();
    let scan = mock!(aws_sdk_dynamodb::Client::scan).then_compute_output(move |_| {
        let (offset, more) = next_page(&cursor, PAGE_SIZE);
        ScanOutput::builder()
            .set_items(Some((offset..offset + PAGE_SIZE).map(item).collect()))
            .set_last_evaluated_key(more.then(|| item(offset)))
            .build()
    });
    let client = mock_client!(
        aws_sdk_dynamodb,
        RuleMode::MatchAny,
        [&write, &query, &scan]
    );
    let items: Vec<Item> = (0..ITEMS).map(item).collect();
    let options = || AwsDynamoDBOptions {
        table_name: "perf".into(),
        ..Default::default()
    };
    type Write = fn(aws_sdk_dynamodb::Client, DataStream<Item>, AwsDynamoDBOptions) -> W;
    type W = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>>>>;
    let writes: [(&str, Write); 2] = [
        ("awsDynamoDBPutItemStream", |c, i, o| {
            Box::pin(aws_dynamodb_put_item_stream(c, i, o))
        }),
        ("awsDynamoDBDeleteItemStream", |c, i, o| {
            Box::pin(aws_dynamodb_delete_item_stream(c, i, o))
        }),
    ];
    for (name, write) in writes {
        c.bench_function(&format!("{name}/{ITEMS} items"), |b| {
            b.to_async(rt).iter(|| {
                write(
                    client.clone(),
                    create_readable_stream(items.clone()),
                    options(),
                )
            })
        });
    }
    c.bench_function(
        &format!("awsDynamoDBQueryStream/{ITEMS} items, {PAGE_SIZE}/page"),
        |b| {
            b.to_async(rt).iter(|| {
                drain(aws_dynamodb_query_stream(
                    client.query().table_name("perf"),
                    None,
                ))
            })
        },
    );
    c.bench_function(
        &format!("awsDynamoDBScanStream/{ITEMS} items, {PAGE_SIZE}/page"),
        |b| {
            b.to_async(rt).iter(|| {
                drain(aws_dynamodb_scan_stream(
                    client.scan().table_name("perf"),
                    None,
                ))
            })
        },
    );
}

fn s3(c: &mut Criterion, rt: &tokio::runtime::Runtime) {
    let object = vec![b'x'; 1024 * 1024];
    let body = object.clone();
    let get = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(body.clone()))
            .build()
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&get]);
    c.bench_function("awsS3GetObjectStream/1MB object", |b| {
        b.to_async(rt).iter(|| async {
            let request = client.get_object().bucket("perf").key("object");
            drain(aws_s3_get_object_stream(request, None).await.unwrap()).await
        })
    });
    // 64KiB chunks, like a node fs stream.
    let chunks: Vec<Vec<u8>> = object.chunks(64 * 1024).map(<[u8]>::to_vec).collect();
    for algorithm in ["SHA256", "SHA1"] {
        c.bench_function(
            &format!("awsS3ChecksumStream/1MB {algorithm} checksum"),
            |b| {
                b.to_async(rt).iter(|| async {
                    let options = AwsS3ChecksumOptions {
                        checksum_algorithm: Some(algorithm.into()),
                        ..Default::default()
                    };
                    let input = create_readable_stream(chunks.clone());
                    drain(aws_s3_checksum_stream(input, options).unwrap().0).await
                })
            },
        );
    }
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    sqs_sns(c, &rt);
    dynamodb(c, &rt);
    s3(c, &rt);
}

criterion_group!(index, benches);
criterion_main!(index);
