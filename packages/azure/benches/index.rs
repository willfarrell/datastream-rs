// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Blob and Queue over a mock HTTP transport. Event Hubs (AMQP) and Cosmos DB
//! (container clients fetch metadata on creation) need a live service.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use azure_core::http::headers::Headers;
use azure_core::http::{AsyncRawResponse, ClientOptions, HttpClient, Request, Transport, Url};
use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient, BlockBlobClientOptions};
use azure_storage_queue::models::QueueClientReceiveMessagesOptions;
use azure_storage_queue::{QueueClient, QueueClientOptions};
use criterion::{criterion_group, criterion_main, Criterion};
use datastream_azure::blob::{
    azure_blob_download_stream, azure_blob_upload_stream, AzureBlobUploadOptions,
};
use datastream_azure::queue::{
    azure_queue_receive_messages_stream, azure_queue_send_message_stream,
};
use datastream_core::{create_readable_stream, DataStream, StreamExt};

const ITEMS: usize = 1_000;
const PAGE_SIZE: usize = 32;
const BLOB_URL: &str = "https://account.blob.core.windows.net/container/blob";
const QUEUE_URL: &str = "https://account.queue.core.windows.net/queue";

type Handler = dyn Fn(&Request) -> (u16, Vec<u8>) + Send + Sync;

/// Answers every request with `handler`.
struct MockHttpClient(Box<Handler>);

impl std::fmt::Debug for MockHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MockHttpClient")
    }
}

#[async_trait::async_trait]
impl HttpClient for MockHttpClient {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        let (status, body) = (self.0)(request);
        Ok(AsyncRawResponse::from_bytes(
            status.into(),
            Headers::new(),
            body,
        ))
    }
}

fn mock(
    handler: impl Fn(&Request) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Arc<MockHttpClient> {
    Arc::new(MockHttpClient(Box::new(handler)))
}

fn client_options(mock: &Arc<MockHttpClient>) -> ClientOptions {
    ClientOptions {
        transport: Some(Transport::new(mock.clone())),
        retry: azure_core::http::RetryOptions::none(),
        ..Default::default()
    }
}

/// `PAGE_SIZE` messages per receive until `ITEMS`, then an empty page (which
/// ends the stream) that rewinds for the next iteration.
fn receive_page(cursor: &AtomicUsize) -> Vec<u8> {
    let offset = cursor.fetch_add(PAGE_SIZE, Ordering::SeqCst);
    let messages: String = if offset >= ITEMS {
        cursor.store(0, Ordering::SeqCst);
        String::new()
    } else {
        (offset..(offset + PAGE_SIZE).min(ITEMS))
            .map(|id| {
                format!(
                    "<QueueMessage><MessageId>{id}</MessageId><PopReceipt>r{id}</PopReceipt>\
                     <MessageText>message {id}</MessageText><DequeueCount>1</DequeueCount></QueueMessage>"
                )
            })
            .collect()
    };
    format!("<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList>{messages}</QueueMessagesList>")
        .into_bytes()
}

async fn drain<T>(mut stream: DataStream<T>) {
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let object = vec![b'x'; 1024 * 1024];

    let body = object.clone();
    let options = BlobClientOptions {
        client_options: client_options(&mock(move |_| (200, body.clone()))),
        ..Default::default()
    };
    let blob = BlobClient::new(Url::parse(BLOB_URL).unwrap(), None, Some(options)).unwrap();
    c.bench_function("azureBlobDownloadStream/1MB object", |b| {
        b.to_async(&rt).iter(|| async {
            drain(azure_blob_download_stream(&blob, None, None).await.unwrap()).await
        })
    });

    let staged = mock(|_| (201, Vec::new()));
    let block_blob = || {
        let options = BlockBlobClientOptions {
            client_options: client_options(&staged),
            ..Default::default()
        };
        BlockBlobClient::new(Url::parse(BLOB_URL).unwrap(), None, Some(options)).unwrap()
    };
    // 64KiB chunks, like a node fs stream.
    let chunks: Vec<Vec<u8>> = object.chunks(64 * 1024).map(<[u8]>::to_vec).collect();
    c.bench_function("azureBlobUploadStream/1MB object, 256KiB blocks", |b| {
        b.to_async(&rt).iter(|| async {
            let options = AzureBlobUploadOptions {
                block_size: Some(256 * 1024),
                signal: None,
            };
            azure_blob_upload_stream(
                create_readable_stream(chunks.clone()),
                block_blob(),
                options,
            )
            .await
            .unwrap()
        })
    });

    let cursor = AtomicUsize::new(0);
    let sent = b"<QueueMessagesList/>".to_vec();
    let service = mock(move |request| match request.method().as_str() {
        "GET" => (200, receive_page(&cursor)),
        _ => (201, sent.clone()),
    });
    let queue = || {
        let options = QueueClientOptions {
            client_options: client_options(&service),
            ..Default::default()
        };
        QueueClient::new(Url::parse(QUEUE_URL).unwrap(), None, Some(options)).unwrap()
    };
    let messages: Vec<String> = (0..ITEMS).map(|i| format!("message {i}")).collect();
    c.bench_function(
        &format!("azureQueueSendMessageStream/{ITEMS} messages"),
        |b| {
            b.to_async(&rt).iter(|| async {
                let input = create_readable_stream(messages.clone());
                azure_queue_send_message_stream(input, queue(), None, None)
                    .await
                    .unwrap()
            })
        },
    );
    c.bench_function(
        &format!("azureQueueReceiveMessagesStream/{ITEMS} messages, {PAGE_SIZE}/page"),
        |b| {
            b.to_async(&rt).iter(|| async {
                let request = QueueClientReceiveMessagesOptions {
                    number_of_messages: Some(PAGE_SIZE as i32),
                    ..Default::default()
                };
                drain(azure_queue_receive_messages_stream(
                    queue(),
                    Some(request),
                    Default::default(),
                ))
                .await
            })
        },
    );
}

criterion_group!(index, benches);
criterion_main!(index);
