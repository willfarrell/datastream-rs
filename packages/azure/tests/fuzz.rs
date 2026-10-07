// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Event Hubs (AMQP) and Cosmos DB (container clients fetch metadata on
//! creation) need a live service, so only Blob, Queue and the Kafka token are
//! fuzzed here, through a mock HTTP transport.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
use azure_core::http::headers::Headers;
use azure_core::http::{AsyncRawResponse, ClientOptions, HttpClient, Request, Transport, Url};
use azure_core::time::OffsetDateTime;
use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient, BlockBlobClientOptions};
use azure_storage_queue::{QueueClient, QueueClientOptions};
use datastream_azure::blob::{
    azure_blob_download_stream, azure_blob_upload_stream, AzureBlobUploadOptions,
};
use datastream_azure::event_hubs_kafka::azure_event_hubs_kafka_mechanism;
use datastream_azure::queue::{
    azure_queue_receive_messages_stream, azure_queue_send_message_stream,
};
use datastream_core::{create_readable_stream, stream_to_array};
use proptest::prelude::*;

/// Replays `responses` in order (then `fallback`), recording each request's
/// `"METHOD url"` and body.
#[derive(Debug)]
struct MockHttpClient {
    responses: Mutex<VecDeque<(u16, Vec<u8>)>>,
    fallback: (u16, Vec<u8>),
    requests: Mutex<Vec<(String, Vec<u8>)>>,
}

impl MockHttpClient {
    fn new(responses: Vec<(u16, Vec<u8>)>, fallback: (u16, &str)) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses.into()),
            fallback: (fallback.0, fallback.1.as_bytes().to_vec()),
            requests: Mutex::default(),
        })
    }

    fn client_options(self: &Arc<Self>) -> ClientOptions {
        ClientOptions {
            transport: Some(Transport::new(self.clone())),
            retry: azure_core::http::RetryOptions::none(),
            ..Default::default()
        }
    }

    fn requests(&self) -> Vec<(String, Vec<u8>)> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl HttpClient for MockHttpClient {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        let body = match request.body() {
            azure_core::http::Body::Bytes(bytes) => bytes.to_vec(),
            #[allow(unreachable_patterns)]
            _ => Vec::new(),
        };
        let line = format!("{} {}", request.method(), request.url());
        self.requests.lock().unwrap().push((line, body));
        let (status, body) = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.fallback.clone());
        Ok(AsyncRawResponse::from_bytes(
            status.into(),
            Headers::new(),
            body,
        ))
    }
}

const BLOB_URL: &str = "https://account.blob.core.windows.net/container/blob";

fn queue_client(mock: &Arc<MockHttpClient>) -> QueueClient {
    let options = QueueClientOptions {
        client_options: mock.client_options(),
        ..Default::default()
    };
    let url = Url::parse("https://account.queue.core.windows.net/queue").unwrap();
    QueueClient::new(url, None, Some(options)).unwrap()
}

/// A receive page whose messages carry the given (already XML-escaped) text.
fn page(first_id: usize, texts: &[String]) -> Vec<u8> {
    let messages: String = texts
        .iter()
        .enumerate()
        .map(|(i, text)| {
            let id = first_id + i;
            format!(
                "<QueueMessage><MessageId>{id}</MessageId><PopReceipt>r{id}</PopReceipt>\
                 <MessageText>{text}</MessageText><DequeueCount>1</DequeueCount></QueueMessage>"
            )
        })
        .collect();
    format!("<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList>{messages}</QueueMessagesList>")
        .into_bytes()
}

/// The `<MessageText>` content of a sent message body, as serialized.
fn message_text(body: &[u8]) -> String {
    let body = String::from_utf8(body.to_vec()).unwrap();
    match (body.find("<MessageText>"), body.find("</MessageText>")) {
        (Some(start), Some(end)) => body[start + "<MessageText>".len()..end].to_string(),
        _ => String::new(), // <MessageText/>
    }
}

#[derive(Debug, Default)]
struct StaticCredential {
    scopes: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TokenCredential for StaticCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        _options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        self.scopes
            .lock()
            .unwrap()
            .extend(scopes.iter().map(|s| s.to_string()));
        Ok(AccessToken::new(
            "token",
            OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        ))
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_azure_blob_download_stream_body(body in prop::collection::vec(any::<u8>(), 0..4_096)) {
        let mock = MockHttpClient::new(vec![(200, body.clone())], (500, ""));
        let options = BlobClientOptions { client_options: mock.client_options(), ..Default::default() };
        let client = BlobClient::new(Url::parse(BLOB_URL).unwrap(), None, Some(options)).unwrap();
        let chunks = runtime().block_on(async {
            let stream = azure_blob_download_stream(&client, None, None).await.unwrap();
            stream_to_array(stream, None).await.unwrap()
        });
        prop_assert_eq!(chunks.concat(), body);
    }

    // However the input is chunked, every block but the last is exactly
    // block_size, the blocks rebuild the input, and all are committed.
    #[test]
    fn fuzz_azure_blob_upload_stream_blocks(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..20),
        block_size in 1..64usize,
    ) {
        let mock = MockHttpClient::new(Vec::new(), (201, ""));
        let options = BlockBlobClientOptions { client_options: mock.client_options(), ..Default::default() };
        let client = BlockBlobClient::new(Url::parse(BLOB_URL).unwrap(), None, Some(options)).unwrap();
        let data = chunks.concat();
        runtime()
            .block_on(azure_blob_upload_stream(
                create_readable_stream(chunks),
                client,
                AzureBlobUploadOptions { block_size: Some(block_size), signal: None },
            ))
            .unwrap();
        let requests = mock.requests();
        let (commit, stages) = requests.split_last().unwrap();
        prop_assert!(commit.0.contains("comp=blocklist"), "{}", commit.0);
        prop_assert!(stages.iter().all(|(line, _)| line.contains("comp=block&") || line.ends_with("comp=block")));
        prop_assert_eq!(stages.len(), data.len().div_ceil(block_size));
        let blocks: Vec<&Vec<u8>> = stages.iter().map(|(_, body)| body).collect();
        if let Some((last, full)) = blocks.split_last() {
            prop_assert!(full.iter().all(|b| b.len() == block_size));
            prop_assert!(!last.is_empty() && last.len() <= block_size);
        }
        prop_assert_eq!(blocks.into_iter().flatten().copied().collect::<Vec<u8>>(), data);
        let commit = String::from_utf8(commit.1.clone()).unwrap();
        prop_assert_eq!(commit.matches("<Latest>").count(), stages.len());
    }

    // Message text survives the XML round trip: what is sent comes back on receive.
    #[test]
    fn fuzz_azure_queue_send_receive_text(texts in prop::collection::vec(any::<String>(), 1..10)) {
        let sent = MockHttpClient::new(Vec::new(), (201, "<QueueMessagesList/>"));
        let result = runtime().block_on(azure_queue_send_message_stream(
            create_readable_stream(texts.clone()),
            queue_client(&sent),
            None,
            None,
        ));
        prop_assert!(result.is_ok(), "{:?}", result);
        let escaped: Vec<String> = sent.requests().iter().map(|(_, body)| message_text(body)).collect();
        prop_assert_eq!(escaped.len(), texts.len());

        let mock = MockHttpClient::new(vec![(200, page(0, &escaped))], (200, "<QueueMessagesList/>"));
        let stream = azure_queue_receive_messages_stream(queue_client(&mock), None, Default::default());
        let received = runtime().block_on(stream_to_array(stream, None)).unwrap();
        let received: Vec<String> = received.into_iter().map(|m| m.message_text.unwrap_or_default()).collect();
        prop_assert_eq!(received, texts);
    }

    // Every message of every page comes out once, in order, until an empty page.
    #[test]
    fn fuzz_azure_queue_receive_messages_page_sizes(pages in prop::collection::vec(1..32usize, 0..8)) {
        let mut responses = Vec::new();
        let mut next = 0;
        for size in &pages {
            let texts: Vec<String> = (next..next + size).map(|i| format!("text {i}")).collect();
            responses.push((200, page(next, &texts)));
            next += size;
        }
        let mock = MockHttpClient::new(responses, (200, "<QueueMessagesList/>"));
        let stream = azure_queue_receive_messages_stream(queue_client(&mock), None, Default::default());
        let received = runtime().block_on(stream_to_array(stream, None)).unwrap();
        let ids: Vec<String> = received.into_iter().map(|m| m.message_id.unwrap()).collect();
        prop_assert_eq!(ids, (0..next).map(|i| i.to_string()).collect::<Vec<_>>());
        prop_assert_eq!(mock.requests().len(), pages.len() + 1);
    }

    #[test]
    fn fuzz_azure_event_hubs_kafka_mechanism_namespace(namespace in ".{0,60}") {
        let credential = Arc::new(StaticCredential::default());
        let host = namespace.trim_end_matches(":9093");
        match azure_event_hubs_kafka_mechanism(&namespace, credential.clone()) {
            Ok(mechanism) => {
                prop_assert!(!host.is_empty());
                prop_assert_eq!(mechanism.mechanism, "oauthbearer");
                let token = runtime().block_on(mechanism.oauth_bearer_provider()).unwrap();
                prop_assert_eq!(token.value, "token");
                prop_assert_eq!(token.expiry_time, 1_700_000_000_000);
                prop_assert_eq!(credential.scopes.lock().unwrap().clone(), vec![format!("https://{host}/.default")]);
            }
            Err(e) => {
                prop_assert!(host.is_empty());
                prop_assert_eq!(e.to_string(), "azureEventHubsKafkaMechanism: namespace required");
            }
        }
    }
}
