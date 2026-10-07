// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Blob Storage download and upload streams.

use async_stream::try_stream;
use azure_core::http::RequestContent;
use azure_storage_blob::models::{BlobClientDownloadOptions, BlockLookupList};
use azure_storage_blob::{BlobClient, BlockBlobClient};
use datastream_core::{CancellationToken, DataStream, Result, StreamExt};

use crate::client::send;

/// Stream a blob's content.
pub async fn azure_blob_download_stream(
    client: &BlobClient,
    options: Option<BlobClientDownloadOptions<'static>>,
    signal: Option<CancellationToken>,
) -> Result<DataStream<Vec<u8>>> {
    let output = send(client.download(options), signal.as_ref()).await?;
    let mut body = output.body;
    Ok(Box::pin(try_stream! {
        while let Some(bytes) = send(async { body.next().await.transpose() }, signal.as_ref()).await? {
            yield bytes.to_vec();
        }
    }))
}

// Matches @azure/storage-blob's uploadStream default buffer size.
const DEFAULT_BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[derive(Default, Clone, Debug)]
pub struct AzureBlobUploadOptions {
    /// Staged block size (default 8MiB, max 4000MiB, at most 50,000 blocks).
    pub block_size: Option<usize>,
    pub signal: Option<CancellationToken>,
}

/// Upload a byte stream as a block blob: stage each block, then commit the
/// list. Uncommitted blocks left by a failure are discarded by the service.
// ponytail: blocks stage one at a time; add bounded concurrency if throughput matters.
pub async fn azure_blob_upload_stream(
    mut input: DataStream<Vec<u8>>,
    client: BlockBlobClient,
    options: AzureBlobUploadOptions,
) -> Result<()> {
    let block_size = options.block_size.unwrap_or(DEFAULT_BLOCK_SIZE).max(1);
    let signal = options.signal.as_ref();
    let mut block_ids = Vec::new();
    let mut buffer = Vec::new();
    let mut ended = false;
    while !ended {
        match input.next().await {
            Some(chunk) => buffer.extend_from_slice(&chunk?),
            None => ended = true,
        }
        while buffer.len() >= block_size || (ended && !buffer.is_empty()) {
            let rest = buffer.split_off(block_size.min(buffer.len()));
            let block = std::mem::replace(&mut buffer, rest);
            block_ids.push(stage(&client, block_ids.len(), block, signal).await?);
        }
    }
    let blocks = BlockLookupList {
        committed: None,
        latest: Some(block_ids),
        uncommitted: None,
    };
    send(client.commit_block_list(blocks.try_into()?, None), signal).await?;
    Ok(())
}

async fn stage(
    client: &BlockBlobClient,
    index: usize,
    block: Vec<u8>,
    signal: Option<&CancellationToken>,
) -> Result<Vec<u8>> {
    // Block ids must all be the same length within a blob.
    let id = format!("{index:010}").into_bytes();
    let length = u64::try_from(block.len())?;
    send(
        client.stage_block(&id, length, RequestContent::from(block), None),
        signal,
    )
    .await?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock::MockHttpClient;
    use azure_core::http::Url;
    use azure_storage_blob::{BlobClientOptions, BlockBlobClientOptions};
    use datastream_core::{create_readable_stream, stream_to_array};

    const URL: &str = "https://account.blob.core.windows.net/container/blob";

    fn block_client(mock: &std::sync::Arc<MockHttpClient>) -> BlockBlobClient {
        let options = BlockBlobClientOptions {
            client_options: mock.client_options(),
            ..Default::default()
        };
        BlockBlobClient::new(Url::parse(URL).unwrap(), None, Some(options)).unwrap()
    }

    #[tokio::test]
    async fn download_stream_yields_body() {
        let mock = MockHttpClient::new(&[(200, "hello")]);
        let options = BlobClientOptions {
            client_options: mock.client_options(),
            ..Default::default()
        };
        let client = BlobClient::new(Url::parse(URL).unwrap(), None, Some(options)).unwrap();
        let stream = azure_blob_download_stream(&client, None, None)
            .await
            .unwrap();
        let chunks = stream_to_array(stream, None).await.unwrap();
        assert_eq!(chunks.concat(), b"hello");
    }

    #[tokio::test]
    async fn download_stream_surfaces_errors() {
        let mock = MockHttpClient::new(&[(404, "")]);
        let options = BlobClientOptions {
            client_options: mock.client_options(),
            ..Default::default()
        };
        let client = BlobClient::new(Url::parse(URL).unwrap(), None, Some(options)).unwrap();
        assert!(azure_blob_download_stream(&client, None, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn upload_stream_stages_blocks_then_commits() {
        let mock = MockHttpClient::new(&[(201, ""), (201, ""), (201, ""), (201, "")]);
        let input = create_readable_stream([b"abcd".to_vec(), b"efg".to_vec()]);
        let options = AzureBlobUploadOptions {
            block_size: Some(3),
            ..Default::default()
        };
        azure_blob_upload_stream(input, block_client(&mock), options)
            .await
            .unwrap();
        let requests = mock.requests();
        assert_eq!(requests.len(), 4);
        assert!(
            requests[..3].iter().all(|r| r.contains("comp=block")),
            "{requests:?}"
        );
        assert!(requests[3].contains("comp=blocklist"), "{requests:?}");
        let bodies = mock.bodies.lock().unwrap().clone();
        assert_eq!(
            bodies[..3],
            [b"abc".to_vec(), b"def".to_vec(), b"g".to_vec()]
        );
        let commit = String::from_utf8(bodies[3].clone()).unwrap();
        assert_eq!(commit.matches("<Latest>").count(), 3, "{commit}");
    }

    #[tokio::test]
    async fn upload_stream_commits_empty_blob() {
        let mock = MockHttpClient::new(&[(201, "")]);
        let input = create_readable_stream(Vec::<Vec<u8>>::new());
        azure_blob_upload_stream(input, block_client(&mock), Default::default())
            .await
            .unwrap();
        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("comp=blocklist"));
    }

    #[tokio::test]
    async fn upload_stream_fails_on_stage_error() {
        let mock = MockHttpClient::new(&[(403, "")]);
        let input = create_readable_stream([b"abc".to_vec()]);
        let e = azure_blob_upload_stream(input, block_client(&mock), Default::default()).await;
        assert!(e.is_err());
        assert_eq!(mock.requests().len(), 1);
    }
}
