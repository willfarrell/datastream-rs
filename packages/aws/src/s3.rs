// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! S3 get, put and multipart checksum streams.

use async_stream::try_stream;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::base64;
use datastream_core::{CancellationToken, DataStream, Result, StreamExt, StreamResult, Value};
use serde_json::json;
use sha2::digest::DynDigest;

use crate::client::send;

/// Stream an object's body.
pub async fn aws_s3_get_object_stream(
    request: GetObjectFluentBuilder,
    signal: Option<CancellationToken>,
) -> Result<DataStream<Vec<u8>>> {
    let output = send(request.send(), signal.as_ref()).await?;
    let mut body = output.body;
    Ok(Box::pin(try_stream! {
        while let Some(bytes) = send(body.try_next(), signal.as_ref()).await? {
            yield bytes.to_vec();
        }
    }))
}

// S3's minimum part size, also the lib-storage default.
const DEFAULT_PART_SIZE: usize = 5 * 1024 * 1024;

#[derive(Default, Clone, Debug)]
pub struct AwsS3PutObjectOptions {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    /// URL-encoded tags, e.g. `"key1=value1&key2=value2"`.
    pub tagging: Option<String>,
    /// Multipart part size (default 5MiB). Objects that fit in one part use PutObject.
    pub part_size: Option<usize>,
    pub signal: Option<CancellationToken>,
}

/// Upload a byte stream: a single PutObject when it fits in one part,
/// otherwise a multipart upload (aborted if anything fails).
// ponytail: parts upload one at a time; add lib-storage's queueSize concurrency if throughput matters.
pub async fn aws_s3_put_object_stream(
    client: Client,
    mut input: DataStream<Vec<u8>>,
    options: AwsS3PutObjectOptions,
) -> Result<()> {
    let part_size = options.part_size.unwrap_or(DEFAULT_PART_SIZE).max(1);
    let signal = options.signal.as_ref();
    let mut upload_id = None;
    let mut parts = Vec::new();
    let result: Result<()> = async {
        let mut buffer = Vec::new();
        while let Some(chunk) = input.next().await {
            buffer.extend_from_slice(&chunk?);
            // Keep at least one byte back so the last part is uploaded by the end
            // step below (and a one-part object becomes a PutObject).
            while buffer.len() > part_size {
                let rest = buffer.split_off(part_size);
                let part = std::mem::replace(&mut buffer, rest);
                if upload_id.is_none() {
                    let request = client
                        .create_multipart_upload()
                        .bucket(&options.bucket)
                        .key(&options.key)
                        .set_content_type(options.content_type.clone())
                        .set_tagging(options.tagging.clone());
                    let output = send(request.send(), signal).await?;
                    upload_id = Some(
                        output
                            .upload_id
                            .ok_or("S3.CreateMultipartUpload has no UploadId")?,
                    );
                }
                let id = upload_id.as_deref().unwrap_or_default();
                let part = upload_part(&client, &options, id, parts.len() + 1, part).await?;
                parts.push(part);
            }
        }
        let Some(id) = upload_id.as_deref() else {
            let request = client
                .put_object()
                .bucket(&options.bucket)
                .key(&options.key)
                .set_content_type(options.content_type.clone())
                .set_tagging(options.tagging.clone())
                .body(ByteStream::from(buffer));
            send(request.send(), signal).await?;
            return Ok(());
        };
        let part = upload_part(&client, &options, id, parts.len() + 1, buffer).await?;
        parts.push(part);
        let request = client
            .complete_multipart_upload()
            .bucket(&options.bucket)
            .key(&options.key)
            .upload_id(id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(std::mem::take(&mut parts)))
                    .build(),
            );
        send(request.send(), signal).await?;
        Ok(())
    }
    .await;
    if let (Err(_), Some(id)) = (&result, &upload_id) {
        // Best effort: the original error is the one worth reporting.
        let request = client
            .abort_multipart_upload()
            .bucket(&options.bucket)
            .key(&options.key)
            .upload_id(id);
        let _ = request.send().await;
    }
    result
}

async fn upload_part(
    client: &Client,
    options: &AwsS3PutObjectOptions,
    upload_id: &str,
    part_number: usize,
    part: Vec<u8>,
) -> Result<CompletedPart> {
    let part_number = i32::try_from(part_number)?;
    let request = client
        .upload_part()
        .bucket(&options.bucket)
        .key(&options.key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(part));
    let output = send(request.send(), options.signal.as_ref()).await?;
    Ok(CompletedPart::builder()
        .set_e_tag(output.e_tag)
        .part_number(part_number)
        .build())
}

#[derive(Default, Clone, Debug)]
pub struct AwsS3ChecksumOptions {
    /// `"SHA256"` (default) or `"SHA1"`.
    pub checksum_algorithm: Option<String>,
    /// Default ~16MB, just under S3's multipart minimum.
    pub part_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Compute the S3 multipart checksum of a byte stream, e.g. for uploading via
/// presigned URLs. The result is `{checksum, checksums, partSize}`, where
/// `checksum` is the single part's digest or `<digest of digests>-<parts>`.
pub fn aws_s3_checksum_stream<T>(
    mut input: DataStream<T>,
    options: AwsS3ChecksumOptions,
) -> Result<(DataStream<T>, StreamResult)>
where
    T: AsRef<[u8]> + Send + 'static,
{
    let algorithm = options.checksum_algorithm.as_deref().unwrap_or("SHA256");
    let mut hasher: Box<dyn DynDigest + Send> = match algorithm {
        "SHA1" => Box::new(sha1::Sha1::default()),
        "SHA256" => Box::new(sha2::Sha256::default()),
        _ => return Err(format!("Unsupported ChecksumAlgorithm: {algorithm}").into()),
    };
    let part_size = options.part_size.unwrap_or(17_179_870).max(1);
    let result = StreamResult::new(
        options.result_key.unwrap_or_else(|| "s3".into()),
        Value::Null,
    );
    let output = result.clone();
    let stream: DataStream<T> = Box::pin(try_stream! {
        let mut parts: Vec<Box<[u8]>> = Vec::new();
        let mut filled = 0;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            for bytes in chunk.as_ref().chunks(part_size) {
                // Split at the current part boundary.
                let (head, tail) = bytes.split_at(bytes.len().min(part_size - filled));
                for piece in [head, tail] {
                    hasher.update(piece);
                    filled += piece.len();
                    if filled == part_size {
                        parts.push(hasher.finalize_reset());
                        filled = 0;
                    }
                }
            }
            yield chunk;
        }
        if filled > 0 {
            parts.push(hasher.finalize_reset());
        }
        let checksum = match parts.len() {
            0 => String::new(),
            1 => base64::encode(&parts[0]),
            count => {
                for part in &parts {
                    hasher.update(part);
                }
                format!("{}-{count}", base64::encode(hasher.finalize_reset()))
            }
        };
        let checksums: Vec<String> = parts.iter().map(base64::encode).collect();
        output.set(json!({"checksum": checksum, "checksums": checksums, "partSize": part_size}));
    });
    Ok((stream, result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, pipeline, stream_to_buffer};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn get_object_stream_returns_body() {
        let rule = mock!(Client::get_object)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("file.ext"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(b"contents"))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let request = client.get_object().bucket("bucket").key("file.ext");
        let stream = aws_s3_get_object_stream(request, None).await.unwrap();
        assert_eq!(stream_to_buffer(stream, None).await.unwrap(), b"contents");
    }

    #[tokio::test]
    async fn get_object_stream_aborts() {
        let rule = mock!(Client::get_object).then_output(|| GetObjectOutput::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        signal.cancel();
        let e = aws_s3_get_object_stream(client.get_object(), Some(signal))
            .await
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "Aborted");
    }

    struct Calls {
        put: usize,
        create: usize,
        parts: Vec<(i32, usize)>,
        completed: Vec<i32>,
        aborted: usize,
    }

    fn put_client() -> (Client, Arc<Mutex<Calls>>) {
        let calls = Arc::new(Mutex::new(Calls {
            put: 0,
            create: 0,
            parts: Vec::new(),
            completed: Vec::new(),
            aborted: 0,
        }));
        let (c1, c2, c3, c4, c5) = (
            calls.clone(),
            calls.clone(),
            calls.clone(),
            calls.clone(),
            calls.clone(),
        );
        let put = mock!(Client::put_object).then_compute_output(move |req| {
            assert_eq!(req.tagging(), Some("a=b"));
            c1.lock().unwrap().put += 1;
            PutObjectOutput::builder().build()
        });
        let create = mock!(Client::create_multipart_upload).then_compute_output(move |req| {
            assert_eq!(req.tagging(), Some("a=b"));
            c2.lock().unwrap().create += 1;
            CreateMultipartUploadOutput::builder()
                .upload_id("1")
                .build()
        });
        let part = mock!(Client::upload_part).then_compute_output(move |req| {
            let len = req.body().bytes().map_or(0, <[u8]>::len);
            c3.lock()
                .unwrap()
                .parts
                .push((req.part_number().unwrap(), len));
            UploadPartOutput::builder().e_tag("etag").build()
        });
        let complete = mock!(Client::complete_multipart_upload).then_compute_output(move |req| {
            let parts = req.multipart_upload().unwrap().parts();
            c4.lock().unwrap().completed = parts.iter().map(|p| p.part_number().unwrap()).collect();
            CompleteMultipartUploadOutput::builder().build()
        });
        let abort = mock!(Client::abort_multipart_upload).then_compute_output(move |_| {
            c5.lock().unwrap().aborted += 1;
            AbortMultipartUploadOutput::builder().build()
        });
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            [&put, &create, &part, &complete, &abort]
        );
        (client, calls)
    }

    fn put_options(part_size: usize) -> AwsS3PutObjectOptions {
        AwsS3PutObjectOptions {
            bucket: "bucket".into(),
            key: "file.ext".into(),
            tagging: Some("a=b".into()),
            part_size: Some(part_size),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn put_object_stream_uses_put_object_for_one_part() {
        let (client, calls) = put_client();
        let input = create_readable_stream(vec![b"abc".to_vec(), b"de".to_vec()]);
        aws_s3_put_object_stream(client, input, put_options(5))
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!((calls.put, calls.create), (1, 0));
    }

    #[tokio::test]
    async fn put_object_stream_uploads_parts() {
        let (client, calls) = put_client();
        let input = create_readable_stream(vec![vec![1; 4], vec![2; 4], vec![3; 3]]);
        aws_s3_put_object_stream(client, input, put_options(5))
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!((calls.put, calls.create, calls.aborted), (0, 1, 0));
        assert_eq!(calls.parts, [(1, 5), (2, 5), (3, 1)]);
        assert_eq!(calls.completed, [1, 2, 3]);
    }

    #[tokio::test]
    async fn put_object_stream_aborts_multipart_on_error() {
        let (client, calls) = put_client();
        let chunks: Vec<Result<Vec<u8>>> = vec![Ok(vec![1; 12]), Err("boom".into())];
        let input: DataStream<Vec<u8>> = Box::pin(futures::stream::iter(chunks));
        let e = aws_s3_put_object_stream(client, input, put_options(5))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.parts, [(1, 5), (2, 5)]);
        assert_eq!(calls.aborted, 1);
        assert!(calls.completed.is_empty());
    }

    async fn checksum(input: Vec<Vec<u8>>, options: AwsS3ChecksumOptions) -> Value {
        let (stream, result) =
            aws_s3_checksum_stream(create_readable_stream(input), options).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        Value::Object(output)
    }

    fn sha256(part_size: Option<usize>) -> AwsS3ChecksumOptions {
        AwsS3ChecksumOptions {
            checksum_algorithm: Some("SHA256".into()),
            part_size,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn checksum_single_part() {
        let one = checksum(vec![vec![b'x'; 16_384]], sha256(None)).await;
        assert_eq!(
            one,
            json!({"s3": {
                "checksum": "FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM=",
                "checksums": ["FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM="],
                "partSize": 17_179_870,
            }})
        );
        let two = checksum(vec![vec![b'x'; 16_384]; 2], sha256(None)).await;
        assert_eq!(
            two["s3"]["checksum"],
            "Qnll9JqFcXTjCGWCJzJdvSP/Tsy+OZ1a1IF92j7Hn4c="
        );
        let sha1 = AwsS3ChecksumOptions {
            checksum_algorithm: Some("SHA1".into()),
            ..Default::default()
        };
        let sha1 = checksum(vec![vec![b'x'; 16_384]], sha1).await;
        assert_eq!(sha1["s3"]["checksum"], "XhuZUWG9FmZw1UqD0xSn0ik7bD0=");
    }

    #[tokio::test]
    async fn checksum_defaults_and_result_key() {
        let output = checksum(vec![vec![b'x'; 16_384]], AwsS3ChecksumOptions::default()).await;
        assert_eq!(
            output["s3"]["checksum"],
            "FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM="
        );
        let options = AwsS3ChecksumOptions {
            result_key: Some("checksum".into()),
            ..Default::default()
        };
        let output = checksum(vec![vec![b'x'; 16_384]], options).await;
        assert_eq!(
            output["checksum"]["checksum"],
            "FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM="
        );
    }

    #[tokio::test]
    async fn checksum_multi_part() {
        let part = "d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=";
        let output = checksum(vec![vec![b'x'; 100]], sha256(Some(50))).await;
        assert_eq!(
            output["s3"],
            json!({
                "checksum": "//kFcRsCAXRHbjZsPUmCkRBx+J1hJiSiqKAF/q7oMi0=-2",
                "checksums": [part, part],
                "partSize": 50,
            })
        );
        // Parts are peeled across chunks and the remainder is carried forward.
        let output = checksum(vec![vec![b'x'; 60]; 2], sha256(Some(50))).await;
        assert_eq!(
            output["s3"]["checksums"],
            json!([part, part, "1PwdtmVEZQfcUbDJOS3ZZJKRWBv+G0jiQbKwgDKztkc="])
        );
        assert_eq!(
            output["s3"]["checksum"],
            "kE1tc6lRu6Azw5+k/yKQ/QDXDG236y62PebJpxEZQhQ=-3"
        );
    }

    #[tokio::test]
    async fn checksum_part_counts() {
        let count = |output: Value| output["s3"]["checksums"].as_array().unwrap().len();
        assert_eq!(
            count(checksum(vec![vec![b'x'; 130]], sha256(Some(50))).await),
            3
        );
        assert_eq!(
            count(checksum(vec![vec![7; 30]; 4], sha256(Some(50))).await),
            3
        );
        assert_eq!(
            count(checksum(vec![vec![b'x'; 50]], sha256(Some(50))).await),
            1
        );
        let empty = checksum(Vec::new(), sha256(None)).await;
        assert_eq!(empty["s3"]["checksum"], "");
        assert_eq!(count(empty), 0);
    }

    #[tokio::test]
    async fn checksum_passes_chunks_through() {
        let (stream, _) = aws_s3_checksum_stream(
            create_readable_stream(vec!["ab".to_string(), "c".to_string()]),
            AwsS3ChecksumOptions::default(),
        )
        .unwrap();
        let chunks = datastream_core::stream_to_array(stream, None)
            .await
            .unwrap();
        assert_eq!(chunks, ["ab", "c"]);
    }

    #[test]
    fn checksum_rejects_unsupported_algorithm() {
        let options = AwsS3ChecksumOptions {
            checksum_algorithm: Some("NOPE".into()),
            ..Default::default()
        };
        let e = aws_s3_checksum_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options)
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "Unsupported ChecksumAlgorithm: NOPE");
    }
}
