// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! S3 get, list, put and multipart checksum streams.

use async_stream::try_stream;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Object, ObjectCannedAcl, ServerSideEncryption,
    StorageClass,
};
use aws_sdk_s3::Client;
use aws_smithy_types::base64;
use datastream_core::{CancellationToken, DataStream, Result, StreamExt, StreamResult, Value};
use serde_json::json;
use sha2::digest::DynDigest;
use std::collections::HashMap;
use std::sync::Arc;

use crate::client::{send, IntoVec};

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

/// Yield each object entry (ListObjectsV2 `Contents` item), following
/// `NextContinuationToken` until the listing is complete.
pub fn aws_s3_list_objects_stream(
    mut request: ListObjectsV2FluentBuilder,
    signal: Option<CancellationToken>,
) -> DataStream<Object> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), signal.as_ref()).await?;
            for object in output.contents.into_vec() {
                yield object;
            }
            match output.next_continuation_token.filter(|token| !token.is_empty()) {
                Some(token) => request = request.continuation_token(token),
                None => break,
            }
        }
    })
}

// Shared default part size for aws_s3_put_object_stream and
// aws_s3_checksum_stream: a composite checksum depends on the part boundaries,
// so both defaults must match. Above S3's 5 MiB multipart minimum; 10,000 parts
// caps an object at ~160 GiB.
const DEFAULT_PART_SIZE: usize = 17_179_870;
// S3 multipart limits: every part but the last is >=5 MiB, <=10,000 parts.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;

/// Upload progress, reported after each uploaded part (or the single
/// PutObject).
#[derive(Clone, Debug, PartialEq)]
pub struct AwsS3UploadProgress {
    /// Bytes uploaded so far.
    pub loaded: usize,
    /// Object size: known only for a single-part PutObject.
    pub total: Option<usize>,
    pub part: usize,
    pub key: String,
    pub bucket: String,
}

/// Upload progress callback (`onProgress`).
#[derive(Clone)]
pub struct AwsS3OnProgress(pub Arc<dyn Fn(AwsS3UploadProgress) + Send + Sync>);

impl AwsS3OnProgress {
    pub fn new(on_progress: impl Fn(AwsS3UploadProgress) + Send + Sync + 'static) -> Self {
        Self(Arc::new(on_progress))
    }
}

impl std::fmt::Debug for AwsS3OnProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AwsS3OnProgress")
    }
}

#[derive(Default, Clone, Debug)]
pub struct AwsS3PutObjectOptions {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    /// URL-encoded tags, e.g. `"key1=value1&key2=value2"`.
    pub tagging: Option<String>,
    pub acl: Option<ObjectCannedAcl>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub server_side_encryption: Option<ServerSideEncryption>,
    pub ssekms_key_id: Option<String>,
    pub storage_class: Option<StorageClass>,
    /// Multipart part size (default 17,179,870 bytes, matching
    /// [`aws_s3_checksum_stream`]; at least 5 MiB). Objects that fit in one
    /// part use PutObject.
    pub part_size: Option<usize>,
    pub on_progress: Option<AwsS3OnProgress>,
    pub signal: Option<CancellationToken>,
}

// The object fields PutObject and CreateMultipartUpload share.
macro_rules! object_fields {
    ($request:expr, $options:expr) => {
        $request
            .bucket(&$options.bucket)
            .key(&$options.key)
            .set_content_type($options.content_type.clone())
            .set_tagging($options.tagging.clone())
            .set_acl($options.acl.clone())
            .set_cache_control($options.cache_control.clone())
            .set_content_disposition($options.content_disposition.clone())
            .set_content_encoding($options.content_encoding.clone())
            .set_metadata($options.metadata.clone())
            .set_server_side_encryption($options.server_side_encryption.clone())
            .set_ssekms_key_id($options.ssekms_key_id.clone())
            .set_storage_class($options.storage_class.clone())
    };
}

/// Upload a byte stream: a single PutObject when it fits in one part,
/// otherwise a multipart upload (aborted if anything fails). A part size below
/// 5 MiB fails before anything is read or sent.
// ponytail: parts upload one at a time; add lib-storage's queueSize concurrency if throughput matters.
pub async fn aws_s3_put_object_stream(
    client: Client,
    mut input: DataStream<Vec<u8>>,
    options: AwsS3PutObjectOptions,
) -> Result<()> {
    let part_size = options.part_size.unwrap_or(DEFAULT_PART_SIZE);
    if part_size < MIN_PART_SIZE {
        return Err(format!(
            "EntityTooSmall: Your proposed upload part size [{part_size}] is smaller than the minimum allowed size [{MIN_PART_SIZE}] (5MB)"
        )
        .into());
    }
    let signal = options.signal.as_ref();
    let mut upload_id = None;
    let mut parts = Vec::new();
    let mut loaded = 0;
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
                    let request = object_fields!(client.create_multipart_upload(), options);
                    let output = send(request.send(), signal).await?;
                    upload_id = Some(
                        output
                            .upload_id
                            .ok_or("S3.CreateMultipartUpload has no UploadId")?,
                    );
                }
                let id = upload_id.as_deref().unwrap_or_default();
                let part =
                    upload_part(&client, &options, id, parts.len() + 1, &mut loaded, part).await?;
                parts.push(part);
            }
        }
        let Some(id) = upload_id.as_deref() else {
            let total = buffer.len();
            let request =
                object_fields!(client.put_object(), options).body(ByteStream::from(buffer));
            send(request.send(), signal).await?;
            progress(&options, total, Some(total), 1);
            return Ok(());
        };
        let part = upload_part(&client, &options, id, parts.len() + 1, &mut loaded, buffer).await?;
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

fn progress(options: &AwsS3PutObjectOptions, loaded: usize, total: Option<usize>, part: usize) {
    if let Some(on_progress) = &options.on_progress {
        (on_progress.0)(AwsS3UploadProgress {
            loaded,
            total,
            part,
            key: options.key.clone(),
            bucket: options.bucket.clone(),
        });
    }
}

fn check_part_count(options: &AwsS3PutObjectOptions, part_number: usize) -> Result<()> {
    if part_number > MAX_PARTS {
        return Err(format!(
            "Exceeded {MAX_PARTS} parts in multipart upload to Bucket: {} Key: {}.",
            options.bucket, options.key
        )
        .into());
    }
    Ok(())
}

// Upload part `number`, adding its size to `loaded`.
async fn upload_part(
    client: &Client,
    options: &AwsS3PutObjectOptions,
    upload_id: &str,
    number: usize,
    loaded: &mut usize,
    part: Vec<u8>,
) -> Result<CompletedPart> {
    check_part_count(options, number)?;
    let size = part.len();
    let part_number = i32::try_from(number)?;
    let request = client
        .upload_part()
        .bucket(&options.bucket)
        .key(&options.key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(part));
    let output = send(request.send(), options.signal.as_ref()).await?;
    // CompleteMultipartUpload needs every part's ETag; a missing one is usually
    // a bucket CORS policy that does not expose the header.
    let Some(e_tag) = output.e_tag else {
        return Err(format!(
            "Part {number} is missing ETag in UploadPart response. Missing Bucket CORS configuration for ETag header?"
        )
        .into());
    };
    *loaded += size;
    progress(options, *loaded, None, number);
    Ok(CompletedPart::builder()
        .e_tag(e_tag)
        .part_number(part_number)
        .build())
}

#[derive(Default, Clone, Debug)]
pub struct AwsS3ChecksumOptions {
    /// `"SHA256"` (default) or `"SHA1"`.
    pub checksum_algorithm: Option<String>,
    /// Must match the upload's part size: the composite checksum depends on
    /// the part boundaries. Default 17,179,870 bytes, as for
    /// [`aws_s3_put_object_stream`].
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
    let part_size = options.part_size.unwrap_or(DEFAULT_PART_SIZE).max(1);
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
        // Empty input still digests the (empty) remainder, so the checksum is
        // the algorithm's empty-input digest, matching what S3 reports for 0
        // bytes.
        if filled > 0 || parts.is_empty() {
            parts.push(hasher.finalize_reset());
        }
        let checksum = match parts.len() {
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
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
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
        aws_s3_put_object_stream(client, input, put_options(MIN_PART_SIZE))
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!((calls.put, calls.create), (1, 0));
    }

    #[tokio::test]
    async fn put_object_stream_uploads_parts() {
        let (client, calls) = put_client();
        let m = MIN_PART_SIZE;
        let input = create_readable_stream(vec![vec![1; m - 1], vec![2; m - 1], vec![3; 3]]);
        aws_s3_put_object_stream(client, input, put_options(m))
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!((calls.put, calls.create, calls.aborted), (0, 1, 0));
        assert_eq!(calls.parts, [(1, m), (2, m), (3, 1)]);
        assert_eq!(calls.completed, [1, 2, 3]);
    }

    #[tokio::test]
    async fn put_object_stream_aborts_multipart_on_error() {
        let (client, calls) = put_client();
        let m = MIN_PART_SIZE;
        let chunks: Vec<Result<Vec<u8>>> = vec![Ok(vec![1; 2 * m + 2]), Err("boom".into())];
        let input: DataStream<Vec<u8>> = Box::pin(futures::stream::iter(chunks));
        let e = aws_s3_put_object_stream(client, input, put_options(m))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.parts, [(1, m), (2, m)]);
        assert_eq!(calls.aborted, 1);
        assert!(calls.completed.is_empty());
    }

    #[tokio::test]
    async fn put_object_stream_rejects_a_part_size_below_5mib() {
        let (client, calls) = put_client();
        let input = create_readable_stream(vec![b"abc".to_vec()]);
        let e = aws_s3_put_object_stream(client, input, put_options(MIN_PART_SIZE - 1))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "EntityTooSmall: Your proposed upload part size [5242879] is smaller than the minimum allowed size [5242880] (5MB)"
        );
        let calls = calls.lock().unwrap();
        assert_eq!((calls.put, calls.create), (0, 0));
    }

    // Progress follows each uploaded part (lib-storage's httpUploadProgress):
    // cumulative bytes, and a total only for a single-part PutObject.
    #[tokio::test]
    async fn put_object_stream_reports_progress() {
        let m = MIN_PART_SIZE;
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let options = AwsS3PutObjectOptions {
            on_progress: Some(AwsS3OnProgress::new(move |p| seen.lock().unwrap().push(p))),
            ..put_options(m)
        };
        let progress = |loaded, total, part| AwsS3UploadProgress {
            loaded,
            total,
            part,
            key: "file.ext".into(),
            bucket: "bucket".into(),
        };
        let (client, _) = put_client();
        let input = create_readable_stream(vec![vec![b'x'; m + 1]]);
        aws_s3_put_object_stream(client, input, options.clone())
            .await
            .unwrap();
        assert_eq!(
            std::mem::take(&mut *events.lock().unwrap()),
            [progress(m, None, 1), progress(m + 1, None, 2)]
        );

        let (client, _) = put_client();
        let input = create_readable_stream(vec![b"abcde".to_vec()]);
        aws_s3_put_object_stream(client, input, options)
            .await
            .unwrap();
        assert_eq!(*events.lock().unwrap(), [progress(5, Some(5), 1)]);
    }

    #[tokio::test]
    async fn put_object_stream_forwards_object_fields() {
        let check = |sse: Option<&ServerSideEncryption>,
                     kms: Option<&str>,
                     class: Option<&StorageClass>,
                     metadata: Option<&HashMap<String, String>>,
                     cache: Option<&str>| {
            assert_eq!(sse, Some(&ServerSideEncryption::AwsKms));
            assert_eq!(kms, Some("key"));
            assert_eq!(class, Some(&StorageClass::StandardIa));
            assert_eq!(metadata.unwrap()["a"], "b");
            assert_eq!(cache, Some("no-cache"));
        };
        let put = mock!(Client::put_object).then_compute_output(move |req| {
            check(
                req.server_side_encryption(),
                req.ssekms_key_id(),
                req.storage_class(),
                req.metadata(),
                req.cache_control(),
            );
            assert_eq!(req.acl(), Some(&ObjectCannedAcl::Private));
            PutObjectOutput::builder().build()
        });
        let create = mock!(Client::create_multipart_upload).then_compute_output(move |req| {
            check(
                req.server_side_encryption(),
                req.ssekms_key_id(),
                req.storage_class(),
                req.metadata(),
                req.cache_control(),
            );
            assert_eq!(req.content_disposition(), Some("inline"));
            assert_eq!(req.content_encoding(), Some("gzip"));
            CreateMultipartUploadOutput::builder()
                .upload_id("1")
                .build()
        });
        let part = mock!(Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("etag").build());
        let complete = mock!(Client::complete_multipart_upload)
            .then_output(|| CompleteMultipartUploadOutput::builder().build());
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            [&put, &create, &part, &complete]
        );
        let options = AwsS3PutObjectOptions {
            acl: Some(ObjectCannedAcl::Private),
            cache_control: Some("no-cache".into()),
            content_disposition: Some("inline".into()),
            content_encoding: Some("gzip".into()),
            metadata: Some(HashMap::from([("a".into(), "b".into())])),
            server_side_encryption: Some(ServerSideEncryption::AwsKms),
            ssekms_key_id: Some("key".into()),
            storage_class: Some(StorageClass::StandardIa),
            ..put_options(MIN_PART_SIZE)
        };
        let input = create_readable_stream(vec![b"abc".to_vec()]);
        aws_s3_put_object_stream(client.clone(), input, options.clone())
            .await
            .unwrap();
        let input = create_readable_stream(vec![vec![b'x'; MIN_PART_SIZE + 1]]);
        aws_s3_put_object_stream(client, input, options)
            .await
            .unwrap();
        assert_eq!((put.num_calls(), create.num_calls()), (1, 1));
    }

    // CompleteMultipartUpload needs every ETag: fail (and abort) instead.
    #[tokio::test]
    async fn put_object_stream_rejects_a_part_without_etag() {
        let create = mock!(Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("1")
                .build()
        });
        let part = mock!(Client::upload_part).then_output(|| UploadPartOutput::builder().build());
        let abort = mock!(Client::abort_multipart_upload)
            .then_output(|| AbortMultipartUploadOutput::builder().build());
        let complete = mock!(Client::complete_multipart_upload)
            .then_output(|| CompleteMultipartUploadOutput::builder().build());
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            [&create, &part, &abort, &complete]
        );
        let input = create_readable_stream(vec![vec![b'x'; MIN_PART_SIZE + 1]]);
        let e = aws_s3_put_object_stream(client, input, put_options(MIN_PART_SIZE))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Part 1 is missing ETag in UploadPart response. Missing Bucket CORS configuration for ETag header?"
        );
        assert_eq!(part.num_calls(), 1);
        assert_eq!(abort.num_calls(), 1);
        assert_eq!(complete.num_calls(), 0);
    }

    #[test]
    fn put_object_stream_caps_parts_at_10000() {
        let options = put_options(MIN_PART_SIZE);
        assert!(check_part_count(&options, MAX_PARTS).is_ok());
        let e = check_part_count(&options, MAX_PARTS + 1).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Exceeded 10000 parts in multipart upload to Bucket: bucket Key: file.ext."
        );
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
    }

    // Empty input -> the empty-input digest (what S3 reports for a 0-byte
    // object), not "".
    #[tokio::test]
    async fn checksum_of_empty_input_is_the_empty_digest() {
        let empty = checksum(Vec::new(), sha256(None)).await;
        let digest = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        assert_eq!(empty["s3"]["checksum"], digest);
        assert_eq!(empty["s3"]["checksums"], json!([digest]));
        let sha1 = AwsS3ChecksumOptions {
            checksum_algorithm: Some("SHA1".into()),
            ..Default::default()
        };
        let empty = checksum(Vec::new(), sha1).await;
        assert_eq!(empty["s3"]["checksum"], "2jmj7l5rSw0yVb/vlWAYkK/YBwk=");
    }

    // Both streams share one default part size, so a default checksum matches
    // a default upload's parts.
    #[tokio::test]
    async fn put_and_checksum_share_the_default_part_size() {
        let input = vec![b'x'; DEFAULT_PART_SIZE + 1];
        let output = checksum(vec![input.clone()], AwsS3ChecksumOptions::default()).await;
        assert_eq!(output["s3"]["partSize"], DEFAULT_PART_SIZE);
        assert_eq!(output["s3"]["checksums"].as_array().unwrap().len(), 2);

        let (client, calls) = put_client();
        let options = AwsS3PutObjectOptions {
            part_size: None,
            ..put_options(0)
        };
        aws_s3_put_object_stream(client, create_readable_stream(vec![input]), options)
            .await
            .unwrap();
        assert_eq!(
            calls.lock().unwrap().parts,
            [(1, DEFAULT_PART_SIZE), (2, 1)]
        );
    }

    #[tokio::test]
    async fn list_objects_stream_follows_continuation_token() {
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let seen = tokens.clone();
        let rule = mock!(Client::list_objects_v2).then_compute_output(move |req| {
            let mut tokens = seen.lock().unwrap();
            tokens.push((
                req.continuation_token().map(String::from),
                req.prefix().map(String::from),
            ));
            let object = |key: &str| Object::builder().key(key).build();
            match tokens.len() {
                1 => ListObjectsV2Output::builder()
                    .contents(object("a"))
                    .contents(object("b"))
                    .is_truncated(true)
                    .next_continuation_token("t1")
                    .build(),
                2 => ListObjectsV2Output::builder().build(),
                _ => panic!("listed past the last page"),
            }
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let request = client.list_objects_v2().bucket("bucket").prefix("p/");
        let stream = aws_s3_list_objects_stream(request.clone(), None);
        let objects = datastream_core::stream_to_array(stream, None)
            .await
            .unwrap();
        let keys: Vec<_> = objects.iter().map(|o| o.key().unwrap()).collect();
        assert_eq!(keys, ["a", "b"]);
        let prefix = Some("p/".to_string());
        assert_eq!(
            *tokens.lock().unwrap(),
            [(None, prefix.clone()), (Some("t1".to_string()), prefix)]
        );
        // The caller's builder is untouched.
        assert!(request.get_continuation_token().is_none());
    }

    #[tokio::test]
    async fn list_objects_stream_aborts() {
        let rule =
            mock!(Client::list_objects_v2).then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        signal.cancel();
        let stream = aws_s3_list_objects_stream(client.list_objects_v2().bucket("b"), Some(signal));
        let e = datastream_core::stream_to_array(stream, None)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
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
