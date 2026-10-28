// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! FIPS endpoint defaults, plus the abort / polling / batching helpers the
//! service modules share.

use std::future::Future;
use std::time::Duration;

use datastream_core::{timeout, CancellationToken, DataStream, Error, Result, StreamExt};

// AWS regions that expose FIPS 140-2/140-3 validated endpoints, including both
// GovCloud regions.
const FIPS_REGIONS: [&str; 8] = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "ca-central-1",
    "ca-west-1",
    "us-gov-east-1",
    "us-gov-west-1",
];

pub fn aws_region_supports_fips(region: &str) -> bool {
    FIPS_REGIONS.contains(&region)
}

/// Whether a new client should use FIPS endpoints (pass to the SDK config's
/// `use_fips`). Reads `AWS_REGION` on every call, not once at startup.
pub fn aws_client_defaults_use_fips_endpoint() -> bool {
    std::env::var("AWS_REGION").is_ok_and(|region| aws_region_supports_fips(&region))
}

/// An error that also carries the JS `cause` (failed entries, counts, ...).
#[derive(Debug)]
pub struct CauseError {
    pub message: String,
    pub cause: String,
}

impl std::fmt::Display for CauseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CauseError {}

pub(crate) fn cause_error(message: impl Into<String>, cause: impl Into<String>) -> Error {
    Box::new(CauseError {
        message: message.into(),
        cause: cause.into(),
    })
}

/// Options for the polling readables (SQS, Kinesis, DynamoDB Streams,
/// CloudWatch Logs).
#[derive(Default, Clone, Debug)]
pub struct AwsPollingOptions {
    /// Keep polling after an empty page instead of ending the stream.
    pub polling_active: bool,
    /// Idle wait after an empty page while polling (default 1s).
    pub polling_delay: Option<Duration>,
    pub signal: Option<CancellationToken>,
}

impl AwsPollingOptions {
    pub(crate) async fn idle(&self) -> Result<()> {
        let delay = self.polling_delay.unwrap_or(Duration::from_secs(1));
        if delay.is_zero() {
            return Ok(());
        }
        timeout(delay, self.signal.as_ref()).await
    }
}

/// Options for the batching writables (SQS, SNS, Kinesis).
#[derive(Default, Clone, Debug)]
pub struct AwsWriteOptions {
    /// Retries for partially failed batches (default 10; `u32::MAX` retries
    /// without limit).
    pub retry_max_count: Option<u32>,
    pub signal: Option<CancellationToken>,
}

/// Await an SDK call, failing with "Aborted" as soon as `signal` is cancelled.
pub(crate) async fn send<T, E: Into<Error>>(
    request: impl Future<Output = Result<T, E>>,
    signal: Option<&CancellationToken>,
) -> Result<T> {
    match signal {
        Some(signal) => tokio::select! {
            biased;
            _ = signal.cancelled() => Err("Aborted".into()),
            result = request => result.map_err(Into::into),
        },
        None => request.await.map_err(Into::into),
    }
}

// Partial failures are throttling-driven: floor the first retries at 50ms so
// they give capacity time to recover, and cap at 3^10ms (~59s).
const BACKOFF_FLOOR_MS: u64 = 50;
const BACKOFF_CAP_MS: u64 = 59_049;

pub(crate) async fn backoff(retry_count: u32, signal: Option<&CancellationToken>) -> Result<()> {
    let ms = 3u64
        .saturating_pow(retry_count)
        .clamp(BACKOFF_FLOOR_MS, BACKOFF_CAP_MS);
    timeout(Duration::from_millis(ms), signal).await
}

/// A `retry_max_count` of `u32::MAX` retries without limit (JS `null`).
pub(crate) fn retries_exhausted(retry_count: u32, max_count: u32) -> bool {
    max_count != u32::MAX && retry_count >= max_count
}

/// SDK list fields are `Vec<T>` or `Option<Vec<T>>` depending on the model.
pub(crate) trait IntoVec<T> {
    fn into_vec(self) -> Vec<T>;
}

impl<T> IntoVec<T> for Vec<T> {
    fn into_vec(self) -> Vec<T> {
        self
    }
}

impl<T> IntoVec<T> for Option<Vec<T>> {
    fn into_vec(self) -> Vec<T> {
        self.unwrap_or_default()
    }
}

/// A writer stopped (input error, abort, oversize entry, ...) with entries
/// still buffered below a batch limit. They were never sent, so they are handed
/// back for the caller to recover or retry (the `KafkaProduceError` contract).
/// A failed request is never reported here: its batch was already handed off.
#[derive(Debug)]
pub struct AwsUnsentError<E> {
    pub error: Error,
    pub failed_messages: Vec<E>,
}

impl<E> std::fmt::Display for AwsUnsentError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<E: std::fmt::Debug> std::error::Error for AwsUnsentError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
}

/// Attach the still-buffered entries (if any) to `error`.
pub(crate) fn unsent<E>(error: Error, buffered: Vec<E>) -> Error
where
    E: std::fmt::Debug + Send + Sync + 'static,
{
    if buffered.is_empty() {
        return error;
    }
    Box::new(AwsUnsentError {
        error,
        failed_messages: buffered,
    })
}

/// The next input entry (`None` at the end), failing with "Aborted" as soon as
/// `signal` is cancelled so a writer stops without waiting on its input.
pub(crate) async fn next_entry<E>(
    input: &mut DataStream<E>,
    signal: Option<&CancellationToken>,
) -> Result<Option<E>> {
    send(async { input.next().await.transpose() }, signal).await
}

/// Batch limits and sizing for [`batch_write`].
pub(crate) struct Batcher<E> {
    pub max_entries: usize,
    pub max_entry_bytes: usize,
    pub max_batch_bytes: usize,
    pub size: fn(&E) -> usize,
    pub oversize: fn(&E, usize) -> Error,
}

pub(crate) struct Retry {
    pub max_count: u32,
    pub signal: Option<CancellationToken>,
    pub message: &'static str,
    /// Turns the failure descriptions into the error's cause.
    pub describe: fn(Vec<String>) -> String,
}

/// Describe failures as a list, e.g. `[Entry { .. }, Entry { .. }]`.
pub(crate) fn describe_list(failures: Vec<String>) -> String {
    format!("[{}]", failures.join(", "))
}

/// What one batch request left undone.
pub(crate) struct Sent<E> {
    /// Entries to send again.
    pub retry: Vec<E>,
    /// Describes the failures behind `retry`.
    pub failed: Vec<String>,
    /// Failures retrying cannot fix (SNS/SQS `SenderFault`): set aside rather
    /// than retried, and reported with the rest once the batch settles.
    pub permanent: Vec<String>,
}

impl<E> Sent<E> {
    pub fn ok() -> Self {
        Sent {
            retry: Vec::new(),
            failed: Vec::new(),
            permanent: Vec::new(),
        }
    }
}

/// Write `input` in batches within `batcher`'s limits. `send` gets each batch
/// and returns what is left to retry. An input error, abort or oversize entry
/// hands the still-buffered entries back as [`AwsUnsentError`].
pub(crate) async fn batch_write<E, F, Fut>(
    mut input: DataStream<E>,
    batcher: Batcher<E>,
    retry: Retry,
    mut send: F,
) -> Result<()>
where
    E: std::fmt::Debug + Send + Sync + 'static,
    F: FnMut(Vec<E>) -> Fut,
    Fut: Future<Output = Result<Sent<E>>>,
{
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    while let Some(entry) = next_entry(&mut input, retry.signal.as_ref())
        .await
        .map_err(|error| unsent(error, std::mem::take(&mut batch)))?
    {
        let bytes = (batcher.size)(&entry);
        if bytes > batcher.max_entry_bytes {
            return Err(unsent((batcher.oversize)(&entry, bytes), batch));
        }
        if batch.len() == batcher.max_entries
            || (!batch.is_empty() && batch_bytes + bytes > batcher.max_batch_bytes)
        {
            retry_send(std::mem::take(&mut batch), &retry, &mut send).await?;
            batch_bytes = 0;
        }
        batch.push(entry);
        batch_bytes += bytes;
    }
    if !batch.is_empty() {
        retry_send(batch, &retry, &mut send).await?;
    }
    Ok(())
}

async fn retry_send<E, F, Fut>(mut entries: Vec<E>, retry: &Retry, send: &mut F) -> Result<()>
where
    F: FnMut(Vec<E>) -> Fut,
    Fut: Future<Output = Result<Sent<E>>>,
{
    let mut retry_count = 0;
    let mut permanent = Vec::new();
    loop {
        let sent = send(entries).await?;
        permanent.extend(sent.permanent);
        if sent.retry.is_empty() || retries_exhausted(retry_count, retry.max_count) {
            let mut failed = permanent;
            failed.extend(sent.failed);
            if failed.is_empty() {
                return Ok(());
            }
            return Err(cause_error(retry.message, (retry.describe)(failed)));
        }
        backoff(retry_count, retry.signal.as_ref()).await?;
        retry_count = retry_count.saturating_add(1);
        entries = sent.retry;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::create_readable_stream;
    use std::sync::{Arc, Mutex};

    #[test]
    fn fips_regions() {
        for region in ["us-east-1", "ca-west-1", "us-gov-east-1", "us-gov-west-1"] {
            assert!(aws_region_supports_fips(region), "{region}");
        }
        for region in ["eu-west-1", "ap-southeast-2", "", "US-EAST-1"] {
            assert!(!aws_region_supports_fips(region), "{region}");
        }
    }

    #[test]
    fn use_fips_endpoint_reads_aws_region_lazily() {
        std::env::set_var("AWS_REGION", "us-gov-west-1");
        assert!(aws_client_defaults_use_fips_endpoint());
        std::env::set_var("AWS_REGION", "eu-west-1");
        assert!(!aws_client_defaults_use_fips_endpoint());
        std::env::remove_var("AWS_REGION");
        assert!(!aws_client_defaults_use_fips_endpoint());
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_floored_and_capped() {
        let start = tokio::time::Instant::now();
        backoff(0, None).await.unwrap();
        let elapsed = start.elapsed().as_millis();
        assert!((50..60).contains(&elapsed), "{elapsed}");
        let start = tokio::time::Instant::now();
        backoff(40, None).await.unwrap();
        let elapsed = start.elapsed().as_millis();
        assert!((59_049..59_060).contains(&elapsed), "{elapsed}");
    }

    #[tokio::test]
    async fn send_aborts_on_signal() {
        let signal = CancellationToken::new();
        signal.cancel();
        let e = send(std::future::pending::<Result<()>>(), Some(&signal))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    fn batcher() -> Batcher<usize> {
        Batcher {
            max_entries: 3,
            max_entry_bytes: 10,
            max_batch_bytes: 10,
            size: |n| *n,
            oversize: |_, bytes| format!("too big {bytes}").into(),
        }
    }

    fn retry(max_count: u32) -> Retry {
        Retry {
            max_count,
            signal: None,
            message: "failed",
            describe: describe_list,
        }
    }

    // Entry `n` fails (retryably) while `n` is in `retryable`, permanently when
    // in `permanent`.
    fn sent(batch: Vec<usize>, retryable: &[usize], permanent: &[usize]) -> Sent<usize> {
        let describe = |n: &usize| format!("entry {n}");
        let retry: Vec<usize> = batch
            .into_iter()
            .filter(|n| retryable.contains(n))
            .collect();
        Sent {
            failed: retry.iter().map(describe).collect(),
            retry,
            permanent: permanent.iter().map(describe).collect(),
        }
    }

    #[tokio::test]
    async fn batch_write_splits_on_count_and_bytes() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let seen = batches.clone();
        let input = create_readable_stream([1, 1, 1, 1, 5, 5, 4]);
        batch_write(input, batcher(), retry(0), move |batch| {
            seen.lock().unwrap().push(batch);
            async { Ok(Sent::ok()) }
        })
        .await
        .unwrap();
        // 3 entries max; 1+5+5 > 10 so the second 5 starts a new batch; 5+4 fits.
        assert_eq!(
            *batches.lock().unwrap(),
            [vec![1, 1, 1], vec![1, 5], vec![5, 4]]
        );
    }

    #[tokio::test]
    async fn batch_write_rejects_oversize_entry() {
        let input = create_readable_stream([11]);
        let e = batch_write(input, batcher(), retry(0), |_| async { Ok(Sent::ok()) })
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "too big 11");
        assert!(e.downcast_ref::<AwsUnsentError<usize>>().is_none());
    }

    #[tokio::test]
    async fn batch_write_attaches_buffered_entries_to_oversize_error() {
        let calls = Arc::new(Mutex::new(0));
        let seen = calls.clone();
        let input = create_readable_stream([1, 2, 11]);
        let e = batch_write(input, batcher(), retry(0), move |_| {
            *seen.lock().unwrap() += 1;
            async { Ok(Sent::ok()) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "too big 11");
        let unsent = e.downcast_ref::<AwsUnsentError<usize>>().unwrap();
        assert_eq!(unsent.failed_messages, [1, 2]);
        assert_eq!(*calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn batch_write_attaches_buffered_entries_to_input_error() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        // One full batch is sent; the two entries after it are not.
        let input: Vec<Result<usize>> = vec![Ok(1), Ok(1), Ok(1), Ok(2), Ok(3), Err("boom".into())];
        let input: DataStream<usize> = Box::pin(futures::stream::iter(input));
        let e = batch_write(input, batcher(), retry(0), move |batch| {
            seen.lock().unwrap().push(batch);
            async { Ok(Sent::ok()) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        let unsent = e.downcast_ref::<AwsUnsentError<usize>>().unwrap();
        assert_eq!(unsent.failed_messages, [2, 3]);
        assert_eq!(*calls.lock().unwrap(), [vec![1, 1, 1]]);

        // Nothing buffered: the input error comes through as is.
        let input: DataStream<usize> = Box::pin(futures::stream::iter(vec![Err("boom".into())]));
        let e = batch_write(input, batcher(), retry(0), |_| async { Ok(Sent::ok()) })
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
        assert!(e.downcast_ref::<AwsUnsentError<usize>>().is_none());
    }

    #[tokio::test]
    async fn batch_write_abort_attaches_buffered_entries_without_sending() {
        let signal = CancellationToken::new();
        let (sender, input) = datastream_core::create_readable_channel::<usize>(Some(4));
        let calls = Arc::new(Mutex::new(0));
        let seen = calls.clone();
        let retry = Retry {
            signal: Some(signal.clone()),
            ..retry(0)
        };
        let task = tokio::spawn(batch_write(input, batcher(), retry, move |_| {
            *seen.lock().unwrap() += 1;
            async { Ok(Sent::ok()) }
        }));
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        let e = task.await.unwrap().unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        let unsent = e.downcast_ref::<AwsUnsentError<usize>>().unwrap();
        assert_eq!(unsent.failed_messages, [1, 2]);
        assert_eq!(*calls.lock().unwrap(), 0);
        drop(sender);
    }

    #[tokio::test]
    async fn batch_write_does_not_report_a_failed_request_as_unsent() {
        let input = create_readable_stream([1, 1, 1, 1]);
        let e = batch_write(input, batcher(), retry(0), |_| async {
            Err::<Sent<usize>, Error>("send failed".into())
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "send failed");
        assert!(e.downcast_ref::<AwsUnsentError<usize>>().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn batch_write_retries_failed_then_throws() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let input = create_readable_stream([1, 2]);
        let e = batch_write(input, batcher(), retry(2), move |batch: Vec<usize>| {
            seen.lock().unwrap().push(batch.clone());
            async move { Ok(sent(batch, &[2], &[])) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "failed");
        assert_eq!(e.downcast_ref::<CauseError>().unwrap().cause, "[entry 2]");
        assert_eq!(*calls.lock().unwrap(), [vec![1, 2], vec![2], vec![2]]);
    }

    // 11 failures: one more than the default retry_max_count of 10.
    #[tokio::test(start_paused = true)]
    async fn batch_write_retries_without_limit_at_u32_max() {
        let calls = Arc::new(Mutex::new(0));
        let seen = calls.clone();
        let input = create_readable_stream([1]);
        batch_write(
            input,
            batcher(),
            retry(u32::MAX),
            move |batch: Vec<usize>| {
                let mut calls = seen.lock().unwrap();
                *calls += 1;
                let sent = if *calls <= 11 {
                    sent(batch, &[1], &[])
                } else {
                    Sent::ok()
                };
                async move { Ok(sent) }
            },
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), 12);
        assert!(!retries_exhausted(u32::MAX, u32::MAX));
        assert!(retries_exhausted(10, 10));
    }

    // Permanent failures are set aside while the retryable rest backs off, then
    // reported together.
    #[tokio::test(start_paused = true)]
    async fn batch_write_retries_retryable_alongside_permanent_failures() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let input = create_readable_stream([1, 2, 3]);
        let e = batch_write(input, batcher(), retry(1), move |batch: Vec<usize>| {
            let first = seen.lock().unwrap().is_empty();
            seen.lock().unwrap().push(batch.clone());
            let permanent: &[usize] = if first { &[2] } else { &[] };
            async move { Ok(sent(batch, &[1], permanent)) }
        })
        .await
        .unwrap_err();
        assert_eq!(
            e.downcast_ref::<CauseError>().unwrap().cause,
            "[entry 2, entry 1]"
        );
        assert_eq!(*calls.lock().unwrap(), [vec![1, 2, 3], vec![1]]);

        // Retryable entries that then succeed leave only the permanent ones.
        let calls = Arc::new(Mutex::new(0));
        let seen = calls.clone();
        let input = create_readable_stream([1, 2]);
        let e = batch_write(input, batcher(), retry(5), move |batch: Vec<usize>| {
            let mut calls = seen.lock().unwrap();
            *calls += 1;
            let sent = if *calls == 1 {
                sent(batch, &[1], &[2])
            } else {
                Sent::ok()
            };
            async move { Ok(sent) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.downcast_ref::<CauseError>().unwrap().cause, "[entry 2]");
        assert_eq!(*calls.lock().unwrap(), 2);

        // Only permanent failures: no retry at all.
        let calls = Arc::new(Mutex::new(0));
        let seen = calls.clone();
        let input = create_readable_stream([1]);
        batch_write(input, batcher(), retry(5), move |batch: Vec<usize>| {
            *seen.lock().unwrap() += 1;
            async move { Ok(sent(batch, &[], &[1])) }
        })
        .await
        .unwrap_err();
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn batch_write_aborts_retry_backoff() {
        let signal = CancellationToken::new();
        let retry = Retry {
            signal: Some(signal.clone()),
            ..retry(5)
        };
        let input = create_readable_stream([1]);
        let e = batch_write(input, batcher(), retry, move |batch| {
            // Cancelled once the batch is in flight, so the backoff is what aborts.
            signal.cancel();
            async move { Ok(sent(batch, &[1], &[])) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
