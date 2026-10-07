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
    /// Retries for partially failed batches (default 10).
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
}

/// Write `input` in batches within `batcher`'s limits. `send` gets each batch
/// and returns the entries to retry plus a description of the failures.
pub(crate) async fn batch_write<E, F, Fut>(
    mut input: DataStream<E>,
    batcher: Batcher<E>,
    retry: Retry,
    mut send: F,
) -> Result<()>
where
    F: FnMut(Vec<E>) -> Fut,
    Fut: Future<Output = Result<(Vec<E>, String)>>,
{
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    while let Some(entry) = input.next().await {
        let entry = entry?;
        let bytes = (batcher.size)(&entry);
        if bytes > batcher.max_entry_bytes {
            return Err((batcher.oversize)(&entry, bytes));
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
    Fut: Future<Output = Result<(Vec<E>, String)>>,
{
    let mut retry_count = 0;
    loop {
        let (failed, cause) = send(entries).await?;
        if failed.is_empty() {
            return Ok(());
        }
        if retry_count >= retry.max_count {
            return Err(cause_error(retry.message, cause));
        }
        backoff(retry_count, retry.signal.as_ref()).await?;
        retry_count += 1;
        entries = failed;
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
        }
    }

    #[tokio::test]
    async fn batch_write_splits_on_count_and_bytes() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let seen = batches.clone();
        let input = create_readable_stream([1, 1, 1, 1, 5, 5, 4]);
        batch_write(input, batcher(), retry(0), move |batch| {
            seen.lock().unwrap().push(batch);
            async { Ok((Vec::new(), String::new())) }
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
        let e = batch_write(input, batcher(), retry(0), |_| async {
            Ok((Vec::new(), String::new()))
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "too big 11");
    }

    #[tokio::test(start_paused = true)]
    async fn batch_write_retries_failed_then_throws() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let input = create_readable_stream([1, 2]);
        let e = batch_write(input, batcher(), retry(2), move |batch: Vec<usize>| {
            seen.lock().unwrap().push(batch.clone());
            let failed = batch.into_iter().filter(|n| *n == 2).collect();
            async move { Ok((failed, "entry 2".to_string())) }
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "failed");
        assert_eq!(e.downcast_ref::<CauseError>().unwrap().cause, "entry 2");
        assert_eq!(*calls.lock().unwrap(), [vec![1, 2], vec![2], vec![2]]);
    }

    #[tokio::test]
    async fn batch_write_aborts_retry_backoff() {
        let signal = CancellationToken::new();
        signal.cancel();
        let retry = Retry {
            max_count: 5,
            signal: Some(signal),
            message: "failed",
        };
        let input = create_readable_stream([1]);
        let e = batch_write(input, batcher(), retry, |batch| async move {
            Ok((batch, String::new()))
        })
        .await
        .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
