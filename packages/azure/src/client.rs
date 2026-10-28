// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Abort and polling helpers the service modules share.

use std::future::Future;
use std::time::Duration;

use datastream_core::{timeout, CancellationToken, Error, Result};

/// Options for the polling readables (Queue Storage).
#[derive(Default, Clone, Debug)]
pub struct AzurePollingOptions {
    /// Keep polling after an empty page instead of ending the stream.
    pub polling_active: bool,
    /// Idle wait after an empty page while polling (default 1s).
    pub polling_delay: Option<Duration>,
    pub signal: Option<CancellationToken>,
}

impl AzurePollingOptions {
    #[cfg_attr(not(feature = "queue"), allow(dead_code))]
    pub(crate) async fn idle(&self) -> Result<()> {
        let delay = self.polling_delay.unwrap_or(Duration::from_secs(1));
        if delay.is_zero() {
            return Ok(());
        }
        timeout(delay, self.signal.as_ref()).await
    }
}

/// A send stream ended early (input error or abort) with a partly filled batch:
/// `failed_messages` were buffered but never sent, so callers can retry them.
#[derive(Debug)]
pub struct AzureFailedMessagesError<T> {
    pub error: Error,
    pub failed_messages: Vec<T>,
}

impl<T> std::fmt::Display for AzureFailedMessagesError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<T: std::fmt::Debug> std::error::Error for AzureFailedMessagesError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
}

/// Attach the unsent entries to `error` (left as-is when there are none).
#[cfg_attr(not(feature = "event-hubs"), allow(dead_code))]
pub(crate) fn with_failed_messages<T>(error: Error, failed_messages: Vec<T>) -> Error
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    if failed_messages.is_empty() {
        return error;
    }
    Box::new(AzureFailedMessagesError {
        error,
        failed_messages,
    })
}

/// Await an SDK call, failing with "Aborted" as soon as `signal` is cancelled.
#[cfg_attr(
    not(any(
        feature = "blob",
        feature = "cosmos",
        feature = "event-hubs",
        feature = "queue"
    )),
    allow(dead_code)
)]
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

/// An `HttpClient` that replays canned responses and records requests, for
/// the Storage client tests.
#[cfg(all(test, any(feature = "blob", feature = "queue")))]
pub(crate) mod mock {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use azure_core::http::headers::Headers;
    use azure_core::http::{AsyncRawResponse, ClientOptions, HttpClient, Request, Transport};

    #[derive(Debug, Default)]
    pub struct MockHttpClient {
        responses: Mutex<VecDeque<(u16, String)>>,
        /// `"METHOD url"` per request.
        pub requests: Mutex<Vec<String>>,
        /// Request bodies, in order.
        pub bodies: Mutex<Vec<Vec<u8>>>,
    }

    impl MockHttpClient {
        pub fn new(responses: &[(u16, &str)]) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.iter().map(|(s, b)| (*s, b.to_string())).collect()),
                ..Default::default()
            })
        }

        pub fn client_options(self: &Arc<Self>) -> ClientOptions {
            ClientOptions {
                transport: Some(Transport::new(self.clone())),
                retry: azure_core::http::RetryOptions::none(),
                ..Default::default()
            }
        }

        pub fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for MockHttpClient {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            self.requests
                .lock()
                .unwrap()
                .push(format!("{} {}", request.method(), request.url()));
            let body: Vec<u8> = match request.body() {
                azure_core::http::Body::Bytes(bytes) => bytes.to_vec(),
                #[allow(unreachable_patterns)]
                _ => Vec::new(),
            };
            self.bodies.lock().unwrap().push(body);
            let (status, body) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            Ok(AsyncRawResponse::from_bytes(
                status.into(),
                Headers::new(),
                body,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_aborts_on_signal() {
        let signal = CancellationToken::new();
        signal.cancel();
        let e = send(std::future::pending::<Result<()>>(), Some(&signal))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[test]
    fn with_failed_messages_attaches_entries() {
        let e = with_failed_messages("boom".into(), vec!["a", "b"]);
        assert_eq!(e.to_string(), "boom");
        let e = e.downcast_ref::<AzureFailedMessagesError<&str>>().unwrap();
        assert_eq!(e.failed_messages, ["a", "b"]);
        assert_eq!(std::error::Error::source(e).unwrap().to_string(), "boom");
    }

    #[test]
    fn with_failed_messages_keeps_error_without_entries() {
        let e = with_failed_messages("boom".into(), Vec::<&str>::new());
        assert_eq!(e.to_string(), "boom");
        assert!(e.downcast_ref::<AzureFailedMessagesError<&str>>().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_defaults_to_one_second() {
        let start = tokio::time::Instant::now();
        AzurePollingOptions::default().idle().await.unwrap();
        assert_eq!(start.elapsed().as_secs(), 1);
    }
}
