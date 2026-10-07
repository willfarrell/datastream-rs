// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! CloudWatch Logs get and filter log events streams.

use async_stream::try_stream;
use aws_sdk_cloudwatchlogs::operation::filter_log_events::builders::FilterLogEventsFluentBuilder;
use aws_sdk_cloudwatchlogs::operation::get_log_events::builders::GetLogEventsFluentBuilder;
use aws_sdk_cloudwatchlogs::types::{FilteredLogEvent, OutputLogEvent};
use datastream_core::{CancellationToken, DataStream};

use crate::client::{send, AwsPollingOptions, IntoVec};

/// Read a log stream (from the head unless `start_from_head` is set) until
/// caught up (or forever with `polling_active`).
pub fn aws_cloudwatch_logs_get_log_events_stream(
    mut request: GetLogEventsFluentBuilder,
    options: AwsPollingOptions,
) -> DataStream<OutputLogEvent> {
    if request.get_start_from_head().is_none() {
        request = request.start_from_head(true);
    }
    Box::pin(try_stream! {
        loop {
            let sent = request.get_next_token().clone();
            let output = send(request.clone().send(), options.signal.as_ref()).await?;
            for event in output.events.into_vec() {
                yield event;
            }
            // CloudWatch echoes the token you sent once there are no further
            // events, so an unchanged token means caught up.
            let caught_up = output.next_forward_token == sent;
            request = request.set_next_token(output.next_forward_token);
            if caught_up {
                if !options.polling_active {
                    break;
                }
                options.idle().await?;
            }
        }
    })
}

/// Read every page of a FilterLogEvents query.
pub fn aws_cloudwatch_logs_filter_log_events_stream(
    mut request: FilterLogEventsFluentBuilder,
    signal: Option<CancellationToken>,
) -> DataStream<FilteredLogEvent> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), signal.as_ref()).await?;
            for event in output.events.into_vec() {
                yield event;
            }
            match output.next_token {
                Some(token) => request = request.next_token(token),
                None => break,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_cloudwatchlogs::operation::filter_log_events::FilterLogEventsOutput;
    use aws_sdk_cloudwatchlogs::operation::get_log_events::GetLogEventsOutput;
    use aws_sdk_cloudwatchlogs::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{stream_to_array, StreamExt};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn get_output(messages: &[&str], token: &str) -> GetLogEventsOutput {
        let events = messages
            .iter()
            .map(|m| OutputLogEvent::builder().message(*m).build());
        GetLogEventsOutput::builder()
            .set_events(Some(events.collect()))
            .next_forward_token(token)
            .build()
    }

    fn messages<T>(events: &[T], message: fn(&T) -> Option<&str>) -> Vec<&str> {
        events.iter().map(|e| message(e).unwrap()).collect()
    }

    #[tokio::test]
    async fn get_log_events_stops_when_token_repeats() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::get_log_events).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            calls.push((req.next_token().map(String::from), req.start_from_head()));
            match calls.len() {
                1 => get_output(&["a", "b"], "f1"),
                2 => get_output(&["c"], "f2"),
                _ => get_output(&[], "f2"),
            }
        });
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let request = client
            .get_log_events()
            .log_group_name("g")
            .log_stream_name("s");
        let stream =
            aws_cloudwatch_logs_get_log_events_stream(request, AwsPollingOptions::default());
        let events = stream_to_array(stream, None).await.unwrap();
        assert_eq!(messages(&events, OutputLogEvent::message), ["a", "b", "c"]);
        assert_eq!(
            *calls.lock().unwrap(),
            [
                (None, Some(true)),
                (Some("f1".to_string()), Some(true)),
                (Some("f2".to_string()), Some(true)),
            ]
        );
    }

    #[tokio::test]
    async fn get_log_events_handles_empty_events_and_keeps_start_from_head_false() {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let seen = starts.clone();
        let rule = mock!(Client::get_log_events).then_compute_output(move |req| {
            seen.lock().unwrap().push(req.start_from_head());
            GetLogEventsOutput::builder().build()
        });
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let request = client.get_log_events().start_from_head(false);
        let stream =
            aws_cloudwatch_logs_get_log_events_stream(request, AwsPollingOptions::default());
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(*starts.lock().unwrap(), [Some(false)]);
    }

    #[tokio::test(start_paused = true)]
    async fn get_log_events_polls_with_delay() {
        let rule = mock!(Client::get_log_events)
            .sequence()
            .output(|| get_output(&[], ""))
            .output(|| get_output(&["a"], "f1"))
            .repeatedly()
            .build();
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        // Page 1 has no events but a new token (None -> ""), so no wait; page 2
        // and 3 each yield "a", and page 3 echoes "f1", so page 4 waits first.
        let mut stream =
            aws_cloudwatch_logs_get_log_events_stream(client.get_log_events(), options);
        stream.next().await.unwrap().unwrap();
        stream.next().await.unwrap().unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));
        stream.next().await.unwrap().unwrap();
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn get_log_events_aborts_idle_poll() {
        let rule =
            mock!(Client::get_log_events).then_output(|| GetLogEventsOutput::builder().build());
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        let options = AwsPollingOptions {
            polling_active: true,
            polling_delay: Some(Duration::from_secs(60)),
            signal: Some(signal.clone()),
        };
        let stream = aws_cloudwatch_logs_get_log_events_stream(client.get_log_events(), options);
        let task = tokio::spawn(stream_to_array(stream, None));
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        assert_eq!(task.await.unwrap().unwrap_err().to_string(), "Aborted");
    }

    #[tokio::test]
    async fn filter_log_events_paginates_on_next_token() {
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let seen = tokens.clone();
        let rule = mock!(Client::filter_log_events).then_compute_output(move |req| {
            let mut tokens = seen.lock().unwrap();
            tokens.push((
                req.next_token().map(String::from),
                req.filter_pattern().map(String::from),
            ));
            let event = FilteredLogEvent::builder()
                .message(format!("m{}", tokens.len()))
                .build();
            let output = FilterLogEventsOutput::builder().events(event);
            match tokens.len() {
                1 => output.next_token("t1").build(),
                _ => output.build(),
            }
        });
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let request = client
            .filter_log_events()
            .log_group_name("g")
            .filter_pattern("ERROR");
        let stream = aws_cloudwatch_logs_filter_log_events_stream(request, None);
        let events = stream_to_array(stream, None).await.unwrap();
        assert_eq!(messages(&events, FilteredLogEvent::message), ["m1", "m2"]);
        let pattern = Some("ERROR".to_string());
        assert_eq!(
            *tokens.lock().unwrap(),
            [(None, pattern.clone()), (Some("t1".to_string()), pattern)]
        );
    }

    #[tokio::test]
    async fn filter_log_events_handles_empty_events_and_abort() {
        let rule = mock!(Client::filter_log_events)
            .then_output(|| FilterLogEventsOutput::builder().build());
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let stream = aws_cloudwatch_logs_filter_log_events_stream(client.filter_log_events(), None);
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());

        let signal = CancellationToken::new();
        signal.cancel();
        let stream =
            aws_cloudwatch_logs_filter_log_events_stream(client.filter_log_events(), Some(signal));
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
