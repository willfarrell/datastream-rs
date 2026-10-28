// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! CloudWatch Logs get, filter and put log events streams.

use async_stream::try_stream;
use aws_sdk_cloudwatchlogs::operation::filter_log_events::builders::FilterLogEventsFluentBuilder;
use aws_sdk_cloudwatchlogs::operation::get_log_events::builders::GetLogEventsFluentBuilder;
use aws_sdk_cloudwatchlogs::operation::put_log_events::builders::PutLogEventsFluentBuilder;
use aws_sdk_cloudwatchlogs::types::{
    FilteredLogEvent, InputLogEvent, OutputLogEvent, RejectedLogEventsInfo,
};
use datastream_core::{CancellationToken, DataStream, Result};

use crate::client::{cause_error, next_entry, send, unsent, AwsPollingOptions, IntoVec};

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
            match output.next_token.filter(|token| !token.is_empty()) {
                Some(token) => request = request.next_token(token),
                None => break,
            }
        }
    })
}

// PutLogEvents batch limits: <=10,000 events, <=1,048,576 bytes (each event
// counts its UTF-8 message bytes plus 26), and a time span of <=24 hours.
const PUT_LOG_EVENTS_MAX_EVENTS: usize = 10_000;
const PUT_LOG_EVENTS_MAX_BYTES: usize = 1_048_576;
const PUT_LOG_EVENTS_EVENT_OVERHEAD_BYTES: usize = 26;
const PUT_LOG_EVENTS_MAX_SPAN_MS: u64 = 24 * 60 * 60 * 1000;

/// PutLogEvents accepted the batch but dropped these events (too new, too old
/// or past retention). The service's indexes point into the sorted batch the
/// caller never sees, so the events themselves are attached.
#[derive(Debug)]
pub struct AwsCloudWatchLogsRejectedEvents {
    pub rejected_log_events_info: RejectedLogEventsInfo,
    pub too_new: Vec<InputLogEvent>,
    pub too_old: Vec<InputLogEvent>,
    pub expired: Vec<InputLogEvent>,
}

impl std::fmt::Display for AwsCloudWatchLogsRejectedEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("awsCloudWatchLogsPutLogEvents has rejected events")
    }
}

impl std::error::Error for AwsCloudWatchLogsRejectedEvents {}

/// Put log events with PutLogEvents, batching within the service limits and
/// sorting each batch by timestamp. No sequence token: PutLogEvents has ignored
/// it since 2023. Rejected events fail with
/// [`AwsCloudWatchLogsRejectedEvents`]; on an input error, abort or oversize
/// event, events still buffered come back as
/// [`AwsUnsentError`](crate::AwsUnsentError) (not sent).
pub async fn aws_cloudwatch_logs_put_log_events_stream(
    mut input: DataStream<InputLogEvent>,
    request: PutLogEventsFluentBuilder,
    signal: Option<CancellationToken>,
) -> Result<()> {
    let signal = signal.as_ref();
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    // (earliest, latest) timestamp in the batch.
    let mut span = (i64::MAX, i64::MIN);
    while let Some(event) = next_entry(&mut input, signal)
        .await
        .map_err(|error| unsent(error, std::mem::take(&mut batch)))?
    {
        let bytes = event.message().len() + PUT_LOG_EVENTS_EVENT_OVERHEAD_BYTES;
        // Surface an event that can never fit a batch up front (mirroring
        // Kinesis) instead of letting the service reject the whole request.
        if bytes > PUT_LOG_EVENTS_MAX_BYTES {
            let cause = format!("bytes: {bytes}, limit: {PUT_LOG_EVENTS_MAX_BYTES}");
            let error = cause_error(
                "awsCloudWatchLogsPutLogEvents event exceeds 1MiB limit",
                cause,
            );
            return Err(unsent(error, batch));
        }
        let timestamp = event.timestamp();
        let (earliest, latest) = (span.0.min(timestamp), span.1.max(timestamp));
        // An empty batch never flushes here: the event alone fits (checked above)
        // and its span is 0.
        if batch.len() == PUT_LOG_EVENTS_MAX_EVENTS
            || batch_bytes + bytes > PUT_LOG_EVENTS_MAX_BYTES
            || latest.abs_diff(earliest) > PUT_LOG_EVENTS_MAX_SPAN_MS
        {
            put_log_events(&request, std::mem::take(&mut batch), signal).await?;
            batch_bytes = 0;
            span = (timestamp, timestamp);
        } else {
            span = (earliest, latest);
        }
        batch.push(event);
        batch_bytes += bytes;
    }
    if !batch.is_empty() {
        put_log_events(&request, batch, signal).await?;
    }
    Ok(())
}

async fn put_log_events(
    request: &PutLogEventsFluentBuilder,
    mut events: Vec<InputLogEvent>,
    signal: Option<&CancellationToken>,
) -> Result<()> {
    // The service rejects a batch whose events are not in chronological order.
    events.sort_by_key(InputLogEvent::timestamp);
    let request = request.clone().set_log_events(Some(events.clone()));
    let output = send(request.send(), signal).await?;
    // Too old / too new / expired events are dropped while the rest of the
    // batch IS accepted; surface that instead of losing them silently. tooNew
    // starts at its (inclusive) index, tooOld and expired end before their
    // (exclusive) index.
    let Some(info) = output.rejected_log_events_info else {
        return Ok(());
    };
    let index = |index: Option<i32>, absent: usize| {
        index.map_or(absent, |i| {
            usize::try_from(i).unwrap_or(0).min(events.len())
        })
    };
    let too_new = index(info.too_new_log_event_start_index, events.len());
    let too_old = index(info.too_old_log_event_end_index, 0);
    let expired = index(info.expired_log_event_end_index, 0);
    Err(Box::new(AwsCloudWatchLogsRejectedEvents {
        too_new: events[too_new..].to_vec(),
        too_old: events[..too_old].to_vec(),
        expired: events[..expired].to_vec(),
        rejected_log_events_info: info,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_cloudwatchlogs::operation::filter_log_events::FilterLogEventsOutput;
    use aws_sdk_cloudwatchlogs::operation::get_log_events::GetLogEventsOutput;
    use aws_sdk_cloudwatchlogs::operation::put_log_events::PutLogEventsOutput;
    use aws_sdk_cloudwatchlogs::Client;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, stream_to_array, StreamExt};
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

    // An empty nextToken ends the pagination, as an absent one does.
    #[tokio::test]
    async fn filter_log_events_stops_on_empty_next_token() {
        let rule = mock!(Client::filter_log_events).then_output(|| {
            FilterLogEventsOutput::builder()
                .events(FilteredLogEvent::builder().message("m").build())
                .next_token("")
                .build()
        });
        let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]);
        let stream = aws_cloudwatch_logs_filter_log_events_stream(client.filter_log_events(), None);
        let events = stream_to_array(stream, None).await.unwrap();
        assert_eq!(messages(&events, FilteredLogEvent::message), ["m"]);
        assert_eq!(rule.num_calls(), 1);
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

    // --- PutLogEventsStream ---

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    fn event(timestamp: i64, message: &str) -> InputLogEvent {
        InputLogEvent::builder()
            .timestamp(timestamp)
            .message(message)
            .build()
            .unwrap()
    }

    // Records each request's timestamps and answers with `rejected` (if any).
    fn put_client(
        rejected: Option<RejectedLogEventsInfo>,
    ) -> (aws_sdk_cloudwatchlogs::Client, Arc<Mutex<Vec<Vec<i64>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::put_log_events).then_compute_output(move |req| {
            assert_eq!(req.log_group_name(), Some("g"));
            assert_eq!(req.sequence_token(), None);
            let timestamps = req.log_events().iter().map(|e| e.timestamp()).collect();
            seen.lock().unwrap().push(timestamps);
            PutLogEventsOutput::builder()
                .set_rejected_log_events_info(rejected.clone())
                .build()
        });
        (
            mock_client!(aws_sdk_cloudwatchlogs, RuleMode::MatchAny, [&rule]),
            calls,
        )
    }

    async fn put(
        client: &aws_sdk_cloudwatchlogs::Client,
        events: Vec<InputLogEvent>,
    ) -> datastream_core::Result<()> {
        let request = client
            .put_log_events()
            .log_group_name("g")
            .log_stream_name("s");
        aws_cloudwatch_logs_put_log_events_stream(create_readable_stream(events), request, None)
            .await
    }

    #[tokio::test]
    async fn put_log_events_sends_one_batch_sorted_by_timestamp() {
        let (client, calls) = put_client(None);
        put(&client, vec![event(3, "c"), event(1, "a"), event(2, "b")])
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec![1, 2, 3]]);

        let (client, calls) = put_client(None);
        put(&client, Vec::new()).await.unwrap();
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_log_events_flushes_at_10000_events() {
        let (client, calls) = put_client(None);
        put(&client, (0..10_001).map(|i| event(i, "m")).collect())
            .await
            .unwrap();
        let sizes: Vec<_> = calls.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, [10_000, 1]);
    }

    // Each event counts its UTF-8 message bytes plus 26. "é" is 2 bytes, so two
    // 524,262-byte messages fill the 1,048,576-byte batch exactly.
    #[tokio::test]
    async fn put_log_events_byte_limit() {
        let half = "é".repeat(262_131);
        let (client, calls) = put_client(None);
        put(
            &client,
            vec![event(1, &half), event(2, &half), event(3, "a")],
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec![1, 2], vec![3]]);

        let (client, calls) = put_client(None);
        put(
            &client,
            vec![event(1, &half), event(2, &format!("{half}a"))],
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec![1], vec![2]]);
    }

    // The span is measured against both the earliest and the latest timestamp
    // seen so far, whatever order the events arrive in.
    #[tokio::test]
    async fn put_log_events_span_limit() {
        let (client, calls) = put_client(None);
        put(&client, vec![event(1000 + DAY_MS, "b"), event(1000, "a")])
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), [vec![1000, 1000 + DAY_MS]]);

        let (client, calls) = put_client(None);
        let events = vec![event(1000 + DAY_MS, "c"), event(1000, "b"), event(999, "a")];
        put(&client, events).await.unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            [vec![1000, 1000 + DAY_MS], vec![999]]
        );

        let (client, calls) = put_client(None);
        let events = vec![
            event(1000, "b"),
            event(995, "a"),
            event(996 + DAY_MS, "c"),
            event(997 + DAY_MS, "d"),
        ];
        put(&client, events).await.unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            [vec![995, 1000], vec![996 + DAY_MS, 997 + DAY_MS]]
        );

        // Extreme timestamps must not overflow the span arithmetic.
        let (client, calls) = put_client(None);
        put(&client, vec![event(i64::MIN, "a"), event(i64::MAX, "b")])
            .await
            .unwrap();
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn put_log_events_rejects_oversize_event() {
        let (client, calls) = put_client(None);
        put(&client, vec![event(1, &"x".repeat(1_048_550))])
            .await
            .unwrap();
        assert_eq!(calls.lock().unwrap().len(), 1);

        let (client, calls) = put_client(None);
        let events = vec![event(1, "a"), event(2, &"x".repeat(1_048_551))];
        let e = put(&client, events).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsCloudWatchLogsPutLogEvents event exceeds 1MiB limit"
        );
        let unsent = e
            .downcast_ref::<crate::AwsUnsentError<InputLogEvent>>()
            .unwrap();
        assert_eq!(unsent.failed_messages, [event(1, "a")]);
        let cause = unsent.error.downcast_ref::<crate::CauseError>().unwrap();
        assert_eq!(cause.cause, "bytes: 1048577, limit: 1048576");
        assert!(calls.lock().unwrap().is_empty());
    }

    // Rejected events are dropped while the rest are accepted: surface them,
    // resolved against the sorted batch the stream sent.
    #[tokio::test]
    async fn put_log_events_attaches_rejected_events() {
        let info = RejectedLogEventsInfo::builder()
            .too_old_log_event_end_index(1)
            .expired_log_event_end_index(2)
            .too_new_log_event_start_index(3)
            .build();
        let (client, _) = put_client(Some(info.clone()));
        let events = vec![
            event(5, "e"),
            event(1, "a"),
            event(4, "d"),
            event(2, "b"),
            event(3, "c"),
        ];
        let e = put(&client, events).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsCloudWatchLogsPutLogEvents has rejected events"
        );
        let rejected = e.downcast_ref::<AwsCloudWatchLogsRejectedEvents>().unwrap();
        assert_eq!(rejected.rejected_log_events_info, info);
        assert_eq!(rejected.too_new, [event(4, "d"), event(5, "e")]);
        assert_eq!(rejected.too_old, [event(1, "a")]);
        assert_eq!(rejected.expired, [event(1, "a"), event(2, "b")]);

        // Absent indexes reject nothing of that kind.
        let info = RejectedLogEventsInfo::builder()
            .too_new_log_event_start_index(1)
            .build();
        let (client, _) = put_client(Some(info));
        let e = put(&client, vec![event(2, "b"), event(1, "a")])
            .await
            .unwrap_err();
        let rejected = e.downcast_ref::<AwsCloudWatchLogsRejectedEvents>().unwrap();
        assert_eq!(rejected.too_new, [event(2, "b")]);
        assert!(rejected.too_old.is_empty() && rejected.expired.is_empty());

        let info = RejectedLogEventsInfo::builder()
            .too_old_log_event_end_index(1)
            .build();
        let (client, _) = put_client(Some(info));
        let e = put(&client, vec![event(2, "b"), event(1, "a")])
            .await
            .unwrap_err();
        let rejected = e.downcast_ref::<AwsCloudWatchLogsRejectedEvents>().unwrap();
        assert_eq!(rejected.too_old, [event(1, "a")]);
        assert!(rejected.too_new.is_empty() && rejected.expired.is_empty());
    }

    #[tokio::test]
    async fn put_log_events_abort_attaches_buffered_events() {
        let (client, calls) = put_client(None);
        let signal = CancellationToken::new();
        let (sender, input) = datastream_core::create_readable_channel(None);
        let request = client.put_log_events().log_group_name("g");
        let task = tokio::spawn(aws_cloudwatch_logs_put_log_events_stream(
            input,
            request,
            Some(signal.clone()),
        ));
        sender.send(event(1, "a")).await.unwrap();
        sender.send(event(2, "b")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
        let e = task.await.unwrap().unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
        let unsent = e
            .downcast_ref::<crate::AwsUnsentError<InputLogEvent>>()
            .unwrap();
        assert_eq!(unsent.failed_messages, [event(1, "a"), event(2, "b")]);
        assert!(calls.lock().unwrap().is_empty());
    }
}
