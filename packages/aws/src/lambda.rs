// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Lambda response stream.

use async_stream::try_stream;
use aws_sdk_lambda::operation::invoke_with_response_stream::builders::InvokeWithResponseStreamFluentBuilder;
use aws_sdk_lambda::types::InvokeWithResponseStreamResponseEvent as Event;
use datastream_core::{CancellationToken, DataStream, Error};

use crate::client::{cause_error, send};

/// Invoke each request in turn with InvokeWithResponseStream and stream the
/// payload chunks. Fails fast: an error stops the remaining invocations.
pub fn aws_lambda_readable_stream(
    requests: Vec<InvokeWithResponseStreamFluentBuilder>,
    signal: Option<CancellationToken>,
) -> DataStream<Vec<u8>> {
    Box::pin(try_stream! {
        for (index, request) in requests.into_iter().enumerate() {
            let function_name = request.get_function_name().clone();
            let mut output = send(request.send(), signal.as_ref()).await?;
            while let Some(event) = send(output.event_stream.recv(), signal.as_ref()).await? {
                match event {
                    Event::PayloadChunk(chunk) => {
                        if let Some(payload) = chunk.payload {
                            yield payload.into_inner();
                        }
                    }
                    Event::InvokeComplete(complete) => {
                        if let Some(code) = complete.error_code {
                            let cause = format!(
                                "FunctionName: {function_name:?}, index: {index}, ErrorDetails: {:?}",
                                complete.error_details
                            );
                            Err::<(), Error>(cause_error(code, cause))?;
                        }
                    }
                    _ => {}
                }
            }
        }
    })
}

pub use aws_lambda_readable_stream as aws_lambda_response_stream;

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_lambda::Client;
    use aws_smithy_eventstream::frame::write_message_to;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
    use datastream_core::stream_to_array;

    // `(event type, payload)` frames encoded as an event stream HTTP response.
    fn response(events: &[(&'static str, &'static str)]) -> Response {
        let mut body = Vec::new();
        for &(event_type, payload) in events {
            let content_type = match event_type {
                "PayloadChunk" => "application/octet-stream",
                _ => "application/json",
            };
            let header = |name: &'static str, value: &'static str| {
                Header::new(name, HeaderValue::String(value.into()))
            };
            let message = Message::new(payload.as_bytes().to_vec())
                .add_header(header(":message-type", "event"))
                .add_header(header(":event-type", event_type))
                .add_header(header(":content-type", content_type));
            write_message_to(&message, &mut body).unwrap();
        }
        let mut response = Response::new(StatusCode::try_from(200).unwrap(), SdkBody::from(body));
        response
            .headers_mut()
            .insert("content-type", "application/vnd.amazon.eventstream");
        response
    }

    async fn invoke(client: &Client, names: &[&str]) -> Result<String, Error> {
        let requests = names
            .iter()
            .map(|n| client.invoke_with_response_stream().function_name(*n))
            .collect();
        let chunks = stream_to_array(aws_lambda_readable_stream(requests, None), None).await?;
        Ok(String::from_utf8(chunks.concat()).unwrap())
    }

    #[tokio::test]
    async fn streams_payload_chunks() {
        let rule = mock!(Client::invoke_with_response_stream).then_http_response(|| {
            response(&[
                ("PayloadChunk", "1"),
                ("PayloadChunk", "2"),
                ("InvokeComplete", "{}"),
            ])
        });
        let client = mock_client!(aws_sdk_lambda, RuleMode::MatchAny, [&rule]);
        assert_eq!(invoke(&client, &["fn"]).await.unwrap(), "12");
    }

    #[tokio::test]
    async fn invokes_each_request_in_order() {
        let fn1 = mock!(Client::invoke_with_response_stream)
            .match_requests(|req| req.function_name() == Some("fn1"))
            .then_http_response(|| response(&[("PayloadChunk", "a"), ("PayloadChunk", "b")]));
        let fn2 = mock!(Client::invoke_with_response_stream)
            .match_requests(|req| req.function_name() == Some("fn2"))
            .then_http_response(|| response(&[("PayloadChunk", "c")]));
        let client = mock_client!(aws_sdk_lambda, RuleMode::MatchAny, [&fn1, &fn2]);
        assert_eq!(invoke(&client, &["fn1", "fn2"]).await.unwrap(), "abc");
    }

    #[tokio::test]
    async fn fails_fast_on_invoke_complete_error() {
        let fn1 = mock!(Client::invoke_with_response_stream)
            .match_requests(|req| req.function_name() == Some("fn1"))
            .then_http_response(|| {
                response(&[(
                    "InvokeComplete",
                    r#"{"ErrorCode":"ErrorCode","ErrorDetails":"ErrorDetails"}"#,
                )])
            });
        let fn2 = mock!(Client::invoke_with_response_stream)
            .match_requests(|req| req.function_name() == Some("fn2"))
            .then_http_response(|| response(&[("PayloadChunk", "c")]));
        let client = mock_client!(aws_sdk_lambda, RuleMode::MatchAny, [&fn1, &fn2]);
        let e = invoke(&client, &["fn1", "fn2"]).await.unwrap_err();
        assert_eq!(e.to_string(), "ErrorCode");
        let cause = &e.downcast_ref::<crate::CauseError>().unwrap().cause;
        assert_eq!(
            cause,
            r#"FunctionName: Some("fn1"), index: 0, ErrorDetails: Some("ErrorDetails")"#
        );
        assert_eq!(fn2.num_calls(), 0);
    }

    #[tokio::test]
    async fn aborts_via_signal() {
        let rule = mock!(Client::invoke_with_response_stream)
            .then_http_response(|| response(&[("PayloadChunk", "1")]));
        let client = mock_client!(aws_sdk_lambda, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        signal.cancel();
        let stream =
            aws_lambda_readable_stream(vec![client.invoke_with_response_stream()], Some(signal));
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
