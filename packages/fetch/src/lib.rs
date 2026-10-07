// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! HTTP fetch streams, ported from `@datastream/fetch`.
//!
//! JSON responses (`application/json`, `application/*+json`) are parsed and
//! paginated (Link header, `next_path`, or offset query); anything else is
//! streamed as raw bytes. Requests are spaced by `rate_limit`, 429s are
//! retried with backoff, and redirects are pinned to the original origin.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use datastream_core::{
    timeout, BoxFuture, CancellationToken, DataStream, Map, Result, StreamExt, TryStreamExt, Value,
};
use reqwest::header::{CONTENT_TYPE, LINK, LOCATION, RETRY_AFTER};
use reqwest::{redirect::Policy, Body, Client, Method, Response, StatusCode};
use url::Url;

const MAX_REDIRECTS: usize = 20;

/// Redirect handling. `None` (the default) follows same-origin redirects only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Redirect {
    /// Follow redirects to any origin.
    Follow,
    /// Fail on any redirect.
    Error,
    /// Don't follow; the 3xx is returned and fails as a non-ok response.
    Manual,
}

#[derive(Default, Clone, Debug)]
pub struct FetchOptions {
    pub url: String,
    /// Default `GET`.
    pub method: Option<String>,
    /// Merged over the defaults (`Accept: application/json`).
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
    pub redirect: Option<Redirect>,
    /// Seconds between request starts. Default 0.01 (100 per second).
    pub rate_limit: Option<f64>,
    /// Dotted path to the data in a JSON body. Default: the body root.
    pub data_path: Option<String>,
    /// Dotted path to the next page URL in a JSON body.
    pub next_path: Option<String>,
    /// Appended to `url` as a query string (string or number values).
    pub qs: Map<String, Value>,
    pub offset_param: Option<String>,
    pub offset_amount: Option<i64>,
    /// Default 10.
    pub retry_max_count: Option<u32>,
    /// Client to send with. It should not follow redirects itself
    /// (`redirect::Policy::none()`), or origin pinning can't see them.
    pub client: Option<Client>,
}

#[derive(Default, Clone, Debug)]
pub struct FetchStreamOptions {
    /// Items fetched at once (default 1). Output stays in input order.
    pub concurrency: Option<usize>,
    pub signal: Option<CancellationToken>,
}

/// A parsed JSON item, or a raw chunk of a non-JSON body.
#[derive(Clone, Debug, PartialEq)]
pub enum FetchChunk {
    Json(Value),
    Bytes(Vec<u8>),
}

// *** Defaults *** //

static DEFAULTS: Mutex<Option<FetchOptions>> = Mutex::new(None);

fn defaults() -> MutexGuard<'static, Option<FetchOptions>> {
    DEFAULTS.lock().unwrap_or_else(|e| e.into_inner())
}

fn merge(d: FetchOptions, o: FetchOptions) -> FetchOptions {
    let mut headers = d.headers;
    headers.extend(o.headers);
    let mut qs = d.qs;
    qs.extend(o.qs);
    FetchOptions {
        url: if o.url.is_empty() { d.url } else { o.url },
        method: o.method.or(d.method),
        headers,
        body: o.body.or(d.body),
        redirect: o.redirect.or(d.redirect),
        rate_limit: o.rate_limit.or(d.rate_limit),
        data_path: o.data_path.or(d.data_path),
        next_path: o.next_path.or(d.next_path),
        qs,
        offset_param: o.offset_param.or(d.offset_param),
        offset_amount: o.offset_amount.or(d.offset_amount),
        retry_max_count: o.retry_max_count.or(d.retry_max_count),
        client: o.client.or(d.client),
    }
}

fn with_defaults(options: FetchOptions) -> FetchOptions {
    let d = defaults().clone().unwrap_or_else(|| FetchOptions {
        method: Some("GET".into()),
        // Accept-Encoding (br, gzip, deflate) is added and decoded by reqwest.
        headers: HashMap::from([("Accept".into(), "application/json".into())]),
        rate_limit: Some(0.01),
        ..Default::default()
    });
    merge(d, options)
}

/// Merge `options` into the process-wide defaults used by every fetch.
pub fn fetch_set_defaults(options: FetchOptions) {
    let merged = with_defaults(options);
    *defaults() = Some(merged);
}

// *** Helpers *** //

/// `parseInt`: leading integer, ignoring trailing junk.
fn parse_int(s: &str) -> Option<i64> {
    let s = s.trim_start();
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, s.strip_prefix('+').unwrap_or(s)),
    };
    let end = digits
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(digits.len());
    digits[..end].parse::<i64>().ok().map(|n| sign * n)
}

// ponytail: std's randomly keyed hasher as a jitter source; not a CSPRNG, fine for backoff.
fn random_unit() -> f64 {
    let n = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (n >> 11) as f64 / (1u64 << 53) as f64
}

fn origin_of(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .map(|u| u.origin().ascii_serialization())
}

fn redact_url(url: &str) -> String {
    let Ok(mut url) = Url::parse(url) else {
        return "[INVALID URL]".into();
    };
    if url.query().is_some_and(|q| !q.is_empty()) {
        url.set_query(Some("[REDACTED]"));
    }
    if !url.username().is_empty() {
        let _ = url.set_username("[REDACTED]");
    }
    if url.password().is_some_and(|p| !p.is_empty()) {
        let _ = url.set_password(Some("[REDACTED]"));
    }
    url.to_string()
}

/// `^application/(.+\+)?json($|;)`
fn is_json(content_type: &str) -> bool {
    let Some(rest) = content_type.strip_prefix("application/") else {
        return false;
    };
    rest.match_indices("json").any(|(i, _)| {
        let after = &rest[i + 4..];
        (after.is_empty() || after.starts_with(';'))
            && (i == 0 || (i >= 2 && rest[..i].ends_with('+')))
    })
}

/// URL of the `rel="next"` entry in a Link header.
fn next_link(link: &str) -> Option<String> {
    let end = link.find(">; rel=\"next\"")?;
    let start = link[..end].rfind('<')? + 1;
    Some(link[start..end].to_string())
}

/// Dotted path as a JSON pointer; "" is the root.
fn pointer(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    path.split('.')
        .map(|key| format!("/{}", key.replace('~', "~0").replace('/', "~1")))
        .collect()
}

fn paginate_using_query(o: &FetchOptions) -> Option<String> {
    let param = o.offset_param.as_deref()?;
    let amount = o.offset_amount.filter(|a| *a != 0)?;
    let mut url = Url::parse(&o.url).ok()?;
    let offset = url
        .query_pairs()
        .find(|(k, _)| k == param)
        .map(|(_, v)| v.into_owned())?;
    let offset = parse_int(&offset)? + amount;
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != param)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.query_pairs_mut()
        .clear()
        .extend_pairs(pairs)
        .append_pair(param, &offset.to_string());
    Some(url.to_string())
}

fn validate_pagination_url(next: &str, origin: &str) -> Result<()> {
    let url = Url::parse(next).map_err(|_| format!("Invalid pagination URL: {next}"))?;
    let next_origin = url.origin().ascii_serialization();
    if next_origin != origin {
        return Err(format!(
            "Pagination URL origin ({next_origin}) does not match initial URL origin ({origin})"
        )
        .into());
    }
    Ok(())
}

// *** Requests *** //

type Pacer = Arc<Mutex<Option<Instant>>>;

/// Reserve the next request slot `rate_limit` seconds after the previous one.
async fn pace(
    pacer: &Mutex<Option<Instant>>,
    rate_limit: f64,
    signal: Option<&CancellationToken>,
) -> Result<()> {
    let now = Instant::now();
    let at = {
        let mut next = pacer.lock().unwrap_or_else(|e| e.into_inner());
        let at = next.map_or(now, |next| next.max(now));
        *next = Some(at + Duration::try_from_secs_f64(rate_limit).unwrap_or_default());
        at
    };
    if at > now {
        timeout(at - now, signal).await?;
    }
    Ok(())
}

async fn send(
    client: &Client,
    method: &Method,
    o: &FetchOptions,
    signal: Option<&CancellationToken>,
    body: Option<Body>,
) -> Result<Response> {
    let mut request = client.request(method.clone(), o.url.as_str());
    for (key, value) in &o.headers {
        request = request.header(key, value);
    }
    if let Some(body) = body {
        request = request.body(body);
    }
    let response = request.send();
    Ok(match signal {
        Some(signal) => tokio::select! {
            _ = signal.cancelled() => return Err("Aborted".into()),
            response = response => response?,
        },
        None => response.await?,
    })
}

fn is_redirect(response: &Response) -> bool {
    matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308)
        && response.headers().contains_key(LOCATION)
}

async fn request(
    o: &mut FetchOptions,
    pacer: &Mutex<Option<Instant>>,
    signal: Option<&CancellationToken>,
    body: &mut (dyn FnMut() -> Option<Body> + Send),
) -> Result<Response> {
    let method_name = o.method.clone().unwrap_or_else(|| "GET".into());
    let method = Method::from_bytes(method_name.as_bytes())?;
    let client = match &o.client {
        Some(client) => client.clone(),
        None => {
            let client = Client::builder().redirect(Policy::none()).build()?;
            o.client = Some(client.clone());
            client
        }
    };
    // Redirects are pinned to this origin (SSRF defence).
    let initial_origin = origin_of(&o.url);
    let mut retry_count = 0;
    loop {
        pace(pacer, o.rate_limit.unwrap_or(0.01), signal).await?;
        let mut response = send(&client, &method, o, signal, body()).await?;

        let mut redirects = 0;
        while o.redirect != Some(Redirect::Manual) && is_redirect(&response) {
            let safe_url = redact_url(&o.url);
            if o.redirect == Some(Redirect::Error) {
                return Err(format!(
                    "fetch {method_name} {safe_url} redirected with redirect: error"
                )
                .into());
            }
            redirects += 1;
            if redirects > MAX_REDIRECTS {
                return Err(format!(
                    "fetch {method_name} {safe_url} exceeded {MAX_REDIRECTS} redirects"
                )
                .into());
            }
            let target = response
                .headers()
                .get(LOCATION)
                .and_then(|location| location.to_str().ok())
                .and_then(|location| Url::parse(&o.url).ok()?.join(location).ok());
            let Some(target) = target else {
                return Err(format!(
                    "fetch {method_name} {safe_url} returned an invalid redirect Location"
                )
                .into());
            };
            let target_origin = target.origin().ascii_serialization();
            if o.redirect.is_none() && Some(&target_origin) != initial_origin.as_ref() {
                return Err(format!(
                    "fetch {method_name} {safe_url} blocked cross-origin redirect ({target_origin} does not match {})",
                    initial_origin.as_deref().unwrap_or("undefined")
                )
                .into());
            }
            o.url = target.to_string();
            response = send(&client, &method, o, signal, body()).await?;
        }

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let safe_url = redact_url(&o.url);
        if status != StatusCode::TOO_MANY_REQUESTS {
            return Err(format!("fetch {} {method_name} {safe_url}", status.as_u16()).into());
        }
        retry_count += 1;
        let max = o.retry_max_count.unwrap_or(10);
        if retry_count >= max {
            return Err(
                format!("fetch 429 {method_name} {safe_url} max retries ({max}) exceeded").into(),
            );
        }
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
            .map(|v| {
                parse_int(v)
                    .filter(|s| *s != 0)
                    .map_or(1000.0, |s| s as f64 * 1000.0)
            });
        // Full jitter avoids retry storms; Retry-After is honoured exactly.
        let wait_ms = retry_after.unwrap_or_else(|| {
            random_unit() * (1000.0 * 2f64.powi(retry_count as i32 - 1)).min(30_000.0)
        });
        drop(response);
        timeout(Duration::from_millis(wait_ms as u64), signal).await?;
    }
}

/// One request (with defaults applied, rate limited, retried on 429, with
/// same-origin redirects followed). Errors on non-2xx responses.
pub async fn fetch_rate_limit(
    options: FetchOptions,
    signal: Option<CancellationToken>,
) -> Result<Response> {
    let mut o = with_defaults(options);
    let bytes = o.body.clone();
    request(&mut o, &Mutex::new(None), signal.as_ref(), &mut move || {
        bytes.clone().map(Body::from)
    })
    .await
}

fn fetch_item(
    options: FetchOptions,
    pacer: Pacer,
    signal: Option<CancellationToken>,
) -> DataStream<FetchChunk> {
    Box::pin(try_stream! {
        let mut o = with_defaults(options);
        if let Some(param) = &o.offset_param {
            o.qs.entry(param.clone()).or_insert(Value::from(0));
        }
        if !o.qs.is_empty() {
            let qs = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(o.qs.iter().map(|(k, v)| match v {
                    Value::String(s) => (k, s.clone()),
                    v => (k, v.to_string()),
                }))
                .finish();
            o.url = format!("{}?{}", o.url, qs.replace('+', "%20"));
        }
        let origin = origin_of(&o.url).ok_or_else(|| format!("Invalid URL: {}", o.url))?;
        let bytes = o.body.clone();
        let mut body = move || bytes.clone().map(Body::from);

        let response = request(&mut o, &pacer, signal.as_ref(), &mut body).await?;
        let content_type = response.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok());
        if !content_type.is_some_and(is_json) {
            let mut chunks = Box::pin(response.bytes_stream());
            while let Some(chunk) = chunks.next().await {
                yield FetchChunk::Bytes(chunk?.to_vec());
            }
        } else {
            let mut prefetched = Some(response);
            loop {
                let response = match prefetched.take() {
                    Some(response) => response,
                    None => request(&mut o, &pacer, signal.as_ref(), &mut body).await?,
                };
                let link = response.headers().get(LINK).and_then(|v| v.to_str().ok()).and_then(next_link);
                // Buffers the whole page: keep pages bounded for untrusted endpoints.
                let mut page: Value = serde_json::from_slice(&response.bytes().await?)?;
                let next = link
                    .or_else(|| match page.pointer(&pointer(o.next_path.as_deref()?)) {
                        Some(Value::String(next)) if !next.is_empty() => Some(next.clone()),
                        _ => None,
                    })
                    .or_else(|| paginate_using_query(&o));
                if let Some(next) = &next {
                    validate_pagination_url(next, &origin)?;
                }
                let mut empty_page = false;
                match page.pointer_mut(&pointer(o.data_path.as_deref().unwrap_or_default())).map(Value::take) {
                    Some(Value::Array(items)) => {
                        empty_page = items.is_empty();
                        for item in items {
                            yield FetchChunk::Json(item);
                        }
                    }
                    Some(data) => {
                        yield FetchChunk::Json(data);
                    }
                    None => {}
                }
                match next {
                    Some(next) if !(empty_page && o.offset_param.is_some()) => o.url = next,
                    _ => break,
                }
            }
        }
    })
}

/// Readable over one or more requests, in order.
pub fn fetch_readable_stream(
    fetch_options: Vec<FetchOptions>,
    stream_options: FetchStreamOptions,
) -> DataStream<FetchChunk> {
    let concurrency = stream_options.concurrency.unwrap_or(1).max(1);
    let signal = stream_options.signal;
    let pacer = Pacer::default();
    // Item streams are lazy, so building them all up front sends no requests yet.
    let items: Vec<DataStream<FetchChunk>> = fetch_options
        .into_iter()
        .map(|o| fetch_item(o, pacer.clone(), signal.clone()))
        .collect();
    if concurrency == 1 {
        return Box::pin(futures::stream::iter(items).flatten());
    }
    // Each item is collected so results come out in input order. Boxed futures
    // (not adapter closures) avoid a higher-ranked lifetime inference bug.
    let collected: Vec<BoxFuture<Result<Vec<FetchChunk>>>> = items
        .into_iter()
        .map(|item| Box::pin(item.try_collect()) as BoxFuture<_>)
        .collect();
    let mut results = futures::stream::iter(collected).buffered(concurrency);
    Box::pin(try_stream! {
        while let Some(chunks) = results.next().await {
            for chunk in chunks? {
                yield chunk;
            }
        }
    })
}
pub use fetch_readable_stream as fetch_response_stream;

/// Stream `input` as the request body and return the response.
pub async fn fetch_writable_stream(
    input: DataStream<Vec<u8>>,
    options: FetchOptions,
    signal: Option<CancellationToken>,
) -> Result<Response> {
    let mut o = with_defaults(options);
    // A streamed body can only be sent once; a redirect or retry resends none.
    let mut body = Some(Body::wrap_stream(input));
    request(&mut o, &Mutex::new(None), signal.as_ref(), &mut move || {
        body.take()
    })
    .await
}
pub use fetch_writable_stream as fetch_request_stream;

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, stream_to_array};
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn get(url: String) -> FetchOptions {
        FetchOptions {
            url,
            ..Default::default()
        }
    }

    async fn collect(
        options: Vec<FetchOptions>,
        stream_options: FetchStreamOptions,
    ) -> Result<Vec<FetchChunk>> {
        stream_to_array(fetch_readable_stream(options, stream_options), None).await
    }

    async fn json(options: FetchOptions) -> Result<Vec<Value>> {
        let chunks = collect(vec![options], FetchStreamOptions::default()).await?;
        Ok(chunks
            .into_iter()
            .map(|c| match c {
                FetchChunk::Json(v) => v,
                FetchChunk::Bytes(b) => panic!("unexpected bytes {b:?}"),
            })
            .collect())
    }

    async fn error(options: FetchOptions) -> String {
        json(options).await.unwrap_err().to_string()
    }

    fn ok_json(body: Value) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(body)
    }

    async fn mount(server: &MockServer, route: &str, response: ResponseTemplate) {
        Mock::given(path(route))
            .respond_with(response)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn non_json_body_streams_as_bytes() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/csv",
            ResponseTemplate::new(200).set_body_raw("a,b\n1,2\n", "text/csv"),
        )
        .await;
        mount(
            &server,
            "/jsonl",
            ResponseTemplate::new(200).set_body_raw("{}\n", "application/jsonl"),
        )
        .await;
        for (route, expected) in [("/csv", "a,b\n1,2\n"), ("/jsonl", "{}\n")] {
            let chunks = collect(
                vec![get(format!("{}{route}", server.uri()))],
                FetchStreamOptions::default(),
            )
            .await
            .unwrap();
            let bytes: Vec<u8> = chunks
                .into_iter()
                .flat_map(|c| match c {
                    FetchChunk::Bytes(b) => b,
                    FetchChunk::Json(_) => panic!("unexpected json"),
                })
                .collect();
            assert_eq!(bytes, expected.as_bytes());
        }
    }

    #[tokio::test]
    async fn json_with_qs_data_path_and_default_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data"))
            .and(query_param("a", "b c"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"{"items":{"list":[1,2]}}"#,
                "application/ld+json; charset=utf-8",
            ))
            .mount(&server)
            .await;
        let mut qs = Map::new();
        qs.insert("a".into(), json!("b c"));
        qs.insert("n".into(), json!(1));
        let options = FetchOptions {
            qs,
            data_path: Some("items.list".into()),
            ..get(format!("{}/data", server.uri()))
        };
        assert_eq!(json(options).await.unwrap(), vec![json!(1), json!(2)]);

        let request = &server.received_requests().await.unwrap()[0];
        assert_eq!(request.url.query(), Some("a=b%20c&n=1"));
        assert_eq!(request.headers.get("accept").unwrap(), "application/json");
        assert!(request
            .headers
            .get("accept-encoding")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("gzip"));
    }

    #[tokio::test]
    async fn whole_body_and_missing_data_path() {
        let server = MockServer::start().await;
        mount(&server, "/obj", ok_json(json!({"a": null}))).await;
        let url = format!("{}/obj", server.uri());
        assert_eq!(
            json(get(url.clone())).await.unwrap(),
            vec![json!({"a": null})]
        );
        let missing = FetchOptions {
            data_path: Some("a.b.c".into()),
            ..get(url)
        };
        assert!(json(missing).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn paginates_with_link_header() {
        let server = MockServer::start().await;
        Mock::given(path("/items"))
            .and(query_param("page", "2"))
            .respond_with(
                ok_json(json!([3])).insert_header("Link", r#"<http://x/first>; rel="first""#),
            )
            .mount(&server)
            .await;
        let link = format!(
            r#"<{}/first>; rel="prev", <{}/items?page=2>; rel="next""#,
            server.uri(),
            server.uri()
        );
        Mock::given(path("/items"))
            .and(query_param_is_missing("page"))
            .respond_with(ok_json(json!([1, 2])).insert_header("Link", link.as_str()))
            .mount(&server)
            .await;
        assert_eq!(
            json(get(format!("{}/items", server.uri()))).await.unwrap(),
            vec![json!(1), json!(2), json!(3)]
        );
    }

    #[tokio::test]
    async fn paginates_with_next_path() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/p1",
            ok_json(json!({"data": [1], "next": format!("{}/p2", server.uri())})),
        )
        .await;
        mount(&server, "/p2", ok_json(json!({"data": [2], "next": null}))).await;
        let options = FetchOptions {
            data_path: Some("data".into()),
            next_path: Some("next".into()),
            ..get(format!("{}/p1", server.uri()))
        };
        assert_eq!(json(options).await.unwrap(), vec![json!(1), json!(2)]);
    }

    #[tokio::test]
    async fn paginates_with_offset_query() {
        let server = MockServer::start().await;
        for (offset, body) in [("0", json!([1, 2])), ("2", json!([3])), ("4", json!([]))] {
            Mock::given(path("/rows"))
                .and(query_param("offset", offset))
                .respond_with(ok_json(body))
                .mount(&server)
                .await;
        }
        let options = FetchOptions {
            offset_param: Some("offset".into()),
            offset_amount: Some(2),
            ..get(format!("{}/rows", server.uri()))
        };
        assert_eq!(
            json(options).await.unwrap(),
            vec![json!(1), json!(2), json!(3)]
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 3);

        // Without offset_amount only the first page is fetched.
        let options = FetchOptions {
            offset_param: Some("offset".into()),
            ..get(format!("{}/rows", server.uri()))
        };
        assert_eq!(json(options).await.unwrap(), vec![json!(1), json!(2)]);
    }

    #[tokio::test]
    async fn rejects_unsafe_pagination_urls() {
        let server = MockServer::start().await;
        let other = MockServer::start().await;
        let link = format!(r#"<{}/steal>; rel="next""#, other.uri());
        mount(
            &server,
            "/cross",
            ok_json(json!([1])).insert_header("Link", link.as_str()),
        )
        .await;
        mount(
            &server,
            "/invalid",
            ok_json(json!([1])).insert_header("Link", r#"<not a url>; rel="next""#),
        )
        .await;
        mount(
            &server,
            "/nolink",
            ok_json(json!([1])).insert_header("Link", r#"<http://x/a>; rel="prev""#),
        )
        .await;
        assert_eq!(
            error(get(format!("{}/cross", server.uri()))).await,
            format!(
                "Pagination URL origin ({}) does not match initial URL origin ({})",
                other.uri(),
                server.uri()
            )
        );
        assert_eq!(
            error(get(format!("{}/invalid", server.uri()))).await,
            "Invalid pagination URL: not a url"
        );
        assert_eq!(
            json(get(format!("{}/nolink", server.uri()))).await.unwrap(),
            vec![json!(1)]
        );
    }

    #[tokio::test]
    async fn non_ok_response_errors_with_redacted_url() {
        let server = MockServer::start().await;
        mount(&server, "/missing", ResponseTemplate::new(404)).await;
        let mut qs = Map::new();
        qs.insert("token".into(), json!("secret"));
        let options = FetchOptions {
            qs,
            ..get(format!("{}/missing", server.uri()))
        };
        assert_eq!(
            error(options).await,
            format!("fetch 404 GET {}/missing?[REDACTED]", server.uri())
        );
    }

    #[tokio::test]
    async fn retries_429_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(path("/busy"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        mount(&server, "/busy", ok_json(json!([1]))).await;
        assert_eq!(
            json(get(format!("{}/busy", server.uri()))).await.unwrap(),
            vec![json!(1)]
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn retries_429_up_to_retry_max_count() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/busy",
            ResponseTemplate::new(429).insert_header("Retry-After", "0"),
        )
        .await;
        let options = FetchOptions {
            retry_max_count: Some(2),
            ..get(format!("{}/busy?k=v", server.uri()))
        };
        let start = Instant::now();
        assert_eq!(
            fetch_rate_limit(options, None)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "fetch 429 GET {}/busy?[REDACTED] max retries (2) exceeded",
                server.uri()
            )
        );
        // Retry-After: 0 falls back to 1000ms.
        assert!(start.elapsed() >= Duration::from_millis(1000));
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn redirects() {
        let server = MockServer::start().await;
        let other = MockServer::start().await;
        let redirect =
            |to: String| ResponseTemplate::new(302).insert_header("Location", to.as_str());
        mount(&other, "/ok", ok_json(json!(["other"]))).await;
        mount(&server, "/ok", ok_json(json!(["same"]))).await;
        mount(&server, "/same", redirect("/ok".into())).await;
        mount(&server, "/cross", redirect(format!("{}/ok", other.uri()))).await;
        mount(&server, "/loop", redirect("/loop".into())).await;
        mount(&server, "/invalid", redirect("http://[invalid".into())).await;
        mount(&server, "/nolocation", ResponseTemplate::new(302)).await;
        let url = |route: &str| format!("{}{route}", server.uri());

        assert_eq!(json(get(url("/same"))).await.unwrap(), vec![json!("same")]);
        assert_eq!(
            error(get(url("/cross"))).await,
            format!(
                "fetch GET {} blocked cross-origin redirect ({} does not match {})",
                url("/cross"),
                other.uri(),
                server.uri()
            )
        );
        let follow = FetchOptions {
            redirect: Some(Redirect::Follow),
            ..get(url("/cross"))
        };
        assert_eq!(json(follow).await.unwrap(), vec![json!("other")]);
        let manual = FetchOptions {
            redirect: Some(Redirect::Manual),
            ..get(url("/same"))
        };
        assert_eq!(
            error(manual).await,
            format!("fetch 302 GET {}", url("/same"))
        );
        let error_mode = FetchOptions {
            redirect: Some(Redirect::Error),
            ..get(url("/same"))
        };
        assert!(error(error_mode)
            .await
            .contains("redirected with redirect: error"));
        assert_eq!(
            error(get(url("/loop"))).await,
            format!("fetch GET {} exceeded 20 redirects", url("/loop"))
        );
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.url.path() == "/loop")
                .count(),
            21
        );
        assert_eq!(
            error(get(url("/invalid"))).await,
            format!(
                "fetch GET {} returned an invalid redirect Location",
                url("/invalid")
            )
        );
        assert_eq!(
            error(get(url("/nolocation"))).await,
            format!("fetch 302 GET {}", url("/nolocation"))
        );
    }

    #[tokio::test]
    async fn sequential_requests_are_rate_limited() {
        let server = MockServer::start().await;
        mount(&server, "/a", ok_json(json!(["a"]))).await;
        mount(&server, "/b", ok_json(json!(["b"]))).await;
        let options = |route: &str| FetchOptions {
            rate_limit: Some(0.2),
            ..get(format!("{}{route}", server.uri()))
        };
        let start = Instant::now();
        let chunks = collect(
            vec![options("/a"), options("/b")],
            FetchStreamOptions::default(),
        )
        .await
        .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(200));
        assert_eq!(
            chunks,
            vec![FetchChunk::Json(json!("a")), FetchChunk::Json(json!("b"))]
        );
    }

    #[tokio::test]
    async fn concurrency_preserves_order_and_surfaces_errors() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/slow",
            ok_json(json!(["slow"])).set_delay(Duration::from_millis(200)),
        )
        .await;
        mount(&server, "/fast", ok_json(json!(["fast"]))).await;
        mount(&server, "/fail", ResponseTemplate::new(500)).await;
        let url = |route: &str| get(format!("{}{route}", server.uri()));
        let stream_options = FetchStreamOptions {
            concurrency: Some(2),
            ..Default::default()
        };

        let start = Instant::now();
        let chunks = collect(
            vec![url("/slow"), url("/slow"), url("/fast")],
            stream_options.clone(),
        )
        .await
        .unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(390),
            "requests ran concurrently"
        );
        let values: Vec<_> = ["slow", "slow", "fast"]
            .map(|v| FetchChunk::Json(json!(v)))
            .into();
        assert_eq!(chunks, values);

        let err = collect(vec![url("/fast"), url("/fail")], stream_options)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("fetch 500 GET {}/fail", server.uri())
        );
    }

    #[tokio::test]
    async fn signal_aborts() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/slow",
            ok_json(json!([1])).set_delay(Duration::from_secs(5)),
        )
        .await;
        let signal = CancellationToken::new();
        let stream_options = FetchStreamOptions {
            signal: Some(signal.clone()),
            ..Default::default()
        };
        let pending = tokio::spawn(collect(
            vec![get(format!("{}/slow", server.uri()))],
            stream_options,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.cancel();
        assert_eq!(pending.await.unwrap().unwrap_err().to_string(), "Aborted");
    }

    #[tokio::test]
    async fn writable_streams_the_request_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;
        let input = create_readable_stream(vec![b"chunk1,".to_vec(), b"chunk2".to_vec()]);
        let options = FetchOptions {
            method: Some("POST".into()),
            ..get(format!("{}/upload", server.uri()))
        };
        let response = fetch_writable_stream(input, options, None).await.unwrap();
        assert_eq!(response.status(), 201);
        assert_eq!(
            server.received_requests().await.unwrap()[0].body,
            b"chunk1,chunk2"
        );
    }

    #[tokio::test]
    async fn set_defaults_merges_headers() {
        fetch_set_defaults(FetchOptions {
            headers: HashMap::from([("X-Datastream-Test".into(), "1".into())]),
            ..Default::default()
        });
        let server = MockServer::start().await;
        mount(&server, "/h", ok_json(json!([]))).await;
        fetch_rate_limit(get(format!("{}/h", server.uri())), None)
            .await
            .unwrap();
        let request = &server.received_requests().await.unwrap()[0];
        assert_eq!(request.headers.get("x-datastream-test").unwrap(), "1");
        assert_eq!(request.headers.get("accept").unwrap(), "application/json");
    }

    #[test]
    fn json_content_type_detection() {
        for ct in [
            "application/json",
            "application/json;charset=utf-8",
            "application/ld+json",
            "application/vnd.api+json;",
        ] {
            assert!(is_json(ct), "{ct}");
        }
        for ct in [
            "text/application-json",
            "application/jsonl",
            "application/+json",
            "text/json",
            "application/xjson",
        ] {
            assert!(!is_json(ct), "{ct}");
        }
    }

    #[test]
    fn link_header_and_url_helpers() {
        assert_eq!(
            next_link(r#"<http://a/1>; rel="prev", <http://a/2>; rel="next""#).as_deref(),
            Some("http://a/2")
        );
        assert_eq!(next_link(r#"<http://a/1>; rel="prev""#), None);
        assert_eq!(
            redact_url("http://user:pass@host/p?q=1#h"),
            "http://%5BREDACTED%5D:%5BREDACTED%5D@host/p?[REDACTED]#h"
        );
        assert_eq!(redact_url("http://host/p"), "http://host/p");
        assert_eq!(redact_url("not a url"), "[INVALID URL]");
        assert_eq!(parse_int(" 12abc"), Some(12));
        assert_eq!(parse_int("-3"), Some(-3));
        assert_eq!(parse_int("abc"), None);
        assert_eq!(pointer("a.b/c.0"), "/a/b~1c/0");
        assert_eq!(pointer(""), "");
        let r = random_unit();
        assert!((0.0..1.0).contains(&r));
    }
}
