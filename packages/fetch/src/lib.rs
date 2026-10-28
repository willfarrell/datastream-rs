// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! HTTP fetch streams, ported from `@datastream/fetch`.
//!
//! JSON responses (`application/json`, `application/*+json`) are parsed and
//! paginated (Link header, `next_path`, or offset query); anything else is
//! streamed as raw bytes. Requests are spaced by `rate_limit`; 429s, transient
//! 5xx and network errors are retried with backoff, and redirects are pinned
//! to the original origin.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use async_stream::try_stream;
use datastream_core::{
    timeout, BoxFuture, CancellationToken, DataStream, Error, Map, Result, StreamExt, TryStreamExt,
    Value,
};
use reqwest::header::{CONTENT_TYPE, LINK, LOCATION, RETRY_AFTER};
use reqwest::{redirect::Policy, Body, Client, Method, Response, StatusCode};
use url::Url;

const MAX_REDIRECTS: usize = 20;
const DEFAULT_MAX_PAGES: usize = 10_000;
const DEFAULT_MAX_BODY_SIZE: usize = 16_777_216;
const DEFAULT_RETRY_AFTER_MAX: Duration = Duration::from_secs(60);

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
    /// Must be `http:` or `https:`.
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
    /// JSON pages fetched per request config. Default 10,000; `usize::MAX`
    /// for unlimited.
    pub max_pages: Option<usize>,
    /// Bytes per JSON page body. Default 16 MiB; `usize::MAX` for unlimited.
    pub max_body_size: Option<usize>,
    /// Attempts on 429 / 5xx / network error before failing. Default 10.
    pub retry_max_count: Option<u32>,
    /// Upper bound for a `Retry-After` wait. Default 60s.
    pub retry_after_max: Option<Duration>,
    /// Per attempt, response body included. Default none.
    pub timeout: Option<Duration>,
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
        max_pages: o.max_pages.or(d.max_pages),
        max_body_size: o.max_body_size.or(d.max_body_size),
        retry_max_count: o.retry_max_count.or(d.retry_max_count),
        retry_after_max: o.retry_after_max.or(d.retry_after_max),
        timeout: o.timeout.or(d.timeout),
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

fn method_of(o: &FetchOptions) -> &str {
    o.method.as_deref().unwrap_or("GET")
}

/// reqwest accepts only http(s), but rejecting other schemes (file:, data:,
/// ...) up front gives a clear, redacted error before any request is made.
fn assert_http_url(o: &FetchOptions) -> Result<()> {
    match Url::parse(&o.url) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => Ok(()),
        _ => Err(format!(
            "fetch {} {} is not an http: or https: URL",
            method_of(o),
            redact_url(&o.url)
        )
        .into()),
    }
}

/// `^application/(.+\+)?json($|;)`, case-insensitive (media types are, per
/// RFC 9110 §8.3.1).
fn is_json(content_type: &str) -> bool {
    let content_type = content_type.to_ascii_lowercase();
    let Some(rest) = content_type.strip_prefix("application/") else {
        return false;
    };
    rest.match_indices("json").any(|(i, _)| {
        let after = &rest[i + 4..];
        (after.is_empty() || after.starts_with(';'))
            && (i == 0 || (i >= 2 && rest[..i].ends_with('+')))
    })
}

/// First `rel` value of a link's params: `;\s*rel\s*=\s*"?([^";,]*)`, so it
/// must start a parameter (not sit inside e.g. `title="rel=next"`).
fn rel_param(params: &str) -> Option<&str> {
    params.match_indices(';').find_map(|(i, _)| {
        let rest = params[i + 1..].trim_start();
        let rest = &rest[rest
            .get(..3)
            .filter(|r| r.eq_ignore_ascii_case("rel"))?
            .len()..];
        let rest = rest.trim_start().strip_prefix('=')?.trim_start();
        let rest = rest.strip_prefix('"').unwrap_or(rest);
        Some(&rest[..rest.find(['"', ';', ',']).unwrap_or(rest.len())])
    })
}

/// URL of the `rel="next"` entry in a Link header (RFC 8288): each link is
/// `<target>` followed by its params, up to the next `<`. rel may be quoted
/// or not, is case-insensitive and may hold several space-separated types.
fn next_link(link: &str) -> Option<String> {
    link.split('<').skip(1).find_map(|part| {
        let (target, params) = part.split_once('>')?;
        let params = params.split('>').next().unwrap_or_default();
        rel_param(params)?
            .to_lowercase()
            .split(' ')
            .any(|rel| rel == "next")
            .then(|| target.to_string())
    })
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
    let offset = (parse_int(&offset)? + amount).to_string();
    // Like URLSearchParams.set(): the first value is replaced in place and
    // any repeats are dropped, so the parameter order is kept.
    let mut replaced = false;
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter_map(|(k, v)| {
            if k != param {
                return Some((k.into_owned(), v.into_owned()));
            }
            (!std::mem::replace(&mut replaced, true)).then(|| (k.into_owned(), offset.clone()))
        })
        .collect();
    url.query_pairs_mut().clear().extend_pairs(pairs);
    Some(url.to_string())
}

/// Relative targets (valid per RFC 8288) resolve against the current page
/// url. An empty target ends pagination.
fn resolve_pagination_url(next: &str, current: &str, origin: &str) -> Result<Option<String>> {
    if next.is_empty() {
        return Ok(None);
    }
    // Unparseable even as a relative reference, so there is nothing to
    // redact-and-show.
    let url = Url::parse(current)
        .and_then(|current| current.join(next))
        .map_err(|_| "Invalid pagination URL")?;
    let next_origin = url.origin().ascii_serialization();
    if next_origin != origin {
        return Err(format!(
            "Pagination URL origin ({next_origin}) does not match initial URL origin ({origin})"
        )
        .into());
    }
    Ok(Some(url.to_string()))
}

/// RFC 9110 Retry-After: delay-seconds or an HTTP-date; `None` when neither.
/// Capped at `max`: a hostile or huge value would otherwise stall the stream.
fn retry_after(value: &str, max: Duration) -> Option<Duration> {
    let wait = if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        Duration::from_secs(value.parse().unwrap_or(u64::MAX))
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(SystemTime::now())
            .unwrap_or_default()
    };
    Some(wait.min(max))
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

/// The method and body of a request, as re-sent on a retry or redirect.
struct Outgoing {
    method: Method,
    /// A streamed body is consumed by the first request, so (unlike `body`
    /// bytes) it can't be re-sent on a retry or a body-preserving redirect.
    streamed: bool,
    stream: Option<Body>,
}

impl Outgoing {
    fn body(&mut self, o: &FetchOptions) -> Option<Body> {
        if self.streamed {
            self.stream.take()
        } else {
            o.body.clone().map(Body::from)
        }
    }

    /// RFC 9110 §9.2.2: repeating an idempotent method leaves the server as
    /// one request would.
    fn can_retry(&self, o: &FetchOptions) -> bool {
        !self.streamed
            && ["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]
                .iter()
                .any(|m| method_of(o).eq_ignore_ascii_case(m))
    }
}

/// A connection-level failure (DNS, refused, reset). A timeout or an abort is
/// never retried.
fn is_network_error(error: &Error) -> bool {
    error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(|e| e.is_request() && !e.is_timeout())
}

fn cannot_replay(status: StatusCode, method_name: &str, safe_url: &str) -> Error {
    format!(
        "fetch {} {method_name} {safe_url} cannot replay streamed request body",
        status.as_u16()
    )
    .into()
}

async fn send(
    client: &Client,
    o: &FetchOptions,
    out: &mut Outgoing,
    signal: Option<&CancellationToken>,
) -> Result<Response> {
    let mut request = client.request(out.method.clone(), o.url.as_str());
    for (key, value) in &o.headers {
        request = request.header(key, value);
    }
    if let Some(body) = out.body(o) {
        request = request.body(body);
    }
    // A fresh timeout per attempt, so a retry is not born already expired.
    if let Some(duration) = o.timeout {
        request = request.timeout(duration);
    }
    // reqwest errors embed the full url; only the redacted one is shown.
    let response = request.send();
    Ok(match signal {
        Some(signal) => tokio::select! {
            _ = signal.cancelled() => return Err("Aborted".into()),
            response = response => response.map_err(reqwest::Error::without_url)?,
        },
        None => response.await.map_err(reqwest::Error::without_url)?,
    })
}

fn is_redirect(response: &Response) -> bool {
    matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308)
        && response.headers().contains_key(LOCATION)
}

/// Sends the request, following (and origin-checking) redirects.
async fn follow(
    client: &Client,
    o: &mut FetchOptions,
    out: &mut Outgoing,
    signal: Option<&CancellationToken>,
    initial_origin: Option<&str>,
) -> Result<Response> {
    let mut response = send(client, o, out, signal).await?;
    let mut redirects = 0;
    while o.redirect != Some(Redirect::Manual) && is_redirect(&response) {
        let method_name = method_of(o).to_string();
        let safe_url = redact_url(&o.url);
        if o.redirect == Some(Redirect::Error) {
            return Err(
                format!("fetch {method_name} {safe_url} redirected with redirect: error").into(),
            );
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
        if o.redirect.is_none() && Some(target_origin.as_str()) != initial_origin {
            return Err(format!(
                "fetch {method_name} {safe_url} blocked cross-origin redirect ({target_origin} does not match {})",
                initial_origin.unwrap_or("undefined")
            )
            .into());
        }
        o.url = target.to_string();
        // Fetch spec: 303 (except HEAD), and 301/302 after a POST, re-request
        // with GET and no body; resending would submit the body twice. Set on
        // `o` too so a retry of the new url stays GET.
        let status = response.status();
        let upper = method_name.to_ascii_uppercase();
        if (status == StatusCode::SEE_OTHER && upper != "HEAD")
            || (matches!(status.as_u16(), 301 | 302) && upper == "POST")
        {
            o.method = Some("GET".into());
            o.body = None;
            *out = Outgoing {
                method: Method::GET,
                streamed: false,
                stream: None,
            };
        }
        // 307/308 (and 301/302 for non-POST) re-send the body.
        if out.streamed {
            return Err(cannot_replay(status, &method_name, &safe_url));
        }
        drop(response);
        response = send(client, o, out, signal).await?;
    }
    Ok(response)
}

async fn request(
    o: &mut FetchOptions,
    pacer: &Mutex<Option<Instant>>,
    signal: Option<&CancellationToken>,
    stream: Option<Body>,
) -> Result<Response> {
    assert_http_url(o)?;
    let mut out = Outgoing {
        method: Method::from_bytes(method_of(o).as_bytes())?,
        streamed: stream.is_some(),
        stream,
    };
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
    let max = o.retry_max_count.unwrap_or(10);
    let mut retry_count = 0;
    loop {
        pace(pacer, o.rate_limit.unwrap_or(0.01), signal).await?;
        let response = match follow(&client, o, &mut out, signal, initial_origin.as_deref()).await {
            Ok(response) => response,
            Err(error) => {
                if !is_network_error(&error) || !out.can_retry(o) {
                    return Err(error);
                }
                retry_count += 1;
                if retry_count >= max {
                    return Err(error);
                }
                backoff(o, retry_count, None, signal).await?;
                continue;
            }
        };

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let method_name = method_of(o);
        let safe_url = redact_url(&o.url);
        let code = status.as_u16();
        // 429 is retried for every method: the server refused the request
        // without processing it. Transient 5xx only when repeating is safe.
        let retryable = status == StatusCode::TOO_MANY_REQUESTS
            || (matches!(code, 500 | 502 | 503 | 504) && out.can_retry(o));
        if !retryable {
            return Err(format!("fetch {code} {method_name} {safe_url}").into());
        }
        retry_count += 1;
        if retry_count >= max {
            return Err(format!(
                "fetch {code} {method_name} {safe_url} max retries ({max}) exceeded"
            )
            .into());
        }
        if out.streamed {
            return Err(cannot_replay(status, method_name, &safe_url));
        }
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        drop(response);
        backoff(o, retry_count, retry_after.as_deref(), signal).await?;
    }
}

/// Waits `Retry-After` when given, else full jitter (avoids retry storms).
async fn backoff(
    o: &FetchOptions,
    retry_count: u32,
    retry_after_header: Option<&str>,
    signal: Option<&CancellationToken>,
) -> Result<()> {
    let max = o.retry_after_max.unwrap_or(DEFAULT_RETRY_AFTER_MAX);
    let wait = retry_after_header
        .and_then(|value| retry_after(value, max))
        .unwrap_or_else(|| {
            let exponent = i32::try_from(retry_count.saturating_sub(1)).unwrap_or(i32::MAX);
            let ceiling = (1000.0 * 2f64.powi(exponent)).min(30_000.0);
            Duration::from_secs_f64(random_unit() * ceiling / 1000.0)
        });
    timeout(wait, signal).await
}

/// One request (with defaults applied, rate limited, retried on 429, 5xx and
/// network errors, with same-origin redirects followed). Errors on non-2xx
/// responses.
pub async fn fetch_rate_limit(
    options: FetchOptions,
    signal: Option<CancellationToken>,
) -> Result<Response> {
    let mut o = with_defaults(options);
    request(&mut o, &Mutex::new(None), signal.as_ref(), None).await
}

/// Buffers one JSON page, capped at `max_body_size`. Bytes are counted as they
/// arrive: Content-Length can be absent, or wrong under compression.
async fn read_json_body(response: Response, o: &FetchOptions) -> Result<Value> {
    let max = o.max_body_size.unwrap_or(DEFAULT_MAX_BODY_SIZE);
    let too_large = || -> Error {
        format!(
            "fetch {} {} response body exceeds max_body_size ({max} bytes)",
            method_of(o),
            redact_url(&o.url)
        )
        .into()
    };
    if response
        .content_length()
        .is_some_and(|n| usize::try_from(n).map_or(true, |n| n > max))
    {
        return Err(too_large());
    }
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(reqwest::Error::without_url)?;
        if chunk.len() > max - body.len() {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    // A UTF-8 BOM is stripped, like response.json().
    let body = body.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(&body);
    Ok(serde_json::from_slice(body)?)
}

fn fetch_item(
    options: FetchOptions,
    pacer: Pacer,
    signal: Option<CancellationToken>,
) -> DataStream<FetchChunk> {
    Box::pin(try_stream! {
        let mut o = with_defaults(options);
        assert_http_url(&o)?;
        if let Some(param) = &o.offset_param {
            o.qs.entry(param.clone()).or_insert(Value::from(0));
        }
        let mut url = Url::parse(&o.url)?;
        if !o.qs.is_empty() {
            let qs = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(o.qs.iter().map(|(k, v)| match v {
                    Value::String(s) => (k, s.clone()),
                    v => (k, v.to_string()),
                }))
                .finish()
                .replace('+', "%20");
            // Appended to (rather than replacing) any query already on the url.
            let query = match url.query() {
                Some(query) if !query.is_empty() => format!("{query}&{qs}"),
                _ => qs,
            };
            url.set_query(Some(&query));
            o.url = url.to_string();
        }
        let origin = url.origin().ascii_serialization();

        let response = request(&mut o, &pacer, signal.as_ref(), None).await?;
        let content_type = response.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok());
        // No body (204, 205, HEAD): nothing to stream or parse, even with a JSON type.
        let no_body = matches!(response.status().as_u16(), 204 | 205)
            || method_of(&o).eq_ignore_ascii_case("HEAD");
        if no_body {
            // Nothing to yield.
        } else if !content_type.is_some_and(is_json) {
            let mut chunks = Box::pin(response.bytes_stream());
            while let Some(chunk) = chunks.next().await {
                yield FetchChunk::Bytes(chunk.map_err(reqwest::Error::without_url)?.to_vec());
            }
        } else {
            let max_pages = o.max_pages.unwrap_or(DEFAULT_MAX_PAGES);
            let mut pages = 0;
            let mut prefetched = Some(response);
            loop {
                // Bounds a server that paginates forever (e.g. a cycle of next links).
                pages += 1;
                if pages > max_pages {
                    Err::<(), Error>(format!(
                        "fetch {} {} exceeded max_pages ({max_pages})",
                        method_of(&o),
                        redact_url(&o.url)
                    ).into())?;
                }
                let response = match prefetched.take() {
                    Some(response) => response,
                    None => request(&mut o, &pacer, signal.as_ref(), None).await?,
                };
                let link = response.headers().get(LINK).and_then(|v| v.to_str().ok()).and_then(next_link);
                // Buffers the whole page (unlike the byte branch), so it is
                // capped at max_body_size.
                let mut page = read_json_body(response, &o).await?;
                let next = link
                    .or_else(|| match page.pointer(&pointer(o.next_path.as_deref()?)) {
                        Some(Value::String(next)) if !next.is_empty() => Some(next.clone()),
                        _ => None,
                    })
                    .or_else(|| paginate_using_query(&o));
                let next = match next {
                    Some(next) => resolve_pagination_url(&next, &o.url, &origin)?,
                    None => None,
                };
                // A page linking to itself would be re-fetched forever.
                if let Some(next) = &next {
                    if Url::parse(&o.url).is_ok_and(|current| current.as_str() == next) {
                        Err::<(), Error>(format!(
                            "fetch {} {} pagination next URL repeats the current page",
                            method_of(&o),
                            redact_url(next)
                        ).into())?;
                    }
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
    // The first error, or the consumer dropping the stream, drops `results`
    // and with it every in-flight sibling request.
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

/// Stream `input` as the request body and return the response. The body is
/// pulled as the server reads it (backpressure). A streamed body can only be
/// sent once, so a 5xx or network error is not retried, and a 429 or a
/// body-preserving redirect (307/308) fails instead of re-sending it.
pub async fn fetch_writable_stream(
    input: DataStream<Vec<u8>>,
    options: FetchOptions,
    signal: Option<CancellationToken>,
) -> Result<Response> {
    let mut o = with_defaults(options);
    request(
        &mut o,
        &Mutex::new(None),
        signal.as_ref(),
        Some(Body::wrap_stream(input)),
    )
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
            ok_json(json!([1])).insert_header("Link", r#"<http://[invalid>; rel="next""#),
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
            "Invalid pagination URL"
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
        // Retry-After: 0 is honoured exactly (no jitter wait).
        assert!(start.elapsed() < Duration::from_millis(500));
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
        // Not retried (a GET 5xx would be).
        mount(&server, "/fail", ResponseTemplate::new(404)).await;
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
            format!("fetch 404 GET {}/fail", server.uri())
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
            "Application/JSON; charset=utf-8",
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
        // Unquoted, case-insensitive, one of several types, relative.
        assert_eq!(next_link("</2>; REL=next").as_deref(), Some("/2"));
        assert_eq!(
            next_link(r#"<http://a/2>;rel = "prefetch Next""#).as_deref(),
            Some("http://a/2")
        );
        // rel must start a parameter, and only the first rel counts.
        assert_eq!(
            next_link(r#"<http://a/2>; title="x;rel=prev"; rel="next""#),
            None
        );
        assert_eq!(next_link(r#"<http://a/2>; title="rel=next""#), None);
        assert_eq!(next_link("<http://a/2>"), None);
        assert_eq!(next_link("http://a/2; rel=next"), None);
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

    fn post(url: String, body: &[u8]) -> FetchOptions {
        FetchOptions {
            method: Some("POST".into()),
            body: Some(body.to_vec()),
            ..get(url)
        }
    }

    async fn requests_to(server: &MockServer, route: &str) -> Vec<wiremock::Request> {
        server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.url.path() == route)
            .collect()
    }

    /// Accepts connections and closes them unanswered (a network error),
    /// counting the attempts.
    async fn closing_server() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = count.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(socket);
            }
        });
        (url, count)
    }

    #[tokio::test]
    async fn query_pagination_keeps_the_offset_parameter_in_place() {
        let server = MockServer::start().await;
        Mock::given(path("/offset-first"))
            .and(query_param("$offset", "0"))
            .respond_with(ok_json(json!({"data": [1, 2]})))
            .mount(&server)
            .await;
        mount(&server, "/offset-first", ok_json(json!({"data": []}))).await;
        let mut qs = Map::new();
        qs.insert("$offset".into(), json!(0));
        qs.insert("$limit".into(), json!(2));
        let options = FetchOptions {
            qs,
            offset_param: Some("$offset".into()),
            offset_amount: Some(2),
            data_path: Some("data".into()),
            rate_limit: Some(0.0),
            ..get(format!("{}/offset-first", server.uri()))
        };
        assert_eq!(json(options).await.unwrap(), vec![json!(1), json!(2)]);
        let queries: Vec<_> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.query().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            queries,
            ["%24offset=0&%24limit=2", "%24offset=2&%24limit=2"]
        );
    }

    #[tokio::test]
    async fn qs_is_appended_to_an_existing_query() {
        let server = MockServer::start().await;
        mount(&server, "/q", ok_json(json!([]))).await;
        let mut qs = Map::new();
        qs.insert("b".into(), json!("2"));
        let options = FetchOptions {
            qs,
            ..get(format!("{}/q?a=1", server.uri()))
        };
        json(options).await.unwrap();
        let request = &server.received_requests().await.unwrap()[0];
        assert_eq!(request.url.query(), Some("a=1&b=2"));
    }

    #[tokio::test]
    async fn retries_500_502_503_and_504_for_idempotent_methods() {
        let server = MockServer::start().await;
        for status in [500, 502, 503, 504] {
            for method_name in ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "get"] {
                server.reset().await;
                Mock::given(path("/flaky"))
                    .respond_with(ResponseTemplate::new(status).insert_header("Retry-After", "0"))
                    .up_to_n_times(1)
                    .mount(&server)
                    .await;
                mount(&server, "/flaky", ok_json(json!([]))).await;
                let options = FetchOptions {
                    method: Some(method_name.into()),
                    ..get(format!("{}/flaky", server.uri()))
                };
                let response = fetch_rate_limit(options, None).await.unwrap();
                assert_eq!(response.status(), 200, "{status} {method_name}");
                assert_eq!(
                    server.received_requests().await.unwrap().len(),
                    2,
                    "{status} {method_name}"
                );
            }
        }
    }

    #[tokio::test]
    async fn does_not_retry_5xx_for_non_idempotent_methods_or_other_statuses() {
        let server = MockServer::start().await;
        for (status, method_name) in [(503, "POST"), (500, "PATCH"), (501, "GET"), (505, "GET")] {
            server.reset().await;
            mount(&server, "/once", ResponseTemplate::new(status)).await;
            let options = FetchOptions {
                method: Some(method_name.into()),
                ..get(format!("{}/once", server.uri()))
            };
            assert_eq!(
                fetch_rate_limit(options, None)
                    .await
                    .unwrap_err()
                    .to_string(),
                format!("fetch {status} {method_name} {}/once", server.uri())
            );
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn persistent_503_fails_after_retry_max_count() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/down",
            ResponseTemplate::new(503).insert_header("Retry-After", "0"),
        )
        .await;
        let options = FetchOptions {
            retry_max_count: Some(3),
            ..get(format!("{}/down?token=secret", server.uri()))
        };
        assert_eq!(
            fetch_rate_limit(options, None)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "fetch 503 GET {}/down?[REDACTED] max retries (3) exceeded",
                server.uri()
            )
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn retry_after_is_capped_by_retry_after_max() {
        let server = MockServer::start().await;
        Mock::given(path("/busy"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "3600"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        mount(&server, "/busy", ok_json(json!([]))).await;
        let options = FetchOptions {
            retry_after_max: Some(Duration::from_millis(50)),
            ..get(format!("{}/busy", server.uri()))
        };
        let start = Instant::now();
        fetch_rate_limit(options, None).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn retry_after_parses_seconds_and_http_dates() {
        let max = Duration::from_secs(60);
        assert_eq!(retry_after("7", max), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("0", max), Some(Duration::ZERO));
        assert_eq!(retry_after("99999999999999999999999", max), Some(max));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:00 GMT", max),
            Some(Duration::ZERO)
        );
        let soon = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(30));
        let wait = retry_after(&soon, max).unwrap();
        assert!(wait > Duration::from_secs(25) && wait <= Duration::from_secs(30));
        assert_eq!(retry_after("-1", max), None);
        assert_eq!(retry_after("1.5", max), None);
        assert_eq!(retry_after("", max), None);
    }

    #[tokio::test]
    async fn retries_a_network_error_for_idempotent_methods_only() {
        let (url, attempts) = closing_server().await;
        let options = FetchOptions {
            retry_max_count: Some(2),
            ..get(format!("{url}/net?token=secret"))
        };
        let error = fetch_rate_limit(options, None).await.unwrap_err();
        // The network error itself is rethrown, without the unredacted url.
        assert!(error.downcast_ref::<reqwest::Error>().is_some());
        assert!(!error.to_string().contains("secret"), "{error}");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

        let (url, attempts) = closing_server().await;
        fetch_rate_limit(post(format!("{url}/net"), b"x"), None)
            .await
            .unwrap_err();
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timeout_fails_the_attempt_without_retrying() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/slow",
            ok_json(json!([1])).set_delay(Duration::from_secs(5)),
        )
        .await;
        let options = FetchOptions {
            timeout: Some(Duration::from_millis(50)),
            ..get(format!("{}/slow", server.uri()))
        };
        let start = Instant::now();
        let error = fetch_rate_limit(options.clone(), None).await.unwrap_err();
        assert!(error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_timeout));
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        // Passed through to each request of a readable.
        let error = collect(vec![options], FetchStreamOptions::default())
            .await
            .unwrap_err();
        assert!(error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_timeout));
    }

    #[tokio::test]
    async fn rejects_non_http_urls_before_fetching() {
        for (url, shown) in [
            ("file:///etc/passwd", "file:///etc/passwd"),
            ("data:text/plain,hi", "data:text/plain,hi"),
            ("javascript:alert(1)", "javascript:alert(1)"),
            (
                "ftp://example.org/x?token=secret",
                "ftp://example.org/x?[REDACTED]",
            ),
            ("not a url", "[INVALID URL]"),
            ("/relative/path", "[INVALID URL]"),
        ] {
            assert_eq!(
                fetch_rate_limit(get(url.into()), None)
                    .await
                    .unwrap_err()
                    .to_string(),
                format!("fetch GET {shown} is not an http: or https: URL")
            );
        }
        assert_eq!(
            error(get("file:///etc/passwd".into())).await,
            "fetch GET file:///etc/passwd is not an http: or https: URL"
        );
        let input = create_readable_stream(vec![b"a".to_vec()]);
        let options = FetchOptions {
            method: Some("PUT".into()),
            ..get("data:text/plain,x".into())
        };
        assert_eq!(
            fetch_writable_stream(input, options, None)
                .await
                .unwrap_err()
                .to_string(),
            "fetch PUT data:text/plain,x is not an http: or https: URL"
        );
        // Both allowed schemes, in any case.
        let server = MockServer::start().await;
        mount(&server, "/ok", ok_json(json!([]))).await;
        let upper = server.uri().replacen("http://", "HTTP://", 1);
        fetch_rate_limit(get(format!("{upper}/ok")), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pagination_limits_and_relative_links() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/file",
            ok_json(json!({"items": [1], "next": "file:///etc/passwd"})),
        )
        .await;
        mount(
            &server,
            "/self",
            ok_json(json!([1])).insert_header("Link", r#"</self>; rel="next""#),
        )
        .await;
        mount(
            &server,
            "/relative",
            ok_json(json!([1])).insert_header("Link", r#"<last>; rel="next""#),
        )
        .await;
        mount(&server, "/last", ok_json(json!([2]))).await;
        mount(&server, "/forever", ok_json(json!([1]))).await;
        let url = |route: &str| format!("{}{route}", server.uri());

        // An origin includes the scheme, so a next link can't switch to file:.
        let file = FetchOptions {
            next_path: Some("next".into()),
            data_path: Some("items".into()),
            ..get(url("/file"))
        };
        assert_eq!(
            error(file).await,
            format!(
                "Pagination URL origin (null) does not match initial URL origin ({})",
                server.uri()
            )
        );
        assert_eq!(requests_to(&server, "/file").await.len(), 1);
        assert_eq!(
            error(get(url("/self"))).await,
            format!(
                "fetch GET {} pagination next URL repeats the current page",
                url("/self")
            )
        );
        assert_eq!(
            json(get(url("/relative"))).await.unwrap(),
            vec![json!(1), json!(2)]
        );
        let forever = FetchOptions {
            offset_param: Some("offset".into()),
            offset_amount: Some(1),
            max_pages: Some(3),
            rate_limit: Some(0.0),
            ..get(url("/forever"))
        };
        assert_eq!(
            error(forever).await,
            format!(
                "fetch GET {}?[REDACTED] exceeded max_pages (3)",
                url("/forever")
            )
        );
        assert_eq!(requests_to(&server, "/forever").await.len(), 3);
    }

    #[tokio::test]
    async fn json_body_is_capped_at_max_body_size() {
        let server = MockServer::start().await;
        mount(&server, "/big", ok_json(json!(["0123456789"]))).await;
        mount(
            &server,
            "/bom",
            ResponseTemplate::new(200)
                .set_body_raw(b"\xEF\xBB\xBF[1]".to_vec(), "application/json"),
        )
        .await;
        let options = FetchOptions {
            max_body_size: Some(10),
            ..get(format!("{}/big", server.uri()))
        };
        assert_eq!(
            error(options).await,
            format!(
                "fetch GET {}/big response body exceeds max_body_size (10 bytes)",
                server.uri()
            )
        );
        let options = FetchOptions {
            max_body_size: Some(14),
            ..get(format!("{}/big", server.uri()))
        };
        assert_eq!(json(options).await.unwrap(), vec![json!("0123456789")]);
        assert_eq!(
            json(get(format!("{}/bom", server.uri()))).await.unwrap(),
            vec![json!(1)]
        );
    }

    #[tokio::test]
    async fn no_body_responses_yield_nothing() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/empty",
            ResponseTemplate::new(204).insert_header("Content-Type", "application/json"),
        )
        .await;
        Mock::given(method("HEAD"))
            .and(path("/head"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("Content-Type", "application/json"),
            )
            .mount(&server)
            .await;
        assert!(json(get(format!("{}/empty", server.uri())))
            .await
            .unwrap()
            .is_empty());
        let head = FetchOptions {
            method: Some("HEAD".into()),
            ..get(format!("{}/head", server.uri()))
        };
        assert!(json(head).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn redirects_switch_to_get_without_body_per_fetch_spec() {
        let server = MockServer::start().await;
        for (route, expected_method, expected_body) in [
            ("/303", "GET", &b""[..]),
            ("/301", "GET", b""),
            ("/307", "POST", b"payload"),
        ] {
            server.reset().await;
            for (route, status) in [("/303", 303), ("/301", 301), ("/307", 307)] {
                mount(
                    &server,
                    route,
                    ResponseTemplate::new(status).insert_header("Location", "/target"),
                )
                .await;
            }
            mount(&server, "/target", ResponseTemplate::new(200)).await;
            fetch_rate_limit(post(format!("{}{route}", server.uri()), b"payload"), None)
                .await
                .unwrap();
            let target = &requests_to(&server, "/target").await[0];
            assert_eq!(target.method.as_str(), expected_method, "{route}");
            assert_eq!(target.body, expected_body, "{route}");
        }
    }

    #[tokio::test]
    async fn streamed_body_is_never_replayed() {
        let server = MockServer::start().await;
        let upload = |route: &str, method_name: &str| {
            (
                create_readable_stream(vec![b"a".to_vec()]),
                FetchOptions {
                    method: Some(method_name.into()),
                    ..get(format!("{}{route}", server.uri()))
                },
            )
        };
        mount(
            &server,
            "/busy",
            ResponseTemplate::new(429).insert_header("Retry-After", "0"),
        )
        .await;
        mount(&server, "/down", ResponseTemplate::new(503)).await;
        mount(
            &server,
            "/moved",
            ResponseTemplate::new(307).insert_header("Location", "/target"),
        )
        .await;
        mount(
            &server,
            "/other",
            ResponseTemplate::new(303).insert_header("Location", "/target"),
        )
        .await;
        mount(&server, "/target", ResponseTemplate::new(200)).await;
        let url = |route: &str| format!("{}{route}", server.uri());

        let (input, options) = upload("/busy", "PUT");
        assert_eq!(
            fetch_writable_stream(input, options, None)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "fetch 429 PUT {} cannot replay streamed request body",
                url("/busy")
            )
        );
        let (input, options) = upload("/down", "PUT");
        assert_eq!(
            fetch_writable_stream(input, options, None)
                .await
                .unwrap_err()
                .to_string(),
            format!("fetch 503 PUT {}", url("/down"))
        );
        let (input, options) = upload("/moved", "POST");
        assert_eq!(
            fetch_writable_stream(input, options, None)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "fetch 307 POST {} cannot replay streamed request body",
                url("/moved")
            )
        );
        assert_eq!(requests_to(&server, "/busy").await.len(), 1);
        assert_eq!(requests_to(&server, "/down").await.len(), 1);
        assert!(requests_to(&server, "/target").await.is_empty());
        // A 303 re-requests with GET and no body, so it can be followed.
        let (input, options) = upload("/other", "POST");
        let response = fetch_writable_stream(input, options, None).await.unwrap();
        assert_eq!(response.status(), 200);

        // A bytes body is re-sent on a 429 and a 307.
        server.reset().await;
        Mock::given(path("/again"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        mount(
            &server,
            "/again",
            ResponseTemplate::new(307).insert_header("Location", "/target"),
        )
        .await;
        mount(&server, "/target", ResponseTemplate::new(200)).await;
        fetch_rate_limit(post(url("/again"), b"payload"), None)
            .await
            .unwrap();
        let bodies: Vec<_> = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.body)
            .collect();
        assert_eq!(bodies, [b"payload"; 3]);
    }

    #[tokio::test]
    async fn concurrency_drops_in_flight_siblings() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/slow",
            ok_json(json!(["slow"])).set_delay(Duration::from_secs(5)),
        )
        .await;
        mount(&server, "/fast", ok_json(json!(["fast"]))).await;
        mount(&server, "/fail", ResponseTemplate::new(404)).await;
        let url = |route: &str| get(format!("{}{route}", server.uri()));
        let stream_options = FetchStreamOptions {
            concurrency: Some(2),
            ..Default::default()
        };
        // The first error ends the stream without waiting on the slow sibling.
        let start = Instant::now();
        collect(vec![url("/fail"), url("/slow")], stream_options.clone())
            .await
            .unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(2));
        // As does the consumer stopping early.
        let start = Instant::now();
        let mut stream = fetch_readable_stream(vec![url("/fast"), url("/slow")], stream_options);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            FetchChunk::Json(json!("fast"))
        );
        drop(stream);
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
