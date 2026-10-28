# datastream-fetch

HTTP readable and writable streams with JSON pagination, rate limiting, retries (429, plus 5xx and network errors for idempotent methods), per-attempt timeouts, page and body size limits, `http:`/`https:`-only URLs, and same-origin redirect pinning.

- `fetch_set_defaults`
- `fetch_readable_stream` (alias `fetch_response_stream`)
- `fetch_writable_stream` (alias `fetch_request_stream`)
- `fetch_rate_limit`
