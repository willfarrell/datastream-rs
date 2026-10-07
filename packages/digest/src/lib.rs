// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Hashing pass-through stream, ported from `@datastream/digest`.

use std::sync::{Arc, Mutex};

use datastream_core::{create_pass_through_stream, DataStream, Result, StreamResult, Value};
use digest::DynDigest;

#[derive(Default, Clone, Debug)]
pub struct DigestOptions {
    /// `SHA2-256`, `SHA2-384`, `SHA2-512`, `SHA3-256`, `SHA3-384`, `SHA3-512`,
    /// or the aliases `SHA256`, `SHA384`, `SHA512`.
    pub algorithm: String,
    pub result_key: Option<String>,
}

fn hasher(algorithm: &str) -> Result<(String, Box<dyn DynDigest + Send>)> {
    let canonical = match algorithm {
        "SHA256" => "SHA2-256",
        "SHA384" => "SHA2-384",
        "SHA512" => "SHA2-512",
        other => other,
    };
    let hash: Box<dyn DynDigest + Send> = match canonical {
        "SHA2-256" => Box::new(sha2::Sha256::default()),
        "SHA2-384" => Box::new(sha2::Sha384::default()),
        "SHA2-512" => Box::new(sha2::Sha512::default()),
        "SHA3-256" => Box::new(sha3::Sha3_256::default()),
        "SHA3-384" => Box::new(sha3::Sha3_384::default()),
        "SHA3-512" => Box::new(sha3::Sha3_512::default()),
        _ => return Err(format!("Unsupported algorithm: {algorithm}").into()),
    };
    Ok((canonical.to_string(), hash))
}

/// Hash every chunk as it passes through. Once the stream ends the result
/// (default key `digest`) holds `"<ALGORITHM>:<hex>"`; before that it is null.
pub fn digest_stream<T>(
    input: DataStream<T>,
    options: DigestOptions,
) -> Result<(DataStream<T>, StreamResult)>
where
    T: AsRef<[u8]> + Send + 'static,
{
    let (canonical, hash) = hasher(&options.algorithm)?;
    let key = options.result_key.unwrap_or_else(|| "digest".into());
    let result = StreamResult::new(key, Value::Null);
    let output = result.clone();
    // Shared by the per-chunk and flush closures.
    let hash = Arc::new(Mutex::new(hash));
    let update = hash.clone();
    let stream = create_pass_through_stream(
        input,
        move |chunk: &T| {
            update
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .update(chunk.as_ref());
            Ok(())
        },
        move || {
            let checksum = hash
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .finalize_reset();
            output.set(Value::String(format!(
                "{canonical}:{}",
                hex::encode(checksum)
            )));
            Ok(())
        },
    );
    Ok((stream, result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline};
    use serde_json::json;

    const ALGORITHMS: [&str; 6] = [
        "SHA2-256", "SHA2-384", "SHA2-512", "SHA3-256", "SHA3-384", "SHA3-512",
    ];

    async fn run(chunks: &[&str], algorithm: &str) -> Value {
        let input =
            create_readable_stream(chunks.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let options = DigestOptions {
            algorithm: algorithm.into(),
            ..Default::default()
        };
        let (stream, result) = digest_stream(input, options).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(output["digest"], result.get());
        result.get()
    }

    #[tokio::test]
    async fn sha2_256_digest() {
        let expected =
            json!("SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25");
        assert_eq!(run(&["1,2,3,4"], "SHA2-256").await, expected);
        assert_eq!(run(&["1,", "2,", "3,", "4"], "SHA2-256").await, expected);
    }

    #[tokio::test]
    async fn aliases_normalize_to_canonical() {
        for (alias, canonical) in [
            ("SHA256", "SHA2-256"),
            ("SHA384", "SHA2-384"),
            ("SHA512", "SHA2-512"),
        ] {
            assert_eq!(run(&["test"], alias).await, run(&["test"], canonical).await);
        }
    }

    #[tokio::test]
    async fn all_algorithms() {
        for algorithm in ALGORITHMS {
            let value = run(&["The quick brown fox"], algorithm).await;
            assert!(value
                .as_str()
                .unwrap()
                .starts_with(&format!("{algorithm}:")));
        }
        // Known SHA3-256 of "" to pin the sha3 wiring.
        assert_eq!(
            run(&[], "SHA3-256").await,
            json!("SHA3-256:a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
        );
    }

    #[test]
    fn unsupported_algorithm() {
        let input = create_readable_stream(Vec::<Vec<u8>>::new());
        let options = DigestOptions {
            algorithm: "MD5".into(),
            ..Default::default()
        };
        let e = digest_stream(input, options).err().unwrap();
        assert_eq!(e.to_string(), "Unsupported algorithm: MD5");
    }

    #[tokio::test]
    async fn result_before_finish_and_key() {
        let input = create_readable_stream([b"test".to_vec()]);
        let options = DigestOptions {
            algorithm: "SHA256".into(),
            result_key: Some("checksum".into()),
        };
        let (stream, result) = digest_stream(input, options).unwrap();
        assert_eq!(result.get(), Value::Null);
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(result.key(), "checksum");
        assert!(output["checksum"]
            .as_str()
            .unwrap()
            .starts_with("SHA2-256:"));
    }
}
