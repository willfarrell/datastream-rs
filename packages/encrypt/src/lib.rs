// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Symmetric encryption/decryption streams, ported from `@datastream/encrypt`.
//!
//! The wire format matches the JS package: the stream carries the raw
//! ciphertext, while the IV and (for AEAD modes) the 16-byte auth tag are
//! reported separately in the [`StreamResult`]. Data encrypted by either
//! implementation decrypts with the other.
//!
//! AEAD nonces: the default IV is a fresh random 96-bit value, which is safe
//! for a bounded number of messages per key (~2^32). Never reuse an explicit
//! IV with the same key.

use std::str::FromStr;

use aes::{Aes128, Aes256};
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes128Gcm, Aes256Gcm};
use async_stream::try_stream;
use chacha20poly1305::ChaCha20Poly1305;
use ctr::cipher::{KeyIvInit, StreamCipher};
use datastream_core::{DataStream, Error, Result, StreamExt, StreamResult};
use serde_json::json;

pub const DEFAULT_MAX_INPUT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Algorithm {
    Aes128Gcm,
    #[default]
    Aes256Gcm,
    Aes128Ctr,
    Aes256Ctr,
    Chacha20Poly1305,
}

impl Algorithm {
    pub fn name(self) -> &'static str {
        match self {
            Self::Aes128Gcm => "AES-128-GCM",
            Self::Aes256Gcm => "AES-256-GCM",
            Self::Aes128Ctr => "AES-128-CTR",
            Self::Aes256Ctr => "AES-256-CTR",
            Self::Chacha20Poly1305 => "CHACHA20-POLY1305",
        }
    }
    fn key_size(self) -> usize {
        match self {
            Self::Aes128Gcm | Self::Aes128Ctr => 16,
            _ => 32,
        }
    }
    fn is_aead(self) -> bool {
        !matches!(self, Self::Aes128Ctr | Self::Aes256Ctr)
    }
    fn iv_size(self) -> usize {
        if self.is_aead() {
            12
        } else {
            16
        }
    }
}

impl FromStr for Algorithm {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        [
            Self::Aes128Gcm,
            Self::Aes256Gcm,
            Self::Aes128Ctr,
            Self::Aes256Ctr,
            Self::Chacha20Poly1305,
        ]
        .into_iter()
        .find(|a| a.name() == s)
        .ok_or_else(|| format!("Unsupported algorithm: {s}").into())
    }
}

/// Options for [`encrypt_stream`]. `iv` defaults to random bytes.
#[derive(Default, Clone)]
pub struct EncryptOptions {
    pub algorithm: Algorithm,
    pub key: Vec<u8>,
    pub iv: Option<Vec<u8>>,
    pub aad: Option<Vec<u8>>,
    /// Defaults to 64MB for AEAD modes; CTR is unlimited unless set.
    pub max_input_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Options for [`decrypt_stream`]. `auth_tag` is required for AEAD modes.
#[derive(Default, Clone)]
pub struct DecryptOptions {
    pub algorithm: Algorithm,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
    pub auth_tag: Option<Vec<u8>>,
    pub aad: Option<Vec<u8>>,
    /// AEAD only (the whole ciphertext is buffered). Defaults to 64MB.
    pub max_input_size: Option<usize>,
    pub max_output_size: Option<usize>,
}

fn validate(algorithm: Algorithm, key: &[u8], iv: &[u8], aad: &Option<Vec<u8>>) -> Result<()> {
    let key_size = algorithm.key_size();
    if key.len() != key_size {
        return Err(format!(
            "Encryption key must be {key_size} bytes ({} bits), got {}",
            key_size * 8,
            key.len()
        )
        .into());
    }
    if iv.len() != algorithm.iv_size() {
        return Err(format!(
            "IV for {} must be {} bytes, got {}",
            algorithm.name(),
            algorithm.iv_size(),
            iv.len()
        )
        .into());
    }
    // AAD only has meaning for authenticated modes; silently dropping it for
    // CTR would give a false sense of integrity binding.
    if aad.is_some() && !algorithm.is_aead() {
        return Err(format!(
            "aad is not supported for {} (not authenticated)",
            algorithm.name()
        )
        .into());
    }
    Ok(())
}

fn random_bytes(len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; len];
    getrandom::getrandom(&mut buf).map_err(|e| e.to_string())?;
    Ok(buf)
}

fn seal<C: KeyInit + AeadInPlace>(
    key: &[u8],
    iv: &[u8],
    aad: &[u8],
    buf: &mut [u8],
) -> Result<Vec<u8>> {
    let cipher = C::new_from_slice(key).map_err(|e| e.to_string())?;
    let tag = cipher
        .encrypt_in_place_detached(GenericArray::from_slice(iv), aad, buf)
        .map_err(|e| e.to_string())?;
    Ok(tag.to_vec())
}

fn open<C: KeyInit + AeadInPlace>(
    key: &[u8],
    iv: &[u8],
    aad: &[u8],
    tag: &[u8],
    buf: &mut [u8],
) -> Result<()> {
    let cipher = C::new_from_slice(key).map_err(|e| e.to_string())?;
    cipher
        .decrypt_in_place_detached(
            GenericArray::from_slice(iv),
            aad,
            buf,
            GenericArray::from_slice(tag),
        )
        .map_err(|_| "Unsupported state or unable to authenticate data".into())
}

// ponytail: one short-lived cipher per stream, boxing buys nothing.
#[allow(clippy::large_enum_variant)]
enum Ctr {
    A128(ctr::Ctr128BE<Aes128>),
    A256(ctr::Ctr128BE<Aes256>),
}

impl Ctr {
    // Full 128-bit big-endian counter, matching OpenSSL's aes-*-ctr.
    fn new(algorithm: Algorithm, key: &[u8], iv: &[u8]) -> Result<Self> {
        Ok(match algorithm {
            Algorithm::Aes128Ctr => {
                Self::A128(KeyIvInit::new_from_slices(key, iv).map_err(|e| e.to_string())?)
            }
            _ => Self::A256(KeyIvInit::new_from_slices(key, iv).map_err(|e| e.to_string())?),
        })
    }
    fn apply(&mut self, buf: &mut [u8]) {
        match self {
            Self::A128(c) => c.apply_keystream(buf),
            Self::A256(c) => c.apply_keystream(buf),
        }
    }
}

/// Encrypt a byte (or string) stream. The result (default key `encrypt`) is
/// `{algorithm, iv, authTag?}` with byte arrays as JSON number arrays;
/// `authTag` is set once the stream ends. AEAD modes buffer the input and emit
/// the ciphertext at the end; CTR streams chunk by chunk.
pub fn encrypt_stream<T>(
    mut input: DataStream<T>,
    options: EncryptOptions,
) -> Result<(DataStream<Vec<u8>>, StreamResult)>
where
    T: AsRef<[u8]> + Send + 'static,
{
    let EncryptOptions {
        algorithm,
        key,
        iv,
        aad,
        max_input_size,
        result_key,
    } = options;
    let iv = match iv {
        Some(iv) => iv,
        None => random_bytes(algorithm.iv_size())?,
    };
    validate(algorithm, &key, &iv, &aad)?;
    let aead = algorithm.is_aead();
    let max = match max_input_size {
        None if !aead => None,
        max => Some(max.unwrap_or(DEFAULT_MAX_INPUT_SIZE)),
    };
    let mut ctr = if aead {
        None
    } else {
        Some(Ctr::new(algorithm, &key, &iv)?)
    };
    let result = StreamResult::new(
        result_key.unwrap_or_else(|| "encrypt".into()),
        json!({ "algorithm": algorithm.name(), "iv": iv }),
    );
    let output = result.clone();
    let stream = Box::pin(try_stream! {
        let mut size = 0usize;
        let mut buf = Vec::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let bytes = chunk.as_ref();
            size = size.saturating_add(bytes.len());
            if let Some(max) = max {
                if size > max {
                    Err::<(), Error>(format!(
                        "Encryption input exceeds maxInputSize ({max} bytes). Use AES-256-CTR for large data."
                    ).into())?;
                }
            }
            match ctr.as_mut() {
                Some(ctr) => {
                    let mut out = bytes.to_vec();
                    ctr.apply(&mut out);
                    yield out;
                }
                None => buf.extend_from_slice(bytes),
            }
        }
        if aead {
            let aad = aad.as_deref().unwrap_or_default();
            let tag = match algorithm {
                Algorithm::Aes128Gcm => seal::<Aes128Gcm>(&key, &iv, aad, &mut buf)?,
                Algorithm::Aes256Gcm => seal::<Aes256Gcm>(&key, &iv, aad, &mut buf)?,
                _ => seal::<ChaCha20Poly1305>(&key, &iv, aad, &mut buf)?,
            };
            output.update(|v| v["authTag"] = json!(tag));
            if !buf.is_empty() {
                yield buf;
            }
        }
    });
    Ok((stream, result))
}

/// Decrypt a ciphertext stream. AEAD modes buffer the whole ciphertext and
/// release plaintext only after the auth tag verifies; CTR streams.
pub fn decrypt_stream<T>(
    mut input: DataStream<T>,
    options: DecryptOptions,
) -> Result<DataStream<Vec<u8>>>
where
    T: AsRef<[u8]> + Send + 'static,
{
    let DecryptOptions {
        algorithm,
        key,
        iv,
        auth_tag,
        aad,
        max_input_size,
        max_output_size,
    } = options;
    validate(algorithm, &key, &iv, &aad)?;
    let max_output = max_output_size.unwrap_or(usize::MAX);
    let output_error = move || -> Error {
        format!("Decryption output exceeds maxOutputSize ({max_output} bytes)").into()
    };

    if !algorithm.is_aead() {
        let mut ctr = Ctr::new(algorithm, &key, &iv)?;
        return Ok(Box::pin(try_stream! {
            let mut size = 0usize;
            while let Some(chunk) = input.next().await {
                let mut out = chunk?.as_ref().to_vec();
                size = size.saturating_add(out.len());
                if size > max_output {
                    Err::<(), Error>(output_error())?;
                }
                ctr.apply(&mut out);
                yield out;
            }
        }));
    }

    let tag = auth_tag.unwrap_or_default();
    if tag.len() != 16 {
        return Err(format!(
            "authTag for {} must be 16 bytes, got {}",
            algorithm.name(),
            tag.len()
        )
        .into());
    }
    let max_input = max_input_size.unwrap_or(DEFAULT_MAX_INPUT_SIZE);
    Ok(Box::pin(try_stream! {
        let mut buf = Vec::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            // Bound memory before buffering: the tag is verified only at the end.
            if buf.len().saturating_add(chunk.as_ref().len()) > max_input {
                Err::<(), Error>(format!("Decryption input exceeds maxInputSize ({max_input} bytes)").into())?;
            }
            buf.extend_from_slice(chunk.as_ref());
        }
        let aad = aad.as_deref().unwrap_or_default();
        match algorithm {
            Algorithm::Aes128Gcm => open::<Aes128Gcm>(&key, &iv, aad, &tag, &mut buf)?,
            Algorithm::Aes256Gcm => open::<Aes256Gcm>(&key, &iv, aad, &tag, &mut buf)?,
            _ => open::<ChaCha20Poly1305>(&key, &iv, aad, &tag, &mut buf)?,
        }
        if buf.len() > max_output {
            Err::<(), Error>(output_error())?;
        }
        if !buf.is_empty() {
            yield buf;
        }
    }))
}

/// Random key of `bits` (128 or 256, default 256).
pub fn generate_encryption_key(bits: Option<usize>) -> Result<Vec<u8>> {
    match bits.unwrap_or(256) {
        bits @ (128 | 256) => random_bytes(bits / 8),
        bits => Err(format!("Unsupported key size: {bits}. Must be 128 or 256.").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array, stream_to_buffer};

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn bytes(v: &serde_json::Value) -> Vec<u8> {
        serde_json::from_value(v.clone()).unwrap()
    }

    fn chunks(data: &[u8], size: usize) -> DataStream<Vec<u8>> {
        create_readable_stream(data.chunks(size).map(<[u8]>::to_vec).collect::<Vec<_>>())
    }

    async fn encrypt(
        data: &[u8],
        size: usize,
        options: EncryptOptions,
    ) -> Result<(Vec<u8>, serde_json::Value)> {
        let (stream, result) = encrypt_stream(chunks(data, size), options)?;
        let out = stream_to_buffer(stream, None).await?;
        Ok((out, result.get()))
    }

    async fn decrypt(data: &[u8], size: usize, options: DecryptOptions) -> Result<Vec<u8>> {
        stream_to_buffer(decrypt_stream(chunks(data, size), options)?, None).await
    }

    fn err_msg<T>(r: Result<T>) -> String {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => e.to_string(),
        }
    }

    const PLAINTEXT: &[u8] = b"hello, datastream!";

    #[tokio::test]
    async fn matches_node_crypto_vectors() {
        // Vectors produced by node:crypto (the JS node build), proving wire compatibility.
        let aad = Some(b"meta".to_vec());
        let vectors = vec![
            (
                Algorithm::Aes256Gcm,
                vec![7; 32],
                vec![9; 12],
                aad.clone(),
                "4fe0e8f8d1dce105c116aa4b9298c6beb991",
                "a6d2281dcd709304a49b16e94f4c1f65",
            ),
            (
                Algorithm::Aes128Gcm,
                vec![7; 16],
                vec![9; 12],
                None,
                "cacb063d36f9eccf6b153973bd3dd1115404",
                "ae7b1502cab55183bce4d2003b42b027",
            ),
            (
                Algorithm::Chacha20Poly1305,
                vec![7; 32],
                vec![9; 12],
                aad,
                "941c97bb8b22a5c1bc1c6f4bb495c1b0132f",
                "7dd782f635ec5919b2b138776fce1246",
            ),
            (
                Algorithm::Aes256Ctr,
                vec![7; 32],
                vec![9; 16],
                None,
                "6fdc944e9a1efc3bbf14ae8407b3f6b1d231",
                "",
            ),
            (
                Algorithm::Aes128Ctr,
                vec![7; 16],
                vec![9; 16],
                None,
                "f92a7eecc6890f0f274d300ef8bfb7dd2a17",
                "",
            ),
        ];
        for (algorithm, key, iv, aad, ct, tag) in vectors {
            let options = EncryptOptions {
                algorithm,
                key: key.clone(),
                iv: Some(iv.clone()),
                aad: aad.clone(),
                ..Default::default()
            };
            let (out, value) = encrypt(PLAINTEXT, 5, options).await.unwrap();
            assert_eq!(out, hex(ct), "{}", algorithm.name());
            assert_eq!(value["algorithm"], algorithm.name());
            assert_eq!(bytes(&value["iv"]), iv);
            if tag.is_empty() {
                assert!(value.get("authTag").is_none());
            } else {
                assert_eq!(bytes(&value["authTag"]), hex(tag));
            }
            let auth_tag = (!tag.is_empty()).then(|| hex(tag));
            let options = DecryptOptions {
                algorithm,
                key,
                iv,
                auth_tag,
                aad,
                ..Default::default()
            };
            assert_eq!(decrypt(&hex(ct), 7, options).await.unwrap(), PLAINTEXT);
        }
    }

    #[tokio::test]
    async fn ctr_counter_wraps_and_is_chunking_independent() {
        let expected = hex("b9f67b439310f1a14dd70a7a46208c72490eadc4a42fa3973f2d5f9c842e63e01d98123a15a274bd57d48d83ea364f3a");
        let options = EncryptOptions {
            algorithm: Algorithm::Aes256Ctr,
            key: vec![7; 32],
            iv: Some(vec![0xff; 16]),
            ..Default::default()
        };
        for size in [48, 16, 5] {
            let (out, _) = encrypt(&[0x43; 48], size, options.clone()).await.unwrap();
            assert_eq!(out, expected);
        }
        let options = DecryptOptions {
            algorithm: Algorithm::Aes256Ctr,
            key: vec![7; 32],
            iv: vec![0xff; 16],
            ..Default::default()
        };
        assert_eq!(
            decrypt(&expected, 7, options).await.unwrap(),
            vec![0x43; 48]
        );
    }

    #[tokio::test]
    async fn roundtrip_with_random_iv_and_string_input() {
        let key = generate_encryption_key(None).unwrap();
        for algorithm in [
            Algorithm::Aes256Gcm,
            Algorithm::Aes256Ctr,
            Algorithm::Chacha20Poly1305,
        ] {
            let input = create_readable_stream(vec!["secret ".to_string(), "data".to_string()]);
            let (stream, result) = encrypt_stream(
                input,
                EncryptOptions {
                    algorithm,
                    key: key.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            let ct = stream_to_buffer(stream, None).await.unwrap();
            let value = result.get();
            let iv = bytes(&value["iv"]);
            assert_eq!(iv.len(), algorithm.iv_size());
            let auth_tag = value.get("authTag").map(bytes);
            let options = DecryptOptions {
                algorithm,
                key: key.clone(),
                iv,
                auth_tag,
                ..Default::default()
            };
            assert_eq!(decrypt(&ct, 3, options).await.unwrap(), b"secret data");
        }
    }

    #[tokio::test]
    async fn result_via_pipeline_and_result_key() {
        let key = vec![1; 32];
        let (stream, result) = encrypt_stream(
            create_readable_stream(vec![b"x".to_vec()]),
            EncryptOptions {
                key: key.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        let out = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(out["encrypt"]["algorithm"], "AES-256-GCM");
        assert_eq!(bytes(&out["encrypt"]["authTag"]).len(), 16);

        let options = EncryptOptions {
            key,
            result_key: Some("enc".into()),
            ..Default::default()
        };
        let (_, result) =
            encrypt_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options).unwrap();
        assert_eq!(result.key(), "enc");
    }

    #[tokio::test]
    async fn empty_input_roundtrips() {
        let key = vec![1; 32];
        let (stream, result) = encrypt_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            EncryptOptions {
                key: key.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        let value = result.get();
        let options = DecryptOptions {
            key,
            iv: bytes(&value["iv"]),
            auth_tag: Some(bytes(&value["authTag"])),
            ..Default::default()
        };
        assert!(decrypt(&[], 1, options).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn aead_rejects_wrong_key_aad_and_tamper_without_releasing_plaintext() {
        for algorithm in [Algorithm::Aes256Gcm, Algorithm::Chacha20Poly1305] {
            let key = vec![1; 32];
            let input = vec![b'a'; 64 * 1024];
            let options = EncryptOptions {
                algorithm,
                key: key.clone(),
                aad: Some(b"meta".to_vec()),
                ..Default::default()
            };
            let (mut ct, value) = encrypt(&input, 4096, options).await.unwrap();
            let base = DecryptOptions {
                algorithm,
                key,
                iv: bytes(&value["iv"]),
                auth_tag: Some(bytes(&value["authTag"])),
                aad: Some(b"meta".to_vec()),
                ..Default::default()
            };
            let msg = "Unsupported state or unable to authenticate data";
            let wrong_key = DecryptOptions {
                key: vec![2; 32],
                ..base.clone()
            };
            assert_eq!(err_msg(decrypt(&ct, 4096, wrong_key).await), msg);
            let wrong_aad = DecryptOptions {
                aad: Some(b"other".to_vec()),
                ..base.clone()
            };
            assert_eq!(err_msg(decrypt(&ct, 4096, wrong_aad).await), msg);

            *ct.last_mut().unwrap() ^= 0xff;
            let mut stream = decrypt_stream(chunks(&ct, 4096), base).unwrap();
            let first = stream.next().await.unwrap();
            assert_eq!(err_msg(first), msg);
        }
    }

    #[tokio::test]
    async fn encrypt_max_input_size_boundaries() {
        for algorithm in [
            Algorithm::Aes256Gcm,
            Algorithm::Aes256Ctr,
            Algorithm::Chacha20Poly1305,
        ] {
            let options = EncryptOptions {
                algorithm,
                key: vec![1; 32],
                max_input_size: Some(10),
                ..Default::default()
            };
            assert!(encrypt(&[0; 10], 3, options.clone()).await.is_ok());
            assert_eq!(
                err_msg(encrypt(&[0; 11], 3, options).await),
                "Encryption input exceeds maxInputSize (10 bytes). Use AES-256-CTR for large data."
            );
        }
    }

    #[tokio::test]
    async fn decrypt_size_limits() {
        let key = vec![1; 32];
        let (ct, value) = encrypt(
            &[5; 100],
            10,
            EncryptOptions {
                key: key.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let base = DecryptOptions {
            key: key.clone(),
            iv: bytes(&value["iv"]),
            auth_tag: Some(bytes(&value["authTag"])),
            ..Default::default()
        };
        assert!(decrypt(
            &ct,
            10,
            DecryptOptions {
                max_input_size: Some(100),
                max_output_size: Some(100),
                ..base.clone()
            }
        )
        .await
        .is_ok());
        assert_eq!(
            err_msg(
                decrypt(
                    &ct,
                    10,
                    DecryptOptions {
                        max_input_size: Some(99),
                        ..base.clone()
                    }
                )
                .await
            ),
            "Decryption input exceeds maxInputSize (99 bytes)"
        );
        assert_eq!(
            err_msg(
                decrypt(
                    &ct,
                    10,
                    DecryptOptions {
                        max_output_size: Some(99),
                        ..base
                    }
                )
                .await
            ),
            "Decryption output exceeds maxOutputSize (99 bytes)"
        );

        // CTR accumulates output across chunks and stops at the limit.
        let options = EncryptOptions {
            algorithm: Algorithm::Aes256Ctr,
            key: key.clone(),
            ..Default::default()
        };
        let (ct, value) = encrypt(&[5; 100], 10, options).await.unwrap();
        let base = DecryptOptions {
            algorithm: Algorithm::Aes256Ctr,
            key,
            iv: bytes(&value["iv"]),
            ..Default::default()
        };
        assert_eq!(
            decrypt(
                &ct,
                30,
                DecryptOptions {
                    max_output_size: Some(100),
                    ..base.clone()
                }
            )
            .await
            .unwrap(),
            vec![5; 100]
        );
        let mut stream = decrypt_stream(
            chunks(&ct, 30),
            DecryptOptions {
                max_output_size: Some(50),
                ..base
            },
        )
        .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), vec![5; 30]);
        assert_eq!(
            err_msg(stream.next().await.unwrap()),
            "Decryption output exceeds maxOutputSize (50 bytes)"
        );
    }

    #[test]
    fn validation_errors() {
        let enc = |options: EncryptOptions| err_msg(encrypt_stream(chunks(&[], 1), options));
        let dec = |options: DecryptOptions| err_msg(decrypt_stream(chunks(&[], 1), options));
        assert_eq!(
            enc(EncryptOptions::default()),
            "Encryption key must be 32 bytes (256 bits), got 0"
        );
        assert_eq!(
            enc(EncryptOptions {
                algorithm: Algorithm::Aes128Gcm,
                key: vec![0; 32],
                ..Default::default()
            }),
            "Encryption key must be 16 bytes (128 bits), got 32"
        );
        assert_eq!(
            enc(EncryptOptions {
                key: vec![0; 32],
                iv: Some(vec![0; 16]),
                ..Default::default()
            }),
            "IV for AES-256-GCM must be 12 bytes, got 16"
        );
        assert_eq!(
            enc(EncryptOptions {
                algorithm: Algorithm::Aes256Ctr,
                key: vec![0; 32],
                aad: Some(vec![1]),
                ..Default::default()
            }),
            "aad is not supported for AES-256-CTR (not authenticated)"
        );
        assert_eq!(
            dec(DecryptOptions {
                key: vec![0; 32],
                ..Default::default()
            }),
            "IV for AES-256-GCM must be 12 bytes, got 0"
        );
        assert_eq!(
            dec(DecryptOptions {
                key: vec![0; 32],
                iv: vec![0; 12],
                ..Default::default()
            }),
            "authTag for AES-256-GCM must be 16 bytes, got 0"
        );
        assert_eq!(
            dec(DecryptOptions {
                algorithm: Algorithm::Chacha20Poly1305,
                key: vec![0; 32],
                iv: vec![0; 12],
                auth_tag: Some(vec![0; 15]),
                ..Default::default()
            }),
            "authTag for CHACHA20-POLY1305 must be 16 bytes, got 15"
        );
        assert!(decrypt_stream(
            chunks(&[], 1),
            DecryptOptions {
                algorithm: Algorithm::Aes128Ctr,
                key: vec![0; 16],
                iv: vec![0; 16],
                ..Default::default()
            }
        )
        .is_ok());
    }

    #[test]
    fn algorithm_names_parse() {
        assert_eq!(
            "CHACHA20-POLY1305".parse::<Algorithm>().unwrap(),
            Algorithm::Chacha20Poly1305
        );
        assert_eq!(
            "AES-128-CTR".parse::<Algorithm>().unwrap(),
            Algorithm::Aes128Ctr
        );
        assert_eq!(
            err_msg("AES-512-GCM".parse::<Algorithm>()),
            "Unsupported algorithm: AES-512-GCM"
        );
    }

    #[test]
    fn generate_encryption_key_sizes() {
        assert_eq!(generate_encryption_key(None).unwrap().len(), 32);
        assert_eq!(generate_encryption_key(Some(128)).unwrap().len(), 16);
        assert_ne!(
            generate_encryption_key(None).unwrap(),
            generate_encryption_key(None).unwrap()
        );
        assert_eq!(
            err_msg(generate_encryption_key(Some(192))),
            "Unsupported key size: 192. Must be 128 or 256."
        );
    }
}
