// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! MSK IAM auth tokens for Kafka SASL/OAUTHBEARER, following
//! `aws-msk-iam-sasl-signer-js`: a SigV4-presigned `kafka-cluster:Connect`
//! URL, base64url-encoded.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
};
use aws_sigv4::sign::v4;
use aws_smithy_types::base64;
use datastream_core::Result;

const USER_AGENT: &str = concat!("datastream-aws/", env!("CARGO_PKG_VERSION"));

#[derive(Default, Clone, Debug)]
pub struct AwsMskIamOptions {
    pub region: Option<String>,
    /// Token lifetime in seconds (default 900).
    pub ttl: Option<u64>,
    /// Defaults to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
    /// `AWS_SESSION_TOKEN` from the environment.
    pub credentials_provider: Option<SharedCredentialsProvider>,
}

#[derive(Clone, Debug)]
pub struct AwsMskIamMechanism {
    /// Always `"oauthbearer"`.
    pub mechanism: &'static str,
    region: String,
    ttl: u64,
    credentials_provider: Option<SharedCredentialsProvider>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OAuthBearerToken {
    pub value: String,
    /// Expiry in milliseconds since the Unix epoch.
    pub expiry_time: u64,
}

pub fn aws_msk_iam_mechanism(options: AwsMskIamOptions) -> Result<AwsMskIamMechanism> {
    let Some(region) = options.region.filter(|region| !region.is_empty()) else {
        return Err("awsMskIamMechanism: region required".into());
    };
    Ok(AwsMskIamMechanism {
        mechanism: "oauthbearer",
        region,
        ttl: options.ttl.unwrap_or(900),
        credentials_provider: options.credentials_provider,
    })
}

impl AwsMskIamMechanism {
    /// Sign a fresh token.
    pub async fn oauth_bearer_provider(&self) -> Result<OAuthBearerToken> {
        let credentials = match &self.credentials_provider {
            Some(provider) => provider.provide_credentials().await?,
            None => env_credentials()?,
        };
        generate_auth_token(&self.region, &credentials, self.ttl, SystemTime::now())
    }
}

fn env_credentials() -> Result<Credentials> {
    let var = |name| std::env::var(name).ok().filter(|v: &String| !v.is_empty());
    match (var("AWS_ACCESS_KEY_ID"), var("AWS_SECRET_ACCESS_KEY")) {
        (Some(id), Some(secret)) => Ok(Credentials::new(
            id,
            secret,
            var("AWS_SESSION_TOKEN"),
            None,
            "environment",
        )),
        _ => Err("awsMskIamMechanism: credentials required".into()),
    }
}

fn generate_auth_token(
    region: &str,
    credentials: &Credentials,
    ttl: u64,
    time: SystemTime,
) -> Result<OAuthBearerToken> {
    let host = format!("kafka.{region}.amazonaws.com");
    let mut url = format!("https://{host}/?Action=kafka-cluster%3AConnect");
    let identity = credentials.clone().into();
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(Duration::from_secs(ttl));
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("kafka-cluster")
        .time(time)
        .settings(settings)
        .build()?
        .into();
    let request = SignableRequest::new(
        "GET",
        url.clone(),
        [("host", host.as_str())].into_iter(),
        SignableBody::Bytes(&[]),
    )?;
    let (instructions, _signature) = sign(request, &params)?.into_parts();
    for (name, value) in instructions.into_parts().1 {
        url.push_str(&format!("&{}={}", encode(name), encode(&value)));
    }
    url.push_str(&format!("&User-Agent={}", encode(USER_AGENT)));
    let value = base64::encode(&url)
        .replace('+', "-")
        .replace('/', "_")
        .trim_end_matches('=')
        .to_string();
    let signed_at = time.duration_since(UNIX_EPOCH)?.as_millis() as u64;
    Ok(OAuthBearerToken {
        value,
        expiry_time: signed_at + ttl * 1000,
    })
}

// RFC 3986 percent-encoding: everything but unreserved characters.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(session_token: Option<&str>) -> Credentials {
        Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token.map(String::from),
            None,
            "test",
        )
    }

    fn decode(token: &str) -> String {
        let mut padded = token.replace('-', "+").replace('_', "/");
        while !padded.len().is_multiple_of(4) {
            padded.push('=');
        }
        String::from_utf8(base64::decode(padded).unwrap()).unwrap()
    }

    #[test]
    fn mechanism_requires_region() {
        let e = aws_msk_iam_mechanism(AwsMskIamOptions::default()).unwrap_err();
        assert_eq!(e.to_string(), "awsMskIamMechanism: region required");
        let options = AwsMskIamOptions {
            region: Some("us-east-1".into()),
            ..Default::default()
        };
        assert_eq!(
            aws_msk_iam_mechanism(options).unwrap().mechanism,
            "oauthbearer"
        );
    }

    #[test]
    fn token_is_a_presigned_connect_url() {
        // 2026-01-01T00:00:00Z
        let time = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
        let token = generate_auth_token("us-east-1", &credentials(None), 900, time).unwrap();
        assert_eq!(token.expiry_time, 1_767_225_600_000 + 900_000);
        assert!(!token.value.contains(['+', '/', '=']));
        let url = decode(&token.value);
        assert!(
            url.starts_with(
                "https://kafka.us-east-1.amazonaws.com/?Action=kafka-cluster%3AConnect&"
            ),
            "{url}"
        );
        for part in [
            "X-Amz-Algorithm=AWS4-HMAC-SHA256",
            "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fkafka-cluster%2Faws4_request",
            "X-Amz-Date=20260101T000000Z",
            "X-Amz-Expires=900",
            "X-Amz-SignedHeaders=host",
            "X-Amz-Signature=",
            "User-Agent=datastream-aws%2F",
        ] {
            assert!(url.contains(part), "{part} missing from {url}");
        }
        assert!(!url.contains("X-Amz-Security-Token"));
        // Signing is deterministic for the same inputs.
        let again = generate_auth_token("us-east-1", &credentials(None), 900, time).unwrap();
        assert_eq!(again, token);
    }

    #[test]
    fn token_includes_session_token_and_ttl() {
        let time = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
        let token = generate_auth_token("eu-west-1", &credentials(Some("a/b")), 60, time).unwrap();
        let url = decode(&token.value);
        assert!(url.contains("X-Amz-Security-Token=a%2Fb"), "{url}");
        assert!(url.contains("X-Amz-Expires=60"), "{url}");
        assert!(url.contains("kafka.eu-west-1.amazonaws.com"), "{url}");
        assert_eq!(token.expiry_time, 1_767_225_600_000 + 60_000);
    }

    #[tokio::test]
    async fn provider_uses_given_credentials() {
        let options = AwsMskIamOptions {
            region: Some("us-east-1".into()),
            credentials_provider: Some(SharedCredentialsProvider::new(credentials(None))),
            ..Default::default()
        };
        let token = aws_msk_iam_mechanism(options)
            .unwrap()
            .oauth_bearer_provider()
            .await
            .unwrap();
        assert!(decode(&token.value).contains("X-Amz-Credential=AKIAIOSFODNN7EXAMPLE"));
        assert!(token.expiry_time > 0);
    }

    #[test]
    fn encode_keeps_only_unreserved() {
        assert_eq!(encode("aZ09-_.~ /:="), "aZ09-_.~%20%2F%3A%3D");
    }
}
