// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Entra ID tokens for the Event Hubs Kafka endpoint (SASL/OAUTHBEARER), the
//! Azure counterpart to `datastream-aws`'s MSK IAM mechanism.

use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use datastream_core::Result;

#[derive(Clone, Debug)]
pub struct AzureEventHubsKafkaMechanism {
    /// Always `"oauthbearer"`.
    pub mechanism: &'static str,
    scope: String,
    credential: Arc<dyn TokenCredential>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OAuthBearerToken {
    pub value: String,
    /// Expiry in milliseconds since the Unix epoch.
    pub expiry_time: u64,
}

/// `namespace` is the fully qualified host, e.g. `myns.servicebus.windows.net`
/// (the Kafka bootstrap server without `:9093`).
pub fn azure_event_hubs_kafka_mechanism(
    namespace: &str,
    credential: Arc<dyn TokenCredential>,
) -> Result<AzureEventHubsKafkaMechanism> {
    let host = namespace.trim_end_matches(":9093");
    if host.is_empty() {
        return Err("azureEventHubsKafkaMechanism: namespace required".into());
    }
    Ok(AzureEventHubsKafkaMechanism {
        mechanism: "oauthbearer",
        scope: format!("https://{host}/.default"),
        credential,
    })
}

impl AzureEventHubsKafkaMechanism {
    /// Fetch a token (the credential caches and refreshes it).
    pub async fn oauth_bearer_provider(&self) -> Result<OAuthBearerToken> {
        let token = self.credential.get_token(&[&self.scope], None).await?;
        let expiry_ms = token.expires_on.unix_timestamp_nanos() / 1_000_000;
        Ok(OAuthBearerToken {
            value: token.token.secret().to_string(),
            expiry_time: u64::try_from(expiry_ms)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::credentials::{AccessToken, TokenRequestOptions};
    use azure_core::time::OffsetDateTime;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct StaticCredential {
        scopes: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl TokenCredential for StaticCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.scopes
                .lock()
                .unwrap()
                .extend(scopes.iter().map(|s| s.to_string()));
            Ok(AccessToken::new(
                "token",
                OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            ))
        }
    }

    #[tokio::test]
    async fn provider_requests_namespace_scope() {
        let credential = Arc::new(StaticCredential::default());
        let mechanism = azure_event_hubs_kafka_mechanism(
            "myns.servicebus.windows.net:9093",
            credential.clone(),
        )
        .unwrap();
        assert_eq!(mechanism.mechanism, "oauthbearer");
        let token = mechanism.oauth_bearer_provider().await.unwrap();
        assert_eq!(
            token,
            OAuthBearerToken {
                value: "token".into(),
                expiry_time: 1_700_000_000_000,
            }
        );
        assert_eq!(
            *credential.scopes.lock().unwrap(),
            ["https://myns.servicebus.windows.net/.default"]
        );
    }

    #[test]
    fn namespace_required() {
        let e = azure_event_hubs_kafka_mechanism("", Arc::new(StaticCredential::default()))
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "azureEventHubsKafkaMechanism: namespace required"
        );
    }
}
