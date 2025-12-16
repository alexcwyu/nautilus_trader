// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Provides the HTTP client integration for the Architect REST API.

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    num::NonZeroU32,
    sync::LazyLock,
};

use nautilus_core::consts::NAUTILUS_USER_AGENT;
use nautilus_network::{
    http::HttpClient,
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryManager},
};
use reqwest::{Method, header::USER_AGENT};
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;

use super::{
    error::ArchitectHttpError,
    models::{
        ArchitectBalancesResponse, ArchitectInstrument, ArchitectInstrumentsResponse,
        ArchitectPositionsResponse, ArchitectTicker, ArchitectTickersResponse, ArchitectWhoAmI,
    },
    query::{GetInstrumentParams, GetTickerParams},
};
use crate::common::{consts::ARCHITECT_HTTP_URL, credential::Credential};

/// Default Architect REST API rate limit.
///
/// Conservative default of 10 requests per second.
pub static ARCHITECT_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(10).expect("Should be a valid non-zero u32"))
});

const ARCHITECT_GLOBAL_RATE_KEY: &str = "architect:global";

/// Raw HTTP client for low-level Architect API operations.
///
/// This client handles request/response operations with the Architect API,
/// returning venue-specific response types. It does not parse to Nautilus domain types.
pub struct ArchitectRawHttpClient {
    base_url: String,
    client: HttpClient,
    credential: Option<Credential>,
    session_token: Option<String>,
    retry_manager: RetryManager<ArchitectHttpError>,
    cancellation_token: CancellationToken,
}

impl Default for ArchitectRawHttpClient {
    fn default() -> Self {
        Self::new(None, Some(60), None, None, None, None)
            .expect("Failed to create default ArchitectRawHttpClient")
    }
}

impl Debug for ArchitectRawHttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchitectRawHttpClient")
            .field("base_url", &self.base_url)
            .field("has_credentials", &self.credential.is_some())
            .field("has_session_token", &self.session_token.is_some())
            .finish()
    }
}

impl ArchitectRawHttpClient {
    /// Cancel all pending HTTP requests.
    pub fn cancel_all_requests(&self) {
        self.cancellation_token.cancel();
    }

    /// Get the cancellation token for this client.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Creates a new [`ArchitectRawHttpClient`] using the default Architect HTTP URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: Option<String>,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self, ArchitectHttpError> {
        let retry_config = RetryConfig {
            max_retries: max_retries.unwrap_or(3),
            initial_delay_ms: retry_delay_ms.unwrap_or(1000),
            max_delay_ms: retry_delay_max_ms.unwrap_or(10_000),
            backoff_factor: 2.0,
            jitter_ms: 1000,
            operation_timeout_ms: Some(60_000),
            immediate_first: false,
            max_elapsed_ms: Some(180_000),
        };

        let retry_manager = RetryManager::new(retry_config);

        Ok(Self {
            base_url: base_url.unwrap_or_else(|| ARCHITECT_HTTP_URL.to_string()),
            client: HttpClient::new(
                Self::default_headers(),
                vec![],
                Self::rate_limiter_quotas(),
                Some(*ARCHITECT_REST_QUOTA),
                timeout_secs,
                proxy_url,
            )
            .map_err(|e| {
                ArchitectHttpError::NetworkError(format!("Failed to create HTTP client: {e}"))
            })?,
            credential: None,
            session_token: None,
            retry_manager,
            cancellation_token: CancellationToken::new(),
        })
    }

    /// Creates a new [`ArchitectRawHttpClient`] configured with credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    #[allow(clippy::too_many_arguments)]
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        base_url: Option<String>,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self, ArchitectHttpError> {
        let retry_config = RetryConfig {
            max_retries: max_retries.unwrap_or(3),
            initial_delay_ms: retry_delay_ms.unwrap_or(1000),
            max_delay_ms: retry_delay_max_ms.unwrap_or(10_000),
            backoff_factor: 2.0,
            jitter_ms: 1000,
            operation_timeout_ms: Some(60_000),
            immediate_first: false,
            max_elapsed_ms: Some(180_000),
        };

        let retry_manager = RetryManager::new(retry_config);

        Ok(Self {
            base_url: base_url.unwrap_or_else(|| ARCHITECT_HTTP_URL.to_string()),
            client: HttpClient::new(
                Self::default_headers(),
                vec![],
                Self::rate_limiter_quotas(),
                Some(*ARCHITECT_REST_QUOTA),
                timeout_secs,
                proxy_url,
            )
            .map_err(|e| {
                ArchitectHttpError::NetworkError(format!("Failed to create HTTP client: {e}"))
            })?,
            credential: Some(Credential::new(api_key, api_secret)),
            session_token: None,
            retry_manager,
            cancellation_token: CancellationToken::new(),
        })
    }

    /// Sets the session token for authenticated requests.
    ///
    /// The session token is obtained through the login flow and used for bearer token authentication.
    pub fn set_session_token(&mut self, token: String) {
        self.session_token = Some(token);
    }

    fn default_headers() -> HashMap<String, String> {
        HashMap::from([(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string())])
    }

    fn rate_limiter_quotas() -> Vec<(String, Quota)> {
        vec![(ARCHITECT_GLOBAL_RATE_KEY.to_string(), *ARCHITECT_REST_QUOTA)]
    }

    fn rate_limit_keys(endpoint: &str) -> Vec<String> {
        let normalized = endpoint.split('?').next().unwrap_or(endpoint);
        let route = format!("architect:{normalized}");

        vec![ARCHITECT_GLOBAL_RATE_KEY.to_string(), route]
    }

    fn auth_headers(&self) -> Result<HashMap<String, String>, ArchitectHttpError> {
        let credential = self
            .credential
            .as_ref()
            .ok_or(ArchitectHttpError::MissingCredentials)?;

        let session_token = self.session_token.as_ref().ok_or_else(|| {
            ArchitectHttpError::ValidationError("Session token not set".to_string())
        })?;

        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            credential.bearer_token(session_token),
        );

        Ok(headers)
    }

    async fn send_request<T: DeserializeOwned, P: Serialize>(
        &self,
        method: Method,
        endpoint: &str,
        params: Option<&P>,
        _body: Option<Vec<u8>>,
        authenticate: bool,
    ) -> Result<T, ArchitectHttpError> {
        let endpoint = endpoint.to_string();
        let url = format!("{}{endpoint}", self.base_url);

        // Serialize params for GET requests
        let params_str = if method == Method::GET {
            params
                .map(serde_urlencoded::to_string)
                .transpose()
                .map_err(|e| {
                    ArchitectHttpError::JsonError(format!("Failed to serialize params: {e}"))
                })?
        } else {
            None
        };

        let operation = || {
            let url = url.clone();
            let method = method.clone();
            let endpoint = endpoint.clone();
            let params_str = params_str.clone();

            async move {
                let mut headers = Self::default_headers();

                if authenticate {
                    let auth_headers = self.auth_headers()?;
                    headers.extend(auth_headers);
                }

                let full_url = if let Some(ref query) = params_str {
                    if query.is_empty() {
                        url
                    } else {
                        format!("{url}?{query}")
                    }
                } else {
                    url
                };

                let rate_limit_keys = Self::rate_limit_keys(&endpoint);

                let response = self
                    .client
                    .request(
                        method,
                        full_url,
                        None,
                        Some(headers),
                        None, // body
                        None,
                        Some(rate_limit_keys),
                    )
                    .await?;

                let status = response.status;
                let body = String::from_utf8_lossy(&response.body).to_string();

                if !status.is_success() {
                    return Err(ArchitectHttpError::UnexpectedStatus {
                        status: status.as_u16(),
                        body,
                    });
                }

                serde_json::from_str(&body).map_err(|e| {
                    ArchitectHttpError::JsonError(format!(
                        "Failed to deserialize response: {e}\nBody: {body}"
                    ))
                })
            }
        };

        let should_retry = |_error: &ArchitectHttpError| -> bool {
            // For now, don't retry any errors
            // TODO: Implement proper retry logic based on error type
            false
        };

        let create_error = |msg: String| -> ArchitectHttpError {
            if msg == "canceled" {
                ArchitectHttpError::Canceled("Adapter disconnecting or shutting down".to_string())
            } else {
                ArchitectHttpError::NetworkError(msg)
            }
        };

        self.retry_manager
            .execute_with_retry_with_cancel(
                endpoint.as_str(),
                operation,
                should_retry,
                create_error,
                &self.cancellation_token,
            )
            .await
    }

    /// Fetches the current authenticated user information.
    ///
    /// # Endpoint
    /// `GET /whoami`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_whoami(&self) -> Result<ArchitectWhoAmI, ArchitectHttpError> {
        self.send_request::<ArchitectWhoAmI, ()>(Method::GET, "/whoami", None, None, true)
            .await
    }

    /// Fetches all available instruments.
    ///
    /// # Endpoint
    /// `GET /instruments`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_instruments(
        &self,
    ) -> Result<ArchitectInstrumentsResponse, ArchitectHttpError> {
        self.send_request::<ArchitectInstrumentsResponse, ()>(
            Method::GET,
            "/instruments",
            None,
            None,
            false,
        )
        .await
    }

    /// Fetches all account balances for the authenticated user.
    ///
    /// # Endpoint
    /// `GET /balances`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_balances(&self) -> Result<ArchitectBalancesResponse, ArchitectHttpError> {
        self.send_request::<ArchitectBalancesResponse, ()>(
            Method::GET,
            "/balances",
            None,
            None,
            true,
        )
        .await
    }

    /// Fetches all open positions for the authenticated user.
    ///
    /// # Endpoint
    /// `GET /positions`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_positions(&self) -> Result<ArchitectPositionsResponse, ArchitectHttpError> {
        self.send_request::<ArchitectPositionsResponse, ()>(
            Method::GET,
            "/positions",
            None,
            None,
            true,
        )
        .await
    }

    /// Fetches all tickers.
    ///
    /// # Endpoint
    /// `GET /tickers`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_tickers(&self) -> Result<ArchitectTickersResponse, ArchitectHttpError> {
        self.send_request::<ArchitectTickersResponse, ()>(Method::GET, "/tickers", None, None, true)
            .await
    }

    /// Fetches a single ticker by symbol.
    ///
    /// # Endpoint
    /// `GET /ticker?symbol=<symbol>`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_ticker(&self, symbol: &str) -> Result<ArchitectTicker, ArchitectHttpError> {
        let params = GetTickerParams::new(symbol);
        self.send_request::<ArchitectTicker, _>(Method::GET, "/ticker", Some(&params), None, true)
            .await
    }

    /// Fetches a single instrument by symbol.
    ///
    /// # Endpoint
    /// `GET /instrument?symbol=<symbol>`
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_instrument(
        &self,
        symbol: &str,
    ) -> Result<ArchitectInstrument, ArchitectHttpError> {
        let params = GetInstrumentParams::new(symbol);
        self.send_request::<ArchitectInstrument, _>(
            Method::GET,
            "/instrument",
            Some(&params),
            None,
            false,
        )
        .await
    }
}
