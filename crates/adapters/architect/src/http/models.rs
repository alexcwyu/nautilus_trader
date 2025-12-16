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

//! Data transfer objects for deserializing Architect HTTP API payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::common::enums::ArchitectInstrumentState;

/// Default instrument state when not provided by API.
fn default_instrument_state() -> ArchitectInstrumentState {
    ArchitectInstrumentState::Open
}

/// Response payload returned by `GET /whoami`.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/user-management/whoami>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectWhoAmI {
    /// User account UUID.
    pub id: String,
    /// Username for the account.
    pub username: String,
    /// Account creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Whether two-factor authentication is enabled.
    pub enabled_2fa: bool,
    /// Whether the user has completed onboarding.
    pub is_onboarded: bool,
    /// Whether the account is frozen.
    pub is_frozen: bool,
    /// Whether the user has admin privileges.
    pub is_admin: bool,
    /// Whether the account is in close-only mode.
    pub is_close_only: bool,
    /// Maker fee rate as string.
    pub maker_fee: String,
    /// Taker fee rate as string.
    pub taker_fee: String,
}

/// Individual instrument definition.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/symbols-instruments/get-instruments>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectInstrument {
    /// Trading symbol for the instrument.
    pub symbol: Ustr,
    /// Current trading state of the instrument (defaults to Open if not provided).
    #[serde(default = "default_instrument_state")]
    pub state: ArchitectInstrumentState,
    /// Contract multiplier.
    pub multiplier: String,
    /// Minimum order size.
    pub minimum_order_size: String,
    /// Price tick size.
    pub tick_size: String,
    /// Quote currency symbol.
    pub quote_currency: String,
    // TODO: Rename to `funding_settlement_currency` once fixed
    /// Funding settlement currency.
    #[serde(alias = "funding_settlement_currency")]
    pub finding_settlement_currency: String,
    /// Maintenance margin percentage.
    pub maintenance_margin_pct: String,
    /// Initial margin percentage.
    pub initial_margin_pct: String,
    /// Current mark price for the contract (optional).
    #[serde(default)]
    pub contract_mark_price: Option<String>,
    /// Contract size (optional).
    #[serde(default)]
    pub contract_size: Option<String>,
    /// Instrument description (optional).
    #[serde(default)]
    pub description: Option<String>,
    /// Funding calendar schedule (optional).
    #[serde(default)]
    pub funding_calendar_schedule: Option<String>,
    /// Funding frequency (optional).
    #[serde(default)]
    pub funding_frequency: Option<String>,
    /// Lower cap for funding rate percentage (optional).
    #[serde(default)]
    pub funding_rate_cap_lower_pct: Option<String>,
    /// Upper cap for funding rate percentage (optional).
    #[serde(default)]
    pub funding_rate_cap_upper_pct: Option<String>,
    /// Lower deviation percentage for price bands (optional).
    #[serde(default)]
    pub price_band_lower_deviation_pct: Option<String>,
    /// Upper deviation percentage for price bands (optional).
    #[serde(default)]
    pub price_band_upper_deviation_pct: Option<String>,
    /// Price bands configuration (optional).
    #[serde(default)]
    pub price_bands: Option<String>,
    /// Price quotation format (optional).
    #[serde(default)]
    pub price_quotation: Option<String>,
    /// Underlying benchmark price (optional).
    #[serde(default)]
    pub underlying_benchmark_price: Option<String>,
}

/// Response payload returned by `GET /instruments`.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/symbols-instruments/get-instruments>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectInstrumentsResponse {
    /// List of instruments.
    pub instruments: Vec<ArchitectInstrument>,
}

/// Individual balance entry.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/portfolio-management/get-balances>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectBalance {
    /// Asset symbol.
    pub symbol: Ustr,
    /// Available balance amount.
    pub amount: String,
}

/// Response payload returned by `GET /balances`.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/portfolio-management/get-balances>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectBalancesResponse {
    /// List of balances.
    pub balances: Vec<ArchitectBalance>,
}

/// Individual position entry.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/portfolio-management/get-positions>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectPosition {
    /// User account UUID.
    pub user_id: String,
    /// Instrument symbol.
    pub symbol: Ustr,
    /// Open quantity (positive for long, negative for short).
    pub open_quantity: i64,
    /// Open notional value.
    pub open_notional: String,
    /// Position timestamp.
    pub timestamp: DateTime<Utc>,
    /// Realized profit and loss.
    pub realized_pnl: String,
}

/// Response payload returned by `GET /positions`.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/portfolio-management/get-positions>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectPositionsResponse {
    /// List of positions.
    pub positions: Vec<ArchitectPosition>,
}

/// Individual ticker entry.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/marketdata/get-ticker>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectTicker {
    /// Instrument symbol.
    pub symbol: Ustr,
    /// Best bid price.
    #[serde(default)]
    pub bid: Option<String>,
    /// Best ask price.
    #[serde(default)]
    pub ask: Option<String>,
    /// Last trade price.
    #[serde(default)]
    pub last: Option<String>,
    /// Mark price.
    #[serde(default)]
    pub mark: Option<String>,
    /// Index price.
    #[serde(default)]
    pub index: Option<String>,
    /// 24-hour volume.
    #[serde(default)]
    pub volume_24h: Option<String>,
    /// 24-hour high price.
    #[serde(default)]
    pub high_24h: Option<String>,
    /// 24-hour low price.
    #[serde(default)]
    pub low_24h: Option<String>,
    /// Ticker timestamp.
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
}

/// Response payload returned by `GET /tickers`.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/marketdata/get-tickers>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ArchitectTickersResponse {
    /// List of tickers.
    pub tickers: Vec<ArchitectTicker>,
}
