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

//! Request parameter structures for the Architect REST API.
//!
//! Each struct corresponds to an Architect REST endpoint and is annotated
//! using `serde` so that it can be serialized directly into the query string
//! or request body expected by the exchange.
//!
//! Parameter structs are built using the builder pattern and then passed to
//! `ArchitectRawHttpClient` methods where they are automatically serialized.

use serde::{Deserialize, Serialize};

/// Parameters for the GET /ticker endpoint.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/marketdata/get-ticker>
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetTickerParams {
    /// Instrument symbol, e.g. "GBPUSD-PERP", "EURUSD-PERP".
    pub symbol: String,
}

impl GetTickerParams {
    /// Creates a new [`GetTickerParams`] with the given symbol.
    #[must_use]
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
        }
    }
}

/// Parameters for the GET /instrument endpoint.
///
/// # References
/// - <https://docs.sandbox.x.architect.co/api-reference/symbols-instruments/get-instrument>
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetInstrumentParams {
    /// Instrument symbol, e.g. "GBPUSD-PERP", "EURUSD-PERP".
    pub symbol: String,
}

impl GetInstrumentParams {
    /// Creates a new [`GetInstrumentParams`] with the given symbol.
    #[must_use]
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_get_ticker_params_serialization() {
        let params = GetTickerParams::new("GBPUSD-PERP");
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(qs, "symbol=GBPUSD-PERP");
    }

    #[rstest]
    fn test_get_instrument_params_serialization() {
        let params = GetInstrumentParams::new("EURUSD-PERP");
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(qs, "symbol=EURUSD-PERP");
    }
}
