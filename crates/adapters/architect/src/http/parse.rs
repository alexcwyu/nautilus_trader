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

//! Parsing functions to convert Architect HTTP responses to Nautilus domain types.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::nanos::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, any::InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use super::models::ArchitectInstrument;
use crate::common::{consts::ARCHITECT_VENUE, parse::parse_decimal};

/// Parses a Price from a string field.
///
/// # Errors
///
/// Returns an error if the string cannot be parsed or converted to Price.
fn parse_price(value: &str, field_name: &str) -> anyhow::Result<Price> {
    let decimal = parse_decimal(value)
        .with_context(|| format!("Failed to parse price from field '{field_name}'"))?;
    Price::from_decimal(decimal).context("Failed to convert decimal to Price")
}

/// Parses a Quantity from a string field.
///
/// # Errors
///
/// Returns an error if the string cannot be parsed or converted to Quantity.
fn parse_quantity(value: &str, field_name: &str) -> anyhow::Result<Quantity> {
    let decimal = parse_decimal(value)
        .with_context(|| format!("Failed to parse quantity from field '{field_name}'"))?;
    Quantity::from_decimal(decimal).context("Failed to convert decimal to Quantity")
}

/// Gets or creates a Currency from a currency code string.
#[must_use]
fn get_currency(code: &str) -> Currency {
    Currency::from(code)
}

/// Parses an Architect perpetual futures instrument into a Nautilus CryptoPerpetual.
///
/// # Errors
///
/// Returns an error if any required field cannot be parsed or is invalid.
pub fn parse_perp_instrument(
    definition: &ArchitectInstrument,
    maker_fee: Decimal,
    taker_fee: Decimal,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    // Architect perpetuals use format: {BASE}-PERP, quoted in USD
    let raw_symbol_str = definition.symbol.as_str();
    let raw_symbol = Symbol::new(raw_symbol_str);
    let instrument_id = InstrumentId::new(raw_symbol, *ARCHITECT_VENUE);

    let base_code = raw_symbol_str
        .split('-')
        .next()
        .context("Failed to extract base currency from symbol")?;
    let base_currency = get_currency(base_code);

    let quote_currency = get_currency(&definition.quote_currency);
    let settlement_currency = quote_currency;

    let price_increment = parse_price(&definition.tick_size, "tick_size")?;
    let size_increment = parse_quantity(&definition.minimum_order_size, "minimum_order_size")?;

    let lot_size = Some(size_increment);
    let min_quantity = Some(size_increment);

    let margin_init = Decimal::from_str(&definition.initial_margin_pct)
        .context("Failed to parse initial_margin_pct")?;
    let margin_maint = Decimal::from_str(&definition.maintenance_margin_pct)
        .context("Failed to parse maintenance_margin_pct")?;

    let instrument = CryptoPerpetual::new(
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        settlement_currency,
        false, // Architect perps are linear/USDT-margined
        price_increment.precision,
        size_increment.precision,
        price_increment,
        size_increment,
        None,
        lot_size,
        None,
        min_quantity,
        None,
        None,
        None,
        None,
        Some(margin_init),
        Some(margin_maint),
        Some(maker_fee),
        Some(taker_fee),
        ts_event,
        ts_init,
    );

    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use nautilus_core::nanos::UnixNanos;
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::common::enums::ArchitectInstrumentState;

    fn create_test_instrument() -> ArchitectInstrument {
        ArchitectInstrument {
            symbol: Ustr::from("BTC-PERP"),
            state: ArchitectInstrumentState::Open,
            multiplier: "1.0".to_string(),
            minimum_order_size: "0.001".to_string(),
            tick_size: "0.5".to_string(),
            quote_currency: "USD".to_string(),
            finding_settlement_currency: "USD".to_string(),
            maintenance_margin_pct: "0.005".to_string(),
            initial_margin_pct: "0.01".to_string(),
            contract_mark_price: Some("45000.50".to_string()),
            contract_size: Some("1.0".to_string()),
            description: Some("Bitcoin Perpetual Futures".to_string()),
            funding_calendar_schedule: Some("0,8,16".to_string()),
            funding_frequency: Some("8h".to_string()),
            funding_rate_cap_lower_pct: Some("-0.0075".to_string()),
            funding_rate_cap_upper_pct: Some("0.0075".to_string()),
            price_band_lower_deviation_pct: Some("0.05".to_string()),
            price_band_upper_deviation_pct: Some("0.05".to_string()),
            price_bands: Some("dynamic".to_string()),
            price_quotation: Some("USD".to_string()),
            underlying_benchmark_price: Some("45000.00".to_string()),
        }
    }

    #[rstest]
    fn test_parse_price() {
        let price = parse_price("100.50", "test_field").unwrap();
        assert_eq!(price.as_f64(), 100.50);
    }

    #[rstest]
    fn test_parse_quantity() {
        let qty = parse_quantity("1.5", "test_field").unwrap();
        assert_eq!(qty.as_f64(), 1.5);
    }

    #[rstest]
    fn test_get_currency() {
        let currency = get_currency("USD");
        assert_eq!(currency.code, Ustr::from("USD"));
    }

    #[rstest]
    fn test_parse_perp_instrument() {
        let definition = create_test_instrument();
        let maker_fee = Decimal::new(2, 4);
        let taker_fee = Decimal::new(5, 4);
        let ts_now = UnixNanos::default();

        let result = parse_perp_instrument(&definition, maker_fee, taker_fee, ts_now, ts_now);
        assert!(result.is_ok());

        let instrument = result.unwrap();
        match instrument {
            InstrumentAny::CryptoPerpetual(perp) => {
                assert_eq!(perp.id.symbol.as_str(), "BTC-PERP");
                assert_eq!(perp.id.venue, *ARCHITECT_VENUE);
                assert_eq!(perp.base_currency.code.as_str(), "BTC");
                assert_eq!(perp.quote_currency.code.as_str(), "USD");
                assert!(!perp.is_inverse);
            }
            _ => panic!("Expected CryptoPerpetual instrument"),
        }
    }

    #[rstest]
    fn test_deserialize_instruments_from_test_data() {
        let test_data = include_str!("../../test_data/http_get_instruments.json");
        let response: super::super::models::ArchitectInstrumentsResponse =
            serde_json::from_str(test_data).expect("Failed to deserialize test data");

        assert_eq!(response.instruments.len(), 3);

        let btc = &response.instruments[0];
        assert_eq!(btc.symbol.as_str(), "BTC-PERP");
        assert_eq!(btc.state, ArchitectInstrumentState::Open);
        assert_eq!(btc.tick_size, "0.5");
        assert_eq!(btc.minimum_order_size, "0.001");
        assert!(btc.contract_mark_price.is_some());

        let eth = &response.instruments[1];
        assert_eq!(eth.symbol.as_str(), "ETH-PERP");
        assert_eq!(eth.state, ArchitectInstrumentState::Open);

        // SOL-PERP is suspended with null optional fields
        let sol = &response.instruments[2];
        assert_eq!(sol.symbol.as_str(), "SOL-PERP");
        assert_eq!(sol.state, ArchitectInstrumentState::Suspended);
        assert!(sol.contract_mark_price.is_none());
        assert!(sol.funding_frequency.is_none());
    }

    #[rstest]
    fn test_parse_all_instruments_from_test_data() {
        let test_data = include_str!("../../test_data/http_get_instruments.json");
        let response: super::super::models::ArchitectInstrumentsResponse =
            serde_json::from_str(test_data).expect("Failed to deserialize test data");

        let maker_fee = Decimal::new(2, 4);
        let taker_fee = Decimal::new(5, 4);
        let ts_now = UnixNanos::default();

        let open_instruments: Vec<_> = response
            .instruments
            .iter()
            .filter(|i| i.state == ArchitectInstrumentState::Open)
            .collect();

        assert_eq!(open_instruments.len(), 2);

        for instrument in open_instruments {
            let result = parse_perp_instrument(instrument, maker_fee, taker_fee, ts_now, ts_now);
            assert!(
                result.is_ok(),
                "Failed to parse {}: {:?}",
                instrument.symbol,
                result.err()
            );
        }
    }
}
