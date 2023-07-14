// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
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

use std::ffi::c_char;
use rust_decimal::prelude::*;
// use fixed::types::I64F64;
// type Decimal = I64F64;

use nautilus_core::{string::str_to_cstr, time::UnixNanos};

use super::{
    tick::{QuoteTick, TradeTick},
    Data,
};
use crate::{
    enums::AggressorSide,
    identifiers::{instrument_id::InstrumentId, trade_id::TradeId},
};

#[no_mangle]
pub extern "C" fn quote_tick_new(
    instrument_id: InstrumentId,
    bid_price_raw: i64,
    ask_price_raw: i64,
    bid_price_prec: u8,
    ask_price_prec: u8,
    bid_size_raw: u64,
    ask_size_raw: u64,
    bid_size_prec: u8,
    ask_size_prec: u8,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> QuoteTick {
    QuoteTick::new(
        instrument_id,
        Decimal::new(bid_price_raw, bid_price_prec.to_u32().unwrap()),
        Decimal::new(ask_price_raw, ask_price_prec.to_u32().unwrap()),
        Decimal::new(bid_size_raw.to_i64().unwrap(), bid_size_prec.to_u32().unwrap()),
        Decimal::new(ask_size_raw.to_i64().unwrap(), ask_size_prec.to_u32().unwrap()),
        ts_event,
        ts_init,
    )
}

#[no_mangle]
pub extern "C" fn quote_tick_drop(tick: QuoteTick) {
    drop(tick); // Memory freed here
}

#[no_mangle]
pub extern "C" fn quote_tick_clone(tick: &QuoteTick) -> QuoteTick {
    tick.clone()
}

/// Returns a [`QuoteTick`] as a C string pointer.
#[no_mangle]
pub extern "C" fn quote_tick_to_cstr(tick: &QuoteTick) -> *const c_char {
    str_to_cstr(&tick.to_string())
}

#[no_mangle]
pub extern "C" fn trade_tick_new(
    instrument_id: InstrumentId,
    price_raw: i64,
    price_prec: u8,
    size_raw: u64,
    size_prec: u8,
    aggressor_side: AggressorSide,
    trade_id: TradeId,
    ts_event: u64,
    ts_init: u64,
) -> TradeTick {
    TradeTick::new(
        instrument_id,
        Decimal::new(price_raw, price_prec.to_u32().unwrap()),
        Decimal::new(size_raw.to_i64().unwrap(), size_prec.to_u32().unwrap()),
        aggressor_side,
        trade_id,
        ts_event,
        ts_init,
    )
}

#[no_mangle]
pub extern "C" fn trade_tick_drop(tick: TradeTick) {
    drop(tick); // Memory freed here
}

#[no_mangle]
pub extern "C" fn trade_tick_clone(tick: &TradeTick) -> TradeTick {
    tick.clone()
}

/// Returns a [`TradeTick`] as a C string pointer.
#[no_mangle]
pub extern "C" fn trade_tick_to_cstr(tick: &TradeTick) -> *const c_char {
    str_to_cstr(&tick.to_string())
}

#[no_mangle]
pub extern "C" fn data_drop(data: Data) {
    drop(data); // Memory freed here
}

#[no_mangle]
pub extern "C" fn data_clone(data: &Data) -> Data {
    data.clone()
}
