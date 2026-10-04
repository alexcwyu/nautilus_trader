//! Signal rules shared by the parity examples. Mirror of `quasar_core/backtest/examples/support/signal.rs`:
//! both sides must implement the same rule so the comparison isolates the engine, not the strategy.
//!
//! Selected by environment, nothing is hard-coded in the examples:
//! - `PARITY_STRATEGY` = `ema` (default) | `macd` | `rsi`
//! - `PARITY_FAST` / `PARITY_SLOW`  (default 10 / 20): EMA crossover, or the MACD fast/slow periods
//! - `PARITY_RSI_PERIOD` (default 14), `PARITY_RSI_LOW` / `PARITY_RSI_HIGH` in percent (default 30 / 70)
//!
//! Rules (all on the quote mid price, all exponential):
//! - ema:  BUY when fast crosses above slow, SELL when it crosses below.
//! - macd: value = fast EMA - slow EMA; BUY when it turns positive, SELL when it turns non-positive.
//! - rsi:  BUY on entering the oversold zone (value < low), SELL on entering the overbought zone (value > high).
//! A signal needs both/all indicators initialised and a previous state to compare with.

use nautilus_indicators::{
    average::{MovingAverageType, ema::ExponentialMovingAverage},
    indicator::{Indicator, MovingAverage},
    momentum::{macd::MovingAverageConvergenceDivergence, rsi::RelativeStrengthIndex},
};
use nautilus_model::enums::OrderSide;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |v| v.parse().unwrap_or_else(|_| panic!("{name}")))
}

fn env_percent(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .map_or(default, |v| v.parse::<f64>().unwrap_or_else(|_| panic!("{name}")))
        / 100.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Zone {
    Low,
    Mid,
    High,
}

pub(crate) enum Signal {
    Ema {
        fast: ExponentialMovingAverage,
        slow: ExponentialMovingAverage,
        prev: Option<bool>,
    },
    Macd {
        macd: MovingAverageConvergenceDivergence,
        prev: Option<bool>,
    },
    Rsi {
        rsi: RelativeStrengthIndex,
        low: f64,
        high: f64,
        prev: Option<Zone>,
    },
}

impl Signal {
    #[must_use]
    pub(crate) fn from_env() -> Self {
        let fast = env_usize("PARITY_FAST", 10);
        let slow = env_usize("PARITY_SLOW", 20);
        match std::env::var("PARITY_STRATEGY").as_deref() {
            Ok("macd") => Self::Macd {
                macd: MovingAverageConvergenceDivergence::new(
                    fast,
                    slow,
                    Some(MovingAverageType::Exponential),
                    None,
                ),
                prev: None,
            },
            Ok("rsi") => Self::Rsi {
                rsi: RelativeStrengthIndex::new(
                    env_usize("PARITY_RSI_PERIOD", 14),
                    Some(MovingAverageType::Exponential),
                ),
                low: env_percent("PARITY_RSI_LOW", 30.0),
                high: env_percent("PARITY_RSI_HIGH", 70.0),
                prev: None,
            },
            Ok("ema") | Err(_) => Self::Ema {
                fast: ExponentialMovingAverage::new(fast, None),
                slow: ExponentialMovingAverage::new(slow, None),
                prev: None,
            },
            Ok(other) => panic!("PARITY_STRATEGY={other}"),
        }
    }

    /// Feed one quote mid price; returns the side to trade when the rule fires.
    pub(crate) fn update(&mut self, mid: f64) -> Option<OrderSide> {
        match self {
            Self::Ema { fast, slow, prev } => {
                fast.update_raw(mid);
                slow.update_raw(mid);
                if !fast.initialized() || !slow.initialized() {
                    return None;
                }
                let above = fast.value() > slow.value();
                let signal = match (prev.replace(above), above) {
                    (Some(false), true) => Some(OrderSide::Buy),
                    (Some(true), false) => Some(OrderSide::Sell),
                    _ => None,
                };
                signal
            }
            Self::Macd { macd, prev } => {
                macd.update_raw(mid);
                if !macd.initialized() {
                    return None;
                }
                let positive = macd.value() > 0.0;
                match (prev.replace(positive), positive) {
                    (Some(false), true) => Some(OrderSide::Buy),
                    (Some(true), false) => Some(OrderSide::Sell),
                    _ => None,
                }
            }
            Self::Rsi { rsi, low, high, prev } => {
                rsi.update_raw(mid);
                if !rsi.initialized() {
                    return None;
                }
                let value = rsi.value;
                let zone = if value < *low {
                    Zone::Low
                } else if value > *high {
                    Zone::High
                } else {
                    Zone::Mid
                };
                match (prev.replace(zone), zone) {
                    (Some(before), Zone::Low) if before != Zone::Low => Some(OrderSide::Buy),
                    (Some(before), Zone::High) if before != Zone::High => Some(OrderSide::Sell),
                    _ => None,
                }
            }
        }
    }
}
