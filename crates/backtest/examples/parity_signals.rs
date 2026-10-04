//! Signal-only parity: the mid-price signal rules of `support/signal.rs` (EMA cross, MACD, RSI, chosen by
//! `PARITY_STRATEGY`), without the engine. Reads `ts_ns,bid,ask` from `PARITY_QUOTES`, prints
//! `SIG <n> ts=<ts> side=<BUY|SELL>` and `SIGTOTAL`.

#[path = "support/signal.rs"]
mod signal;

use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny, stubs::currency_pair_btcusdt},
    types::{Price, Quantity},
};

fn main() -> anyhow::Result<()> {
    let instrument_id = InstrumentAny::CurrencyPair(currency_pair_btcusdt()).id();
    let text = std::fs::read_to_string(std::env::var("PARITY_QUOTES")?)?;
    let mut signal = signal::Signal::from_env();
    let (mut quotes, mut signals) = (0_usize, 0_usize);
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let mut parts = line.split(',');
        let ts: u64 = parts.next().expect("ts").parse()?;
        let quote = QuoteTick::new(
            instrument_id as InstrumentId,
            Price::from(parts.next().expect("bid")),
            Price::from(parts.next().expect("ask")),
            Quantity::from("1.000000"),
            Quantity::from("1.000000"),
            ts.into(),
            ts.into(),
        );
        quotes += 1;
        let mid: f64 = quote.extract_price(PriceType::Mid).into();
        match signal.update(mid) {
            Some(OrderSide::Buy) => {
                println!("SIG {signals} ts={ts} side=BUY");
                signals += 1;
            }
            Some(_) => {
                println!("SIG {signals} ts={ts} side=SELL");
                signals += 1;
            }
            None => {}
        }
    }
    println!("SIGTOTAL quotes={quotes} signals={signals}");
    Ok(())
}
