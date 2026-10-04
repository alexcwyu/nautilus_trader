//! Parity harness: EMA crossover over real BTCUSDT quotes.
//!
//! Reads `ts_ns,bid,ask` triples from the CSV named by `PARITY_QUOTES` and prints a canonical
//! trace that the Quasar harness reproduces line for line.
//!
//! Run with:
//! `PARITY_QUOTES=/path/quotes.csv cargo run -p nautilus-backtest --features examples --example parity-ema-cross`

use nautilus_backtest::{
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    engine::BacktestEngine,
};
use nautilus_model::{
    data::{Bar, BarType, Data, QuoteTick, TradeTick},
    accounts::Account,
    events::OrderEventAny,
    position::Position,
    enums::{AccountType, AggressorSide, BookType, OmsType, OrderSide},
    identifiers::{AccountId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::currency_pair_btcusdt},
    orders::Order,
    types::{Currency, Money, Price, Quantity},
};
use std::fmt::Debug;

use nautilus_common::actor::DataActor;
use nautilus_model::{enums::PriceType, identifiers::StrategyId};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

#[path = "support/signal.rs"]
mod signal;
#[path = "support/trace.rs"]
mod trace;

/// Trades the `support/signal.rs` rule (EMA cross by default, MACD or RSI via `PARITY_STRATEGY`) with market
/// orders on the quote mid price. Same order handling as `EmaCross`.
struct SignalStrategy {
    core: StrategyCore,
    instrument_id: InstrumentId,
    trade_size: Quantity,
    /// `PARITY_REVERSE`: every order after the first is twice the size, so each signal flips the position.
    reverse: bool,
    sent: u64,
    signal: signal::Signal,
    /// `PARITY_BAL_PROBE`: print the account balance to stderr whenever the signal fires (diagnosis only).
    probe: bool,
}

/// Which data stream feeds the strategy and the simulated exchange (`PARITY_DATA_MODE`).
#[derive(Clone, Copy, PartialEq)]
enum DataMode {
    Quote,
    Bar,
    Trade,
}

impl DataMode {
    fn from_env() -> Self {
        match std::env::var("PARITY_DATA_MODE").as_deref() {
            Ok("bar") => Self::Bar,
            Ok("trade") => Self::Trade,
            _ => Self::Quote,
        }
    }
}

/// One execution of an order, as the harness prints it.
struct Fill {
    ts: u64,
    qty: String,
    px: String,
    comm: f64,
    ccy: String,
    liq: String,
}

fn bar_type(instrument_id: InstrumentId) -> BarType {
    BarType::from(format!("{instrument_id}-1-MINUTE-LAST-EXTERNAL").as_str())
}

impl SignalStrategy {
    fn submit_for(&mut self, price: f64) -> anyhow::Result<()> {
        if let Some(side) = self.signal.update(price) {
            if self.probe
                && let Some(account) = self.core.cache().account(&AccountId::from("BINANCE-001"))
            {
                let usdt = Currency::USDT();
                eprintln!(
                    "PROBE venue_account={} side={side:?} free={} locked={} total={}",
                    self.core.cache().account_for_venue(&Venue::from("BINANCE")).is_some(),
                    account.balance_free(Some(usdt)).map_or(0.0, |m| m.as_f64()),
                    account.balance_locked(Some(usdt)).map_or(0.0, |m| m.as_f64()),
                    account.balance_total(Some(usdt)).map_or(0.0, |m| m.as_f64()),
                );
            }
            let size = if self.reverse && self.sent > 0 {
                Quantity::from(format!("{:.6}", self.trade_size.as_f64() * 2.0).as_str())
            } else {
                self.trade_size
            };
            self.sent += 1;
            let order = self.core.order_factory().market(
                self.instrument_id,
                side,
                size,
                None, None, None, None, None, None, None,
            );
            self.submit_order(order, None, None, None)?;
        }
        Ok(())
    }

    fn new(instrument_id: InstrumentId, trade_size: Quantity) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("EMA_CROSS-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            instrument_id,
            trade_size,
            reverse: std::env::var("PARITY_REVERSE").is_ok_and(|v| v == "1"),
            sent: 0,
            signal: signal::Signal::from_env(),
            probe: std::env::var("PARITY_BAL_PROBE").is_ok(),
        }
    }
}

nautilus_strategy!(SignalStrategy);

impl Debug for SignalStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalStrategy").finish()
    }
}

impl DataActor for SignalStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        match DataMode::from_env() {
            DataMode::Quote => self.subscribe_quotes(self.instrument_id, None, None),
            DataMode::Bar => self.subscribe_bars(bar_type(self.instrument_id), None, None),
            DataMode::Trade => self.subscribe_trades(self.instrument_id, None, None),
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        match DataMode::from_env() {
            DataMode::Quote => self.unsubscribe_quotes(self.instrument_id, None, None),
            DataMode::Bar => self.unsubscribe_bars(bar_type(self.instrument_id), None, None),
            DataMode::Trade => self.unsubscribe_trades(self.instrument_id, None, None),
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.submit_for(quote.extract_price(PriceType::Mid).into())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.submit_for(bar.close.into())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.submit_for(trade.price.into())
    }
}

/// `PARITY_BARS`: `ts_ns,open,high,low,close,volume` one-minute bars (`ts_ns` is the bar close time).
fn load_bars(instrument_id: InstrumentId) -> anyhow::Result<Vec<Data>> {
    let text = std::fs::read_to_string(std::env::var("PARITY_BARS")?)?;
    let bar_type = bar_type(instrument_id);
    let mut bars = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let f: Vec<&str> = line.split(',').collect();
        let ts: u64 = f[0].parse()?;
        bars.push(Data::Bar(Bar::new(
            bar_type,
            Price::from(f[1]),
            Price::from(f[2]),
            Price::from(f[3]),
            Price::from(f[4]),
            Quantity::from(f[5]),
            ts.into(),
            ts.into(),
        )));
    }
    Ok(bars)
}

/// `PARITY_TRADES`: `ts_ns,price,size,side` with side `BUY` or `SELL` (the aggressor).
fn load_trades(instrument_id: InstrumentId) -> anyhow::Result<Vec<Data>> {
    let text = std::fs::read_to_string(std::env::var("PARITY_TRADES")?)?;
    let mut trades = Vec::new();
    for (index, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
        let f: Vec<&str> = line.split(',').collect();
        let ts: u64 = f[0].parse()?;
        trades.push(Data::Trade(TradeTick::new(
            instrument_id,
            Price::from(f[1]),
            Quantity::from(f[2]),
            if f[3] == "BUY" { AggressorSide::Buyer } else { AggressorSide::Seller },
            TradeId::new((index + 1).to_string()),
            ts.into(),
            ts.into(),
        )));
    }
    Ok(trades)
}

fn load_quotes(instrument_id: InstrumentId) -> anyhow::Result<Vec<Data>> {
    let path = std::env::var("PARITY_QUOTES")?;
    let text = std::fs::read_to_string(&path)?;
    let mut quotes = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let mut parts = line.split(',');
        let ts: u64 = parts.next().expect("ts field").parse()?;
        let bid = parts.next().expect("bid field");
        let ask = parts.next().expect("ask field");
        quotes.push(Data::Quote(QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1.000000"),
            Quantity::from("1.000000"),
            ts.into(),
            ts.into(),
        )));
    }
    Ok(quotes)
}

fn main() -> anyhow::Result<()> {
    // Optional fixed order-insert latency (ns). Quasar delivers strategy orders to its exchange one
    // input event later (DIVERGENCES D1); with 60 s spaced quotes a 60 s latency reproduces that.
    let latency: Option<Box<dyn nautilus_execution::models::latency::LatencyModel>> =
        std::env::var("PARITY_LATENCY_NS").ok().map(|v| {
            let ns: u64 = v.parse().expect("PARITY_LATENCY_NS");
            Box::new(nautilus_execution::models::latency::StaticLatencyModel::new(
                0.into(),
                ns.into(),
                0.into(),
                0.into(),
            )) as Box<dyn nautilus_execution::models::latency::LatencyModel>
        });
    // Timed runs bypass the kernel logger (it writes to stdout inside the timed region); the canonical evidence is the
    // trace this harness prints. `PARITY_LOGGING=1` restores the logs when diagnosing.
    let mut engine = BacktestEngine::new(BacktestEngineConfig {
        // `BacktestEngineConfig.bypass_logging` is not read by the Rust engine; the kernel logger's own
        // `LoggerConfig.bypass_logging` is the switch that stops the stdout logging inside the timed region.
        logging: nautilus_common::logging::logger::LoggerConfig {
            bypass_logging: std::env::var("PARITY_LOGGING").is_err(),
            ..Default::default()
        },
        ..Default::default()
    })?;

    engine.add_venue(
        SimulatedVenueConfig::builder()
            .venue(Venue::from("BINANCE"))
            .oms_type(if std::env::var("PARITY_OMS").as_deref() == Ok("netting") {
                OmsType::Netting
            } else {
                OmsType::Hedging
            })
            .account_type(AccountType::Margin)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from(
                format!("{} USDT", std::env::var("PARITY_START_BALANCE").unwrap_or_else(|_| "1000000".into())).as_str(),
            )])
            .maybe_latency_model(latency)
            .build(),
    )?;

    let instrument = InstrumentAny::CurrencyPair(currency_pair_btcusdt());
    let instrument_id = instrument.id();
    engine.add_instrument(&instrument)?;

    engine.add_strategy(SignalStrategy::new(instrument_id, Quantity::from("0.100000")))?;

    let (label, data) = match DataMode::from_env() {
        DataMode::Quote => ("QUOTES", load_quotes(instrument_id)?),
        DataMode::Bar => ("BARS", load_bars(instrument_id)?),
        DataMode::Trade => ("TRADES", load_trades(instrument_id)?),
    };
    trace::meta("nautilus");
    println!("{label} {}", data.len());
    trace::input_span(
        match DataMode::from_env() {
            DataMode::Quote => "PARITY_QUOTES",
            DataMode::Bar => "PARITY_BARS",
            DataMode::Trade => "PARITY_TRADES",
        },
        data.len(),
    )?;
    engine.add_data(data, None, true, true)?;
    engine.run(None, None, None, false)?;

    let result = engine.get_result();
    // `Cache::orders` is keyed by a hash set, so impose the wire order the trace compares on.
    let mut rows: Vec<(u64, String, String, String, String, f64, String, Vec<Fill>, String)> = {
        let cache = engine.kernel_mut().cache.borrow();
        cache
            .orders(None, None, None, None, None)
            .iter()
            .map(|order| {
                (
                    order.ts_init().as_u64(),
                    match order.order_side() {
                        OrderSide::Buy => "BUY".to_string(),
                        OrderSide::Sell => "SELL".to_string(),
                        other => format!("{other:?}"),
                    },
                    format!("{:.6}", order.quantity().as_f64()),
                    format!("{:.6}", order.filled_qty().as_f64()),
                    order.avg_px().map_or("-".to_string(), |px| format!("{px:.2}")),
                    order.commissions().values().map(|m| m.as_f64()).sum::<f64>(),
                    format!("{:?}", order.status()),
                    order
                        .events()
                        .iter()
                        .filter_map(|event| match event {
                            OrderEventAny::Filled(f) => Some(Fill {
                                ts: f.ts_event.as_u64(),
                                qty: format!("{:.6}", f.last_qty.as_f64()),
                                px: format!("{:.2}", f.last_px.as_f64()),
                                comm: f.commission.map_or(0.0, |m| m.as_f64()),
                                ccy: f.currency.code.to_string(),
                                liq: format!("{:?}", f.liquidity_side).to_uppercase(),
                            }),
                            _ => None,
                        })
                        .collect(),
                    order.client_order_id().to_string(),
                )
            })
            .collect()
    };
    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut fees = 0.0_f64;
    for (index, (ts, side, qty, filled, avg_px, comm, status, _, _)) in rows.iter().enumerate() {
        fees += comm;
        println!(
            "ORDER {index} ts={ts} side={side} qty={qty} filled={filled} avgpx={avg_px} comm={comm:.6} status={status}"
        );
    }
    // One FILL per execution, ordered by (timestamp, order ordinal, fill ordinal within the order).
    let mut fills: Vec<(u64, usize, usize, &Fill)> = rows
        .iter()
        .enumerate()
        .flat_map(|(order, row)| row.7.iter().enumerate().map(move |(n, f)| (f.ts, order, n, f)))
        .collect();
    fills.sort_by_key(|(ts, order, n, _)| (*ts, *order, *n));
    for (_, order, n, f) in &fills {
        println!(
            "FILL order={order} fill={n} ts={} side={} qty={} px={} comm={:.6} ccy={} liq={}",
            f.ts, rows[*order].1, f.qty, f.px, f.comm, f.ccy, f.liq
        );
    }

    println!(
        "RESULT iterations={} orders={} positions={} executions={}",
        result.iterations, result.total_orders, result.total_positions, fills.len()
    );
    println!(
        "RUN start={} end={}",
        result.backtest_start.map_or(0, |t| t.as_u64()),
        result.backtest_end.map_or(0, |t| t.as_u64())
    );

    let cache = engine.kernel_mut().cache.borrow();
    // Every position lifecycle: the cached positions plus the snapshots of reopened netting positions. Positions are
    // identified by the order that opened them (its ORDER ordinal), not by an economic sort key.
    let order_ordinals: std::collections::HashMap<&str, usize> =
        rows.iter().enumerate().map(|(i, r)| (r.8.as_str(), i)).collect();
    let mut positions: Vec<Position> = cache
        .positions(None, None, None, None, None)
        .into_iter()
        .map(|p| Position::clone(&p))
        .collect();
    positions.extend(cache.position_snapshots(None, None));
    let ordinal = |p: &Position| {
        order_ordinals.get(p.opening_order_id.to_string().as_str()).copied().unwrap_or(usize::MAX)
    };
    positions.sort_by_key(|p| (ordinal(p), p.ts_opened.as_u64()));
    let realized: f64 = positions.iter().map(|p| p.realized_pnl.map_or(0.0, |pnl| pnl.as_f64())).sum();
    println!("PNL realized={realized:.2}");
    println!("FEES total={fees:.6}");
    for (index, position) in positions.iter().enumerate() {
        println!(
            "POS {index} entry={} side={} qty={:.6} avgpx={:.2} realized={:.2}",
            ordinal(position),
            format!("{:?}", position.side).to_uppercase(),
            position.quantity.as_f64(),
            position.avg_px_open,
            position.realized_pnl.map_or(0.0, |pnl| pnl.as_f64()),
        );
    }
    if let Some(account) = cache.account(&AccountId::from("BINANCE-001")) {
        let usdt = Currency::USDT();
        println!(
            "BAL free={:.2} total={:.2}",
            account.balance_free(Some(usdt)).map_or(0.0, |m| m.as_f64()),
            account.balance_total(Some(usdt)).map_or(0.0, |m| m.as_f64()),
        );
    }
    drop(cache);
    // Mark-to-market of whatever is still open, and the equity (margin account: total balance + unrealized PnL).
    let portfolio = engine.kernel_mut().portfolio.clone();
    let (venue, usdt) = (Venue::from("BINANCE"), Currency::USDT());
    // Scoped to the account: the unscoped call serves a per-instrument cache that is refreshed on quote, bar and
    // position events but not on trade ticks, so it can lag the market by the whole trade stream.
    let account_id = AccountId::from("BINANCE-001");
    let unrealized = portfolio.borrow_mut().unrealized_pnls(&venue, Some(&account_id));
    let equity = portfolio.borrow_mut().equity(&venue, Some(&account_id));
    println!("UNREAL value={:.2}", unrealized.get(&usdt).map_or(0.0, |m| m.as_f64()));
    println!("EQUITY value={:.2}", equity.get(&usdt).map_or(0.0, |m| m.as_f64()));

    println!("END ok");
    Ok(())
}
