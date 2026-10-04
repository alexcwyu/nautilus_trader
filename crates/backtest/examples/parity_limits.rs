//! Parity harness (limit orders): deterministic limit-order ladder over real BTCUSDT quotes.
//! Phase per 10 quotes: 0 BUY limit at bid-20.00, 3 SELL limit at ask+20.00, 6 and 9 cancel-all.
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
    enums::{AccountType, AggressorSide, BookType, OmsType, OrderSide},
    identifiers::{AccountId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::currency_pair_btcusdt},
    orders::Order,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_model::events::OrderEventAny;
use nautilus_model::position::Position;
use std::fmt::Debug;

#[path = "support/trace.rs"]
mod trace;

use nautilus_common::actor::DataActor;
use nautilus_model::{enums::TimeInForce, identifiers::StrategyId};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

struct LimitLadder {
    core: StrategyCore,
    instrument_id: InstrumentId,
    n: u64,
}

impl LimitLadder {
    fn new(instrument_id: InstrumentId) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("LADDER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            instrument_id,
            n: 0,
        }
    }

    /// `PARITY_ORDER_KIND=stop`: the ladder places stop-market orders (BUY above the ask, SELL below the bid).
    fn place(&mut self, side: OrderSide, price: Price) -> anyhow::Result<()> {
        if std::env::var("PARITY_ORDER_KIND").as_deref() == Ok("stop") {
            let order = self.core.order_factory().stop_market(
                self.instrument_id,
                side,
                Quantity::from("0.100000"),
                price,
                None,
                Some(TimeInForce::Gtc),
                None, None, None, None, None, None, None, None, None, None,
            );
            return self.submit_order(order, None, None, None);
        }
        let order = self.core.order_factory().limit(
            self.instrument_id,
            side,
            Quantity::from("0.100000"),
            price,
            Some(TimeInForce::Gtc),
            None, None, None, None, None, None, None, None, None, None, None,
        );
        self.submit_order(order, None, None, None)
    }
}

nautilus_strategy!(LimitLadder);

impl Debug for LimitLadder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LimitLadder").finish()
    }
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

fn bar_type(instrument_id: InstrumentId) -> BarType {
    BarType::from(format!("{instrument_id}-1-MINUTE-LAST-EXTERNAL").as_str())
}

impl LimitLadder {
    /// One step of the ladder at the reference prices `bid` / `ask` (both the bar close or trade price in bar and
    /// trade modes).
    fn ladder(&mut self, bid: f64, ask: f64) -> anyhow::Result<()> {
        let phase = self.n % 10;
        self.n += 1;
        let stop = std::env::var("PARITY_ORDER_KIND").as_deref() == Ok("stop");
        let dollars: f64 = std::env::var("PARITY_LIMIT_OFFSET").ok().and_then(|v| v.parse().ok()).unwrap_or(20.0);
        match phase {
            // A limit buy rests below the market and a limit sell above it; a stop is the other way round.
            0 if stop => self.place(OrderSide::Buy, Price::new(ask + dollars, 2)),
            3 if stop => self.place(OrderSide::Sell, Price::new(bid - dollars, 2)),
            0 => self.place(OrderSide::Buy, Price::new(bid - dollars, 2)),
            3 => self.place(OrderSide::Sell, Price::new(ask + dollars, 2)),
            6 | 9 if std::env::var("PARITY_NO_CANCEL").is_err() && (phase == 6 || std::env::var("PARITY_CANCEL_TWICE").as_deref() != Ok("0")) => self.cancel_all_orders(self.instrument_id, None, None, None),
            _ => Ok(()),
        }
    }
}

impl DataActor for LimitLadder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        match DataMode::from_env() {
            DataMode::Quote => self.subscribe_quotes(self.instrument_id, None, None),
            DataMode::Bar => self.subscribe_bars(bar_type(self.instrument_id), None, None),
            DataMode::Trade => self.subscribe_trades(self.instrument_id, None, None),
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.ladder(quote.bid_price.as_f64(), quote.ask_price.as_f64())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let close: f64 = bar.close.into();
        self.ladder(close, close)
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        let price: f64 = trade.price.into();
        self.ladder(price, price)
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

/// One execution of an order, as the harness prints it.
struct Fill {
    ts: u64,
    qty: String,
    px: String,
    comm: f64,
    ccy: String,
    liq: String,
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
    // The kernel logger writes to stdout inside the timed region; `PARITY_LOGGING=1` restores it for diagnosis.
    let mut engine = BacktestEngine::new(BacktestEngineConfig {
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
            .starting_balances(vec![Money::from("1_000_000 USDT")])
            .maybe_latency_model(latency)
            .build(),
    )?;

    let instrument = InstrumentAny::CurrencyPair(currency_pair_btcusdt());
    let instrument_id = instrument.id();
    engine.add_instrument(&instrument)?;

    engine.add_strategy(LimitLadder::new(instrument_id))?;

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

    println!("END ok");
    Ok(())
}
