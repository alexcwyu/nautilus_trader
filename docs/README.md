# NautilusTrader

> **Last Updated**: 2026-04-07T00:00:00Z
> **Git Hash**: `aa60b11`

NautilusTrader is an open-source, production-grade, Rust-native engine for multi-asset, multi-venue algorithmic trading. It spans research, deterministic simulation, and live execution within a single event-driven architecture. Python serves as the control plane for strategy logic, configuration, and orchestration while Rust provides the performance-critical core.

**Version**: 1.225.0 (Python) / 0.55.0 (Rust crates)
**License**: LGPL-3.0-or-later
**Platforms**: Linux x86_64/ARM64, macOS ARM64, Windows x86_64
**Languages**: Rust 1.94.1, Python 3.12–3.14

- **Repository**: <https://github.com/nautechsystems/nautilus_trader>
- **Docs**: <https://nautilustrader.io/docs/>
- **Discord**: <https://discord.gg/NautilusTrader>

---

## Key Features

| Feature | Description |
|---|---|
| **Rust-native core** | Performance-critical paths in Rust; no Python GIL in the hot loop |
| **Deterministic simulation** | Identical execution semantics in backtest and live — strategies port without code changes |
| **Nanosecond resolution** | All timestamps and event timings use `UnixNanos` (u64); clocks are fully deterministic in backtests |
| **Multi-venue / multi-asset** | Run strategies across exchanges simultaneously; asset-class agnostic |
| **High-precision arithmetic** | 128-bit integers (up to 16 decimal places) for `Price`, `Quantity`, and `Money` on Linux/macOS |
| **PyO3 bindings** | Full Python API auto-generated from Rust; no Rust toolchain required at install time |
| **Event-sourced state** | `Cache` + `MessageBus` provide observable, replayable system state |
| **Redis persistence** | Optional Redis-backed cache for distributed state and resilience |
| **Advanced order types** | Market, Limit, StopMarket, StopLimit, MIT, LIT, TrailingStop, MarketToLimit, OCO/OUO/OTO contingencies |
| **Time in force** | IOC, FOK, GTC, GTD, DAY, AT_THE_OPEN, AT_THE_CLOSE |
| **14 adapters** | Binance, Bybit, Kraken, OKX, dYdX, Hyperliquid, Deribit, BitMEX, Databento, Tardis, Interactive Brokers, Betfair, Polymarket, AX Exchange |
| **AI training speed** | Engine throughput sufficient to train RL/ES trading agents |

---

## Quick Start

```python
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import (
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
    BacktestDataConfig,
    ImportableStrategyConfig,
)

# Configure a simulated venue
venue = BacktestVenueConfig(
    name="SIM",
    oms_type="HEDGING",
    account_type="MARGIN",
    base_currency="USD",
    starting_balances=["1_000_000 USD"],
)

# Load historical data (e.g. Parquet bar data)
data = BacktestDataConfig(
    catalog_path="/path/to/catalog",
    data_cls="nautilus_trader.model.data:Bar",
    instrument_id="ES.GLBX",
    start_time="2024-01-01",
    end_time="2024-12-31",
)

# Point to a strategy class
strategy = ImportableStrategyConfig(
    strategy_path="my_strategies:MomentumStrategy",
    config_path="my_strategies:MomentumConfig",
    config={"fast_period": 10, "slow_period": 20},
)

run = BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=[strategy]),
    venues=[venue],
    data=[data],
)

node = BacktestNode(configs=[run])
results = node.run()
```

---

## Architecture Summary

NautilusTrader separates concerns into three layers:

```
┌──────────────────────────────────────────────────┐
│  Python Control Plane                            │
│  Strategy / Actor / Config / Adapter Python API  │
│  (PyO3 bindings generated from Rust)             │
├──────────────────────────────────────────────────┤
│  Rust Core Engine                                │
│  Model · MessageBus · Cache · Clock              │
│  DataEngine · ExecutionEngine · RiskEngine       │
│  Portfolio · BacktestEngine / LiveNode           │
├──────────────────────────────────────────────────┤
│  Adapters (Rust + optional Python shim)          │
│  Binance · Bybit · Kraken · dYdX · IB · …       │
└──────────────────────────────────────────────────┘
```

The same `NautilusKernel` drives both backtest and live modes, guaranteeing research-to-live parity. See [`architecture.md`](architecture.md) for crate-level detail.

---

## Documentation Index

This folder contains standardized reference documents alongside the existing API reference and concept guides:

| File | Contents |
|---|---|
| [`README.md`](README.md) | This file — overview, quick start, feature table |
| [`architecture.md`](architecture.md) | System diagram, crate breakdown, component responsibilities |
| [`workflow.md`](workflow.md) | Trading pipeline sequence, backtest vs live data flow, event lifecycle |
| [`state-management.md`](state-management.md) | Order state machine, position lifecycle, account state |
| [`development.md`](development.md) | Environment setup, build commands, config reference, troubleshooting |

### Existing Detailed Docs

The project's canonical documentation lives in the subdirectories below (generated from source and maintained upstream):

| Directory | Contents |
|---|---|
| [`api_reference/`](api_reference/) | Full API reference for every public module and class |
| [`concepts/`](concepts/) | Architecture concepts: cache, message bus, orders, actors, clock |
| [`getting_started/`](getting_started/) | Installation, first strategy, backtesting tutorials |
| [`developer_guide/`](developer_guide/) | Contributor guide, Rust/Cython internals, testing |
| [`how_to/`](how_to/) | Recipes for common tasks |
| [`integrations/`](integrations/) | Per-adapter setup and configuration |
| [`tutorials/`](tutorials/) | End-to-end worked examples |

---

## Tags

`algorithmic-trading` `backtesting` `live-trading` `rust` `python` `pyo3` `event-driven` `multi-venue` `high-frequency` `quantitative-finance` `order-management` `market-making`
