# nautilus-architect

Architect exchange integration adapter for the Nautilus trading engine.

## Overview

Architect is an institutional financial technology platform providing modern infrastructure for regulated trading across multiple asset classes.
This adapter integrates Architect's perpetual futures trading capabilities with NautilusTrader.

**Platform type**: Institutional multi-asset brokerage and trading technology provider.
**Regulatory status**: SEC-registered broker-dealer and NFA-registered introducing broker.
**Initial support**: Perpetual futures on Architect's AX exchange.

## Platform

[NautilusTrader](http://nautilustrader.io) is an open-source, high-performance, production-grade
algorithmic trading platform, providing quantitative traders with the ability to backtest
portfolios of automated trading strategies on historical data with an event-driven engine,
and also deploy those same strategies live, with no code changes.

NautilusTrader's design, architecture, and implementation philosophy prioritizes software correctness and safety at the
highest level, with the aim of supporting mission-critical, trading system backtesting and live deployment workloads.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `extension-module`: Builds as a Python extension module (used with `python`).

## Documentation

See [the docs](https://docs.rs/nautilus-bitmex) for more detailed usage.

## Supported instruments

- Perpetual Futures (FX, rates, metals, and other traditional assets)

## Authentication

Architect uses **bearer token authentication** via HTTP headers. API credentials include:

- API key/secret for session token generation
- Bearer token for API requests

## API documentation

- **Main Website**: <https://architect.co/>
- **API Reference**: <https://docs.sandbox.x.architect.co/api-reference/>
- **Sandbox API**: `https://sandbox.x.architect.co/api`
- **Production API**: `https://x.architect.co/api`

## Development status

This adapter is currently under development. The initial implementation focuses on:

- REST API client for market data and order management.
- WebSocket client for real-time data and execution updates.
- Support for perpetual futures trading.

## Architecture

Following NautilusTrader's standardized adapter patterns:

- Two-layer HTTP client (raw + domain).
- Two-layer WebSocket client (outer orchestrator + inner high-performance I/O handler).
- Bearer token authentication and signing.
- Python bindings via PyO3.

## Usage

Run example binaries to test the adapter:

```bash
# HTTP client example
cargo run -p nautilus-architect --bin architect-http

# WebSocket data client example
cargo run -p nautilus-architect --bin architect-ws-data

# WebSocket execution client example
cargo run -p nautilus-architect --bin architect-ws-exec
```

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).
Contributions to the project are welcome and require the completion of a standard [Contributor License Agreement (CLA)](https://github.com/nautechsystems/nautilus_trader/blob/develop/CLA.md).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="400" height="auto"/>

© 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
