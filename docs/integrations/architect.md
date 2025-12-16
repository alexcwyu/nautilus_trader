# Architect

[Architect](https://architect.co) is an institutional financial technology platform providing modern
infrastructure for regulated trading across multiple asset classes. It operates as an SEC-registered
broker-dealer and NFA-registered introducing broker. This integration supports live market data ingest
and order execution for equities, futures, and perpetual futures.

:::warning
This integration is currently under construction and not yet ready for use.
:::

## Overview

This adapter is implemented in Rust, with optional Python bindings for use in Python-based workflows.
Architect uses REST for market data and order management, with WebSocket for real-time data and
execution updates.

The adapter includes the following components:

- `ArchitectHttpClient`: Low-level HTTP API connectivity.
- `ArchitectWebSocketClient`: Low-level WebSocket API connectivity.
- `ArchitectInstrumentProvider`: Instrument parsing and loading functionality.
- `ArchitectDataClient`: Market data feed manager.
- `ArchitectExecutionClient`: Account management and trade execution gateway.

## Supported instruments

| Product Type        | Data Feed | Trading | Notes                                        |
|---------------------|-----------|---------|----------------------------------------------|
| Perpetual Futures   | ✓         | ✓       | FX, rates, metals, and traditional assets.   |
| Futures             | Planned   | Planned | Traditional futures contracts.               |
| Equities            | Planned   | Planned | US equities.                                 |

## Authentication

Architect uses **bearer token authentication** via HTTP headers:

- API key and secret are used to obtain a session token.
- The session token is then used as a bearer token for subsequent API requests.

## API documentation

- **Main website**: <https://architect.co/>
- **API reference**: <https://docs.sandbox.x.architect.co/api-reference/>
- **Sandbox API**: `https://sandbox.x.architect.co/api`
- **Production API**: `https://x.architect.co/api`
