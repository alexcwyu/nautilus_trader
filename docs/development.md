# NautilusTrader — Development Guide

> **Last Updated**: 2026-04-07T00:00:00Z
> **Git Hash**: `aa60b11`

## Prerequisites

| Tool | Minimum Version | Purpose |
|---|---|---|
| Rust | 1.94.1 (MSRV = latest stable) | Core engine compilation |
| Python | 3.12 | Control plane and tests |
| uv | latest | Python package and venv management |
| clang | any recent | Required by several C dependencies |
| git | 2.x | Version control |

> **Note on MSRV**: NautilusTrader tracks the latest stable Rust release as its MSRV. Always use `rustup update stable` before building.

---

## Environment Setup

### 1. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
# Verify
rustc --version   # must be >= 1.94.1
cargo --version
```

### 2. Install uv (Python package manager)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Clone and initialize the repo

```bash
git clone https://github.com/nautechsystems/nautilus_trader.git
cd nautilus_trader
```

### 4. Install the package (release build with all dependencies)

```bash
make install
```

This builds the Rust extension with `opt-level = 3`, `lto = "fat"`, and `codegen-units = 1` (see `Cargo.toml` `[profile.release]`).

### 5. Install in debug mode (faster compile, slower runtime)

```bash
make install-debug
```

Debug profile: `opt-level = 0`, `debug = false` (for compile speed), `debug-assertions = false` (avoids Cython assertion issues).

---

## Project Structure

```
nautilus_trader/            ← repo root
├── Cargo.toml              ← Rust workspace manifest (version 0.55.0)
├── pyproject.toml          ← Python package (version 1.225.0)
├── Makefile                ← Primary build automation
├── build.py                ← Poetry/Cython build script
├── rust-toolchain.toml     ← Pins Rust version for reproducibility
├── crates/                 ← All Rust source code
│   ├── core/               ← UUID4, UnixNanos, datetime utils
│   ├── model/              ← Domain types (orders, positions, instruments)
│   ├── common/             ← MessageBus, Cache, Clock, Actor
│   ├── data/               ← DataEngine
│   ├── execution/          ← ExecutionEngine, OrderEmulator, RiskEngine
│   ├── backtest/           ← BacktestEngine, SimulatedExchange
│   ├── live/               ← LiveNode (tokio event loop)
│   ├── portfolio/          ← Portfolio aggregation
│   ├── risk/               ← Pre-trade risk checks
│   ├── persistence/        ← Parquet catalog (DataFusion)
│   ├── infrastructure/     ← Redis / Postgres adapters
│   ├── network/            ← WebSocket + HTTP clients
│   ├── pyo3/               ← PyO3 Python module
│   ├── serialization/      ← Arrow / msgpack / JSON
│   └── adapters/           ← Exchange adapters (binance, bybit, kraken, …)
├── nautilus_trader/        ← Python package
│   ├── trading/            ← Strategy, Trader, ExecutionAlgorithm
│   ├── backtest/           ← BacktestNode, BacktestEngineConfig
│   ├── live/               ← TradingNode, LiveDataEngine
│   ├── adapters/           ← Python adapter shims (IB, Betfair, …)
│   ├── config/             ← Config dataclasses (msgspec-based)
│   ├── cache/              ← Cache Python wrapper
│   ├── data/               ← Data types Python wrappers
│   ├── model/              ← Model Python wrappers
│   └── persistence/        ← Catalog Python API
├── docs/                   ← Documentation
│   ├── README.md           ← Overview (this project's custom docs)
│   ├── architecture.md
│   ├── workflow.md
│   ├── state-management.md
│   ├── development.md
│   ├── api_reference/      ← Upstream API reference
│   ├── concepts/           ← Upstream concept guides
│   ├── getting_started/    ← Upstream tutorials
│   ├── integrations/       ← Per-adapter guides
│   └── tutorials/
├── tests/                  ← Python test suite (pytest)
├── examples/               ← Example strategies and configs
└── schema/                 ← Cap'n Proto schemas
```

**Source refs**:
- `Cargo.toml` — workspace members and dependency versions
- `pyproject.toml` — Python project metadata, optional extras, dev dependency groups
- `Makefile` — all build, test, and lint targets

---

## Build Commands

### Rust builds

```bash
# Full release build (used for installed wheel)
make build

# Debug build (faster compile, slower runtime)
make build-debug

# Build with optional hypersync feature
make build HYPERSYNC=true

# Build with DeFi features disabled
make build DEFI=false

# Check compilation without linking
cargo check --all-features
```

### Python / Cython builds

```bash
# Install release (compiles Rust + Cython)
make install

# Install debug
make install-debug

# Run tests (Python pytest + Rust nextest)
make test

# Run only Rust tests
make cargo-test

# Run only Python tests
make pytest

# Run tests with coverage
make test-coverage
```

### Code quality

```bash
# Format Rust code
cargo fmt

# Lint Rust (clippy with workspace config)
cargo clippy --all-features

# Auto-fix clippy warnings
make clippy-fix

# Lint Python (ruff)
uv run ruff check

# Type-check Python
uv run mypy nautilus_trader

# Run pre-commit hooks (includes ruff + mypy + rustfmt)
pre-commit run --all-files
```

---

## Configuration Reference

All configuration objects are `msgspec`-based dataclasses defined in `nautilus_trader/config/`. The key classes:

| Config Class | Purpose | Key Fields |
|---|---|---|
| `BacktestEngineConfig` | Single backtest run engine | `strategies`, `actors`, `risk_engine`, `exec_engine`, `cache` |
| `BacktestRunConfig` | Complete run specification | `engine`, `venues`, `data`, `chunk_size` |
| `BacktestVenueConfig` | Simulated venue parameters | `name`, `oms_type`, `account_type`, `base_currency`, `starting_balances`, `book_type` |
| `BacktestDataConfig` | Historical data spec | `catalog_path`, `data_cls`, `instrument_id`, `start_time`, `end_time` |
| `TradingNodeConfig` | Live trading node | `trader_id`, `log_level`, `data_clients`, `exec_clients`, `cache` |
| `RiskEngineConfig` | Pre-trade risk limits | `bypass`, `max_order_submit_rate`, `max_notional_per_order` |
| `CacheConfig` | Cache behaviour | `tick_capacity`, `bar_capacity`, `database` (Redis URL) |
| `LoggingConfig` | Log level and output | `log_level`, `log_file_path`, `log_format` |
| `ImportableStrategyConfig` | Strategy reference | `strategy_path`, `config_path`, `config` (dict) |

### Venue OMS Types

| Value | Description |
|---|---|
| `NETTING` | All positions in same instrument are netted |
| `HEDGING` | Long and short positions maintained separately |

### Account Types

| Value | Description |
|---|---|
| `CASH` | No leverage; assets only |
| `MARGIN` | Leveraged with collateral |
| `BETTING` | Betting exchange account |

---

## Troubleshooting

### 1. Rust compiler version mismatch

**Symptom**: `error[E0XXX]` on stable Rust features; compiler rejecting valid syntax.

**Fix**: Update Rust to the exact version pinned in `rust-toolchain.toml`:
```bash
rustup update stable
rustc --version
```
The MSRV is always the latest stable; older Rust versions are not supported.

---

### 2. `clang` not found during build

**Symptom**: `error: could not find native library 'clang'` or linker errors involving C dependencies (`ed25519-blake2b`, `aws-lc-rs`).

**Fix**:
```bash
# Ubuntu / Debian
sudo apt-get install clang

# macOS
xcode-select --install
# or: brew install llvm

# Arch Linux
sudo pacman -S clang
```
The `Makefile` sets `CC=clang` and `CXX=clang++` by default.

---

### 3. `glibc` version too old (Linux binary wheels)

**Symptom**: `ImportError: /lib/x86_64-linux-gnu/libc.so.6: version 'GLIBC_2.35' not found`

**Fix**: Confirm your glibc version:
```bash
ldd --version
```
Binary wheels require glibc ≥ 2.35 (Ubuntu 22.04+, Debian 12+). On older systems, build from source or use Docker.

---

### 4. Cython `debug-assertions` build failure

**Symptom**: Build fails with assertion errors from Cython when using a Rust debug build.

**Fix**: The `[profile.dev]` in `Cargo.toml` deliberately sets `debug-assertions = false` to avoid this. If you customized the profile, revert that flag.

---

### 5. Redis connection refused (optional persistence)

**Symptom**: `CacheDatabaseAdapter` raises connection error on startup.

**Fix**: Start Redis:
```bash
docker run -d -p 6379:6379 redis:latest
```
Or set `CacheConfig(database=None)` to disable persistence. The system works fully in-memory without Redis.

---

### 6. `hypersync` feature build failure on Windows

**Symptom**: Compilation error in `nautilus-blockchain` when building with `hypersync` feature on Windows.

**Fix**: The `hypersync` feature is only supported on Linux and macOS. Do not set `HYPERSYNC=true` on Windows. The standard Windows build omits this feature automatically.

---

### 7. High-precision mode unavailable on Windows

**Symptom**: Python wheels on Windows do not support 128-bit `Price`/`Quantity`/`Money`.

**Explanation**: MSVC does not support `__int128`, blocking the Cython/FFI layer. Pure Rust crates support `i128`/`u128` on all platforms. Python wheels on Windows are always standard-precision (64-bit, ≤9 decimal places).

---

## Security

- **Supply-chain vetting**: All dependencies are audited via `cargo-vet` (`.supply-chain/` directory). Run `cargo vet` to verify.
- **Dependency audit**: `cargo audit` checks for known CVEs in dependencies.
- **Cryptography**: TLS via `rustls` + `aws-lc-rs`; Ed25519 signing via `ed25519-dalek`; secrets zeroized on drop via `zeroize`.
- **API keys**: Use environment variables or a secrets manager. Never hardcode credentials. The `dotenvy` crate loads `.env` files in development.
- **LGPL-3.0**: Modifications to the library must be disclosed. Strategies and application code using the library as a dependency are not affected.

**Source ref**: `SECURITY.md`, `deny.toml` (dependency license/vulnerability policy), `osv-scanner.toml`

---

## See Also

- [`architecture.md`](architecture.md) — crate-level design
- [`workflow.md`](workflow.md) — data and event flow
- [`state-management.md`](state-management.md) — order and position lifecycle
- [`developer_guide/`](developer_guide/) — upstream contributor guide (Rust internals, Cython, CI)
- [`getting_started/`](getting_started/) — installation and first strategy tutorial
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — contribution guidelines
