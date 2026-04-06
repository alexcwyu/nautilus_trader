# NautilusTrader — Workflow

> **Last Updated**: 2026-04-07T00:00:00Z
> **Git Hash**: `aa60b11`

## Trading Pipeline Overview

The following sequence diagram shows the complete path from strategy signal to fill confirmation, which is identical in both backtest and live modes.

```mermaid
sequenceDiagram
    participant DataSrc as Data Source<br/>(Feed / File)
    participant DataEng as DataEngine
    participant MsgBus as MessageBus
    participant Strategy as Strategy / Actor
    participant RiskEng as RiskEngine
    participant ExecEng as ExecutionEngine
    participant OrderEmul as OrderEmulator
    participant Venue as Venue / SimulatedExchange

    DataSrc->>DataEng: raw market event<br/>(quote/trade/bar/book)
    DataEng->>MsgBus: publish typed data event
    MsgBus->>Strategy: on_quote_tick / on_bar / …
    Strategy->>MsgBus: submit_order(TradingCommand)
    MsgBus->>RiskEng: pre-trade risk check
    RiskEng-->>MsgBus: approved / denied
    alt conditional order (stop/trailing)
        MsgBus->>OrderEmul: emulate locally
        OrderEmul-->>MsgBus: release on trigger
    end
    MsgBus->>ExecEng: route TradingCommand
    ExecEng->>Venue: submit order (REST/WS)
    Venue-->>ExecEng: OrderAccepted / OrderFilled
    ExecEng->>MsgBus: publish OrderEventAny
    MsgBus->>Strategy: on_order_filled
    ExecEng->>MsgBus: publish PositionEvent
    MsgBus->>Strategy: on_position_changed
```

**Source refs**:
- `crates/common/src/msgbus/mod.rs` — message bus routing
- `crates/execution/src/engine/` — execution engine command handling
- `crates/risk/src/` — risk engine pre-trade checks
- `crates/execution/src/order_emulator/` — conditional order emulation

---

## Backtest Data Flow

```mermaid
flowchart TD
    Catalog["Data Catalog\n(Parquet files via DataFusion)"]
    Iterator["BacktestDataIterator\n(chronological merge of all feeds)"]
    Accumulator["TimeEventAccumulator\n(merges timer events with data)"]
    Engine["BacktestEngine.run()"]
    SimExchange["SimulatedExchange\n(per venue)"]
    MatchingEng["SimulatedMatchingEngine\n(FillModel + FeeModel + LatencyModel)"]
    Cache["Cache\n(in-memory state)"]
    Strategy["Strategy"]

    Catalog --> Iterator
    Iterator --> Accumulator
    Accumulator --> Engine
    Engine -->|"next data event (UnixNanos)"| SimExchange
    Engine -->|"advance TestClock"| Strategy
    SimExchange --> MatchingEng
    MatchingEng -->|"OrderFilled event"| Cache
    Cache -->|"state update"| Strategy
    Strategy -->|"submit_order"| Engine
```

Key design properties:
- All events are processed in **strict nanosecond chronological order**.
- The `TestClock` is advanced to exactly the `ts_event` of each data point, ensuring strategies see realistic time when querying `clock.timestamp_ns()`.
- `SimulatedExchange` maintains a synthetic order book and applies configurable `FillModel` (probabilistic fill) and `LatencyModel` (simulated round-trip delay).
- Multiple venues and instruments run simultaneously in a single engine pass.

**Source refs**:
- `crates/backtest/src/engine.rs` — `BacktestEngine::run_sync()`
- `crates/backtest/src/data_iterator.rs` — chronological data merge
- `crates/backtest/src/accumulator.rs` — time-event accumulation
- `crates/backtest/src/exchange.rs` — `SimulatedExchange`
- `crates/execution/src/models/` — fill, fee, latency models

---

## Live Trading Startup Sequence

```mermaid
sequenceDiagram
    participant User
    participant LiveNode
    participant DataClients as Data Clients
    participant ExecClients as Execution Clients
    participant Cache

    User->>LiveNode: node.run()
    LiveNode->>DataClients: connect() — phase 1
    DataClients-->>Cache: instruments arrive as buffered DataEvents
    LiveNode->>LiveNode: flush_pending_data()<br/>drain channel receivers until empty
    LiveNode->>ExecClients: connect() — phase 2<br/>(instruments now in Cache)
    ExecClients-->>Cache: load_instruments_from_cache()
    LiveNode->>LiveNode: reconcile inflight orders
    loop tokio::select! event loop
        LiveNode->>LiveNode: multiplex DataEvent | ExecutionEvent | TradingCommand | Timer
    end
```

The two-phase startup ensures instruments are in the `Cache` before execution clients attempt to load them, avoiding a race condition between data and execution subscriptions.

**Source ref**: `crates/live/src/node.rs` — startup sequencing doc-comment

---

## Event Lifecycle

Every state change in the system flows through an **event** published on the `MessageBus`. Events are immutable value objects carrying `ts_event` (when it happened on the venue) and `ts_init` (when it was created in the system).

```
Order Events
├── OrderInitialized       — strategy calls submit_order()
├── OrderDenied            — risk check failed
├── OrderEmulated          — routed to OrderEmulator
├── OrderReleased          — conditional trigger fired
├── OrderSubmitted         — sent to venue
├── OrderAccepted          — venue acknowledgement
├── OrderRejected          — venue rejected
├── OrderTriggered         — stop price hit (stop orders)
├── OrderPendingUpdate     — modify request in-flight
├── OrderPendingCancel     — cancel request in-flight
├── OrderCanceled          — cancel confirmed
├── OrderExpired           — GTD expiry
├── OrderPartiallyFilled   — partial fill received
└── OrderFilled            — complete fill received

Position Events
├── PositionOpened         — first fill opens a position
├── PositionChanged        — subsequent fills modify quantity/side
└── PositionClosed         — net quantity reaches zero
```

**Source refs**:
- `crates/model/src/events/order/` — order event structs
- `crates/model/src/events/position/` — position event structs
- `crates/model/src/enums.rs` — `OrderStatus` enum (lines 1266–1295)

---

## See Also

- [`architecture.md`](architecture.md) — crate breakdown and component responsibilities
- [`state-management.md`](state-management.md) — order state machine and position lifecycle
- [`concepts/`](concepts/) — upstream docs on message bus, cache, actors
- [`getting_started/`](getting_started/) — first strategy walkthrough
