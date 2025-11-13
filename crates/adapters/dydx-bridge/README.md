# nautilus-dydx-bridge

**AGPL-3.0-only licensed execution bridge for dYdX v4**

This binary acts as an execution bridge between Nautilus Trader and dYdX v4's Order
Execution Gateway Service (OEGS). It receives standard Nautilus execution commands
via Cap'n Proto RPC and translates them to dYdX v4 gRPC calls.

## Why is this bridge necessary?

dYdX v4's protocol buffer definitions and generated code are licensed under AGPL-3.0,
which requires any software that links against them to also be AGPL-3.0 licensed.
This is incompatible with Nautilus Trader's LGPL-3.0 license.

To maintain Nautilus Trader's more permissive licensing while still supporting dYdX v4
execution, this bridge operates as a **separate process** that:

1. Links against dYdX v4's AGPL-licensed protobuf code.
2. Runs as a standalone binary (never linked into the main Nautilus codebase).
3. Communicates via IPC using standard Nautilus commands/events.
4. Is completely optional - users who don't need dYdX v4 execution never use it.

This architectural separation ensures that:

- The main Nautilus codebase remains LGPL-3.0 licensed.
- dYdX v4 execution functionality is available to users who need it.
- AGPL-3.0 licensing obligations only apply to this isolated bridge binary.

## License

This crate is licensed under AGPL-3.0-only. It is **excluded** from the main
Nautilus workspace and never linked into other crates.

## Architecture

- **Cap'n Proto RPC Server**: Receives standard Nautilus execution commands.
- **Command Translator**: Converts Nautilus commands to dYdX v4 protobuf messages.
- **dYdX OEGS Client**: Forwards transactions to dYdX v4 chain via gRPC.
- **Event Publisher**: Returns standard Nautilus execution events.

### dYdX Protocol Buffer Integration

The bridge uses the [`dydx-proto`](https://crates.io/crates/dydx-proto) crate (v0.4.0), which provides Rust bindings for dYdX v4's AGPL-licensed protocol buffers. Key message types:

- `MsgPlaceOrder` - Submit orders to the orderbook
- `MsgCancelOrder` - Cancel existing orders
- `Order` - Order details (quantums, subticks, time-in-force, etc.)
- `OrderId` - Order identifier with subaccount info
- `SubaccountId` - Subaccount identification

The translation layer (`src/translator.rs`) handles bidirectional conversion between Nautilus domain types and these protobuf messages.

## Configuration

Configure via environment variables or CLI arguments:

- `DYDX_BRIDGE_HOST`: Bind address (default: `127.0.0.1`)
- `DYDX_BRIDGE_PORT`: Port to listen on (default: `8420`)
- `DYDX_GRPC_URL`: dYdX v4 gRPC endpoint
- `DYDX_MNEMONIC`: Wallet mnemonic for signing transactions
- `DYDX_WALLET_ADDRESS`: Wallet address (bech32 encoded dydx address)
- `DYDX_SUBACCOUNT`: Subaccount number (default: `0`)

## Usage

```bash
# Run the bridge
nautilus-dydx-bridge --port 8420

# With environment variables
export DYDX_GRPC_URL="grpc.dydx.network:443"
export DYDX_MNEMONIC="your mnemonic here"
nautilus-dydx-bridge
```

## Building

This crate is excluded from the main workspace. Build separately:

```bash
cd crates/adapters/dydx-bridge
cargo build --release
```
