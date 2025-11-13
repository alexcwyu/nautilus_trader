// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as published
//  by the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program. If not, see <https://www.gnu.org/licenses/>.
// -------------------------------------------------------------------------------------------------

//! Configuration for the dYdX bridge server.

use clap::Parser;

/// Configuration for the dYdX execution bridge.
#[derive(Debug, Parser)]
#[command(name = "nautilus-dydx-bridge")]
#[command(about = "AGPL-licensed execution bridge for dYdX v4")]
#[command(version)]
pub struct Config {
    /// Host address to bind to
    #[arg(long, env = "DYDX_BRIDGE_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to listen on
    #[arg(short = 'p', long, env = "DYDX_BRIDGE_PORT", default_value = "8420")]
    pub port: u16,

    /// dYdX v4 gRPC endpoint URL
    #[arg(long, env = "DYDX_GRPC_ENDPOINT", default_value = "http://localhost:9090")]
    pub grpc_endpoint: String,

    /// dYdX v4 WebSocket endpoint URL
    #[arg(long, env = "DYDX_WS_ENDPOINT", default_value = "wss://indexer.dydx.trade/v4/ws")]
    pub ws_endpoint: String,

    /// Wallet mnemonic for signing transactions
    #[arg(long, env = "DYDX_WALLET_MNEMONIC")]
    pub wallet_mnemonic: Option<String>,

    /// Wallet address (bech32 encoded dydx address)
    #[arg(long, env = "DYDX_WALLET_ADDRESS")]
    pub wallet_address: Option<String>,

    /// Subaccount number
    #[arg(long, env = "DYDX_SUBACCOUNT", default_value = "0")]
    pub subaccount: u32,
}

impl Config {
    /// Returns the full socket address to bind to.
    #[must_use]
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
