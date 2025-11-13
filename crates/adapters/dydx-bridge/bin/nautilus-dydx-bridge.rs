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

//! Binary entry point for the dYdX v4 execution bridge.

use clap::Parser;
use dotenvy::dotenv;
use nautilus_dydx_bridge::{run, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file if present
    let _ = dotenv();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::parse();

    tracing::info!(
        "Starting nautilus-dydx-bridge v{} (AGPL-3.0-only)",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!("Listening on: {}:{}", config.host, config.port);

    if config.wallet_mnemonic.is_none() {
        tracing::error!("DYDX_WALLET_MNEMONIC not set - this is required for signing transactions");
        std::process::exit(1);
    }
    if config.wallet_address.is_none() {
        tracing::warn!("DYDX_WALLET_ADDRESS not set - deriving from mnemonic");
    }

    // Run the Cap'n Proto RPC server
    run(config).await?;

    Ok(())
}
