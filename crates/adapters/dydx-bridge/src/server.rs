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

//! TCP server for dYdX execution bridge.
//!
//! This module implements a TCP server that receives serialized Nautilus
//! execution commands and translates them to dYdX v4 gRPC calls.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    config::Config,
    error::DydxBridgeResult,
    grpc_client::DydxGrpcClient,
    handlers::CommandHandler,
    rpc_server::RpcServer,
    signer::TransactionSigner,
    websocket::{DydxWebSocketClient, DydxWsEvent},
};

/// Bridge server state.
pub struct BridgeServer {
    config: Config,
    handler: Arc<Mutex<CommandHandler>>,
    rpc_server: RpcServer,
    ws_client: DydxWebSocketClient,
}

impl BridgeServer {
    /// Creates a new bridge server.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails (e.g., invalid mnemonic, connection failures).
    pub async fn new(config: Config) -> DydxBridgeResult<Self> {
        let signer = if let Some(ref mnemonic) = config.wallet_mnemonic {
            TransactionSigner::from_mnemonic(mnemonic, 0, 0)?
        } else {
            return Err(crate::error::DydxBridgeError::Config(
                "DYDX_WALLET_MNEMONIC is required".to_string(),
            ));
        };

        let grpc_client = DydxGrpcClient::connect(&config.grpc_endpoint).await?;

        let initial_block_height = 0;
        let wallet_address = config
            .wallet_address
            .clone()
            .unwrap_or_else(|| signer.account_id().to_string());
        let subaccount = config.subaccount;

        let handler = Arc::new(Mutex::new(CommandHandler::new(
            initial_block_height,
            wallet_address.clone(),
            subaccount,
            signer,
            grpc_client,
        )));

        let addr = config
            .socket_addr()
            .parse()
            .map_err(|e| crate::error::DydxBridgeError::Config(format!("Invalid socket address: {e}")))?;
        let rpc_server = RpcServer::new(addr, Arc::clone(&handler));
        let ws_client = DydxWebSocketClient::with_url(config.ws_endpoint.clone());

        Ok(Self {
            handler,
            rpc_server,
            ws_client,
            config,
        })
    }

    /// Starts the server and handles incoming commands.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start or encounters runtime errors.
    pub async fn start(mut self) -> DydxBridgeResult<()> {
        tracing::info!(
            "TCP server ready on {}",
            self.config.socket_addr()
        );

        let subaccount_info = self
            .config
            .wallet_address
            .as_ref()
            .map(|addr| (addr.clone(), self.config.subaccount));

        self.ws_client
            .connect_and_subscribe(subaccount_info)
            .await?;

        let mut event_rx = self
            .ws_client
            .take_event_receiver()
            .ok_or_else(|| crate::error::DydxBridgeError::Internal("Event receiver already taken".to_string()))?;

        let handler = Arc::clone(&self.handler);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    DydxWsEvent::BlockHeight(height) => {
                        tracing::debug!("Received block height update: {}", height);
                        handler.lock().await.update_block_height(height);
                    }
                    DydxWsEvent::OrderUpdate(data) => {
                        tracing::debug!("Received order update: {:?}", data);
                        // TODO: Process order updates
                    }
                    DydxWsEvent::FillUpdate(data) => {
                        tracing::debug!("Received fill update: {:?}", data);
                        // TODO: Process fill updates
                    }
                    DydxWsEvent::Connected => {
                        tracing::info!("WebSocket connected");
                    }
                    DydxWsEvent::Disconnected => {
                        tracing::warn!("WebSocket disconnected");
                    }
                    DydxWsEvent::Error(e) => {
                        tracing::error!("WebSocket error: {}", e);
                    }
                }
            }
        });

        self.rpc_server.serve().await?;

        Ok(())
    }
}

/// Runs the TCP command server.
///
/// # Errors
///
/// Returns an error if the server fails to start or encounters runtime errors.
pub async fn run(config: Config) -> anyhow::Result<()> {
    tracing::info!("Initializing dYdX bridge server...");

    let server = BridgeServer::new(config).await?;
    server.start().await?;

    Ok(())
}
