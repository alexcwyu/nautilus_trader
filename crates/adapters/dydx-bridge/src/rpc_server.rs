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

//! TCP server for receiving serialized Nautilus execution commands.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::{error::DydxBridgeResult, handlers::CommandHandler};

/// TCP server for dYdX bridge.
pub struct RpcServer {
    addr: SocketAddr,
    handler: Arc<Mutex<CommandHandler>>,
}

impl RpcServer {
    /// Creates a new TCP server.
    pub fn new(addr: SocketAddr, handler: Arc<Mutex<CommandHandler>>) -> Self {
        Self { addr, handler }
    }

    /// Starts the TCP server and listens for connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind or encounters runtime errors.
    pub async fn serve(self) -> DydxBridgeResult<()> {
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Command server listening on {}", self.addr);

        let handler = self.handler;

        loop {
            let (mut stream, client_addr) = listener.accept().await?;
            tracing::info!("Accepted connection from {}", client_addr);

            let _handler = Arc::clone(&handler);

            tokio::spawn(async move {
                let mut buffer = Vec::new();

                match stream.read_to_end(&mut buffer).await {
                    Ok(n) => {
                        tracing::debug!("Received {} bytes from {}", n, client_addr);

                        // TODO: Deserialize command using nautilus-serialization
                        // TODO: Route to appropriate handler method based on command type
                        tracing::warn!(
                            "Command deserialization not yet implemented - received {} bytes",
                            n
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to read from {}: {}", client_addr, e);
                    }
                }
            });
        }
    }
}
