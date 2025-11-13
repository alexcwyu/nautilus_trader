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

//! WebSocket client for dYdX v4 Indexer.
//!
//! Provides subscriptions to:
//! - Block height updates (for good_til_block calculations)
//! - Subaccount updates (for order status, fills, positions)

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::error::{DydxBridgeError, DydxBridgeResult};

/// Default dYdX mainnet WebSocket URL
const DYDX_WS_URL: &str = "wss://indexer.dydx.trade/v4/ws";

/// WebSocket message types from dYdX indexer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DydxWsMessage {
    /// Connection established
    #[serde(rename = "connected")]
    Connected {
        #[serde(rename = "connection_id")]
        connection_id: String,
    },
    /// Channel data update
    #[serde(rename = "channel_data")]
    ChannelData {
        #[serde(rename = "connection_id")]
        connection_id: String,
        #[serde(rename = "message_id")]
        message_id: u64,
        channel: String,
        id: Option<String>,
        contents: serde_json::Value,
    },
    /// Subscription response
    #[serde(rename = "subscribed")]
    Subscribed {
        #[serde(rename = "connection_id")]
        connection_id: String,
        #[serde(rename = "message_id")]
        message_id: u64,
        channel: String,
        id: Option<String>,
    },
    /// Unsubscription response
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        #[serde(rename = "connection_id")]
        connection_id: String,
        #[serde(rename = "message_id")]
        message_id: u64,
        channel: String,
        id: Option<String>,
    },
    /// Error response
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "connection_id")]
        connection_id: String,
        #[serde(rename = "message_id")]
        message_id: u64,
        message: String,
    },
}

/// Block height update from dYdX chain
#[derive(Debug, Clone, Deserialize)]
pub struct BlockHeightUpdate {
    pub height: String,
    pub time: String,
}

/// Subaccount update from dYdX indexer
#[derive(Debug, Clone, Deserialize)]
pub struct SubaccountUpdate {
    pub orders: Option<Vec<serde_json::Value>>,
    pub fills: Option<Vec<serde_json::Value>>,
    pub positions: Option<Vec<serde_json::Value>>,
}

/// Events emitted by the WebSocket client
#[derive(Debug, Clone)]
pub enum DydxWsEvent {
    /// Block height updated
    BlockHeight(u64),
    /// Subaccount order update
    OrderUpdate(serde_json::Value),
    /// Subaccount fill update
    FillUpdate(serde_json::Value),
    /// Connection established
    Connected,
    /// Connection closed
    Disconnected,
    /// Error occurred
    Error(String),
}

/// Simplified WebSocket client for dYdX v4 indexer.
pub struct DydxWebSocketClient {
    url: String,
    event_tx: mpsc::UnboundedSender<DydxWsEvent>,
    event_rx: Option<mpsc::UnboundedReceiver<DydxWsEvent>>,
}

impl DydxWebSocketClient {
    /// Creates a new WebSocket client.
    #[must_use]
    pub fn new() -> Self {
        Self::with_url(DYDX_WS_URL.to_string())
    }

    /// Creates a new WebSocket client with a custom URL.
    #[must_use]
    pub fn with_url(url: String) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            url,
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    /// Takes the event receiver channel.
    ///
    /// This can only be called once.
    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<DydxWsEvent>> {
        self.event_rx.take()
    }

    /// Connects to the dYdX indexer WebSocket and subscribes to channels.
    ///
    /// # Errors
    ///
    /// Returns an error if connection or subscription fails.
    pub async fn connect_and_subscribe(
        &self,
        subaccount_address: Option<(String, u32)>,
    ) -> DydxBridgeResult<()> {
        tracing::info!("Connecting to dYdX WebSocket: {}", self.url);

        let (ws_stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| DydxBridgeError::Internal(format!("WebSocket connection failed: {e}")))?;

        tracing::info!("WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to block height
        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "channel": "v4_block_height",
        });
        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .map_err(|e| DydxBridgeError::Internal(format!("Failed to subscribe: {e}")))?;

        tracing::info!("Subscribed to block height channel");

        // Subscribe to subaccount if provided
        if let Some((address, subaccount)) = subaccount_address {
            let subscribe_msg = serde_json::json!({
                "type": "subscribe",
                "channel": "v4_subaccounts",
                "id": format!("{}/{}", address, subaccount),
            });
            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await
                .map_err(|e| DydxBridgeError::Internal(format!("Failed to subscribe: {e}")))?;

            tracing::info!("Subscribed to subaccount channel: {}/{}", address, subaccount);
        }

        // Spawn message handler
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Err(e) = handle_message(&text, &event_tx).await {
                            tracing::error!("Failed to handle message: {}", e);
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::info!("WebSocket closed");
                        let _ = event_tx.send(DydxWsEvent::Disconnected);
                        break;
                    }
                    Ok(Message::Ping(_)) => {
                        // Auto-handled by tungstenite
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("WebSocket error: {}", e);
                        let _ = event_tx.send(DydxWsEvent::Error(e.to_string()));
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}

impl Default for DydxWebSocketClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Handles an incoming WebSocket message.
async fn handle_message(
    text: &str,
    event_tx: &mpsc::UnboundedSender<DydxWsEvent>,
) -> DydxBridgeResult<()> {
    let msg: DydxWsMessage = serde_json::from_str(text)
        .map_err(|e| DydxBridgeError::Internal(format!("Failed to parse message: {e}")))?;

    match msg {
        DydxWsMessage::Connected { connection_id } => {
            tracing::info!("WebSocket connected: {}", connection_id);
            let _ = event_tx.send(DydxWsEvent::Connected);
        }
        DydxWsMessage::ChannelData {
            channel, contents, ..
        } => {
            if channel == "v4_block_height" {
                handle_block_height_update(&contents, event_tx)?;
            } else if channel == "v4_subaccounts" {
                handle_subaccount_update(&contents, event_tx)?;
            }
        }
        DydxWsMessage::Subscribed {
            channel,
            id,
            message_id,
            ..
        } => {
            tracing::info!(
                "Subscribed to {} (id: {:?}, msg: {})",
                channel,
                id,
                message_id
            );
        }
        DydxWsMessage::Unsubscribed { channel, .. } => {
            tracing::info!("Unsubscribed from {}", channel);
        }
        DydxWsMessage::Error { message, .. } => {
            tracing::error!("WebSocket error: {}", message);
            let _ = event_tx.send(DydxWsEvent::Error(message));
        }
    }

    Ok(())
}

/// Handles a block height update message.
fn handle_block_height_update(
    contents: &serde_json::Value,
    event_tx: &mpsc::UnboundedSender<DydxWsEvent>,
) -> DydxBridgeResult<()> {
    let update: BlockHeightUpdate = serde_json::from_value(contents.clone())
        .map_err(|e| DydxBridgeError::Internal(format!("Failed to parse block height: {e}")))?;

    let height: u64 = update
        .height
        .parse()
        .map_err(|e| DydxBridgeError::Internal(format!("Invalid block height: {e}")))?;

    tracing::debug!("Block height: {}", height);
    let _ = event_tx.send(DydxWsEvent::BlockHeight(height));

    Ok(())
}

/// Handles a subaccount update message.
fn handle_subaccount_update(
    contents: &serde_json::Value,
    event_tx: &mpsc::UnboundedSender<DydxWsEvent>,
) -> DydxBridgeResult<()> {
    let update: SubaccountUpdate = serde_json::from_value(contents.clone())
        .map_err(|e| DydxBridgeError::Internal(format!("Failed to parse subaccount update: {e}")))?;

    // Send order updates
    if let Some(orders) = update.orders {
        for order in orders {
            let _ = event_tx.send(DydxWsEvent::OrderUpdate(order));
        }
    }

    // Send fill updates
    if let Some(fills) = update.fills {
        for fill in fills {
            let _ = event_tx.send(DydxWsEvent::FillUpdate(fill));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = DydxWebSocketClient::new();
        assert_eq!(client.url, DYDX_WS_URL);
    }

    #[test]
    fn test_parse_block_height_message() {
        let json = serde_json::json!({
            "type": "channel_data",
            "connection_id": "test",
            "message_id": 1,
            "channel": "v4_block_height",
            "contents": {
                "height": "12345",
                "time": "2024-01-01T00:00:00.000Z"
            }
        });

        let msg: DydxWsMessage = serde_json::from_value(json).unwrap();
        assert!(matches!(msg, DydxWsMessage::ChannelData { .. }));
    }
}
