// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Orders WebSocket message handler for Architect.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use nautilus_model::instruments::{Instrument, InstrumentAny};
use nautilus_network::websocket::{AuthTracker, WebSocketClient};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use crate::websocket::messages::{
    ArchitectOrdersWsMessage, ArchitectWsCancelOrder, ArchitectWsError, ArchitectWsGetOpenOrders,
    ArchitectWsPlaceOrder, OrderMetadata,
};

/// Commands sent from the outer client to the inner orders handler.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Set the WebSocket client for this handler.
    SetClient(WebSocketClient),
    /// Disconnect the WebSocket connection.
    Disconnect,
    /// Authenticate with the provided token.
    Authenticate {
        /// Bearer token for authentication.
        token: String,
    },
    /// Place an order.
    PlaceOrder {
        /// Request ID for correlation.
        request_id: i64,
        /// Order placement message.
        order: ArchitectWsPlaceOrder,
        /// Metadata for response correlation.
        metadata: OrderMetadata,
    },
    /// Cancel an order.
    CancelOrder {
        /// Request ID for correlation.
        request_id: i64,
        /// Order ID to cancel.
        order_id: String,
    },
    /// Get open orders.
    GetOpenOrders {
        /// Request ID for correlation.
        request_id: i64,
    },
    /// Initialize the instrument cache with instruments.
    InitializeInstruments(Vec<InstrumentAny>),
    /// Update a single instrument in the cache.
    UpdateInstrument(Box<InstrumentAny>),
}

/// Orders feed handler that processes WebSocket messages.
///
/// Runs in a dedicated Tokio task and owns the WebSocket client exclusively.
pub(crate) struct FeedHandler {
    signal: Arc<AtomicBool>,
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    #[allow(dead_code)] // TODO: Use for sending parsed messages
    out_tx: tokio::sync::mpsc::UnboundedSender<ArchitectOrdersWsMessage>,
    auth_tracker: AuthTracker,
    instruments: AHashMap<Ustr, InstrumentAny>,
    pending_orders: AHashMap<i64, OrderMetadata>,
    message_queue: VecDeque<ArchitectOrdersWsMessage>,
}

impl FeedHandler {
    /// Creates a new [`FeedHandler`] instance.
    #[must_use]
    pub fn new(
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<ArchitectOrdersWsMessage>,
        auth_tracker: AuthTracker,
    ) -> Self {
        Self {
            signal,
            client: None,
            cmd_rx,
            raw_rx,
            out_tx,
            auth_tracker,
            instruments: AHashMap::new(),
            pending_orders: AHashMap::new(),
            message_queue: VecDeque::new(),
        }
    }

    /// Returns the next message from the handler.
    ///
    /// This method blocks until a message is available or the handler is stopped.
    pub async fn next(&mut self) -> Option<ArchitectOrdersWsMessage> {
        loop {
            if let Some(msg) = self.message_queue.pop_front() {
                return Some(msg);
            }

            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }

                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    if self.signal.load(Ordering::Relaxed) {
                        tracing::debug!("Stop signal received during idle period");
                        return None;
                    }
                    continue;
                }

                msg = self.raw_rx.recv() => {
                    let msg = match msg {
                        Some(msg) => msg,
                        None => {
                            tracing::debug!("WebSocket stream closed");
                            return None;
                        }
                    };

                    if let Message::Ping(data) = &msg {
                        tracing::trace!("Received ping frame with {} bytes", data.len());
                        if let Some(client) = &self.client
                            && let Err(e) = client.send_pong(data.to_vec()).await
                        {
                            tracing::warn!(error = %e, "Failed to send pong frame");
                        }
                        continue;
                    }

                    if let Some(messages) = self.parse_raw_message(msg) {
                        self.message_queue.extend(messages);
                    }

                    if self.signal.load(Ordering::Relaxed) {
                        tracing::debug!("Stop signal received");
                        return None;
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: HandlerCommand) {
        match cmd {
            HandlerCommand::SetClient(client) => {
                tracing::debug!("WebSocketClient received by handler");
                self.client = Some(client);
            }
            HandlerCommand::Disconnect => {
                tracing::debug!("Disconnect command received");
                if let Some(client) = self.client.take() {
                    client.disconnect().await;
                }
            }
            HandlerCommand::Authenticate { token: _ } => {
                tracing::debug!("Authenticate command received");
                // Architect uses Bearer token in connection headers, not a message
                // This is handled at connection time, so we just mark as authenticated
                self.auth_tracker.succeed();
                self.message_queue
                    .push_back(ArchitectOrdersWsMessage::Authenticated);
            }
            HandlerCommand::PlaceOrder {
                request_id,
                order,
                metadata,
            } => {
                tracing::debug!(
                    request_id = request_id,
                    symbol = %order.s,
                    "PlaceOrder command received"
                );
                self.pending_orders.insert(request_id, metadata);

                if let Err(e) = self.send_json(&order).await {
                    tracing::error!(error = %e, "Failed to send place order message");
                    self.pending_orders.remove(&request_id);
                }
            }
            HandlerCommand::CancelOrder {
                request_id,
                order_id,
            } => {
                tracing::debug!(
                    request_id = request_id,
                    order_id = %order_id,
                    "CancelOrder command received"
                );
                self.send_cancel_order(request_id, &order_id).await;
            }
            HandlerCommand::GetOpenOrders { request_id } => {
                tracing::debug!(request_id = request_id, "GetOpenOrders command received");
                self.send_get_open_orders(request_id).await;
            }
            HandlerCommand::InitializeInstruments(instruments) => {
                for inst in instruments {
                    self.instruments.insert(inst.symbol().inner(), inst);
                }
            }
            HandlerCommand::UpdateInstrument(inst) => {
                self.instruments.insert(inst.symbol().inner(), *inst);
            }
        }
    }

    async fn send_cancel_order(&self, request_id: i64, order_id: &str) {
        let msg = ArchitectWsCancelOrder {
            rid: request_id,
            t: "x".to_string(),
            oid: order_id.to_string(),
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send cancel order message");
        }
    }

    async fn send_get_open_orders(&self, request_id: i64) {
        let msg = ArchitectWsGetOpenOrders {
            rid: request_id,
            t: "o".to_string(),
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send get open orders message");
        }
    }

    async fn send_json<T: serde::Serialize>(&self, msg: &T) -> Result<(), String> {
        let Some(client) = &self.client else {
            return Err("No WebSocket client available".to_string());
        };

        let payload = serde_json::to_string(msg).map_err(|e| e.to_string())?;
        tracing::trace!("Sending: {payload}");

        client
            .send_text(payload, None)
            .await
            .map_err(|e| e.to_string())
    }

    fn parse_raw_message(&mut self, msg: Message) -> Option<Vec<ArchitectOrdersWsMessage>> {
        match msg {
            Message::Text(text) => {
                if text == nautilus_network::RECONNECTED {
                    tracing::info!("Received WebSocket reconnected signal");
                    return Some(vec![ArchitectOrdersWsMessage::Reconnected]);
                }

                tracing::trace!("Raw websocket message: {text}");

                let value: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Failed to parse WebSocket message: {e}: {text}");
                        return None;
                    }
                };

                self.classify_and_parse_message(value)
            }
            Message::Binary(data) => {
                tracing::debug!("Received binary message with {} bytes", data.len());
                None
            }
            Message::Close(_) => {
                tracing::debug!("Received close message, waiting for reconnection");
                None
            }
            _ => None,
        }
    }

    fn classify_and_parse_message(
        &mut self,
        value: serde_json::Value,
    ) -> Option<Vec<ArchitectOrdersWsMessage>> {
        let obj = value.as_object()?;

        // Check message type field "t"
        let msg_type = obj.get("t").and_then(|v| v.as_str())?;

        match msg_type {
            "h" => {
                // Heartbeat - just log and ignore
                tracing::trace!("Received heartbeat");
                None
            }
            "n" => {
                // Order acknowledged
                // TODO: Parse to OrderStatusReport
                tracing::debug!("Received order acknowledged");
                None
            }
            "f" => {
                // Order filled
                // TODO: Parse to FillReport
                tracing::debug!("Received order filled");
                None
            }
            "c" => {
                // Order canceled
                // TODO: Parse to OrderStatusReport
                tracing::debug!("Received order canceled");
                None
            }
            "j" => {
                // Order rejected
                // TODO: Parse to OrderRejected event
                tracing::debug!("Received order rejected");
                None
            }
            "x" => {
                // Order expired
                // TODO: Parse to OrderStatusReport
                tracing::debug!("Received order expired");
                None
            }
            "e" => {
                // Cancel rejected
                // TODO: Parse to OrderCancelRejected event
                tracing::debug!("Received cancel rejected");
                None
            }
            _ => {
                // Check for response messages (have "rid" field)
                if obj.contains_key("rid") {
                    tracing::debug!("Received response message");
                    // TODO: Parse place/cancel/open orders responses
                    return None;
                }

                tracing::warn!("Unknown message type: {msg_type}");
                Some(vec![ArchitectOrdersWsMessage::Error(
                    ArchitectWsError::new(format!("Unknown message type: {msg_type}")),
                )])
            }
        }
    }
}
