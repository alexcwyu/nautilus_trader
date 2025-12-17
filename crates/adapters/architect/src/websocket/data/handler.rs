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

//! Market data WebSocket message handler for Architect.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use nautilus_model::instruments::{Instrument, InstrumentAny};
use nautilus_network::websocket::{SubscriptionState, WebSocketClient};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use crate::{
    common::enums::{ArchitectCandleWidth, ArchitectMarketDataLevel},
    websocket::messages::{
        ArchitectMdSubscribe, ArchitectMdSubscribeCandles, ArchitectMdUnsubscribe,
        ArchitectMdUnsubscribeCandles, ArchitectMdWsMessage, ArchitectWsError,
    },
};

/// Commands sent from the outer client to the inner message handler.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Set the WebSocket client for this handler.
    SetClient(WebSocketClient),
    /// Disconnect the WebSocket connection.
    Disconnect,
    /// Subscribe to market data for a symbol.
    Subscribe {
        /// Request ID for correlation.
        request_id: i64,
        /// Instrument symbol.
        symbol: String,
        /// Market data level.
        level: ArchitectMarketDataLevel,
    },
    /// Unsubscribe from market data for a symbol.
    Unsubscribe {
        /// Request ID for correlation.
        request_id: i64,
        /// Instrument symbol.
        symbol: String,
    },
    /// Subscribe to candle data for a symbol.
    SubscribeCandles {
        /// Request ID for correlation.
        request_id: i64,
        /// Instrument symbol.
        symbol: String,
        /// Candle width/interval.
        width: ArchitectCandleWidth,
    },
    /// Unsubscribe from candle data for a symbol.
    UnsubscribeCandles {
        /// Request ID for correlation.
        request_id: i64,
        /// Instrument symbol.
        symbol: String,
        /// Candle width/interval.
        width: ArchitectCandleWidth,
    },
    /// Initialize the instrument cache with instruments.
    InitializeInstruments(Vec<InstrumentAny>),
    /// Update a single instrument in the cache.
    UpdateInstrument(Box<InstrumentAny>),
}

/// Market data feed handler that processes WebSocket messages.
///
/// Runs in a dedicated Tokio task and owns the WebSocket client exclusively.
pub(crate) struct FeedHandler {
    signal: Arc<AtomicBool>,
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    #[allow(dead_code)] // TODO: Use for sending parsed messages
    out_tx: tokio::sync::mpsc::UnboundedSender<ArchitectMdWsMessage>,
    #[allow(dead_code)] // TODO: Use for tracking subscriptions
    subscriptions: SubscriptionState,
    instruments: AHashMap<Ustr, InstrumentAny>,
    message_queue: VecDeque<ArchitectMdWsMessage>,
}

impl FeedHandler {
    /// Creates a new [`FeedHandler`] instance.
    #[must_use]
    pub fn new(
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<ArchitectMdWsMessage>,
        subscriptions: SubscriptionState,
    ) -> Self {
        Self {
            signal,
            client: None,
            cmd_rx,
            raw_rx,
            out_tx,
            subscriptions,
            instruments: AHashMap::new(),
            message_queue: VecDeque::new(),
        }
    }

    /// Returns the next message from the handler.
    ///
    /// This method blocks until a message is available or the handler is stopped.
    pub async fn next(&mut self) -> Option<ArchitectMdWsMessage> {
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
            HandlerCommand::Subscribe {
                request_id,
                symbol,
                level,
            } => {
                tracing::debug!(
                    request_id = request_id,
                    symbol = %symbol,
                    level = ?level,
                    "Subscribe command received"
                );
                self.send_subscribe(request_id, &symbol, level).await;
            }
            HandlerCommand::Unsubscribe { request_id, symbol } => {
                tracing::debug!(
                    request_id = request_id,
                    symbol = %symbol,
                    "Unsubscribe command received"
                );
                self.send_unsubscribe(request_id, &symbol).await;
            }
            HandlerCommand::SubscribeCandles {
                request_id,
                symbol,
                width,
            } => {
                tracing::debug!(
                    request_id = request_id,
                    symbol = %symbol,
                    width = ?width,
                    "SubscribeCandles command received"
                );
                self.send_subscribe_candles(request_id, &symbol, width)
                    .await;
            }
            HandlerCommand::UnsubscribeCandles {
                request_id,
                symbol,
                width,
            } => {
                tracing::debug!(
                    request_id = request_id,
                    symbol = %symbol,
                    width = ?width,
                    "UnsubscribeCandles command received"
                );
                self.send_unsubscribe_candles(request_id, &symbol, width)
                    .await;
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

    async fn send_subscribe(&self, request_id: i64, symbol: &str, level: ArchitectMarketDataLevel) {
        let msg = ArchitectMdSubscribe {
            request_id,
            msg_type: "subscribe".to_string(),
            symbol: symbol.to_string(),
            level,
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send subscribe message");
        }
    }

    async fn send_unsubscribe(&self, request_id: i64, symbol: &str) {
        let msg = ArchitectMdUnsubscribe {
            request_id,
            msg_type: "unsubscribe".to_string(),
            symbol: symbol.to_string(),
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send unsubscribe message");
        }
    }

    async fn send_subscribe_candles(
        &self,
        request_id: i64,
        symbol: &str,
        width: ArchitectCandleWidth,
    ) {
        let msg = ArchitectMdSubscribeCandles {
            request_id,
            msg_type: "subscribe_candles".to_string(),
            symbol: symbol.to_string(),
            width,
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send subscribe_candles message");
        }
    }

    async fn send_unsubscribe_candles(
        &self,
        request_id: i64,
        symbol: &str,
        width: ArchitectCandleWidth,
    ) {
        let msg = ArchitectMdUnsubscribeCandles {
            request_id,
            msg_type: "unsubscribe_candles".to_string(),
            symbol: symbol.to_string(),
            width,
        };

        if let Err(e) = self.send_json(&msg).await {
            tracing::error!(error = %e, "Failed to send unsubscribe_candles message");
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

    fn parse_raw_message(&mut self, msg: Message) -> Option<Vec<ArchitectMdWsMessage>> {
        match msg {
            Message::Text(text) => {
                if text == nautilus_network::RECONNECTED {
                    tracing::info!("Received WebSocket reconnected signal");
                    return Some(vec![ArchitectMdWsMessage::Reconnected]);
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
        &self,
        value: serde_json::Value,
    ) -> Option<Vec<ArchitectMdWsMessage>> {
        let obj = value.as_object()?;

        // Check message type field "t"
        let msg_type = obj.get("t").and_then(|v| v.as_str())?;

        match msg_type {
            "h" => {
                // Heartbeat - just log and ignore
                tracing::trace!("Received heartbeat");
                None
            }
            "s" => {
                // Ticker or trade message
                // TODO: Parse to TradeTick and emit as Data
                tracing::debug!("Received ticker/trade message");
                None
            }
            "c" => {
                // Candle message
                // TODO: Parse to Bar and emit
                tracing::debug!("Received candle message");
                None
            }
            "1" | "2" | "3" => {
                // Order book L1/L2/L3 message
                // TODO: Parse to OrderBookDeltas and emit
                tracing::debug!("Received book L{msg_type} message");
                None
            }
            _ => {
                tracing::warn!("Unknown message type: {msg_type}");
                Some(vec![ArchitectMdWsMessage::Error(ArchitectWsError::new(
                    format!("Unknown message type: {msg_type}"),
                ))])
            }
        }
    }
}
