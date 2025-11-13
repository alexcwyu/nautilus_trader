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

//! Error types for the dYdX bridge.

use std::{
    error::Error,
    fmt::{self, Display},
};

/// Result type for bridge operations.
pub type DydxBridgeResult<T> = Result<T, DydxBridgeError>;

/// Errors that can occur in the dYdX bridge.
#[derive(Debug)]
pub enum DydxBridgeError {
    /// Configuration error
    Config(String),
    /// Translation error (Nautilus to dYdX conversion failed)
    Translation(String),
    /// gRPC communication error
    Grpc(String),
    /// Invalid command received
    InvalidCommand(String),
    /// Order not found
    OrderNotFound(String),
    /// Insufficient balance
    InsufficientBalance(String),
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Network timeout
    Timeout,
    /// Internal error
    Internal(String),
}

impl Display for DydxBridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Translation(msg) => write!(f, "Translation error: {msg}"),
            Self::Grpc(msg) => write!(f, "gRPC error: {msg}"),
            Self::InvalidCommand(msg) => write!(f, "Invalid command: {msg}"),
            Self::OrderNotFound(id) => write!(f, "Order not found: {id}"),
            Self::InsufficientBalance(msg) => write!(f, "Insufficient balance: {msg}"),
            Self::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl Error for DydxBridgeError {}

impl From<std::io::Error> for DydxBridgeError {
    fn from(err: std::io::Error) -> Self {
        Self::Internal(format!("IO error: {err}"))
    }
}
