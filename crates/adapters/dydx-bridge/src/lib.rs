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

//! AGPL-licensed execution bridge for dYdX v4.
//!
//! This library provides the core functionality for bridging Nautilus Trader
//! execution commands to dYdX v4 Order Execution Gateway Service (OEGS) gRPC calls.

pub mod config;
pub mod error;
pub mod events;
pub mod grpc_client;
pub mod handlers;
pub mod rpc_server;
pub mod server;
pub mod signer;
pub mod translator;
pub mod websocket;

pub use config::Config;
pub use server::run;
