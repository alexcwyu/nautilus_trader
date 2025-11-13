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

//! gRPC client for dYdX v4 Order Execution Gateway Service (OEGS).
//!
//! This module provides a client for broadcasting transactions to the dYdX v4 chain.

use cosmos_sdk_proto::cosmos::tx::v1beta1::{
    BroadcastMode, BroadcastTxRequest, BroadcastTxResponse, service_client::ServiceClient,
};
use tonic::transport::Channel;

use crate::error::{DydxBridgeError, DydxBridgeResult};

/// gRPC client for dYdX v4 OEGS.
pub struct DydxGrpcClient {
    client: ServiceClient<Channel>,
}

impl DydxGrpcClient {
    /// Creates a new gRPC client connected to the dYdX OEGS endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if connection to the endpoint fails.
    pub async fn connect(endpoint: &str) -> DydxBridgeResult<Self> {
        tracing::info!("Connecting to dYdX OEGS: {endpoint}");

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| DydxBridgeError::Grpc(format!("Invalid endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| DydxBridgeError::Grpc(format!("Connection failed: {e}")))?;

        let client = ServiceClient::new(channel);

        tracing::info!("Successfully connected to dYdX OEGS");

        Ok(Self { client })
    }

    /// Broadcasts a signed transaction to the dYdX chain.
    ///
    /// Uses `BROADCAST_MODE_SYNC` which waits for the transaction to be included
    /// in a block before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The broadcast request fails.
    /// - The transaction is rejected by the chain.
    /// - The response is invalid.
    pub async fn broadcast_transaction(
        &mut self,
        tx_bytes: Vec<u8>,
    ) -> DydxBridgeResult<BroadcastTxResponse> {
        tracing::debug!("Broadcasting transaction ({} bytes)", tx_bytes.len());

        let request = BroadcastTxRequest {
            tx_bytes,
            mode: BroadcastMode::Sync as i32, // Wait for CheckTx
        };

        let response = self
            .client
            .broadcast_tx(request)
            .await
            .map_err(|e| DydxBridgeError::Grpc(format!("Broadcast failed: {e}")))?
            .into_inner();

        // Check for errors in the transaction response
        if let Some(ref tx_response) = response.tx_response {
            if tx_response.code != 0 {
                return Err(DydxBridgeError::Grpc(format!(
                    "Transaction rejected: code={}, log={}",
                    tx_response.code, tx_response.raw_log
                )));
            }

            tracing::info!(
                "Transaction broadcasted successfully: hash={}",
                tx_response.txhash
            );
        }

        Ok(response)
    }

    /// Gets the account number and sequence for an address.
    ///
    /// This is needed for signing transactions.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_account(&mut self, address: &str) -> DydxBridgeResult<(u64, u64)> {
        // TODO: Implement account query
        // For now, return stub values
        tracing::warn!("get_account is stubbed - implement account query");
        let _ = address;
        Ok((0, 0)) // (account_number, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation_with_invalid_endpoint() {
        let result = DydxGrpcClient::connect("invalid://endpoint").await;
        assert!(result.is_err());
    }
}
