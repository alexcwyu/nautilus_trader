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

//! Command handlers for processing Nautilus execution commands.

use nautilus_model::orders::Order;

use crate::{
    error::{DydxBridgeError, DydxBridgeResult},
    grpc_client::DydxGrpcClient,
    signer::TransactionSigner,
    translator::Translator,
};

/// Handler for processing Nautilus execution commands.
///
/// This struct orchestrates the translation of Nautilus commands to dYdX
/// protobuf messages and sends them to the dYdX v4 chain via gRPC.
pub struct CommandHandler {
    translator: Translator,
    signer: TransactionSigner,
    grpc_client: DydxGrpcClient,
    account_number: u64,
    sequence: u64,
}

impl CommandHandler {
    /// Creates a new command handler.
    #[must_use]
    pub fn new(
        initial_block_height: u64,
        wallet_address: String,
        subaccount_number: u32,
        signer: TransactionSigner,
        grpc_client: DydxGrpcClient,
    ) -> Self {
        Self {
            translator: Translator::new(initial_block_height, wallet_address, subaccount_number),
            signer,
            grpc_client,
            account_number: 0, // TODO: Query from chain
            sequence: 0,        // TODO: Query from chain
        }
    }

    /// Updates the current block height.
    ///
    /// This should be called whenever a block height update is received.
    pub fn update_block_height(&mut self, block_height: u64) {
        self.translator.update_block_height(block_height);
    }

    /// Handles a submit order command.
    ///
    /// This method:
    /// 1. Translates the Nautilus order to a dYdX protobuf `MsgPlaceOrder`
    /// 2. Signs the transaction using the wallet
    /// 3. Sends the transaction to dYdX via gRPC
    /// 4. Returns when transaction is confirmed
    ///
    /// # Errors
    ///
    /// Returns an error if translation fails, signing fails, or the gRPC call fails.
    pub async fn handle_submit_order(&mut self, order: &dyn Order) -> DydxBridgeResult<()> {
        tracing::info!("Handling submit order");

        let msg = self.translator.translate_to_place_order(order)?;
        let tx_bytes = self
            .signer
            .sign_place_order(msg, self.sequence, self.account_number)?;
        let response = self.grpc_client.broadcast_transaction(tx_bytes).await?;

        self.sequence += 1;

        tracing::info!("Order submitted successfully: {:?}", response);
        Ok(())
    }

    /// Handles a cancel order command.
    ///
    /// # Errors
    ///
    /// Returns an error if translation fails or the gRPC call fails.
    pub async fn handle_cancel_order(
        &mut self,
        client_order_id: &str,
        clob_pair_id: u32,
    ) -> DydxBridgeResult<()> {
        tracing::info!("Handling cancel order: {}", client_order_id);

        let msg = self
            .translator
            .translate_to_cancel_order(client_order_id, clob_pair_id)?;
        let tx_bytes = self
            .signer
            .sign_cancel_order(msg, self.sequence, self.account_number)?;
        let response = self.grpc_client.broadcast_transaction(tx_bytes).await?;

        self.sequence += 1;

        tracing::info!("Order cancelled successfully: {:?}", response);
        Ok(())
    }

    /// Handles a modify order command.
    ///
    /// # Errors
    ///
    /// Returns an error as dYdX v4 does not support order modification.
    /// Strategies should cancel and replace instead.
    pub async fn handle_modify_order(&self, _order: &dyn Order) -> DydxBridgeResult<()> {
        Err(DydxBridgeError::InvalidCommand(
            "dYdX v4 does not support order modification. Use cancel/replace instead.".to_string(),
        ))
    }
}
