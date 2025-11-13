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

//! Translation layer between Nautilus and dYdX v4 protobuf types.
//!
//! This module handles bidirectional conversion between Nautilus domain types
//! and dYdX v4 protobuf messages (generated from AGPL-licensed proto files).

use dydx_proto::dydxprotocol::{
    clob::{MsgCancelOrder, MsgPlaceOrder, Order as DydxOrder, OrderId},
    subaccounts::SubaccountId,
};
use nautilus_model::orders::Order;

use crate::error::{DydxBridgeError, DydxBridgeResult};

/// Block buffer for good_til_block calculations
const BLOCK_BUFFER: u32 = 20; // ~20 seconds at 1 block/sec

/// Translates between Nautilus types and dYdX v4 protobuf messages.
///
/// This struct maintains state needed for translation, such as the current
/// block height for calculating good_til_block values.
pub struct Translator {
    /// Current block height from dYdX chain
    current_block_height: u64,
    /// dYdX wallet address (bech32 encoded)
    wallet_address: String,
    /// Subaccount number (typically 0)
    subaccount_number: u32,
}

impl Translator {
    /// Creates a new translator.
    #[must_use]
    pub fn new(
        current_block_height: u64,
        wallet_address: String,
        subaccount_number: u32,
    ) -> Self {
        Self {
            current_block_height,
            wallet_address,
            subaccount_number,
        }
    }

    /// Updates the current block height.
    ///
    /// This should be called whenever a block height update is received
    /// from the dYdX WebSocket feed.
    pub fn update_block_height(&mut self, block_height: u64) {
        self.current_block_height = block_height;
    }

    /// Translates a Nautilus order to a dYdX v4 protobuf `MsgPlaceOrder`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The order type is not supported by dYdX
    /// - Required order parameters are missing
    /// - Numeric conversions fail
    pub fn translate_to_place_order(&self, order: &dyn Order) -> DydxBridgeResult<MsgPlaceOrder> {
        // TODO: Extract order fields from Nautilus Order trait
        // Need to implement proper accessor methods for:
        // - client_order_id -> client_id (u32)
        // - clob_pair_id (from instrument_id)
        // - side (Buy/Sell -> OrderSide enum)
        // - quantums (quantity in base quantums)
        // - subticks (price in subticks)
        // - time_in_force -> TimeInForce enum
        // - reduce_only bool
        // - client_metadata u32

        let _order_id = order.client_order_id();

        // Create the dYdX Order message
        // TODO: Populate all fields from Nautilus order
        let dydx_order = DydxOrder {
            order_id: None, // TODO: Create OrderId from client_order_id and subaccount
            side: 0,        // TODO: Translate order side
            quantums: 0,    // TODO: Convert quantity to quantums
            subticks: 0,    // TODO: Convert price to subticks
            good_til_oneof: Some(dydx_proto::dydxprotocol::clob::order::GoodTilOneof::GoodTilBlock(
                (self.current_block_height as u32) + BLOCK_BUFFER,
            )),
            time_in_force: 0,     // TODO: Translate TIF
            reduce_only: false,   // TODO: Extract from order
            client_metadata: 0,
            condition_type: 0,
            conditional_order_trigger_subticks: 0,
            twap_parameters: None,
            builder_code_parameters: None,
            order_router_address: String::new(),
        };

        Ok(MsgPlaceOrder {
            order: Some(dydx_order),
        })
    }

    /// Translates a Nautilus cancel order command to a dYdX v4 protobuf `MsgCancelOrder`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The client order ID cannot be parsed
    /// - The client order ID format is invalid
    pub fn translate_to_cancel_order(
        &self,
        client_order_id: &str,
        clob_pair_id: u32,
    ) -> DydxBridgeResult<MsgCancelOrder> {
        // Parse client order ID to u32
        let client_id = client_order_id
            .parse::<u32>()
            .map_err(|e| DydxBridgeError::Translation(format!("Invalid client order ID: {e}")))?;

        // Create OrderId
        let order_id = Some(OrderId {
            subaccount_id: Some(SubaccountId {
                owner: self.wallet_address.clone(),
                number: self.subaccount_number,
            }),
            client_id,
            order_flags: 0,
            clob_pair_id,
        });

        Ok(MsgCancelOrder {
            order_id,
            good_til_oneof: Some(
                dydx_proto::dydxprotocol::clob::msg_cancel_order::GoodTilOneof::GoodTilBlock(
                    (self.current_block_height as u32) + BLOCK_BUFFER,
                ),
            ),
        })
    }

    /// Translates a dYdX v4 protobuf order response to Nautilus order status.
    ///
    /// TODO: Implement once dYdX v4 protobuf code is integrated.
    /// This will convert from dYdX-generated protobuf types to Nautilus events.
    ///
    /// # Errors
    ///
    /// Returns an error if the protobuf message is invalid or missing required fields.
    pub fn translate_from_order_response(&self) -> DydxBridgeResult<()> {
        // TODO: Implement translation from dYdX protobuf response
        // This will extract:
        // - Order ID
        // - Status
        // - Fill information
        // - Timestamps
        // And convert to Nautilus event types

        tracing::warn!(
            "translate_from_order_response is stubbed - add dYdX protobuf integration"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translator_block_height_update() {
        let mut translator = Translator::new(
            1000,
            "dydx1test".to_string(),
            0,
        );
        assert_eq!(translator.current_block_height, 1000);

        translator.update_block_height(2000);
        assert_eq!(translator.current_block_height, 2000);
    }

    #[test]
    fn test_translate_to_cancel_order() {
        let translator = Translator::new(
            1000,
            "dydx1test".to_string(),
            0,
        );

        let result = translator.translate_to_cancel_order("12345", 1);
        assert!(result.is_ok());

        let msg = result.unwrap();
        assert!(msg.order_id.is_some());

        let order_id = msg.order_id.unwrap();
        assert_eq!(order_id.client_id, 12345);
        assert_eq!(order_id.clob_pair_id, 1);
    }
}
