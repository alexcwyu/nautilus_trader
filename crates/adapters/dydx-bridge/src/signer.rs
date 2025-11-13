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

//! Cosmos SDK transaction signing for dYdX v4.
//!
//! This module handles:
//! - Deriving keys from BIP39 mnemonic phrases
//! - Signing transactions with secp256k1 private keys
//! - Wrapping dYdX messages in Cosmos transaction envelopes

use bip39::Mnemonic;
use cosmrs::{
    crypto::secp256k1::SigningKey,
    tx::{self, SignDoc, SignerInfo},
    AccountId, Any,
};
use dydx_proto::dydxprotocol::clob::{MsgCancelOrder, MsgPlaceOrder};
use prost::Message;

use crate::error::{DydxBridgeError, DydxBridgeResult};

/// dYdX v4 chain ID (mainnet)
const DYDX_CHAIN_ID: &str = "dydx-mainnet-1";

/// BIP44 coin type for dYdX (118 = Cosmos)
const COIN_TYPE: u32 = 118;

/// Transaction signer for dYdX v4 Cosmos SDK transactions.
pub struct TransactionSigner {
    signing_key: SigningKey,
    account_id: AccountId,
    chain_id: String,
}

impl TransactionSigner {
    /// Creates a new transaction signer from a BIP39 mnemonic.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The mnemonic is invalid
    /// - Key derivation fails
    /// - Address encoding fails
    pub fn from_mnemonic(
        mnemonic: &str,
        account_index: u32,
        address_index: u32,
    ) -> DydxBridgeResult<Self> {
        // Parse mnemonic
        let mnemonic = Mnemonic::parse(mnemonic)
            .map_err(|e| DydxBridgeError::Config(format!("Invalid mnemonic: {e}")))?;

        // Derive seed from mnemonic
        let seed = mnemonic.to_seed("");

        // Derive HD path: m/44'/118'/0'/0/0 (Cosmos standard)
        let derivation_path = format!("m/44'/{COIN_TYPE}'/{account_index}'/0/{address_index}");

        // Derive signing key
        let signing_key = SigningKey::derive_from_path(
            seed,
            &derivation_path
                .parse()
                .map_err(|e| DydxBridgeError::Config(format!("Invalid HD path: {e}")))?,
        )
        .map_err(|e| DydxBridgeError::Config(format!("Key derivation failed: {e}")))?;

        // Get account ID (bech32 address with "dydx" prefix)
        let account_id = signing_key
            .public_key()
            .account_id("dydx")
            .map_err(|e| DydxBridgeError::Config(format!("Failed to derive address: {e}")))?;

        tracing::info!("Initialized signer for address: {}", account_id);

        Ok(Self {
            signing_key,
            account_id,
            chain_id: DYDX_CHAIN_ID.to_string(),
        })
    }

    /// Returns the dYdX wallet address (bech32 encoded).
    #[must_use]
    pub fn address(&self) -> String {
        self.account_id.to_string()
    }

    /// Returns the account ID.
    #[must_use]
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    /// Signs a MsgPlaceOrder transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if transaction encoding or signing fails.
    pub fn sign_place_order(
        &self,
        msg: MsgPlaceOrder,
        sequence: u64,
        account_number: u64,
    ) -> DydxBridgeResult<Vec<u8>> {
        self.sign_messages(vec![encode_any(msg)?], sequence, account_number)
    }

    /// Signs a MsgCancelOrder transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if transaction encoding or signing fails.
    pub fn sign_cancel_order(
        &self,
        msg: MsgCancelOrder,
        sequence: u64,
        account_number: u64,
    ) -> DydxBridgeResult<Vec<u8>> {
        self.sign_messages(vec![encode_any(msg)?], sequence, account_number)
    }

    /// Signs a transaction containing multiple dYdX messages.
    fn sign_messages(
        &self,
        messages: Vec<Any>,
        sequence: u64,
        account_number: u64,
    ) -> DydxBridgeResult<Vec<u8>> {
        // Reasonable defaults for gas and fees
        let gas_limit = 250_000u64;
        let fee_amount = 25_000_000u128; // 0.025 DYDX in adydx (10^-18)

        // Create transaction body
        let body = tx::Body::new(messages, "", 0u32);

        // Create fee
        let fee = tx::Fee::from_amount_and_gas(
            cosmrs::Coin {
                denom: "adydx".parse().unwrap(),
                amount: fee_amount,
            },
            gas_limit,
        );

        // Create signer info
        let signer_info = SignerInfo::single_direct(Some(self.signing_key.public_key()), sequence);

        // Create auth info
        let auth_info = signer_info.auth_info(fee);

        // Create sign doc
        let sign_doc = SignDoc::new(
            &body,
            &auth_info,
            &self.chain_id.parse().unwrap(),
            account_number,
        )
        .map_err(|e| DydxBridgeError::Translation(format!("Failed to create sign doc: {e}")))?;

        // Sign the transaction
        let tx_raw = sign_doc
            .sign(&self.signing_key)
            .map_err(|e| DydxBridgeError::Translation(format!("Failed to sign transaction: {e}")))?;

        // Encode to bytes
        tx_raw
            .to_bytes()
            .map_err(|e| {
                DydxBridgeError::Translation(format!("Failed to encode transaction: {e}"))
            })
    }

    /// Updates the chain ID (for testnet support).
    pub fn set_chain_id(&mut self, chain_id: String) {
        self.chain_id = chain_id;
    }
}

/// Encodes a protobuf message into a Cosmos Any type.
fn encode_any<M: Message>(msg: M) -> DydxBridgeResult<Any> {
    let type_url = get_type_url::<M>();
    let mut value = Vec::new();
    msg.encode(&mut value)
        .map_err(|e| DydxBridgeError::Translation(format!("Failed to encode message: {e}")))?;

    // cosmrs::Any uses prost_types internally, but we need to construct it directly
    Ok(Any {
        type_url,
        value,
    })
}

/// Gets the type URL for a protobuf message type.
fn get_type_url<M: Message>() -> String {
    let type_name = std::any::type_name::<M>();

    // Extract the last component (struct name)
    let struct_name = type_name.split("::").last().unwrap_or(type_name);

    // Map to dYdX proto type URL
    match struct_name {
        "MsgPlaceOrder" => "/dydxprotocol.clob.MsgPlaceOrder".to_string(),
        "MsgCancelOrder" => "/dydxprotocol.clob.MsgCancelOrder".to_string(),
        _ => format!("/dydxprotocol.clob.{struct_name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signer_creation_from_mnemonic() {
        // Test mnemonic (DO NOT USE IN PRODUCTION)
        let mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

        let signer = TransactionSigner::from_mnemonic(mnemonic, 0, 0);
        assert!(signer.is_ok());

        let signer = signer.unwrap();
        assert!(signer.address().starts_with("dydx"));
        assert!(!signer.address().is_empty());
    }

    #[test]
    fn test_signer_invalid_mnemonic() {
        let result = TransactionSigner::from_mnemonic("invalid mnemonic words", 0, 0);
        assert!(result.is_err());
    }
}
