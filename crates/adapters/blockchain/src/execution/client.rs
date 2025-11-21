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

use std::sync::Arc;

use alloy::primitives::Address;
use async_trait::async_trait;
use nautilus_core::UnixNanos;
use nautilus_execution::client::{ExecutionClient, LiveExecutionClient, base::ExecutionClientCore};
use nautilus_model::{
    accounts::AccountAny,
    defi::{SharedChain, validation::validate_address},
    enums::OmsType,
    identifiers::{AccountId, ClientId, Venue},
    types::{AccountBalance, MarginBalance, Money},
};

use crate::{config::BlockchainExecutionClientConfig, rpc::http::BlockchainHttpRpcClient};

#[derive(Debug, Clone)]
pub struct BlockchainExecutionClient {
    core: ExecutionClientCore,
    chain: SharedChain,
    wallet_address: Address,
    connected: bool,
    http_rpc_client: Arc<BlockchainHttpRpcClient>,
}

impl BlockchainExecutionClient {
    /// Creates a new [`BlockchainExecutionClient`] instance for the specified configuration.
    ///
    /// # Panics
    ///
    /// Panics if the wallet address in the configuration is invalid or malformed.
    #[must_use]
    pub fn new(core_client: ExecutionClientCore, config: BlockchainExecutionClientConfig) -> Self {
        let http_rpc_client = Arc::new(BlockchainHttpRpcClient::new(
            config.http_rpc_url.clone(),
            config.rpc_requests_per_second,
        ));
        let wallet_address =
            validate_address(config.wallet_address.as_str()).expect("Invalid wallet address");
        Self {
            core: core_client,
            connected: false,
            chain: Arc::new(config.chain),
            http_rpc_client,
            wallet_address,
        }
    }

    async fn fetch_native_currency_balance(&self) -> anyhow::Result<Money> {
        let balance_u256 = self
            .http_rpc_client
            .get_balance(&self.wallet_address, None)
            .await?;

        let native_currency = self.chain.native_currency();

        // Convert from wei (18 decimals on-chain) to Money
        let balance = Money::from_wei(balance_u256, native_currency);

        Ok(balance)
    }

    async fn refresh_wallet_balances(&self) {
        if let Ok(native_balance) = self.fetch_native_currency_balance().await {
            tracing::info!(
                "Blockchain wallet balance: {} {}",
                native_balance.as_decimal(),
                native_balance.currency
            );
        }
    }
}

impl ExecutionClient for BlockchainExecutionClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        todo!("implement get_account")
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        todo!("implement generate_account_state")
    }

    fn start(&mut self) -> anyhow::Result<()> {
        todo!("implement start")
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        todo!("implement stop")
    }
}

#[async_trait(?Send)]
impl LiveExecutionClient for BlockchainExecutionClient {
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.connected {
            tracing::warn!("Blockchain execution client already connected");
            return Ok(());
        }

        tracing::info!(
            "Connecting to blockchain execution client on chain {}",
            self.chain.name
        );

        self.refresh_wallet_balances().await;

        self.connected = true;
        tracing::info!(
            "Blockchain execution client connected on chain {}",
            self.chain.name
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        todo!("implement disconnect")
    }
}
