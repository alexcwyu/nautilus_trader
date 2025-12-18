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

//! Python bindings for Architect URL helper functions.

use pyo3::prelude::*;

use crate::common::enums::ArchitectEnvironment;

/// Returns the Architect HTTP API base URL for the given environment.
#[pyfunction]
#[pyo3(name = "get_architect_http_url")]
#[must_use]
pub fn py_get_architect_http_url(environment: ArchitectEnvironment) -> &'static str {
    environment.http_url()
}

/// Returns the Architect Orders API base URL for the given environment.
#[pyfunction]
#[pyo3(name = "get_architect_orders_url")]
#[must_use]
pub fn py_get_architect_orders_url(environment: ArchitectEnvironment) -> &'static str {
    environment.orders_url()
}

/// Returns the Architect market data WebSocket URL for the given environment.
#[pyfunction]
#[pyo3(name = "get_architect_ws_md_url")]
#[must_use]
pub fn py_get_architect_ws_md_url(environment: ArchitectEnvironment) -> &'static str {
    environment.ws_md_url()
}

/// Returns the Architect orders WebSocket URL for the given environment.
#[pyfunction]
#[pyo3(name = "get_architect_ws_orders_url")]
#[must_use]
pub fn py_get_architect_ws_orders_url(environment: ArchitectEnvironment) -> &'static str {
    environment.ws_orders_url()
}
