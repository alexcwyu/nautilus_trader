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

//! Python bindings for the Architect adapter.

use pyo3::prelude::*;

/// Registers the Architect adapter Python module.
///
/// This function is called automatically when the Python extension module is loaded.
/// It registers all exported classes, functions, and types with the Python interpreter.
#[pymodule]
pub fn architect(_: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register HTTP client when implemented
    // m.add_class::<ArchitectHttpClient>()?;

    // Register WebSocket client when implemented
    // m.add_class::<ArchitectWebSocketClient>()?;

    // Register configuration types
    // m.add_class::<ArchitectDataClientConfig>()?;
    // m.add_class::<ArchitectExecClientConfig>()?;

    // Register enums when implemented
    // m.add_class::<ArchitectProductType>()?;

    Ok(())
}
