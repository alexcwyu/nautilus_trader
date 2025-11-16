# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Compatibility wrappers for serialization backends.

This module provides compatibility wrappers to make orjson and the standard msgpack
library work with msgspec's interface for easier migration.

"""

import msgpack as _msgpack
import orjson


class OrjsonWrapper:
    """
    Wrapper to provide encode/decode interface for orjson compatible with msgspec.json.
    """

    @staticmethod
    def encode(obj, enc_hook=None, **kwargs):
        """
        Encode object to JSON bytes.

        Parameters
        ----------
        obj : Any
            The object to encode.
        enc_hook : Callable, optional
            Custom encoding hook for types not natively supported by orjson.
            This is forwarded to orjson's `default` parameter.
        **kwargs
            Additional keyword arguments (ignored for compatibility).

        Returns
        -------
        bytes
            The JSON-encoded bytes.

        """
        # Convert msgspec enc_hook to orjson default parameter
        default_fn = enc_hook if enc_hook is not None else None
        return orjson.dumps(obj, default=default_fn)

    @staticmethod
    def decode(data, **kwargs):
        """
        Decode JSON bytes to object.
        """
        return orjson.loads(data)


class MsgPackWrapper:
    """
    Wrapper to provide encode/decode interface for msgpack compatible with
    msgspec.msgpack.
    """

    @staticmethod
    def encode(obj, enc_hook=None, **kwargs):
        """
        Encode object to MessagePack bytes.
        """
        # Convert msgspec enc_hook to msgpack default parameter
        default_fn = enc_hook if enc_hook is not None else None
        return _msgpack.packb(obj, default=default_fn, use_bin_type=True, **kwargs)

    @staticmethod
    def decode(data, **kwargs):
        """
        Decode MessagePack bytes to object.
        """
        return _msgpack.unpackb(data, raw=False, strict_map_key=False, **kwargs)
