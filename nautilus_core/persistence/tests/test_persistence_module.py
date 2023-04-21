# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
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

import os

from nautilus_trader import PACKAGE_ROOT
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetReader
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetReaderType
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetType
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetWriter
from nautilus_trader.core.nautilus_pyo3.persistence import PythonCatalog
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.tick import TradeTick
from nautilus_trader.persistence.wranglers import list_from_capsule


def test_python_catalog_v2():
    file_path = "/Users/twitu/Downloads/nautilus_test_data/single_stream_data/0005-quotes.parquet"
    session = PythonCatalog()
    session.add_file("quote_ticks", file_path, ParquetType.QuoteTick)
    result = session.to_query_result()

    count = 0
    for chunk in result:
        count += len(list_from_capsule(chunk))

    assert count == 9689614


def test_python_catalog_data():
    trades_path = os.path.join(PACKAGE_ROOT, "tests/test_data/trade_tick_data.parquet")
    quotes_path = os.path.join(PACKAGE_ROOT, "tests/test_data/quote_tick_data.parquet")
    session = PythonCatalog()
    session.add_file("trade_ticks", trades_path, ParquetType.TradeTick)
    session.add_file("quote_ticks", quotes_path, ParquetType.QuoteTick)
    result = session.to_query_result()

    ticks = []
    for chunk in result:
        ticks.extend(list_from_capsule(chunk))

    assert len(ticks) == 9600
    is_ascending = all(ticks[i].ts_init <= ticks[i].ts_init for i in range(len(ticks) - 1))
    assert is_ascending


def test_python_catalog_trades():
    trades_path = os.path.join(PACKAGE_ROOT, "tests/test_data/trade_tick_data.parquet")
    session = PythonCatalog()
    session.add_file("trade_ticks", trades_path, ParquetType.TradeTick)
    result = session.to_query_result()

    ticks = []
    for chunk in result:
        ticks.extend(list_from_capsule(chunk))

    assert len(ticks) == 100
    is_ascending = all(ticks[i].ts_init <= ticks[i].ts_init for i in range(len(ticks) - 1))
    assert is_ascending


def test_python_catalog_quotes():
    parquet_data_path = os.path.join(PACKAGE_ROOT, "tests/test_data/quote_tick_data.parquet")
    session = PythonCatalog()
    session.add_file("quote_ticks", parquet_data_path, ParquetType.QuoteTick)
    result = session.to_query_result()

    ticks = []
    for chunk in result:
        ticks.extend(list_from_capsule(chunk))

    assert len(ticks) == 9500
    assert str(ticks[-1]) == "EUR/USD.SIM,1.12130,1.12132,0,0,1577919652000000125"
    is_ascending = all(ticks[i].ts_init <= ticks[i].ts_init for i in range(len(ticks) - 1))
    assert is_ascending


def test_python_parquet_reader():
    parquet_data_path = os.path.join(PACKAGE_ROOT, "tests/test_data/quote_tick_data.parquet")
    reader = ParquetReader(
        parquet_data_path,
        100,
        ParquetType.QuoteTick,
        ParquetReaderType.File,
    )

    total_count = 0
    for chunk in reader:
        tick_list = QuoteTick.list_from_capsule(chunk)
        total_count += len(tick_list)

    reader.drop()

    assert total_count == 9500
    # test on last chunk tick i.e. 9500th record
    assert str(tick_list[-1]) == "EUR/USD.SIM,1.12130,1.12132,0,0,1577919652000000125"


def test_trade_tick_round_trip():
    parquet_data_path = os.path.join(PACKAGE_ROOT, "tests/test_data/trade_tick_data.parquet")
    reader = ParquetReader(
        parquet_data_path,
        100,
        ParquetType.TradeTick,
        ParquetReaderType.File,
    )

    metadata = {
        "instrument_id": "EUR/USD.SIM",
        "price_precision": "4",
        "size_precision": "4",
    }
    writer = ParquetWriter(ParquetType.TradeTick, metadata)

    ticks = []
    for chunk in reader:
        tick_list = TradeTick.list_from_capsule(chunk)
        ticks.extend(tick_list)
        writer.write(chunk)

    data = writer.flush_bytes()
    buf_reader = ParquetReader(
        "",
        100,
        ParquetType.TradeTick,
        ParquetReaderType.Buffer,
        data,
    )

    buf_ticks = []
    for chunk in buf_reader:
        tick_list = TradeTick.list_from_capsule(chunk)
        buf_ticks.extend(tick_list)

    assert len(buf_ticks) == len(ticks)
    # test on last chunk tick i.e. 9500th record
    assert str(ticks[-1]) == "EUR/USD.SIM,1.2340,5.0000,BUYER,123456,0"
    assert str(buf_ticks[-1]) == str(ticks[-1])
