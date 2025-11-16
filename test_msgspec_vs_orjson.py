#!/usr/bin/env python3
"""
Performance comparison: msgspec vs orjson+dataclass

Compares:
1. msgspec.json.Decoder parsing into msgspec.Struct
2. orjson.loads() + manual dataclass construction
"""

import timeit
from dataclasses import dataclass

import msgspec
import orjson


# Test schemas using msgspec.Struct
class MsgspecBookLevel(msgspec.Struct):
    price: str
    size: str


class MsgspecBookSnapshot(msgspec.Struct):
    market: str
    asset_id: str
    bids: list[MsgspecBookLevel]
    asks: list[MsgspecBookLevel]
    timestamp: str


# Test schemas using dataclass
@dataclass
class DataclassBookLevel:
    price: str
    size: str


@dataclass
class DataclassBookSnapshot:
    market: str
    asset_id: str
    bids: list[DataclassBookLevel]
    asks: list[DataclassBookLevel]
    timestamp: str


# Sample data with varying complexity
SMALL_SNAPSHOT_JSON = b'{"market":"test","asset_id":"12345","bids":[{"price":"0.50","size":"100"}],"asks":[{"price":"0.51","size":"100"}],"timestamp":"1234567890"}'

MEDIUM_SNAPSHOT_JSON = b'{"market":"test","asset_id":"12345","bids":[{"price":"0.50","size":"100"},{"price":"0.49","size":"200"},{"price":"0.48","size":"300"},{"price":"0.47","size":"400"},{"price":"0.46","size":"500"}],"asks":[{"price":"0.51","size":"100"},{"price":"0.52","size":"200"},{"price":"0.53","size":"300"},{"price":"0.54","size":"400"},{"price":"0.55","size":"500"}],"timestamp":"1234567890"}'

LARGE_SNAPSHOT_JSON = b'{"market":"test","asset_id":"12345","bids":[' + b",".join([b'{"price":"0.50","size":"100"}'] * 50) + b'],"asks":[' + b",".join([b'{"price":"0.51","size":"100"}'] * 50) + b'],"timestamp":"1234567890"}'


# msgspec approach
def msgspec_decode(json_data: bytes, iterations: int = 10000) -> None:
    decoder = msgspec.json.Decoder(MsgspecBookSnapshot)
    for _ in range(iterations):
        msg = decoder.decode(json_data)


# orjson + dataclass approach
def orjson_decode(json_data: bytes, iterations: int = 10000) -> None:
    for _ in range(iterations):
        data = orjson.loads(json_data)
        bids = [DataclassBookLevel(**b) for b in data["bids"]]
        asks = [DataclassBookLevel(**a) for a in data["asks"]]
        msg = DataclassBookSnapshot(
            market=data["market"],
            asset_id=data["asset_id"],
            bids=bids,
            asks=asks,
            timestamp=data["timestamp"],
        )


def benchmark(json_data: bytes, name: str, iterations: int = 10000, repeats: int = 5) -> None:
    print(f"\n{'='*80}")
    print(f"Benchmark: {name} ({len(json_data)} bytes, {iterations} iterations)")
    print(f"{'='*80}")

    # msgspec
    msgspec_time = timeit.timeit(
        lambda: msgspec_decode(json_data, iterations),
        number=repeats,
    ) / repeats
    msgspec_per_op = (msgspec_time / iterations) * 1_000_000  # microseconds

    # orjson + dataclass
    orjson_time = timeit.timeit(
        lambda: orjson_decode(json_data, iterations),
        number=repeats,
    ) / repeats
    orjson_per_op = (orjson_time / iterations) * 1_000_000  # microseconds

    # Results
    ratio = orjson_time / msgspec_time
    print(f"msgspec:    {msgspec_time:.4f}s total, {msgspec_per_op:.2f}μs per operation")
    print(f"orjson:     {orjson_time:.4f}s total, {orjson_per_op:.2f}μs per operation")
    print(f"Ratio:      {ratio:.2f}x (orjson is {ratio:.1f}x {'slower' if ratio > 1 else 'faster'})")
    print(f"Difference: {abs(orjson_per_op - msgspec_per_op):.2f}μs per operation")


if __name__ == "__main__":
    print("Performance Comparison: msgspec vs orjson+dataclass")
    print(f"Library versions: msgspec={msgspec.__version__}")

    # Run benchmarks
    benchmark(SMALL_SNAPSHOT_JSON, "Small snapshot (1 bid, 1 ask)", iterations=10000)
    benchmark(MEDIUM_SNAPSHOT_JSON, "Medium snapshot (5 bids, 5 asks)", iterations=10000)
    benchmark(LARGE_SNAPSHOT_JSON, "Large snapshot (50 bids, 50 asks)", iterations=5000)

    print(f"\n{'='*80}")
    print("Summary")
    print(f"{'='*80}")
    print("Note: msgspec is optimized for speed with C extensions, while orjson+dataclass")
    print("has the overhead of dict unpacking and manual object construction.")
    print("The performance difference should be weighed against msgspec's maintenance status.")
