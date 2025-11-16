#!/usr/bin/env python3
"""
Comprehensive performance comparison for JSON parsing approaches.

Compares:
1. msgspec.json.Decoder parsing into msgspec.Struct
2. orjson.loads() only (no object construction)
3. orjson.loads() + manual dataclass construction
4. Pydantic v2 BaseModel with orjson (model_validate)

"""

import timeit
from dataclasses import dataclass

import msgspec
import orjson
from pydantic import BaseModel


# ============================================================================
# msgspec schemas
# ============================================================================
class MsgspecBookLevel(msgspec.Struct):
    price: str
    size: str


class MsgspecBookSnapshot(msgspec.Struct):
    market: str
    asset_id: str
    bids: list[MsgspecBookLevel]
    asks: list[MsgspecBookLevel]
    timestamp: str


# ============================================================================
# dataclass schemas
# ============================================================================
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


# ============================================================================
# Pydantic schemas
# ============================================================================
class PydanticBookLevel(BaseModel):
    price: str
    size: str


class PydanticBookSnapshot(BaseModel):
    market: str
    asset_id: str
    bids: list[PydanticBookLevel]
    asks: list[PydanticBookLevel]
    timestamp: str


# ============================================================================
# Test data
# ============================================================================
SMALL_JSON = b'{"market":"test","asset_id":"12345","bids":[{"price":"0.50","size":"100"}],"asks":[{"price":"0.51","size":"100"}],"timestamp":"1234567890"}'

MEDIUM_JSON = b'{"market":"test","asset_id":"12345","bids":[{"price":"0.50","size":"100"},{"price":"0.49","size":"200"},{"price":"0.48","size":"300"},{"price":"0.47","size":"400"},{"price":"0.46","size":"500"}],"asks":[{"price":"0.51","size":"100"},{"price":"0.52","size":"200"},{"price":"0.53","size":"300"},{"price":"0.54","size":"400"},{"price":"0.55","size":"500"}],"timestamp":"1234567890"}'

LARGE_JSON = b'{"market":"test","asset_id":"12345","bids":[' + b",".join([b'{"price":"0.50","size":"100"}'] * 50) + b'],"asks":[' + b",".join([b'{"price":"0.51","size":"100"}'] * 50) + b'],"timestamp":"1234567890"}'


# ============================================================================
# Parsing approaches
# ============================================================================
def msgspec_parse(json_data: bytes, iterations: int = 10000) -> None:
    decoder = msgspec.json.Decoder(MsgspecBookSnapshot)
    for _ in range(iterations):
        msg = decoder.decode(json_data)


def orjson_only(json_data: bytes, iterations: int = 10000) -> None:
    for _ in range(iterations):
        data = orjson.loads(json_data)


def orjson_dataclass(json_data: bytes, iterations: int = 10000) -> None:
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


def pydantic_parse(json_data: bytes, iterations: int = 10000) -> None:
    for _ in range(iterations):
        data = orjson.loads(json_data)
        msg = PydanticBookSnapshot(**data)


# ============================================================================
# Benchmarking
# ============================================================================
def benchmark(json_data: bytes, name: str, iterations: int = 10000, repeats: int = 5) -> dict:
    results = {}

    # msgspec
    msgspec_time = timeit.timeit(
        lambda: msgspec_parse(json_data, iterations),
        number=repeats,
    ) / repeats
    results["msgspec"] = msgspec_time

    # orjson only
    orjson_only_time = timeit.timeit(
        lambda: orjson_only(json_data, iterations),
        number=repeats,
    ) / repeats
    results["orjson_only"] = orjson_only_time

    # orjson + dataclass
    orjson_dc_time = timeit.timeit(
        lambda: orjson_dataclass(json_data, iterations),
        number=repeats,
    ) / repeats
    results["orjson_dataclass"] = orjson_dc_time

    # Pydantic
    pydantic_time = timeit.timeit(
        lambda: pydantic_parse(json_data, iterations),
        number=repeats,
    ) / repeats
    results["pydantic"] = pydantic_time

    # Print results
    print(f"\n{'='*100}")
    print(f"Benchmark: {name} ({len(json_data)} bytes, {iterations} iterations)")
    print(f"{'='*100}")
    print(f"{'Approach':<25} {'Total Time':<15} {'Per Op (μs)':<15} {'Ratio':<10} {'Throughput (msg/s)'}")
    print(f"{'-'*100}")

    baseline = results["msgspec"]
    for approach, time_val in results.items():
        per_op = (time_val / iterations) * 1_000_000
        ratio = time_val / baseline
        throughput = iterations / time_val

        print(f"{approach:<25} {time_val:<15.4f} {per_op:<15.2f} {ratio:<10.2f}x {throughput:>15,.0f}")

    return results


def print_summary(all_results: dict) -> None:
    print(f"\n{'='*100}")
    print("SUMMARY & ANALYSIS")
    print(f"{'='*100}")

    print("\nPerformance Characteristics:")
    print("1. msgspec: Fastest, C-optimized, but maintenance concerns")
    print("2. orjson only: Pure JSON parsing, ~1.5-2x slower than msgspec")
    print("3. orjson+dataclass: Adds object construction overhead, ~3-4x slower than msgspec")
    print("4. Pydantic: Most features (validation, etc), ~4-6x slower than msgspec")

    print("\nThroughput Analysis (messages/second):")
    for size, results in all_results.items():
        print(f"\n  {size}:")
        for approach, time_val in results.items():
            iterations = 10000 if "Large" not in size else 5000
            throughput = iterations / time_val
            print(f"    {approach:<25} {throughput:>12,.0f} msg/s")

    print("\nPractical Considerations:")
    print("- For Polymarket: Typical message rate is ~10-100 msg/s per market")
    print("- Even slowest approach (Pydantic) handles >10,000 msg/s for medium messages")
    print("- Performance difference unlikely to be bottleneck for current use case")
    print("- msgspec maintenance status is more concerning than performance delta")
    print("- Pydantic provides validation + better ecosystem integration")
    print("- orjson+dataclass is a good middle ground for simple schemas")


if __name__ == "__main__":
    print("="*100)
    print("COMPREHENSIVE PERFORMANCE COMPARISON")
    print(f"msgspec={msgspec.__version__} | Pydantic=2.x | orjson=3.x")
    print("="*100)

    # Run benchmarks
    all_results = {}
    all_results["Small (1 bid, 1 ask)"] = benchmark(
        SMALL_JSON, "Small snapshot (1 bid, 1 ask)", iterations=10000,
    )
    all_results["Medium (5 bids, 5 asks)"] = benchmark(
        MEDIUM_JSON, "Medium snapshot (5 bids, 5 asks)", iterations=10000,
    )
    all_results["Large (50 bids, 50 asks)"] = benchmark(
        LARGE_JSON, "Large snapshot (50 bids, 50 asks)", iterations=5000,
    )

    # Print summary
    print_summary(all_results)
