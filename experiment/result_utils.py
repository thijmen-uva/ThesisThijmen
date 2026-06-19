import json
import math
import os
import statistics
from typing import Any


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None

    sorted_values = sorted(values)
    position = (len(sorted_values) - 1) * pct
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return float(sorted_values[lower])

    interpolation = position - lower
    return float(
        sorted_values[lower]
        + (sorted_values[upper] - sorted_values[lower]) * interpolation
    )


def summarize_latencies_ns(latencies_ns: list[int]) -> dict[str, float | int | None]:
    if not latencies_ns:
        return {
            "count": 0,
            "latency_p50_ms": None,
            "latency_p95_ms": None,
            "latency_p99_ms": None,
            "latency_std_ms": None,
            "latency_jitter_mean_ms": None,
            "latency_jitter_std_ms": None,
        }
    latencies_ms = [value / 1_000_000 for value in latencies_ns]

    diffs = [
        abs(latencies_ms[i] - latencies_ms[i - 1]) for i in range(1, len(latencies_ms))
    ]
    jitter_mean = statistics.mean(diffs) if diffs else 0.0
    jitter_std = statistics.pstdev(diffs) if len(diffs) > 1 else 0.0

    return {
        "count": len(latencies_ns),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
        "latency_std_ms": (
            statistics.pstdev(latencies_ms) if len(latencies_ms) > 1 else 0.0
        ),
        "latency_jitter_mean_ms": jitter_mean,
        "latency_jitter_std_ms": jitter_std,
    }


def throughput(
    sent_or_recv_count: int, total_bytes: int, duration_sec: float
) -> dict[str, float]:
    if duration_sec <= 0:
        return {"throughput_msg_per_sec": 0.0, "throughput_mb_per_sec": 0.0}

    return {
        "throughput_msg_per_sec": sent_or_recv_count / duration_sec,
        "throughput_mb_per_sec": (total_bytes / (1024 * 1024)) / duration_sec,
    }


def write_json(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return

    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
