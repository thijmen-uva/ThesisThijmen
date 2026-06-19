#!/usr/bin/env python3
# Minimal Zenoh receiver (broker-only) for benchmarking

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import zenoh

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from experiment.agent_base import BenchmarkReceiverBase
from experiment.communication_patterns import get_run_agent_topic


def _build_zenoh_config(broker_endpoint: str) -> zenoh.Config:
    cfg = zenoh.Config()
    cfg.insert_json5("mode", '"client"')
    cfg.insert_json5("connect/endpoints", f'["{broker_endpoint}"]')
    cfg.insert_json5("listen/endpoints", "[]")
    cfg.insert_json5("scouting/multicast/enabled", "false")
    cfg.insert_json5("scouting/gossip/enabled", "false")
    return cfg


@dataclass
class ZenohReceiverConfig:
    key_prefix: str
    run_id: str
    result_file: Optional[str]
    agent_id: int
    num_agents: int
    broker_endpoint: str


class ZenohReceiver(BenchmarkReceiverBase):
    def __init__(self, config: ZenohReceiverConfig) -> None:
        self.config = config
        super().__init__(config)

        agent_topic = get_run_agent_topic(self.config.run_id, self.config.agent_id)
        self.key_expr = f"{self.config.key_prefix}/{agent_topic}"

        print(f"[INFO] Connecting to broker {self.config.broker_endpoint}")
        self.session = zenoh.open(_build_zenoh_config(self.config.broker_endpoint))
        print("[INFO] Zenoh session opened (client mode)")

        print(f"[INFO] Subscribing to '{self.key_expr}'")
        self.subscriber = self.session.declare_subscriber(self.key_expr, self._listener)

    def _listener(self, sample) -> None:
        try:
            raw = bytes(sample.payload)
            payload = raw.decode("utf-8")
            # Try to parse JSON messages (expected in benchmark runs). If the
            # payload is plain text (like the simple test publisher), fall back
            # to printing the raw payload so we can diagnose traffic.
            try:
                self.process_json_payload(raw)
            except json.JSONDecodeError:
                # Non-JSON payload; print for debugging and still count
                print(f"[DEBUG] Received on {sample.key_expr}: {payload}")
                self.recv_count += 1
                self.recv_bytes += len(raw)
        except Exception as err:
            print(f"[WARN] Failed to process sample: {err}")

    def run(self) -> dict:
        print(f"Agent {self.config.agent_id} receiver: waiting on {self.key_expr}...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.session.close()

        summary = self.write_receiver_summary("zenoh", self.key_expr)
        duration_sec = summary["duration_sec"]

        if latency_summary["count"] > 0:
            print(
                f"Agent {self.config.agent_id} receiver: collected {latency_summary['count']} latencies"
            )
            print(
                "p50={:.3f} ms, p95={:.3f} ms, p99={:.3f} ms".format(
                    latency_summary["latency_p50_ms"],
                    latency_summary["latency_p95_ms"],
                    latency_summary["latency_p99_ms"],
                )
            )
        else:
            print(
                f"Agent {self.config.agent_id} receiver: no latency samples collected."
            )

        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Zenoh receiver (broker-only)")
    parser.add_argument("--key-prefix", default="thesis")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--result-file", default=None)
    parser.add_argument("--agent-id", type=int, default=0)
    parser.add_argument("--num-agents", type=int, default=1)
    parser.add_argument(
        "--broker-endpoint",
        default="tcp/IP:9092:7447",
        help="External zenoh router endpoint",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = ZenohReceiverConfig(
        key_prefix=args.key_prefix,
        run_id=args.run_id,
        result_file=args.result_file,
        agent_id=args.agent_id,
        num_agents=args.num_agents,
        broker_endpoint=args.broker_endpoint,
    )
    ZenohReceiver(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
