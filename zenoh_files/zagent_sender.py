#!/usr/bin/env python3
# Multi-agent Zenoh sender for benchmarking

import argparse
import json
import random
import string
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import zenoh

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from experiment.agent_base import BenchmarkSenderBase
from experiment.communication_patterns import get_run_agent_topic

MESSAGE_TYPE_DEFINITIONS = [
    ("A", 64, 0.60),
    ("B", 1024, 0.25),
    ("C", 10 * 1024, 0.10),
    ("D", 100 * 1024, 0.05),
]

MESSAGE_TYPE_DISTRIBUTIONS = {
    "lightweight-dominant": [0.60, 0.25, 0.10, 0.05],
    "uniform": [0.25, 0.25, 0.25, 0.25],
    "heavy-tail": [0.70, 0.15, 0.10, 0.05],
}

INTENSITY_TO_RATE = {
    "low": 10,
    "medium": 100,
    "high": 500,
}


def generate_entity_id(entity_type: str) -> str:
    prefix_map = {"soldier": "soldier_", "aircraft": "aircraft_", "tank": "tank_"}
    return prefix_map[entity_type] + "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(6)
    )


def generate_entity_type() -> str:
    return random.choice(["soldier", "aircraft", "tank"])


def choose_message_type(distribution: str) -> str:
    types = [item[0] for item in MESSAGE_TYPE_DEFINITIONS]
    probabilities = MESSAGE_TYPE_DISTRIBUTIONS.get(
        distribution, MESSAGE_TYPE_DISTRIBUTIONS["lightweight-dominant"]
    )
    return random.choices(types, weights=probabilities, k=1)[0]


def message_size_bytes(message_type: str) -> int:
    return {item[0]: item[1] for item in MESSAGE_TYPE_DEFINITIONS}[message_type]


def generate_payload_blob(size_bytes: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))


def generate_message(distribution: str, message_size_override: Optional[int]) -> dict:
    entity_type = generate_entity_type()
    entity_id = generate_entity_id(entity_type)
    message_type = choose_message_type(distribution)

    event_type_map = {
        "soldier": ["moved", "fired_weapon", "detected", "destroyed", "repairing"],
        "aircraft": ["moved", "fired_weapon", "detected", "destroyed"],
        "tank": ["moved", "fired_weapon", "detected", "destroyed"],
    }
    event_type = random.choice(event_type_map[entity_type])

    data = None
    if event_type == "moved":
        data = {"x": random.randint(0, 100), "y": random.randint(0, 100)}
    elif event_type == "fired_weapon":
        if entity_type == "aircraft":
            data = {"weapon_type": random.choice(["missile", "turret", "atom_bomb"])}
        elif entity_type == "tank":
            data = {"weapon_type": "cannon"}
        elif entity_type == "soldier":
            data = {
                "weapon_type": random.choice(["rifle", "rifle", "bazooka", "pistol"])
            }
    elif event_type == "detected":
        data = {"enemy_type": random.choice(["soldier", "aircraft", "tank"])}
    elif event_type == "repairing":
        data = {"repair_progress": random.randint(0, 100)}

    payload_size = message_size_override or message_size_bytes(message_type)

    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "event_type": event_type,
        "data": data,
        "message_type": message_type,
        "payload_size_bytes": payload_size,
        "additional_data": generate_payload_blob(payload_size),
    }


def next_interval(
    base_interval: float,
    pattern: str,
    state: dict,
    burst_cycle_sec: float,
    burst_seconds: float,
    burst_multiplier: float,
) -> float:
    if pattern == "poisson":
        rate = 1.0 / base_interval if base_interval > 0 else 1.0
        return random.expovariate(rate)
    if pattern == "burst":
        elapsed = state.setdefault("elapsed_sec", 0.0)
        phase = elapsed % burst_cycle_sec
        interval = (
            base_interval / burst_multiplier
            if phase >= (burst_cycle_sec - burst_seconds)
            else base_interval
        )
        state["elapsed_sec"] = elapsed + interval
        return interval
    return base_interval


def _build_zenoh_config(broker_endpoint: str) -> zenoh.Config:
    """
    Build a zenoh Config that:
      - runs in CLIENT mode (no peer/router behaviour)
      - connects ONLY to the given external router/broker
      - does NOT listen for incoming connections
      - disables ALL scouting (multicast + gossip)
    This guarantees every message is routed via the external broker.
    """
    cfg = zenoh.Config()
    cfg.insert_json5("mode", '"client"')
    cfg.insert_json5("connect/endpoints", f'["{broker_endpoint}"]')
    cfg.insert_json5("listen/endpoints", "[]")
    cfg.insert_json5("scouting/multicast/enabled", "false")
    cfg.insert_json5("scouting/gossip/enabled", "false")
    return cfg


@dataclass
class ZenohSenderConfig:
    key_prefix: str
    in_flight_limit: int
    intensity: str
    rate_msg_per_sec: Optional[float]
    generation_pattern: str
    message_type_distribution: str
    message_size_bytes: Optional[int]
    burst_cycle_sec: float
    burst_seconds: float
    burst_multiplier: float
    run_id: str
    result_file: Optional[str]
    agent_id: int
    num_agents: int
    communication_pattern: str
    broker_endpoint: str  # required — e.g. "tcp/IP:9092:7447"
    reliability: str  # "best_effort" | "reliable"


class ZenohSender(BenchmarkSenderBase):
    def __init__(self, config: ZenohSenderConfig) -> None:
        self.config = config
        self.send_error_count = 0
        super().__init__(config)

        print(f"[INFO] Connecting to external broker: {self.config.broker_endpoint}")
        self.session = zenoh.open(_build_zenoh_config(self.config.broker_endpoint))
        print("[INFO] Zenoh session opened (client mode)")

    # The simple test publisher uses session.put(topic, payload). Using
    # session.put here keeps behavior consistent with test2.py and avoids
    # publisher lifecycle differences that can complicate debugging.

    def run(self) -> dict:
        in_flight = 0  # simple synchronous counter; no threads needed

        try:
            next_send_at = time.perf_counter()
            burst_state: dict = {}

            while True:
                now = time.perf_counter()
                if now < next_send_at:
                    time.sleep(next_send_at - now)
                    continue

                if in_flight >= self.config.in_flight_limit:
                    time.sleep(min(0.001, self.send_interval))
                    continue

                message = self.build_message()
                self.message_seq += 1
                message["sender_agent_id"] = self.config.agent_id
                message["message_id"] = (
                    f"{self.config.run_id}:agent_{self.config.agent_id}:{self.message_seq}"
                )
                message["sent_at"] = time.time_ns()

                target_agent = self.choose_target_agent()
                message["target_agent_id"] = target_agent

                key_expr = (
                    f"{self.config.key_prefix}/"
                    f"{get_run_agent_topic(self.config.run_id, target_agent)}"
                )
                json_message = json.dumps(message).encode("utf-8")

                try:
                    in_flight += 1
                    # Use session.put same as test2.py
                    self.session.put(key_expr, json_message)
                    self.sent_count += 1
                    self.sent_bytes += len(json_message)
                    # debug log for topic
                    if self.sent_count % 100 == 0:
                        print(f"[DEBUG] Sent {self.sent_count} messages to {key_expr}")
                except Exception as error:
                    self.send_error_count += 1
                    print(f"[ERROR] Zenoh put failed: {error}")
                finally:
                    in_flight = max(0, in_flight - 1)

                interval = self.next_interval(burst_state)
                next_send_at = max(next_send_at + interval, time.perf_counter())

        except KeyboardInterrupt:
            pass
        finally:
            # No declared publishers to undeclare when using session.put
            self.session.close()

        summary = self.write_sender_summary(
            "zenoh",
            {
                "broker_endpoint": self.config.broker_endpoint,
                "reliability": self.config.reliability,
                "send_error_count": self.send_error_count,
            },
        )
        duration_sec = summary["duration_sec"]
        print(
            f"Agent {self.config.agent_id} sender: sent={self.sent_count}, "
            f"errors={self.send_error_count}, bytes={self.sent_bytes}, "
            f"duration={duration_sec:.3f}s"
        )
        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Zenoh sender (broker-only)")
    parser.add_argument("--key-prefix", default="thesis")
    parser.add_argument("--k-in-flight", type=int, default=1)
    parser.add_argument(
        "--intensity", choices=["low", "medium", "high"], default="medium"
    )
    parser.add_argument("--rate-msg-per-sec", type=float, default=None)
    parser.add_argument(
        "--generation-pattern",
        choices=["constant", "poisson", "burst"],
        default="constant",
    )
    parser.add_argument(
        "--message-type-distribution",
        choices=["lightweight-dominant", "uniform", "heavy-tail"],
        default="lightweight-dominant",
    )
    parser.add_argument("--message-size-bytes", type=int, default=None)
    parser.add_argument("--burst-cycle-sec", type=float, default=12.0)
    parser.add_argument("--burst-seconds", type=float, default=2.0)
    parser.add_argument("--burst-multiplier", type=float, default=10.0)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--result-file", default=None)
    parser.add_argument("--agent-id", type=int, default=0)
    parser.add_argument("--num-agents", type=int, default=2)
    parser.add_argument("--communication-pattern", default="pairwise")
    parser.add_argument(
        "--broker-endpoint",
        default="tcp/IP:9092:7447",
        help="External zenoh router endpoint (required for broker-only mode)",
    )
    parser.add_argument(
        "--reliability",
        choices=["best_effort", "reliable"],
        default="best_effort",
        help="Publisher reliability QoS",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = ZenohSenderConfig(
        key_prefix=args.key_prefix,
        in_flight_limit=args.k_in_flight,
        intensity=args.intensity,
        rate_msg_per_sec=args.rate_msg_per_sec,
        generation_pattern=args.generation_pattern,
        message_type_distribution=args.message_type_distribution,
        message_size_bytes=args.message_size_bytes,
        burst_cycle_sec=args.burst_cycle_sec,
        burst_seconds=args.burst_seconds,
        burst_multiplier=args.burst_multiplier,
        run_id=args.run_id,
        result_file=args.result_file,
        agent_id=args.agent_id,
        num_agents=args.num_agents,
        communication_pattern=args.communication_pattern,
        broker_endpoint=args.broker_endpoint,
        reliability=args.reliability,
    )
    ZenohSender(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
