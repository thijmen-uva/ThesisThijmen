#!/usr/bin/env python3
# Multi-agent MQTT sender for benchmarking

import argparse
import json
import random
import string
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

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
    entity_types = ["soldier", "aircraft", "tank"]
    return random.choice(entity_types)


def choose_message_type(distribution: str) -> str:
    types = [item[0] for item in MESSAGE_TYPE_DEFINITIONS]
    probabilities = MESSAGE_TYPE_DISTRIBUTIONS.get(
        distribution, MESSAGE_TYPE_DISTRIBUTIONS["lightweight-dominant"]
    )
    return random.choices(types, weights=probabilities, k=1)[0]


def message_size_bytes(message_type: str) -> int:
    size_map = {item[0]: item[1] for item in MESSAGE_TYPE_DEFINITIONS}
    return size_map[message_type]


def generate_payload_blob(size_bytes: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=size_bytes))


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
    elif event_type == "destroyed":
        data = None
    elif event_type == "repairing":
        data = {"repair_progress": random.randint(0, 100)}

    payload_size = message_size_override or message_size_bytes(message_type)
    additional_data = generate_payload_blob(payload_size)

    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "event_type": event_type,
        "data": data,
        "message_type": message_type,
        "payload_size_bytes": payload_size,
        "additional_data": additional_data,
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


@dataclass
class MqttSenderConfig:
    host: str
    port: int
    qos: int
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


class MqttSender(BenchmarkSenderBase):
    def __init__(self, config: MqttSenderConfig) -> None:
        self.config = config
        self.in_flight_messages = 0
        self.in_flight_lock = threading.Lock()
        super().__init__(config)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        rc_value = getattr(reason_code, "value", reason_code)
        if rc_value == 0:
            print(f"Agent {self.config.agent_id} sender connected to MQTT broker")
        else:
            print(
                f"Agent {self.config.agent_id} sender: connection failed with code {rc_value}"
            )

    def _on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties=None
    ):
        rc_value = getattr(reason_code, "value", reason_code)
        if rc_value != 0:
            print(f"Unexpected disconnection: {rc_value}")

    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        with self.in_flight_lock:
            self.in_flight_messages = max(0, self.in_flight_messages - 1)

    def run(self) -> dict:
        self.client.connect(self.config.host, self.config.port, keepalive=60)
        self.client.loop_start()
        try:
            next_send_at = time.perf_counter()
            burst_state = {}
            while True:
                now = time.perf_counter()
                if now < next_send_at:
                    time.sleep(next_send_at - now)
                    continue

                with self.in_flight_lock:
                    can_send = self.in_flight_messages < self.config.in_flight_limit

                if not can_send:
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
                target_topic = get_run_agent_topic(self.config.run_id, target_agent)

                message["target_agent_id"] = target_agent
                json_message = json.dumps(message).encode("utf-8")
                self.sent_count += 1
                self.sent_bytes += len(json_message)

                self.client.publish(target_topic, json_message, qos=self.config.qos)
                with self.in_flight_lock:
                    self.in_flight_messages += 1

                interval = self.next_interval(burst_state)
                next_send_at = max(next_send_at + interval, time.perf_counter())
        except KeyboardInterrupt:
            pass
        finally:
            self.client.loop_stop()
            self.client.disconnect()

        summary = self.write_sender_summary(
            "mqtt",
            {
                "target_agents": self.target_agents,
                "mqtt_qos": self.config.qos,
            },
        )
        duration_sec = summary["duration_sec"]
        print(
            f"Agent {self.config.agent_id} sender: sent={self.sent_count}, "
            f"bytes={self.sent_bytes}, duration={duration_sec:.3f}s"
        )
        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MQTT sender")
    parser.add_argument("--host", default="IP:9092")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, default=0)
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
    parser.add_argument("--num-agents", type=int, default=1)
    parser.add_argument("--communication-pattern", default="pairwise")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = MqttSenderConfig(
        host=args.host,
        port=args.port,
        qos=args.qos,
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
    )
    MqttSender(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
