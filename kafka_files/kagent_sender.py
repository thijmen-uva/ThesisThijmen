#!/usr/bin/env python3
# Multi-agent Kafka sender for benchmarking

import argparse
import json
import random
import signal
import string
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from experiment.agent_base import BenchmarkSenderBase
from experiment.communication_patterns import get_run_agent_topic, get_target_agents
from experiment.result_utils import summarize_latencies_ns

KAFKA_API_VERSION = (3, 5, 0)

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


def parse_acks(value: str) -> int | str:
    normalized = value.strip().lower()
    if normalized == "all":
        return "all"
    if normalized in {"0", "1", "-1"}:
        return int(normalized)
    raise ValueError("acks must be 0, 1, -1, or all")


@dataclass
class KafkaSenderConfig:
    bootstrap_servers: str
    topic: Optional[str]
    acks: int | str
    metadata_timeout_ms: int
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
    communication_pattern: Optional[str]
    target_agent_id: int


class KafkaSender(BenchmarkSenderBase):
    def __init__(self, config: KafkaSenderConfig) -> None:
        print(config)
        self.config = config
        self.shutdown_requested = False
        self.ack_latencies_ns: list[int] = []
        self.in_flight_messages = 0
        self.in_flight_lock = threading.Lock()
        super().__init__(config)

        if self.config.in_flight_limit < 1:
            raise ValueError("k_in_flight must be >= 1")

        self.rate_msg_per_sec = (
            self.config.rate_msg_per_sec
            if self.config.rate_msg_per_sec is not None
            else float(INTENSITY_TO_RATE.get(self.config.intensity, 100))
        )
        if self.rate_msg_per_sec <= 0:
            raise ValueError("Message rate must be > 0")

        self.send_interval = 1.0 / self.rate_msg_per_sec

        pattern = (self.config.communication_pattern or "").strip()
        if pattern:
            self.target_agents = get_target_agents(
                self.config.agent_id, self.config.num_agents, pattern
            )
        else:
            self.target_agents = None

    def _request_shutdown(self, _signum, _frame) -> None:
        self.shutdown_requested = True

    def _on_send_success(self, _record_metadata, sent_at_ns: int | None = None) -> None:
        with self.in_flight_lock:
            self.in_flight_messages = max(0, self.in_flight_messages - 1)
        if sent_at_ns is not None:
            try:
                self.ack_latencies_ns.append(time.time_ns() - int(sent_at_ns))
            except Exception:
                pass

    def _on_send_error(self, error: BaseException) -> None:
        with self.in_flight_lock:
            self.in_flight_messages = max(0, self.in_flight_messages - 1)
        self.send_error_count += 1
        print(f"Kafka send failed: {error}")

    def _create_producer(self) -> KafkaProducer:
        deadline = time.time() + 60
        while True:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.config.bootstrap_servers,
                    api_version=KAFKA_API_VERSION,
                    retries=5,
                    acks=self.config.acks,
                )
            except NoBrokersAvailable as error:
                if time.time() >= deadline:
                    raise RuntimeError(
                        "Kafka producer could not connect to "
                        f"{self.config.bootstrap_servers} after waiting 60 seconds"
                    ) from error
                time.sleep(2)

    def run(self) -> dict:
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)
        producer = self._create_producer()
        try:
            next_send_at = time.perf_counter()
            burst_state = {}
            while not self.shutdown_requested:
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

                if self.target_agents:
                    resolved_target = random.choice(self.target_agents)
                else:
                    resolved_target = self.config.target_agent_id

                message["target_agent_id"] = resolved_target
                json_message = json.dumps(message).encode("utf-8")

                topic = (
                    self.config.topic
                    if self.config.topic
                    else get_run_agent_topic(self.config.run_id, resolved_target)
                )

                with self.in_flight_lock:
                    self.in_flight_messages += 1

                try:
                    future = producer.send(topic, json_message)
                except Exception as error:
                    with self.in_flight_lock:
                        self.in_flight_messages = max(0, self.in_flight_messages - 1)
                    self.send_error_count += 1
                    print(f"Kafka send failed: {error}")
                    continue

                # Capture send timestamp and report ack latency in callback
                send_ts = time.time_ns()
                future.add_callback(
                    lambda rm, sent=send_ts: self._on_send_success(rm, sent)
                )
                future.add_errback(self._on_send_error)

                self.sent_count += 1
                self.sent_bytes += len(json_message)

                interval = self.next_interval(burst_state)
                next_send_at = max(next_send_at + interval, time.perf_counter())
        finally:
            try:
                producer.flush(timeout=5)
            except Exception as error:
                print(
                    "Warning: producer.flush() timed out or failed during shutdown: "
                    f"{error}"
                )
            try:
                producer.close(timeout=5)
            except Exception as error:
                print(f"Warning: producer.close() failed during shutdown: {error}")

        summary = self.write_sender_summary(
            "kafka",
            {
                "target_agent_id": self.config.target_agent_id,
                "target_agents": self.target_agents,
                "send_error_count": self.send_error_count,
                "producer_ack_latency": summarize_latencies_ns(self.ack_latencies_ns),
            },
        )
        duration_sec = summary["duration_sec"]
        print(
            f"Agent {self.config.agent_id} sender: sent={self.sent_count}, "
            f"send_errors={self.send_error_count}, bytes={self.sent_bytes}, "
            f"duration={duration_sec:.3f}s"
        )
        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka sender")
    parser.add_argument("--bootstrap-servers", default="IP:9092")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--acks", default="1")
    parser.add_argument("--metadata-timeout-ms", type=int, default=10000)
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
    parser.add_argument("--target-agent-id", type=int, default=0)
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = KafkaSenderConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        acks=parse_acks(args.acks),
        metadata_timeout_ms=args.metadata_timeout_ms,
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
        target_agent_id=args.target_agent_id,
    )
    KafkaSender(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
