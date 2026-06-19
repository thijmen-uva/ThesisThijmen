#!/usr/bin/env python3
"""Shared benchmark helpers for sender and receiver agents."""

from __future__ import annotations

import json
import random
import string
import time
from abc import ABC
from dataclasses import dataclass
from typing import Optional

from experiment.communication_patterns import get_target_agents
from experiment.result_utils import summarize_latencies_ns, throughput, write_json

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


class BenchmarkSenderBase(ABC):
    def __init__(self, config) -> None:
        self.config = config
        self.sent_count = 0
        self.sent_bytes = 0
        self.send_error_count = 0
        self.message_seq = 0
        self.start_time_ns = time.time_ns()

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
        self.target_agents = (
            get_target_agents(self.config.agent_id, self.config.num_agents, pattern)
            if pattern
            else None
        )

    def build_message(self) -> dict:
        return generate_message(
            self.config.message_type_distribution,
            self.config.message_size_bytes,
        )

    def choose_target_agent(self) -> int:
        return (
            random.choice(self.target_agents)
            if self.target_agents
            else self.config.agent_id
        )

    def next_interval(self, burst_state: dict) -> float:
        return next_interval(
            self.send_interval,
            self.config.generation_pattern,
            burst_state,
            self.config.burst_cycle_sec,
            self.config.burst_seconds,
            self.config.burst_multiplier,
        )

    def build_sender_summary(
        self, middleware: str, extra_fields: Optional[dict] = None
    ) -> dict:
        duration_sec = (time.time_ns() - self.start_time_ns) / 1_000_000_000
        summary = {
            "run_id": self.config.run_id,
            "middleware": middleware,
            "role": "sender",
            "agent_id": self.config.agent_id,
            "num_agents": self.config.num_agents,
            "communication_pattern": self.config.communication_pattern,
            "target_agents": self.target_agents,
            "sent_count": self.sent_count,
            "sent_bytes": self.sent_bytes,
            "send_error_count": self.send_error_count,
            "duration_sec": duration_sec,
            "k_in_flight": self.config.in_flight_limit,
            "configured_rate_msg_per_sec": self.rate_msg_per_sec,
            "generation_pattern": self.config.generation_pattern,
            "message_type_distribution": self.config.message_type_distribution,
        }
        if extra_fields:
            summary.update(extra_fields)
        summary.update(throughput(self.sent_count, self.sent_bytes, duration_sec))
        return summary

    def write_sender_summary(
        self, middleware: str, extra_fields: Optional[dict] = None
    ) -> dict:
        summary = self.build_sender_summary(middleware, extra_fields)
        write_json(self.config.result_file, summary)
        return summary


class BenchmarkReceiverBase(ABC):
    def __init__(self, config) -> None:
        self.config = config
        self.latencies_ns: list[int] = []
        self.recv_count = 0
        self.recv_bytes = 0
        self.start_time_ns = time.time_ns()

    def process_json_payload(self, raw_payload: bytes) -> bool:
        print(raw_payload)
        payload = raw_payload.decode("utf-8")
        data = json.loads(payload)
        sent_at_ns = data.get("sent_at")
        if sent_at_ns is None:
            return False

        latency_ns = time.time_ns() - int(sent_at_ns)
        self.latencies_ns.append(latency_ns)
        self.recv_count += 1
        self.recv_bytes += len(raw_payload)
        return True

    def record_payload(self, raw_payload: bytes) -> None:
        if self.process_json_payload(raw_payload):
            return

    def build_receiver_summary(
        self, middleware: str, topic: str, extra_fields: Optional[dict] = None
    ) -> dict:
        duration_sec = (time.time_ns() - self.start_time_ns) / 1_000_000_000
        latency_summary = summarize_latencies_ns(self.latencies_ns)
        summary = {
            "run_id": self.config.run_id,
            "middleware": middleware,
            "role": "receiver",
            "agent_id": self.config.agent_id,
            "num_agents": self.config.num_agents,
            "topic": topic,
            "recv_count": self.recv_count,
            "recv_bytes": self.recv_bytes,
            "duration_sec": duration_sec,
            **latency_summary,
        }
        if extra_fields:
            summary.update(extra_fields)
        summary.update(throughput(self.recv_count, self.recv_bytes, duration_sec))
        return summary

    def write_receiver_summary(
        self, middleware: str, topic: str, extra_fields: Optional[dict] = None
    ) -> dict:
        summary = self.build_receiver_summary(middleware, topic, extra_fields)
        write_json(self.config.result_file, summary)
        return summary
