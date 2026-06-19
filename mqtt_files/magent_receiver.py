#!/usr/bin/env python3
# Multi-agent MQTT receiver for benchmarking

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from experiment.agent_base import BenchmarkReceiverBase
from experiment.communication_patterns import get_run_agent_topic


@dataclass
class MqttReceiverConfig:
    host: str
    port: int
    run_id: str
    result_file: Optional[str]
    agent_id: int
    num_agents: int


class MqttReceiver(BenchmarkReceiverBase):
    def __init__(self, config: MqttReceiverConfig) -> None:
        self.config = config
        super().__init__(config)
        self.agent_topic = get_run_agent_topic(self.config.run_id, self.config.agent_id)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_message(self, client, userdata, msg):
        try:
            self.process_json_payload(msg.payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            print(f"Error decoding message: {error}")

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        rc_value = getattr(reason_code, "value", reason_code)
        if rc_value == 0:
            client.subscribe(self.agent_topic)
            print(
                f"Agent {self.config.agent_id} receiver connected and subscribed to {self.agent_topic}"
            )
        else:
            print(f"Connection failed with code {rc_value}")

    def run(self) -> dict:
        self.client.connect(self.config.host, self.config.port, keepalive=60)
        self.client.loop_start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.client.loop_stop()
            self.client.disconnect()

        summary = self.write_receiver_summary("mqtt", self.agent_topic)
        if summary["count"] > 0:
            print(
                f"Agent {self.config.agent_id} receiver: collected {summary['count']} latencies"
            )
            print(
                "p50={:.3f} ms, p95={:.3f} ms, p99={:.3f} ms".format(
                    summary["latency_p50_ms"],
                    summary["latency_p95_ms"],
                    summary["latency_p99_ms"],
                )
            )
        else:
            print(
                f"Agent {self.config.agent_id} receiver: no latency samples collected."
            )
        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MQTT receiver")
    parser.add_argument("--host", default="IP:9092")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--result-file", default=None)
    parser.add_argument("--agent-id", type=int, default=0)
    parser.add_argument("--num-agents", type=int, default=1)
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = MqttReceiverConfig(
        host=args.host,
        port=args.port,
        run_id=args.run_id,
        result_file=args.result_file,
        agent_id=args.agent_id,
        num_agents=args.num_agents,
    )
    MqttReceiver(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
