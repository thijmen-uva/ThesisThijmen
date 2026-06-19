#!/usr/bin/env python3
# Multi-agent Kafka receiver for benchmarking

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from experiment.agent_base import BenchmarkReceiverBase
from experiment.communication_patterns import get_run_agent_topic

KAFKA_API_VERSION = (3, 5, 0)


@dataclass
class KafkaReceiverConfig:
    bootstrap_servers: str
    topic: Optional[str]
    run_id: str
    result_file: Optional[str]
    agent_id: int
    num_agents: int
    auto_offset_reset: str = "earliest"


class KafkaReceiver(BenchmarkReceiverBase):
    def __init__(self, config: KafkaReceiverConfig) -> None:
        self.config = config
        print(config.topic)
        super().__init__(config)

    def _create_consumer(self) -> KafkaConsumer:
        deadline = time.time() + 60
        while True:
            try:
                topic = (
                    self.config.topic
                    if self.config.topic
                    else get_run_agent_topic(self.config.run_id, self.config.agent_id)
                )
                return KafkaConsumer(
                    topic,
                    group_id=f"{self.config.run_id}-agent-{self.config.agent_id}",
                    bootstrap_servers=self.config.bootstrap_servers,
                    api_version=KAFKA_API_VERSION,
                    auto_offset_reset=self.config.auto_offset_reset,
                    enable_auto_commit=False,
                    # Reduce server-side batching wait to lower tail latency
                    fetch_max_wait_ms=50,
                    # Allow more records per poll to amortize processing
                    max_poll_records=500,
                )
            except NoBrokersAvailable as error:
                if time.time() >= deadline:
                    raise RuntimeError(
                        "Kafka consumer could not connect to "
                        f"{self.config.bootstrap_servers} after waiting 60 seconds"
                    ) from error
                time.sleep(2)

    def run(self) -> dict:
        print(
            f"Kafka receiver started for agent {self.config.agent_id}. "
            f"broker={self.config.bootstrap_servers}, "
            f"topic={self.config.topic or get_run_agent_topic(self.config.run_id, self.config.agent_id)}"
        )
        consumer = self._create_consumer()
        try:
            while True:
                message_batch = consumer.poll(timeout_ms=50)
                if not message_batch:
                    continue

                for _, records in message_batch.items():
                    for record in records:
                        try:
                            if not self.process_json_payload(record.value):
                                continue
                        except (ValueError, json.JSONDecodeError) as error:
                            print(f"Error: Message format not recognized ({error})")

                try:
                    consumer.commit_async()
                except Exception:
                    # Fallback to synchronous commit if async fails
                    try:
                        consumer.commit()
                    except Exception:
                        pass
        except KeyboardInterrupt:
            pass
        finally:
            # Try to perform a final sync commit to persist offsets before closing
            try:
                consumer.commit()
            except Exception:
                pass
            consumer.close()

        summary = self.write_receiver_summary(
            "kafka",
            self.config.topic
            or get_run_agent_topic(self.config.run_id, self.config.agent_id),
        )
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
    parser = argparse.ArgumentParser(description="Kafka receiver")
    parser.add_argument("--bootstrap-servers", default="IP:9092")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--result-file", default=None)
    parser.add_argument("--agent-id", type=int, default=0)
    parser.add_argument("--num-agents", type=int, default=2)
    parser.add_argument("--auto-offset-reset", default="earliest")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = KafkaReceiverConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        run_id=args.run_id,
        result_file=args.result_file,
        agent_id=args.agent_id,
        num_agents=args.num_agents,
        auto_offset_reset=args.auto_offset_reset,
    )
    KafkaReceiver(config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
