#!/usr/bin/env python3
import argparse
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from experiment.communication_patterns import get_run_agent_topic

RESULTS_ROOT = ROOT / "results"
DEFAULT_KAFKA_BROKER = "IP:9092"
DEFAULT_MQTT_BROKER = "IP"
DEFAULT_ZENOH_BROKER = "tcp/IP:7447"
KAFKA_API_VERSION = (3, 5, 0)

PROTOCOL_MAP = {
    "kafka": {
        "sender": {
            1: ROOT / "kafka_files" / "kagent_sender.py",
            2: ROOT / "kafka_files" / "kagent_sender.py",
        },
        "receiver": {
            1: ROOT / "kafka_files" / "kagent_receiver.py",
            2: ROOT / "kafka_files" / "kagent_receiver.py",
        },
        "agent_sender": ROOT / "kafka_files" / "kagent_sender.py",
        "agent_receiver": ROOT / "kafka_files" / "kagent_receiver.py",
    },
    "mqtt": {
        "sender": {
            1: ROOT / "mqtt_files" / "magent_sender.py",
            2: ROOT / "mqtt_files" / "magent_sender.py",
        },
        "receiver": {
            1: ROOT / "mqtt_files" / "magent_receiver.py",
            2: ROOT / "mqtt_files" / "magent_receiver.py",
        },
        "agent_sender": ROOT / "mqtt_files" / "magent_sender.py",
        "agent_receiver": ROOT / "mqtt_files" / "magent_receiver.py",
    },
    "zenoh": {
        "sender": {
            1: ROOT / "zenoh_files" / "zagent_sender.py",
            2: ROOT / "zenoh_files" / "zagent_sender.py",
        },
        "receiver": {
            1: ROOT / "zenoh_files" / "zagent_receiver.py",
            2: ROOT / "zenoh_files" / "zagent_receiver.py",
        },
        "agent_sender": ROOT / "zenoh_files" / "zagent_sender.py",
        "agent_receiver": ROOT / "zenoh_files" / "zagent_receiver.py",
    },
}


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def kafka_agent_topic(run_id: str, agent_id: int) -> str:
    return get_run_agent_topic(run_id, agent_id)


def ensure_kafka_topics(bootstrap_servers: str, topics: list[str]) -> None:
    deadline = time.time() + 60
    while True:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                api_version=KAFKA_API_VERSION,
            )
            try:
                new_topics = [
                    NewTopic(name=topic, num_partitions=1, replication_factor=1)
                    for topic in topics
                ]
                try:
                    admin.create_topics(new_topics=new_topics, validate_only=False)
                    print(f"Ensured Kafka topics exist: {topics}")
                except TopicAlreadyExistsError:
                    pass
            finally:
                admin.close()
            return
        except NoBrokersAvailable as error:
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Kafka admin client could not connect to {bootstrap_servers} after waiting 60 seconds"
                ) from error
            time.sleep(2)


def append_optional_arg(cmd: list[str], flag: str, value) -> None:
    if value is not None:
        cmd.extend([flag, str(value)])


def build_sender_cmd(
    args,
    script: Path,
    run_id: str,
    result_file: Path,
    agent_id: int,
    num_agents: int,
    target_agent_id_override: int | None = None,
) -> list[str]:
    cmd = [
        sys.executable,
        str(script),
        "--run-id",
        run_id,
        "--result-file",
        str(result_file),
        "--agent-id",
        str(agent_id),
        "--num-agents",
        str(num_agents),
        "--communication-pattern",
        args.communication_pattern,
        "--generation-pattern",
        args.generation_pattern,
        "--message-type-distribution",
        args.message_type_distribution,
        "--intensity",
        args.intensity,
        "--k-in-flight",
        str(args.k),
    ]
    append_optional_arg(cmd, "--rate-msg-per-sec", args.rate)
    append_optional_arg(cmd, "--message-size-bytes", args.message_size_bytes)

    if args.middleware == "kafka":
        target_agent_id = (
            target_agent_id_override
            if target_agent_id_override is not None
            else args.target_agent_id
        )
        cmd.extend(
            [
                "--bootstrap-servers",
                args.kafka_broker,
                "--acks",
                "0" if args.reliability_mode == "best-effort" else "all",
                "--metadata-timeout-ms",
                str(args.kafka_metadata_timeout_ms),
                "--target-agent-id",
                str(target_agent_id),
            ]
        )
    elif args.middleware == "mqtt":
        cmd.extend(
            [
                "--host",
                args.mqtt_broker,
                "--port",
                str(args.mqtt_port),
                "--qos",
                "0" if args.reliability_mode == "best-effort" else "1",
            ]
        )
    elif args.middleware == "zenoh":
        cmd.extend(["--key-prefix", args.zenoh_key_prefix])
        append_optional_arg(cmd, "--broker-endpoint", args.zenoh_broker)
        cmd.extend(
            [
                "--reliability",
                "best_effort" if args.reliability_mode == "best-effort" else "reliable",
            ]
        )

    return cmd


def build_receiver_cmd(
    args,
    script: Path,
    run_id: str,
    result_file: Path,
    agent_id: int,
    num_agents: int,
) -> list[str]:
    cmd = [
        sys.executable,
        str(script),
        "--run-id",
        run_id,
        "--result-file",
        str(result_file),
        "--agent-id",
        str(agent_id),
        "--num-agents",
        str(num_agents),
    ]

    if args.middleware == "kafka":
        cmd.extend(
            [
                "--bootstrap-servers",
                args.kafka_broker,
            ]
        )
    elif args.middleware == "mqtt":
        cmd.extend(["--host", args.mqtt_broker, "--port", str(args.mqtt_port)])
    elif args.middleware == "zenoh":
        cmd.extend(["--key-prefix", args.zenoh_key_prefix])
        append_optional_arg(cmd, "--broker-endpoint", args.zenoh_broker)

    return cmd


def combine_results(args, sender_data: dict, receiver_data: dict, run_id: str) -> dict:
    sent_count = int(sender_data.get("sent_count", 0))
    recv_count = int(receiver_data.get("recv_count", 0))
    loss_rate = (
        max(0.0, (sent_count - recv_count) / sent_count) if sent_count > 0 else None
    )

    combined = {
        "run_id": run_id,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "middleware": args.middleware,
        "stream": args.stream,
        "reliability_mode": args.reliability_mode,
        "num_agents": args.num_agents,
        "communication_pattern": args.communication_pattern,
        "generation_pattern": args.generation_pattern,
        "message_type_distribution": args.message_type_distribution,
        "intensity": args.intensity,
        "rate_msg_per_sec": args.rate,
        "k_in_flight": args.k,
        "duration_sec": args.duration,
        "warmup_sec": args.warmup,
        "cooldown_sec": args.cooldown,
        "msg_type_distribution": "lightweight-dominant",
        "sent_count": sent_count,
        "recv_count": recv_count,
        "loss_rate": loss_rate,
        "sender_throughput_msg_per_sec": sender_data.get("throughput_msg_per_sec"),
        "sender_throughput_mb_per_sec": sender_data.get("throughput_mb_per_sec"),
        "receiver_throughput_msg_per_sec": receiver_data.get("throughput_msg_per_sec"),
        "receiver_throughput_mb_per_sec": receiver_data.get("throughput_mb_per_sec"),
        "latency_p50_ms": receiver_data.get("latency_p50_ms"),
        "latency_p95_ms": receiver_data.get("latency_p95_ms"),
        "latency_p99_ms": receiver_data.get("latency_p99_ms"),
        "latency_std_ms": receiver_data.get("latency_std_ms"),
        "latency_jitter_mean_ms": receiver_data.get("latency_jitter_mean_ms"),
        "latency_jitter_std_ms": receiver_data.get("latency_jitter_std_ms"),
        "sender_summary": sender_data,
        "receiver_summary": receiver_data,
    }
    return combined


def aggregate_agent_results(run_dir: Path, num_agents: int, args) -> dict:
    """
    Aggregate results from multiple agent runs into a single combined result.

    Args:
        run_dir: Directory containing per-agent result files
        num_agents: Number of agents
        args: Benchmark arguments

    Returns:
        Aggregated result dictionary
    """
    import statistics

    all_sender_data = []
    all_receiver_data = []

    for agent_id in range(num_agents):
        sender_file = run_dir / f"sender_agent_{agent_id}.json"
        receiver_file = run_dir / f"receiver_agent_{agent_id}.json"

        if sender_file.exists():
            all_sender_data.append(load_json(sender_file))
        if receiver_file.exists():
            all_receiver_data.append(load_json(receiver_file))

    # Aggregate metrics
    total_sent = sum(int(d.get("sent_count", 0)) for d in all_sender_data)
    total_sent_bytes = sum(int(d.get("sent_bytes", 0)) for d in all_sender_data)
    total_recv = sum(int(d.get("recv_count", 0)) for d in all_receiver_data)
    total_recv_bytes = sum(int(d.get("recv_bytes", 0)) for d in all_receiver_data)

    loss_rate = (
        max(0.0, (total_sent - total_recv) / total_sent) if total_sent > 0 else None
    )

    # Aggregate latencies (if available)
    latency_p50_values = [
        d.get("latency_p50_ms") for d in all_receiver_data if d.get("latency_p50_ms")
    ]
    latency_p95_values = [
        d.get("latency_p95_ms") for d in all_receiver_data if d.get("latency_p95_ms")
    ]
    latency_p99_values = [
        d.get("latency_p99_ms") for d in all_receiver_data if d.get("latency_p99_ms")
    ]
    latency_std_values = [
        d.get("latency_std_ms") for d in all_receiver_data if d.get("latency_std_ms")
    ]
    latency_jitter_mean_values = [
        d.get("latency_jitter_mean_ms")
        for d in all_receiver_data
        if d.get("latency_jitter_mean_ms") is not None
    ]
    latency_jitter_std_values = [
        d.get("latency_jitter_std_ms")
        for d in all_receiver_data
        if d.get("latency_jitter_std_ms") is not None
    ]

    # Compute mean latencies across all agents
    avg_latency_p50 = (
        statistics.mean(latency_p50_values) if latency_p50_values else None
    )
    avg_latency_p95 = (
        statistics.mean(latency_p95_values) if latency_p95_values else None
    )
    avg_latency_p99 = (
        statistics.mean(latency_p99_values) if latency_p99_values else None
    )
    avg_latency_std = (
        statistics.mean(latency_std_values) if latency_std_values else None
    )
    avg_latency_jitter_mean = (
        statistics.mean(latency_jitter_mean_values)
        if latency_jitter_mean_values
        else None
    )
    avg_latency_jitter_std = (
        statistics.mean(latency_jitter_std_values)
        if latency_jitter_std_values
        else None
    )

    # Compute aggregate throughput
    sender_duration = (
        all_sender_data[0].get("duration_sec", 0) if all_sender_data else 0
    )
    receiver_duration = (
        all_receiver_data[0].get("duration_sec", 0) if all_receiver_data else 0
    )

    sender_tput_msg_per_sec = (
        total_sent / sender_duration if sender_duration > 0 else None
    )
    sender_tput_mb_per_sec = (
        (total_sent_bytes / (1024 * 1024)) / sender_duration
        if sender_duration > 0
        else None
    )
    receiver_tput_msg_per_sec = (
        total_recv / receiver_duration if receiver_duration > 0 else None
    )
    receiver_tput_mb_per_sec = (
        (total_recv_bytes / (1024 * 1024)) / receiver_duration
        if receiver_duration > 0
        else None
    )

    combined = {
        "run_id": args.run_id,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "middleware": args.middleware,
        "stream": args.stream,
        "reliability_mode": args.reliability_mode,
        "num_agents": num_agents,
        "communication_pattern": args.communication_pattern,
        "generation_pattern": args.generation_pattern,
        "message_type_distribution": args.message_type_distribution,
        "intensity": args.intensity,
        "rate_msg_per_sec": args.rate,
        "k_in_flight": args.k,
        "duration_sec": args.duration,
        "warmup_sec": args.warmup,
        "cooldown_sec": args.cooldown,
        "msg_type_distribution": "lightweight-dominant",
        "aggregation": {
            "sent_count_total": total_sent,
            "recv_count_total": total_recv,
            "loss_rate": loss_rate,
            "sender_throughput_msg_per_sec": sender_tput_msg_per_sec,
            "sender_throughput_mb_per_sec": sender_tput_mb_per_sec,
            "receiver_throughput_msg_per_sec": receiver_tput_msg_per_sec,
            "receiver_throughput_mb_per_sec": receiver_tput_mb_per_sec,
            "latency_p50_ms_avg": avg_latency_p50,
            "latency_p95_ms_avg": avg_latency_p95,
            "latency_p99_ms_avg": avg_latency_p99,
            "latency_std_ms_avg": avg_latency_std,
            "latency_jitter_mean_ms_avg": avg_latency_jitter_mean,
            "latency_jitter_std_ms_avg": avg_latency_jitter_std,
        },
        "per_agent_results": {
            "senders": all_sender_data,
            "receivers": all_receiver_data,
        },
    }
    return combined


def validate_process(proc: subprocess.Popen, role: str) -> None:
    code = proc.poll()
    if code is None:
        return
    if code != 0:
        raise RuntimeError(f"{role} process exited with code {code}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one benchmark trial and store normalized metrics"
    )
    parser.add_argument(
        "--middleware", choices=["kafka", "mqtt", "zenoh"], required=True
    )
    parser.add_argument("--stream", type=int, choices=[1, 2], default=1)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--cooldown", type=int, default=2)
    parser.add_argument(
        "--intensity", choices=["low", "medium", "high"], default="medium"
    )
    parser.add_argument("--rate", type=float, default=None)
    parser.add_argument("--k", type=int, default=1)
    parser.add_argument("--reliability-mode", default="best-effort")
    parser.add_argument("--num-agents", type=int, default=1)
    parser.add_argument("--communication-pattern", default="pairwise")
    parser.add_argument("--generation-pattern", default="constant")
    parser.add_argument(
        "--message-type-distribution",
        choices=["lightweight-dominant", "uniform", "heavy-tail"],
        default="lightweight-dominant",
    )
    parser.add_argument("--message-size-bytes", type=int, default=None)
    parser.add_argument("--kafka-broker", default=DEFAULT_KAFKA_BROKER)
    parser.add_argument("--kafka-metadata-timeout-ms", type=int, default=10000)
    parser.add_argument("--mqtt-broker", default=DEFAULT_MQTT_BROKER)
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--zenoh-broker", default=DEFAULT_ZENOH_BROKER)
    parser.add_argument("--zenoh-key-prefix", default="thesis")
    parser.add_argument("--target-agent-id", type=int, default=1)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    protocol = PROTOCOL_MAP[args.middleware]

    # Determine if we're in single-agent or multi-agent mode
    if args.num_agents <= 1:
        # Single-agent mode (legacy)
        return run_single_agent_benchmark(args, protocol)
    else:
        # Multi-agent mode
        return run_multi_agent_benchmark(args, protocol)


def run_single_agent_benchmark(args, protocol: dict) -> int:
    """Run a single-agent benchmark (legacy mode)."""
    sender_script = protocol["sender"][args.stream]
    receiver_script = protocol["receiver"][args.stream]

    run_id = args.run_id or f"{args.middleware}-s{args.stream}-{int(time.time())}"
    run_dir = RESULTS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    sender_result = run_dir / "sender.json"
    receiver_result = run_dir / "receiver.json"
    combined_result = run_dir / "combined.json"

    if args.middleware == "kafka":
        ensure_kafka_topics(
            args.kafka_broker,
            [
                kafka_agent_topic(run_id, agent_id)
                for agent_id in range(args.num_agents)
            ],
        )

    sender_cmd = build_sender_cmd(
        args,
        sender_script,
        run_id,
        sender_result,
        agent_id=0,
        num_agents=1,
        target_agent_id_override=0,
    )
    receiver_cmd = build_receiver_cmd(
        args,
        receiver_script,
        run_id,
        receiver_result,
        agent_id=0,
        num_agents=1,
    )

    receiver_proc = subprocess.Popen(receiver_cmd, cwd=str(ROOT))
    time.sleep(1.5)
    sender_proc = subprocess.Popen(sender_cmd, cwd=str(ROOT))

    try:
        time.sleep(0.8)
        validate_process(receiver_proc, "receiver")
        validate_process(sender_proc, "sender")

        if args.warmup > 0:
            time.sleep(args.warmup)
            validate_process(receiver_proc, "receiver")
            validate_process(sender_proc, "sender")
        time.sleep(args.duration)
        validate_process(receiver_proc, "receiver")
        validate_process(sender_proc, "sender")
        if args.cooldown > 0:
            time.sleep(args.cooldown)
            validate_process(receiver_proc, "receiver")
            validate_process(sender_proc, "sender")
    finally:
        stop_process(sender_proc)
        stop_process(receiver_proc)

    if sender_proc.returncode not in (0, 130):
        raise RuntimeError(f"sender process returned {sender_proc.returncode}")
    if receiver_proc.returncode not in (0, 130):
        raise RuntimeError(f"receiver process returned {receiver_proc.returncode}")

    sender_data = load_json(sender_result)
    receiver_data = load_json(receiver_result)
    if not sender_data:
        raise RuntimeError(f"Missing sender summary: {sender_result}")
    if not receiver_data:
        raise RuntimeError(f"Missing receiver summary: {receiver_result}")
    combined = combine_results(args, sender_data, receiver_data, run_id)

    with combined_result.open("w", encoding="utf-8") as handle:
        json.dump(combined, handle, indent=2, sort_keys=True)

    print(f"Run completed. Results in: {run_dir}")
    return 0


def run_multi_agent_benchmark(args, protocol: dict) -> int:
    """Run a multi-agent benchmark."""
    sender_script = protocol["agent_sender"]
    receiver_script = protocol["agent_receiver"]

    run_id = args.run_id or f"{args.middleware}-ma{args.num_agents}-{int(time.time())}"
    args.run_id = run_id  # Store for aggregation
    run_dir = RESULTS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    combined_result = run_dir / "combined.json"

    # Clean stale per-agent artifacts when reusing a run_id.
    for agent_id in range(args.num_agents):
        (run_dir / f"sender_agent_{agent_id}.json").unlink(missing_ok=True)
        (run_dir / f"receiver_agent_{agent_id}.json").unlink(missing_ok=True)
    combined_result.unlink(missing_ok=True)

    if args.middleware == "kafka":
        ensure_kafka_topics(
            args.kafka_broker,
            [
                kafka_agent_topic(run_id, agent_id)
                for agent_id in range(args.num_agents)
            ],
        )

    # Start all receiver processes first
    receiver_procs = []
    for agent_id in range(args.num_agents):
        receiver_result = run_dir / f"receiver_agent_{agent_id}.json"
        receiver_cmd = build_receiver_cmd(
            args,
            receiver_script,
            run_id,
            receiver_result,
            agent_id=agent_id,
            num_agents=args.num_agents,
        )
        receiver_proc = subprocess.Popen(receiver_cmd, cwd=str(ROOT))
        receiver_procs.append((agent_id, receiver_proc))
        print(f"Started receiver for agent {agent_id}")

    # Give receivers time to initialize and subscribe to their topics
    time.sleep(2.0)

    # Start all sender processes
    sender_procs = []
    for agent_id in range(args.num_agents):
        sender_result = run_dir / f"sender_agent_{agent_id}.json"
        sender_cmd = build_sender_cmd(
            args,
            sender_script,
            run_id,
            sender_result,
            agent_id=agent_id,
            num_agents=args.num_agents,
        )
        sender_proc = subprocess.Popen(sender_cmd, cwd=str(ROOT))
        sender_procs.append((agent_id, sender_proc))
        print(f"Started sender for agent {agent_id}")

    try:
        time.sleep(0.8)
        for agent_id, proc in sender_procs + receiver_procs:
            validate_process(proc, f"agent-{agent_id}")

        if args.warmup > 0:
            print(f"Warming up for {args.warmup} seconds...")
            time.sleep(args.warmup)
            for agent_id, proc in sender_procs + receiver_procs:
                validate_process(proc, f"agent-{agent_id}")

        print(f"Running benchmark for {args.duration} seconds...")
        time.sleep(args.duration)
        for agent_id, proc in sender_procs + receiver_procs:
            validate_process(proc, f"agent-{agent_id}")

        if args.cooldown > 0:
            print(f"Cooling down for {args.cooldown} seconds...")
            time.sleep(args.cooldown)
            for agent_id, proc in sender_procs + receiver_procs:
                validate_process(proc, f"agent-{agent_id}")
    finally:
        print("Stopping all agents...")
        for agent_id, proc in sender_procs + receiver_procs:
            stop_process(proc)

    # Check return codes
    for agent_id, proc in sender_procs + receiver_procs:
        if proc.returncode not in (0, 130, -2, -15):  # -2 is SIGINT, -15 is SIGTERM
            print(f"Warning: agent-{agent_id} process returned {proc.returncode}")

    missing_results = []
    for agent_id in range(args.num_agents):
        sender_path = run_dir / f"sender_agent_{agent_id}.json"
        receiver_path = run_dir / f"receiver_agent_{agent_id}.json"
        if not sender_path.exists():
            missing_results.append(str(sender_path))
        if not receiver_path.exists():
            missing_results.append(str(receiver_path))
    if missing_results:
        missing_list = "\n".join(f"  - {path}" for path in missing_results)
        raise RuntimeError(
            "Missing per-agent result files. Some processes were terminated before "
            "writing summaries (often due to broker/send blocking).\n"
            f"{missing_list}"
        )

    # Aggregate results from all agents
    combined = aggregate_agent_results(run_dir, args.num_agents, args)

    with combined_result.open("w", encoding="utf-8") as handle:
        json.dump(combined, handle, indent=2, sort_keys=True)

    print(f"Multi-agent run completed. Results in: {run_dir}")
    agg = combined.get("aggregation") or {}
    sent_total = agg.get("sent_count_total")
    recv_total = agg.get("recv_count_total")
    loss_rate = agg.get("loss_rate")

    loss_rate_str = f"{loss_rate:.4f}" if isinstance(loss_rate, (int, float)) else "n/a"
    sent_str = sent_total if sent_total is not None else "n/a"
    recv_str = recv_total if recv_total is not None else "n/a"

    print(
        f"  Total sent: {sent_str}, "
        f"Total received: {recv_str}, "
        f"Loss rate: {loss_rate_str}"
    )

    if isinstance(sent_total, int) and isinstance(recv_total, int):
        if sent_total > 0 and recv_total == 0:
            print(
                "[WARNING] Messages were sent but none were received. "
                "Check broker endpoint/reachability and topic alignment."
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
