#!/usr/bin/env python3
"""Verify processed Kafka events against the PostgreSQL sink and DLT."""

import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime


BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9092,kafka-2:9092,kafka-3:9092",
)
KAFKA_CLI = "/opt/kafka/bin"


def run_command(command):
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    return result.stdout


def read_sink_events():
    database_user = os.environ.get("SINK_DB_USER", "hw2")
    database_name = os.environ.get("SINK_DB_NAME", "hw2")
    query = (
        "SELECT user_id, seq FROM processed_events "
        "ORDER BY user_id, written_at, seq"
    )
    output = run_command(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-X", "-A", "-t", "-F", "\t",
            "-U", database_user, "-d", database_name, "-c", query,
        ]
    )
    rows = []
    for line in output.splitlines():
        if not line:
            continue
        user_id, sequence = line.split("\t", maxsplit=1)
        rows.append((user_id, int(sequence)))
    return rows


def read_dropped_duplicates():
    database_user = os.environ.get("SINK_DB_USER", "hw2")
    database_name = os.environ.get("SINK_DB_NAME", "hw2")
    query = "SELECT dropped_duplicates FROM duplicate_delivery_stats WHERE id = 1"
    output = run_command(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-X", "-A", "-t",
            "-U", database_user, "-d", database_name, "-c", query,
        ]
    )
    value = output.strip()
    if not value:
        raise RuntimeError("Duplicate-delivery counter is missing from PostgreSQL")
    return int(value)


def read_dead_letter_events():
    offsets_output = run_command(
        [
            "docker", "compose", "exec", "-T", "kafka-1",
            f"{KAFKA_CLI}/kafka-get-offsets.sh",
            "--bootstrap-server", BOOTSTRAP_SERVERS,
            "--topic", "events.dlt",
            "--time", "-1",
        ]
    )
    end_offsets = {}
    for line in offsets_output.splitlines():
        topic, partition, offset = line.rsplit(":", maxsplit=2)
        if topic == "events.dlt":
            end_offsets[int(partition)] = int(offset)

    message_count = sum(end_offsets.values())
    if message_count == 0:
        return []

    output = run_command(
        [
            "docker", "compose", "exec", "-T", "kafka-1",
            f"{KAFKA_CLI}/kafka-console-consumer.sh",
            "--bootstrap-server", BOOTSTRAP_SERVERS,
            "--topic", "events.dlt",
            "--from-beginning",
            "--max-messages", str(message_count),
            "--timeout-ms", "30000",
            "--property", "print.value=true",
            "--property", "print.key=false",
            "--property", "print.headers=false",
        ]
    )
    events = []
    for line in output.splitlines():
        if not line or line.startswith("Processed a total of "):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exception:
            raise RuntimeError(f"Could not decode DLT event: {line}") from exception
        if "user_id" not in event or "seq" not in event:
            raise RuntimeError(f"DLT event has no user_id/seq: {line}")
        events.append(event)

    if len(events) != message_count:
        raise RuntimeError(
            f"Read {len(events)} DLT events, but Kafka end offsets indicate {message_count}"
        )
    return events


def count_inversions(sequences):
    if len(sequences) < 2:
        return 0
    scratch = [0] * len(sequences)

    def merge_count(start, end):
        if end - start < 2:
            return 0
        middle = (start + end) // 2
        inversions = merge_count(start, middle) + merge_count(middle, end)
        left, right, output = start, middle, start
        while left < middle and right < end:
            if sequences[left] <= sequences[right]:
                scratch[output] = sequences[left]
                left += 1
            else:
                scratch[output] = sequences[right]
                right += 1
                inversions += middle - left
            output += 1
        while left < middle:
            scratch[output] = sequences[left]
            left += 1
            output += 1
        while right < end:
            scratch[output] = sequences[right]
            right += 1
            output += 1
        sequences[start:end] = scratch[start:end]
        return inversions

    return merge_count(0, len(sequences))


def main():
    github_login = os.environ.get("GITHUB_LOGIN", "")
    print(
        f"{github_login or 'unknown'} "
        f"{datetime.now().astimezone().isoformat(timespec='seconds')}",
        flush=True,
    )
    if not github_login:
        raise RuntimeError("Set GITHUB_LOGIN to your GitHub username before running verify.py")

    sink_rows = read_sink_events()
    dlt_events = read_dead_letter_events()
    dropped_duplicates = read_dropped_duplicates()

    sequences_by_user = defaultdict(set)
    sink_sequences_by_user = defaultdict(list)
    for user_id, sequence in sink_rows:
        sequences_by_user[user_id].add(sequence)
        sink_sequences_by_user[user_id].append(sequence)
    for event in dlt_events:
        sequences_by_user[event["user_id"]].add(int(event["seq"]))

    missing_sequences = {}
    for user_id, sequences in sequences_by_user.items():
        missing = [
            expected
            for expected in range(1, max(sequences) + 1)
            if expected not in sequences
        ]
        if missing:
            missing_sequences[user_id] = missing

    out_of_order_writes = sum(
        count_inversions(sequences)
        for sequences in sink_sequences_by_user.values()
    )

    print(f"Sink events: {len(sink_rows)}")
    print(f"Events in DLT: {len(dlt_events)}")
    print(f"Duplicates dropped: {dropped_duplicates}")
    print(f"Out-of-order writes: {out_of_order_writes}")
    if missing_sequences:
        print("Sequence gaps:")
        for user_id in sorted(missing_sequences):
            print(f"  {user_id}: {missing_sequences[user_id]}")
    else:
        print("Sequence gaps: none")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as exception:
        print(f"Verification failed: {exception}", file=sys.stderr)
        sys.exit(1)
