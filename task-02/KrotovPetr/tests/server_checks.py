"""Reproducible technical checks. The student writes experiment hypotheses separately."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
PRODUCERS = [f"producer-{i}" for i in range(1, 4)]
CONSUMERS = [f"consumer-{i}" for i in range(1, 4)]


class Check:
    def __init__(self, name, **settings):
        self.run = f"{name}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
        self.directory = ROOT / "artifacts" / self.run
        self.directory.mkdir(parents=True)
        self.env = {**os.environ, "RUN_ID": self.run, "USERS_PER_PRODUCER": "10",
                    "EVENTS_PER_USER": "100", "RATE_PER_SECOND": "100",
                    "PROCESSING_DELAY_MS": "10", "BEFORE_COMMIT_DELAY_MS": "0",
                    "PRODUCER_LINGER_MS": "5", "POISON_RATE": "0.02", "KEY_MODE": "user_id",
                    **{key: str(value) for key, value in settings.items()}}

    def compose(self, *args, file=None, timeout=240, check=True):
        result = subprocess.run([str(ROOT / "local-compose"), *args], env=self.env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        if file:
            (self.directory / file).write_text(result.stdout)
            (self.directory / f"{file}.stderr").write_text(result.stderr)
        if check and result.returncode:
            raise RuntimeError(f"Compose {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")
        return result.stdout

    def start(self):
        print(f"Starting {self.run}", flush=True)
        self.compose("up", "-d", "--no-deps", *CONSUMERS, file="consumers-start.txt")
        self.compose("up", "-d", "--no-deps", *PRODUCERS, file="producers-start.txt")

    def verify(self):
        output = self.compose("run", "--rm", "--no-deps", "verify", "--wait", "300",
                              file="verify.txt", timeout=340)
        result = json.loads(output[output.index("\n{\n") + 1:])
        summary = {key: value for key, value in result.items() if key != "per_user"}
        (self.directory / "verify-summary.txt").write_text(output.splitlines()[0] + "\n" + json.dumps(summary, indent=2))
        self.compose("logs", "--no-color", *PRODUCERS, *CONSUMERS, file="apps.log")
        print(json.dumps(summary), flush=True)
        return result

    def status(self, name):
        result = subprocess.run([str(ROOT / "status")], env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
        (self.directory / name).write_text(result.stdout)
        if result.returncode:
            raise RuntimeError(f"status failed: {result.stdout}")


def brokers():
    run = Check("brokers", EVENTS_PER_USER=200, RATE_PER_SECOND=50)
    run.start()
    try:
        time.sleep(3)
        run.status("01-all-brokers.txt")
        run.compose("stop", "kafka-3", file="02-stop-one.txt")
        run.status("03-one-broker-down.txt")
        run.compose("stop", "kafka-2", file="04-stop-two.txt")
        time.sleep(8)
        run.compose("logs", "--no-color", "--tail", "180", *PRODUCERS, *CONSUMERS,
                    file="05-two-brokers-down.log")
        run.compose("logs", "--no-color", "--tail", "250", "kafka-1", file="06-surviving-broker.log")
    finally:
        run.compose("up", "-d", "--no-deps", "--wait", "kafka-2", "kafka-3", file="07-restore.txt")
    result = run.verify()
    run.status("08-restored.txt")
    if not result["ok"]:
        raise RuntimeError("Broker recovery verification failed")


def keys():
    results = []
    for mode in ("user_id", "none"):
        run = Check(f"key_{mode}", KEY_MODE=mode)
        run.start()
        results.append(run.verify())
    comparison = [{"run_id": row["run_id"], "key_mode": row["key_mode"],
                   "expected": row["expected"], "missing_count": row["missing_count"],
                   "order_violations": row["order_violations"],
                   "users_on_multiple_partitions": sum(len(user["sink_partitions"]) > 1 for user in row["per_user"])}
                  for row in results]
    path = ROOT / "artifacts" / "key-comparison.json"
    path.write_text(json.dumps(comparison, indent=2))
    print(path.read_text(), flush=True)


def sigterm():
    run = Check("sigterm", EVENTS_PER_USER=1000, RATE_PER_SECOND=1000, PRODUCER_LINGER_MS=5000)
    run.start()
    time.sleep(1)
    run.compose("stop", "-t", "150", *PRODUCERS, file="01-sigterm.txt")
    logs = run.compose("logs", "--no-color", *PRODUCERS, file="02-producers.log")
    rows = []
    for line in logs.splitlines():
        try:
            rows.append(json.loads(line[line.index("{"):]))
        except (ValueError, json.JSONDecodeError):
            continue
    flushing = [row for row in rows if row.get("action") == "producer_flushing"]
    finished = [row for row in rows if row.get("action") == "producer_finished"]
    if len(flushing) != 3 or len(finished) != 3 or not any(row["queued"] > 0 for row in flushing):
        raise RuntimeError("The SIGTERM check did not exercise a nonempty producer buffer")
    if not all(row["generated"] == row["acknowledged"] and row["errors"] == 0 for row in finished):
        raise RuntimeError("SIGTERM left messages unconfirmed")
    audit = run.compose("run", "--rm", "--no-deps", "tests", "python", "-m", "src.audit_run",
                        file="03-source-audit.txt")
    print(audit, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("check", choices=("brokers", "keys", "sigterm"))
    args = parser.parse_args()
    {"brokers": brokers, "keys": keys, "sigterm": sigterm}[args.check]()
