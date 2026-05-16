"""Emit synthetic clickstream events into the Kafka topic.

Usage:
    uv run python scripts/seed_kafka.py --rate 50 --duration 600
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


ACTIONS = ["view", "click", "scroll", "purchase", "add_to_cart"]
USERS = [f"u{i:03d}" for i in range(50)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=20, help="events per second")
    parser.add_argument("--duration", type=int, default=300, help="seconds to run")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("LAMBDA_KAFKA_BOOTSTRAP_EXTERNAL", "localhost:19092"),
    )
    parser.add_argument("--topic", default=os.environ.get("LAMBDA_TOPIC", "events"))
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})

    interval = 1.0 / args.rate
    end = time.time() + args.duration
    n = 0
    while time.time() < end:
        evt = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(USERS),
            "action": random.choices(ACTIONS, weights=[5, 3, 2, 1, 2])[0],
            "ts": datetime.now(tz=timezone.utc).isoformat(),
        }
        producer.produce(args.topic, json.dumps(evt).encode("utf-8"))
        n += 1
        if n % 500 == 0:
            producer.poll(0)
            print(f"  ...sent {n}")
        time.sleep(interval)

    producer.flush()
    print(f"done, {n} events sent to {args.topic}")


if __name__ == "__main__":
    main()
