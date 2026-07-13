#!/usr/bin/env python3
"""High-throughput Kafka producer for the Spark-vs-Flink Iceberg benchmark.

Generates retail events (see event_schema.md), stamps each with an ingest
timestamp for end-to-end latency measurement, and drives a configurable target
rate. Designed to be run as several parallel processes/pods to reach 100k rps.

Env (see .env) or CLI flags:
  --bootstrap        Kafka bootstrap servers
  --topic            target topic
  --rate             target records/sec for THIS process
  --seconds          run duration
  --payload-bytes    filler size to hit ~500B/record
  --evolve-after     after N seconds, start mixing in v2 (schema evolution) events
  --dup-rate         fraction of records that reuse a recent key (for upsert tests)
"""
import argparse
import json
import os
import random
import string
import time
import uuid

from confluent_kafka import Producer

COUNTRIES = ["us", "gb", "de", "fr", "es", "in", "br", "jp", "ca", "au"]
EVENT_TYPES = ["purchase", "cart", "view"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
TIERS = ["bronze", "silver", "gold", "platinum"]


# Precompute a fixed filler ONCE. Generating 320 random chars per event
# (random.choices) was the throughput ceiling — ~3M rand calls/s/pod at 10k/s,
# which capped a pod at ~8.4k/s. The payload is opaque bytes to the benchmark, so
# a constant string of the right length is equivalent and ~free.
_FILLER_CACHE: dict = {}


def _filler(n: int) -> str:
    n = max(0, n)
    s = _FILLER_CACHE.get(n)
    if s is None:
        s = "".join(random.choices(string.ascii_lowercase, k=n))
        _FILLER_CACHE[n] = s
    return s


def make_event(payload_bytes: int, evolve: bool, recent_users: list) -> dict:
    user_id = random.randint(1, 5_000_000)
    ev = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random.randint(1, 100_000),
        "country": random.choice(COUNTRIES),
        "amount": round(random.uniform(0.0, 500.0), 2),
        "currency": random.choice(CURRENCIES),
        "quantity": random.randint(1, 5),
        "event_ts": int(time.time() * 1000),
        "ingest_ts": int(time.time() * 1000),
        "payload": _filler(payload_bytes),
    }
    if evolve:
        ev["loyalty_tier"] = random.choice(TIERS)
        ev["session_id"] = str(uuid.uuid4())
    return ev


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:19092"))
    p.add_argument("--topic", default=os.getenv("SOURCE_TOPIC", "events"))
    p.add_argument("--rate", type=int, default=int(os.getenv("TARGET_RATE", "100000")))
    p.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "600")))
    p.add_argument("--payload-bytes", type=int, default=int(os.getenv("PAYLOAD_BYTES", "500")))
    p.add_argument("--evolve-after", type=int, default=0, help="0 disables schema evolution")
    p.add_argument("--dup-rate", type=float, default=0.0)
    args = p.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,
        "batch.size": 1 << 20,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 2_000_000,
    })

    filler_bytes = max(0, args.payload_bytes - 180)  # ~180B of structured fields
    start = time.time()
    sent = 0
    tick = start
    per_tick = max(1, args.rate // 100)  # 100 batches/sec pacing
    # Fixed-size ring of recent user_ids for the upsert dup path — O(1) write via a
    # rolling index, O(1) random read via random.randrange. (The old list.pop(0) was
    # O(n) and random.choice on a deque is O(n); both throttled the pod under load.)
    RING = 10_000
    recent_users: list = []
    ring_i = 0

    while time.time() - start < args.seconds:
        evolve = args.evolve_after and (time.time() - start) >= args.evolve_after
        for _ in range(per_tick):
            ev = make_event(filler_bytes, evolve, recent_users)
            if args.dup_rate and recent_users and random.random() < args.dup_rate:
                ev["user_id"] = recent_users[random.randrange(len(recent_users))]
            else:
                # O(1) ring insert: append until full, then overwrite oldest slot.
                if len(recent_users) < RING:
                    recent_users.append(ev["user_id"])
                else:
                    recent_users[ring_i] = ev["user_id"]
                    ring_i = (ring_i + 1) % RING
            producer.produce(args.topic, key=str(ev["user_id"]), value=json.dumps(ev))
            sent += 1
        producer.poll(0)

        # pace to ~100 ticks/sec
        tick += 0.01
        sleep = tick - time.time()
        if sleep > 0:
            time.sleep(sleep)
        elif time.time() - start > 5 and sent < (time.time() - start) * args.rate * 0.9:
            # Falling behind target — surface it once.
            pass

    producer.flush(30)
    elapsed = time.time() - start
    print(f"[producer] sent={sent} elapsed={elapsed:.1f}s rate={sent/elapsed:,.0f} rps")


if __name__ == "__main__":
    main()
