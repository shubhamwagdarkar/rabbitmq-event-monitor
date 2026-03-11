"""
simulate.py — Populate RabbitMQ with test scenarios to trigger each alert type.

Usage:
    python simulate.py

Scenarios created:
    1. healthy-queue      → 10 messages, no alert expected
    2. orders-queue       → 600 messages, triggers WARNING (threshold: 500)
    3. overloaded-queue   → 2500 messages, triggers CRITICAL (threshold: 2000)
    4. orders.dlq         → 5 messages in dead-letter queue, triggers DLQ alert
    5. abandoned-queue    → 50 messages, 0 consumers, triggers NO_CONSUMERS alert
"""

import os
import time

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

HOST     = os.getenv("RABBITMQ_HOST", "localhost")
PORT     = int(os.getenv("RABBITMQ_PORT", "15672"))
USER     = os.getenv("RABBITMQ_USER", "guest")
PASS     = os.getenv("RABBITMQ_PASS", "guest")
VHOST    = "%2F"  # URL-encoded default vhost "/"

BASE_URL = f"http://{HOST}:{PORT}/api"
AUTH     = HTTPBasicAuth(USER, PASS)


def api(method: str, endpoint: str, payload: dict = None) -> requests.Response:
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.request(method, url, json=payload, auth=AUTH, timeout=10)
    resp.raise_for_status()
    return resp


def create_queue(name: str) -> None:
    api("PUT", f"queues/{VHOST}/{name}", {"durable": False})
    print(f"  Created queue: {name}")


def publish_messages(queue_name: str, count: int, payload: str = "test message") -> None:
    """Publish messages via Management API (good enough for testing)."""
    for i in range(count):
        api("POST", f"exchanges/{VHOST}/amq.default/publish", {
            "properties":       {},
            "routing_key":      queue_name,
            "payload":          f"{payload} #{i+1}",
            "payload_encoding": "string",
        })
    print(f"  Published {count} message(s) to: {queue_name}")


def delete_queue(name: str) -> None:
    try:
        api("DELETE", f"queues/{VHOST}/{name}")
    except Exception:
        pass


def reset() -> None:
    """Clean up all test queues before creating fresh ones."""
    print("\nCleaning up previous test queues...")
    for q in ["healthy-queue", "orders-queue", "overloaded-queue", "orders.dlq", "abandoned-queue"]:
        delete_queue(q)
    time.sleep(1)


def run() -> None:
    print("=" * 55)
    print("  RabbitMQ Alert Simulator")
    print(f"  Target: {HOST}:{PORT}")
    print("=" * 55)

    # Verify connection first
    try:
        api("GET", "overview")
        print(f"\nConnected to RabbitMQ at {HOST}:{PORT}\n")
    except Exception as e:
        print(f"\nERROR: Cannot connect to RabbitMQ at {HOST}:{PORT}")
        print(f"  {e}")
        print("\nMake sure Docker is running and RabbitMQ is up:")
        print("  docker run -d --name rabbitmq-test -p 5672:5672 -p 15672:15672 rabbitmq:3-management")
        return

    reset()

    # Scenario 1 — Healthy (no alert)
    print("Scenario 1: healthy-queue (10 messages — no alert)")
    create_queue("healthy-queue")
    publish_messages("healthy-queue", 10)

    # Scenario 2 — Queue depth WARNING
    print("\nScenario 2: orders-queue (600 messages — WARNING)")
    create_queue("orders-queue")
    publish_messages("orders-queue", 600)

    # Scenario 3 — Queue depth CRITICAL
    print("\nScenario 3: overloaded-queue (2500 messages — CRITICAL)")
    create_queue("overloaded-queue")
    publish_messages("overloaded-queue", 2500)

    # Scenario 4 — Dead-letter queue
    print("\nScenario 4: orders.dlq (5 messages — DLQ alert)")
    create_queue("orders.dlq")
    publish_messages("orders.dlq", 5, payload="failed message")

    # Scenario 5 — No consumers
    print("\nScenario 5: abandoned-queue (50 messages, 0 consumers — CRITICAL)")
    create_queue("abandoned-queue")
    publish_messages("abandoned-queue", 50)

    print("\n" + "=" * 55)
    print("  All scenarios ready.")
    print("  Now run: python main.py --once")
    print("=" * 55)


if __name__ == "__main__":
    run()
