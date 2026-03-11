"""RabbitMQ Management HTTP API client."""

import logging
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, Timeout

from src.models import QueueMetrics

logger = logging.getLogger(__name__)

_DLQ_KEYWORDS = ("dead", "dlq", "dlx", "dead-letter", "failed", "error")


def _is_dead_letter_queue(queue_name: str, queue_data: dict) -> bool:
    """Detect dead-letter queues by name convention or policy arguments."""
    name_lower = queue_name.lower()
    if any(kw in name_lower for kw in _DLQ_KEYWORDS):
        return True
    # Also check if queue was declared with x-dead-letter-exchange argument
    args = queue_data.get("arguments", {})
    return "x-dead-letter-exchange" in args


class RabbitMQManagementClient:
    """Wraps the RabbitMQ Management HTTP API for metric collection."""

    def __init__(self, host: str, port: int, username: str, password: str, use_ssl: bool = False) -> None:
        scheme = "https" if use_ssl else "http"
        self.base_url = f"{scheme}://{host}:{port}/api"
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    def _get(self, endpoint: str, timeout: int = 10) -> Any:
        """Perform a GET request against the Management API."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except ConnectionError as exc:
            logger.error("Cannot connect to RabbitMQ Management API at %s: %s", url, exc)
            raise
        except Timeout:
            logger.error("Timeout connecting to %s", url)
            raise
        except requests.HTTPError as exc:
            logger.error("HTTP error from Management API: %s", exc)
            raise

    def health_check(self) -> bool:
        """Return True if the broker is alive and API is reachable."""
        try:
            self._get("aliveness-test/%2F")  # Check default vhost
            return True
        except Exception:
            return False

    def get_overview(self) -> dict:
        """Return global broker overview stats."""
        return self._get("overview")

    def get_all_queues(self) -> list[dict]:
        """Return raw queue data for every vhost."""
        return self._get("queues")

    def get_queue(self, vhost: str, queue_name: str) -> dict:
        """Return raw data for a specific queue."""
        encoded_vhost = quote(vhost, safe="")
        encoded_name = quote(queue_name, safe="")
        return self._get(f"queues/{encoded_vhost}/{encoded_name}")

    def parse_queue_metrics(self, raw: dict) -> QueueMetrics:
        """Convert raw Management API queue dict into a typed QueueMetrics object."""
        msg_stats = raw.get("message_stats", {})

        publish_details = msg_stats.get("publish_details", {})
        deliver_details = msg_stats.get("deliver_get_details", {})

        return QueueMetrics(
            name=raw["name"],
            vhost=raw.get("vhost", "/"),
            messages_ready=raw.get("messages_ready", 0),
            messages_unacknowledged=raw.get("messages_unacknowledged", 0),
            messages_total=raw.get("messages", 0),
            consumers=raw.get("consumers", 0),
            consumer_utilisation=raw.get("consumer_utilisation") or 0.0,
            message_publish_rate=publish_details.get("rate", 0.0),
            message_deliver_rate=deliver_details.get("rate", 0.0),
            is_dead_letter_queue=_is_dead_letter_queue(raw["name"], raw),
        )

    def collect_all_metrics(self) -> list[QueueMetrics]:
        """Fetch and parse metrics for all queues across all vhosts."""
        raw_queues = self.get_all_queues()
        metrics = []
        for raw in raw_queues:
            try:
                metrics.append(self.parse_queue_metrics(raw))
            except KeyError as exc:
                logger.warning("Skipping malformed queue entry (missing key: %s)", exc)
        logger.info("Collected metrics for %d queues", len(metrics))
        return metrics
