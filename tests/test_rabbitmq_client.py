"""Tests for the RabbitMQ Management API client — uses unittest.mock to avoid real broker."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.rabbitmq_client import RabbitMQManagementClient, _is_dead_letter_queue


# ---------------------------------------------------------------------------
# DLQ detection helper
# ---------------------------------------------------------------------------


class TestIsDlqDetection:
    def test_dead_in_name(self) -> None:
        assert _is_dead_letter_queue("orders.dead", {}) is True

    def test_dlq_in_name(self) -> None:
        assert _is_dead_letter_queue("payments.dlq", {}) is True

    def test_dlx_in_name(self) -> None:
        assert _is_dead_letter_queue("jobs_dlx", {}) is True

    def test_failed_in_name(self) -> None:
        assert _is_dead_letter_queue("failed-events", {}) is True

    def test_normal_queue_name(self) -> None:
        assert _is_dead_letter_queue("orders", {}) is False

    def test_x_dead_letter_exchange_argument(self) -> None:
        args = {"x-dead-letter-exchange": "dlx.exchange"}
        assert _is_dead_letter_queue("normal-queue", {"arguments": args}) is True

    def test_no_arguments_key(self) -> None:
        assert _is_dead_letter_queue("orders", {}) is False

    def test_case_insensitive_dlq(self) -> None:
        assert _is_dead_letter_queue("Orders_DLQ", {}) is True


# ---------------------------------------------------------------------------
# RabbitMQManagementClient — parse_queue_metrics
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> RabbitMQManagementClient:
    return RabbitMQManagementClient(
        host="localhost", port=15672, username="guest", password="guest"
    )


RAW_QUEUE_FULL = {
    "name": "orders",
    "vhost": "/",
    "messages_ready": 42,
    "messages_unacknowledged": 8,
    "messages": 50,
    "consumers": 3,
    "consumer_utilisation": 0.75,
    "arguments": {},
    "message_stats": {
        "publish_details": {"rate": 12.5},
        "deliver_get_details": {"rate": 10.0},
    },
}

RAW_QUEUE_MINIMAL = {
    "name": "simple",
    "vhost": "staging",
    "messages": 0,
    "arguments": {},
}


class TestParseQueueMetrics:
    def test_full_payload_parsed_correctly(self, client: RabbitMQManagementClient) -> None:
        m = client.parse_queue_metrics(RAW_QUEUE_FULL)
        assert m.name == "orders"
        assert m.vhost == "/"
        assert m.messages_ready == 42
        assert m.messages_unacknowledged == 8
        assert m.messages_total == 50
        assert m.consumers == 3
        assert m.consumer_utilisation == 0.75
        assert m.message_publish_rate == 12.5
        assert m.message_deliver_rate == 10.0
        assert m.is_dead_letter_queue is False

    def test_minimal_payload_uses_defaults(self, client: RabbitMQManagementClient) -> None:
        m = client.parse_queue_metrics(RAW_QUEUE_MINIMAL)
        assert m.name == "simple"
        assert m.messages_ready == 0
        assert m.messages_unacknowledged == 0
        assert m.consumers == 0
        assert m.consumer_utilisation == 0.0
        assert m.message_publish_rate == 0.0

    def test_dlq_detected_from_name(self, client: RabbitMQManagementClient) -> None:
        raw = dict(RAW_QUEUE_FULL)
        raw["name"] = "orders.dead-letter"
        m = client.parse_queue_metrics(raw)
        assert m.is_dead_letter_queue is True

    def test_missing_name_raises_key_error(self, client: RabbitMQManagementClient) -> None:
        with pytest.raises(KeyError):
            client.parse_queue_metrics({"vhost": "/"})

    def test_consumer_utilisation_none_defaults_to_zero(self, client: RabbitMQManagementClient) -> None:
        raw = dict(RAW_QUEUE_FULL)
        raw["consumer_utilisation"] = None  # RabbitMQ returns null when no consumers
        m = client.parse_queue_metrics(raw)
        assert m.consumer_utilisation == 0.0


# ---------------------------------------------------------------------------
# RabbitMQManagementClient — HTTP interaction
# ---------------------------------------------------------------------------


class TestRabbitMQClientHTTP:
    def test_health_check_returns_true_on_200(self, client: RabbitMQManagementClient) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "ok"}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            result = client.health_check()

        assert result is True

    def test_health_check_returns_false_on_connection_error(self, client: RabbitMQManagementClient) -> None:
        with patch.object(client.session, "get", side_effect=requests.exceptions.ConnectionError):
            result = client.health_check()

        assert result is False

    def test_collect_all_metrics_skips_malformed_entries(self, client: RabbitMQManagementClient) -> None:
        raw_queues = [
            RAW_QUEUE_FULL,
            {"vhost": "/", "no_name_key": True},  # malformed — missing "name"
            RAW_QUEUE_MINIMAL,
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = raw_queues
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client.session, "get", return_value=mock_resp):
            metrics = client.collect_all_metrics()

        assert len(metrics) == 2  # malformed entry skipped

    def test_collect_all_metrics_returns_empty_on_empty_broker(self, client: RabbitMQManagementClient) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client.session, "get", return_value=mock_resp):
            metrics = client.collect_all_metrics()

        assert metrics == []

    def test_get_raises_on_http_error(self, client: RabbitMQManagementClient) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                client._get("queues")

    def test_base_url_built_correctly(self) -> None:
        c = RabbitMQManagementClient("myhost", 15672, "user", "pass", use_ssl=False)
        assert c.base_url == "http://myhost:15672/api"

    def test_base_url_uses_https_when_ssl(self) -> None:
        c = RabbitMQManagementClient("myhost", 15671, "user", "pass", use_ssl=True)
        assert c.base_url == "https://myhost:15671/api"
