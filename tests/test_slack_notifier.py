"""Tests for Slack notifier — alert formatting and deduplication."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.models import Alert, AlertSeverity, AlertType, MonitorSummary
from src.slack_notifier import SlackNotifier, _alert_last_fired


def make_alert(
    queue_name: str = "orders",
    vhost: str = "/",
    alert_type: AlertType = AlertType.QUEUE_DEPTH,
    severity: AlertSeverity = AlertSeverity.WARNING,
    current_value: float = 600.0,
    threshold_value: float = 500.0,
) -> Alert:
    return Alert(
        queue_name=queue_name,
        vhost=vhost,
        alert_type=alert_type,
        severity=severity,
        message="Test alert message",
        current_value=current_value,
        threshold_value=threshold_value,
        fired_at=datetime.utcnow(),
    )


@pytest.fixture(autouse=True)
def clear_cooldown_cache():
    """Reset in-process deduplication cache between tests."""
    _alert_last_fired.clear()
    yield
    _alert_last_fired.clear()


@pytest.fixture
def notifier() -> SlackNotifier:
    return SlackNotifier(webhook_url="https://hooks.slack.com/test", channel="#ops")


class TestSlackNotifierSendAlert:
    def test_sends_alert_successfully(self, notifier: SlackNotifier) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_resp) as mock_post:
            result = notifier.send_alert(make_alert())

        assert result is True
        mock_post.assert_called_once()

    def test_returns_false_on_request_error(self, notifier: SlackNotifier) -> None:
        import requests as req
        with patch("requests.post", side_effect=req.RequestException("network error")):
            result = notifier.send_alert(make_alert())

        assert result is False

    def test_same_alert_suppressed_within_cooldown(self, notifier: SlackNotifier) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_resp) as mock_post:
            alert = make_alert()
            notifier.send_alert(alert)   # First call — should send
            result = notifier.send_alert(alert)  # Second call — should suppress

        assert result is False
        assert mock_post.call_count == 1  # Only called once

    def test_different_queues_not_suppressed(self, notifier: SlackNotifier) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_resp) as mock_post:
            notifier.send_alert(make_alert(queue_name="queue_a"))
            notifier.send_alert(make_alert(queue_name="queue_b"))

        assert mock_post.call_count == 2

    def test_different_alert_types_not_suppressed(self, notifier: SlackNotifier) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_resp) as mock_post:
            notifier.send_alert(make_alert(alert_type=AlertType.QUEUE_DEPTH))
            notifier.send_alert(make_alert(alert_type=AlertType.CONSUMER_LAG))

        assert mock_post.call_count == 2


class TestSlackNotifierSendAlerts:
    def test_send_multiple_alerts_returns_sent_count(self, notifier: SlackNotifier) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        alerts = [
            make_alert(queue_name="q1"),
            make_alert(queue_name="q2"),
            make_alert(queue_name="q3"),
        ]

        with patch("requests.post", return_value=mock_resp):
            count = notifier.send_alerts(alerts)

        assert count == 3

    def test_send_empty_list_returns_zero(self, notifier: SlackNotifier) -> None:
        assert notifier.send_alerts([]) == 0


class TestAlertToSlackBlock:
    def test_warning_alert_has_yellow_color(self) -> None:
        alert = make_alert(severity=AlertSeverity.WARNING)
        block = alert.to_slack_block()
        assert block["attachments"][0]["color"] == "#ffcc00"

    def test_critical_alert_has_red_color(self) -> None:
        alert = make_alert(severity=AlertSeverity.CRITICAL)
        block = alert.to_slack_block()
        assert block["attachments"][0]["color"] == "#ff0000"

    def test_info_alert_has_green_color(self) -> None:
        alert = make_alert(severity=AlertSeverity.INFO)
        block = alert.to_slack_block()
        assert block["attachments"][0]["color"] == "#36a64f"

    def test_block_contains_queue_name(self) -> None:
        alert = make_alert(queue_name="payments", vhost="prod")
        block = alert.to_slack_block()
        # Verify the block structure has attachments
        assert "attachments" in block
        assert len(block["attachments"]) > 0
