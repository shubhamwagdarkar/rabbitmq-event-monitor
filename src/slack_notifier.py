"""Slack webhook notification for RabbitMQ alerts."""

import logging
from datetime import datetime, timedelta

import requests

from src.models import Alert, MonitorSummary

logger = logging.getLogger(__name__)

# Simple in-process deduplication — suppress repeated alerts for same queue+type within cooldown
_ALERT_COOLDOWN_MINUTES = 15
_alert_last_fired: dict[str, datetime] = {}


def _alert_key(alert: Alert) -> str:
    return f"{alert.vhost}:{alert.queue_name}:{alert.alert_type.value}:{alert.severity.value}"


def _is_suppressed(alert: Alert) -> bool:
    key = _alert_key(alert)
    last = _alert_last_fired.get(key)
    if last and datetime.utcnow() - last < timedelta(minutes=_ALERT_COOLDOWN_MINUTES):
        return True
    return False


def _mark_fired(alert: Alert) -> None:
    _alert_last_fired[_alert_key(alert)] = datetime.utcnow()


class SlackNotifier:
    """Posts alert messages to a Slack incoming webhook."""

    def __init__(self, webhook_url: str, channel: str = "", username: str = "RabbitMQ Monitor") -> None:
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username

    def _post(self, payload: dict) -> bool:
        """Send payload to Slack webhook. Returns True on success."""
        payload["username"] = self.username
        if self.channel:
            payload["channel"] = self.channel

        try:
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            logger.error("Failed to send Slack notification: %s", exc)
            return False

    def send_alert(self, alert: Alert) -> bool:
        """Send a single alert to Slack, respecting cooldown deduplication."""
        if _is_suppressed(alert):
            logger.debug(
                "Alert suppressed (cooldown active): %s/%s %s",
                alert.vhost,
                alert.queue_name,
                alert.alert_type.value,
            )
            return False

        payload = alert.to_slack_block()
        success = self._post(payload)
        if success:
            _mark_fired(alert)
            logger.info(
                "Slack alert sent: %s — %s/%s",
                alert.alert_type.value,
                alert.vhost,
                alert.queue_name,
            )
        return success

    def send_alerts(self, alerts: list[Alert]) -> int:
        """Send multiple alerts. Returns count of successfully sent alerts."""
        sent = 0
        for alert in alerts:
            if self.send_alert(alert):
                sent += 1
        return sent

    def send_summary(self, summary: MonitorSummary) -> bool:
        """Post a brief cycle summary to Slack (useful for health dashboards)."""
        status_emoji = ":white_check_mark:" if summary.alerts_fired == 0 else ":warning:"
        payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"{status_emoji} *RabbitMQ Monitor — Cycle Summary*\n"
                            f"*Queues:* {summary.total_queues} total | "
                            f"{summary.healthy_queues} healthy | "
                            f"{summary.dead_letter_queues} DLQs\n"
                            f"*Alerts fired:* {summary.alerts_fired}\n"
                            f"*Total messages in flight:* {summary.total_messages:,}\n"
                            f"_Polled at {summary.polled_at.strftime('%Y-%m-%d %H:%M:%S')} UTC_"
                        ),
                    },
                }
            ]
        }
        return self._post(payload)
