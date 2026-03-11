"""Data models for RabbitMQ Event Monitor."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(str, Enum):
    QUEUE_DEPTH = "queue_depth"
    CONSUMER_LAG = "consumer_lag"
    DEAD_LETTER = "dead_letter"
    NO_CONSUMERS = "no_consumers"
    CONNECTION_FAILURE = "connection_failure"


@dataclass
class QueueMetrics:
    """Metrics snapshot for a single RabbitMQ queue."""

    name: str
    vhost: str
    messages_ready: int
    messages_unacknowledged: int
    messages_total: int
    consumers: int
    consumer_utilisation: float
    message_publish_rate: float
    message_deliver_rate: float
    is_dead_letter_queue: bool
    sampled_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def consumer_lag(self) -> int:
        """Messages waiting beyond what consumers are processing."""
        return self.messages_unacknowledged

    @property
    def queue_depth(self) -> int:
        return self.messages_total


@dataclass
class Alert:
    """Represents a threshold breach alert."""

    queue_name: str
    vhost: str
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    current_value: float
    threshold_value: float
    fired_at: datetime = field(default_factory=datetime.utcnow)

    def to_slack_block(self) -> dict:
        """Format alert as a Slack Block Kit message."""
        emoji = {"info": ":information_source:", "warning": ":warning:", "critical": ":rotating_light:"}[
            self.severity.value
        ]
        color = {"info": "#36a64f", "warning": "#ffcc00", "critical": "#ff0000"}[self.severity.value]

        return {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{emoji} RabbitMQ Alert — {self.severity.value.upper()}",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Queue:*\n`{self.vhost}/{self.queue_name}`"},
                                {"type": "mrkdwn", "text": f"*Alert Type:*\n{self.alert_type.value}"},
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Current Value:*\n{self.current_value:,.0f}",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Threshold:*\n{self.threshold_value:,.0f}",
                                },
                            ],
                        },
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": f"*Details:*\n{self.message}"},
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"Fired at {self.fired_at.strftime('%Y-%m-%d %H:%M:%S')} UTC",
                                }
                            ],
                        },
                    ],
                }
            ]
        }


@dataclass
class ThresholdConfig:
    """Configurable thresholds for alert conditions."""

    queue_depth_warning: int = 500
    queue_depth_critical: int = 2000
    consumer_lag_warning: int = 100
    consumer_lag_critical: int = 500
    dlq_depth_warning: int = 1
    dlq_depth_critical: int = 50
    alert_on_no_consumers: bool = True
    ignored_prefixes: list = field(default_factory=lambda: ["amq."])


@dataclass
class MonitorSummary:
    """Summary of a single monitoring poll cycle."""

    total_queues: int
    healthy_queues: int
    alerts_fired: int
    dead_letter_queues: int
    total_messages: int
    polled_at: datetime = field(default_factory=datetime.utcnow)
