"""Configuration loader from settings.yaml + environment variable overrides."""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from src.models import ThresholdConfig

logger = logging.getLogger(__name__)


@dataclass
class RabbitMQConfig:
    host: str = "localhost"
    port: int = 15672
    username: str = "guest"
    password: str = "guest"
    use_ssl: bool = False


@dataclass
class SlackConfig:
    webhook_url: str = ""
    channel: str = "#ops-alerts"
    username: str = "RabbitMQ Monitor"
    send_summary: bool = False  # Post cycle summary to Slack each poll


@dataclass
class DatabaseConfig:
    dsn: str = ""  # e.g. postgresql://user:pass@host:5432/dbname
    enabled: bool = False


@dataclass
class AppConfig:
    poll_interval_seconds: int = 60
    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    log_level: str = "INFO"


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Load configuration from YAML file, then apply environment variable overrides.
    Environment variables always take precedence over YAML values.
    """
    raw: dict = {}

    if config_path is None:
        config_path = "config/settings.yaml"

    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        logger.debug("Loaded config from %s", path)
    else:
        logger.warning("Config file not found at %s — using defaults", path)

    rmq = raw.get("rabbitmq", {})
    slack = raw.get("slack", {})
    db = raw.get("database", {})
    thresh = raw.get("thresholds", {})

    return AppConfig(
        poll_interval_seconds=int(_env("POLL_INTERVAL_SECONDS", str(raw.get("poll_interval_seconds", 60)))),
        log_level=_env("LOG_LEVEL", raw.get("log_level", "INFO")),
        rabbitmq=RabbitMQConfig(
            host=_env("RABBITMQ_HOST", rmq.get("host", "localhost")),
            port=int(_env("RABBITMQ_PORT", str(rmq.get("port", 15672)))),
            username=_env("RABBITMQ_USER", rmq.get("username", "guest")),
            password=_env("RABBITMQ_PASS", rmq.get("password", "guest")),
            use_ssl=_env("RABBITMQ_SSL", str(rmq.get("use_ssl", False))).lower() == "true",
        ),
        slack=SlackConfig(
            webhook_url=_env("SLACK_WEBHOOK_URL", slack.get("webhook_url", "")),
            channel=_env("SLACK_CHANNEL", slack.get("channel", "#ops-alerts")),
            username=slack.get("username", "RabbitMQ Monitor"),
            send_summary=slack.get("send_summary", False),
        ),
        database=DatabaseConfig(
            dsn=_env("DATABASE_URL", db.get("dsn", "")),
            enabled=bool(db.get("enabled", False)) or bool(_env("DATABASE_URL")),
        ),
        thresholds=ThresholdConfig(
            queue_depth_warning=thresh.get("queue_depth_warning", 500),
            queue_depth_critical=thresh.get("queue_depth_critical", 2000),
            consumer_lag_warning=thresh.get("consumer_lag_warning", 100),
            consumer_lag_critical=thresh.get("consumer_lag_critical", 500),
            dlq_depth_warning=thresh.get("dlq_depth_warning", 1),
            dlq_depth_critical=thresh.get("dlq_depth_critical", 50),
            alert_on_no_consumers=thresh.get("alert_on_no_consumers", True),
            ignored_prefixes=thresh.get("ignored_prefixes", ["amq."]),
        ),
    )
