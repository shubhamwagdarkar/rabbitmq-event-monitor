"""
rabbitmq-event-monitor — entry point.

Polls RabbitMQ Management API on a configurable interval, evaluates queue
metrics against thresholds, fires Slack alerts on breach, and persists all
data to PostgreSQL for Grafana dashboards.

Usage:
    python main.py                          # Uses config/settings.yaml
    python main.py --config /path/cfg.yaml  # Custom config path
    python main.py --once                   # Single poll then exit (CI / cron mode)
    python main.py --report                 # Print recent alerts table and exit
"""

import argparse
import logging
import sys
import time
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from src.config import AppConfig, load_config
from src.db import Database
from src.monitor import AlertEvaluator, QueueMonitor
from src.rabbitmq_client import RabbitMQManagementClient
from src.slack_notifier import SlackNotifier


def build_components(cfg: AppConfig):
    """Instantiate all service objects from config."""
    rmq_client = RabbitMQManagementClient(
        host=cfg.rabbitmq.host,
        port=cfg.rabbitmq.port,
        username=cfg.rabbitmq.username,
        password=cfg.rabbitmq.password,
        use_ssl=cfg.rabbitmq.use_ssl,
    )
    evaluator = AlertEvaluator(config=cfg.thresholds)
    monitor = QueueMonitor(evaluator=evaluator)
    notifier = SlackNotifier(
        webhook_url=cfg.slack.webhook_url,
        channel=cfg.slack.channel,
        username=cfg.slack.username,
    ) if cfg.slack.webhook_url else None

    db: Optional[Database] = None
    if cfg.database.enabled and cfg.database.dsn:
        db = Database(dsn=cfg.database.dsn)
        db.connect()
        db.ensure_schema()

    return rmq_client, monitor, notifier, db


def poll_once(rmq_client: RabbitMQManagementClient, monitor: QueueMonitor,
              notifier: Optional[SlackNotifier], db: Optional[Database],
              cfg: AppConfig) -> None:
    """Execute one full monitoring cycle."""
    logger = logging.getLogger(__name__)

    # Health check
    if not rmq_client.health_check():
        logger.error("RabbitMQ Management API is unreachable — skipping cycle")
        if notifier:
            from src.models import Alert, AlertSeverity, AlertType
            from datetime import datetime
            alert = Alert(
                queue_name="N/A",
                vhost="N/A",
                alert_type=AlertType.CONNECTION_FAILURE,
                severity=AlertSeverity.CRITICAL,
                message="Cannot reach RabbitMQ Management API. Check broker health and network connectivity.",
                current_value=0.0,
                threshold_value=1.0,
                fired_at=datetime.utcnow(),
            )
            notifier.send_alert(alert)
        return

    # Collect metrics
    metrics_list = rmq_client.collect_all_metrics()

    # Evaluate thresholds
    alerts, summary = monitor.run_cycle(metrics_list)

    # Persist to PostgreSQL
    if db:
        db.insert_queue_metrics(metrics_list)
        for alert in alerts:
            db.insert_alert(alert)
        db.insert_summary(summary)

    # Send Slack alerts
    if notifier:
        sent = notifier.send_alerts(alerts)
        if sent:
            logger.info("Sent %d Slack alert(s)", sent)
        if cfg.slack.send_summary:
            notifier.send_summary(summary)
    elif alerts:
        logger.warning("%d alert(s) fired but no Slack webhook configured", len(alerts))

    # Always print summary to stdout
    print(
        f"[{summary.polled_at.strftime('%H:%M:%S')}] "
        f"Queues: {summary.total_queues} | Healthy: {summary.healthy_queues} | "
        f"Alerts: {summary.alerts_fired} | DLQs: {summary.dead_letter_queues} | "
        f"Messages: {summary.total_messages:,}"
    )


def print_report(db: Database) -> None:
    """Print recent alerts table to stdout."""
    alerts = db.get_recent_alerts(50)
    if not alerts:
        print("No alerts found in database.")
        return

    print(f"\n{'='*90}")
    print(f"{'FIRED AT':<22} {'SEVERITY':<10} {'TYPE':<22} {'VHOST/QUEUE':<30} {'VALUE'}")
    print(f"{'='*90}")
    for row in alerts:
        queue_path = f"{row['vhost']}/{row['queue_name']}"
        print(
            f"{str(row['fired_at']):<22} {row['severity'].upper():<10} "
            f"{row['alert_type']:<22} {queue_path:<30} {row['current_value']:>8.0f}"
        )
    print(f"{'='*90}\n")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level.upper(), logging.INFO),
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RabbitMQ Event Monitor — real-time queue health monitoring with Slack alerts"
    )
    parser.add_argument("--config", default="config/settings.yaml", help="Path to settings YAML")
    parser.add_argument("--once", action="store_true", help="Run a single poll cycle and exit")
    parser.add_argument("--report", action="store_true", help="Print recent alert history and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    configure_logging(cfg.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Starting RabbitMQ Event Monitor (poll interval: %ds)", cfg.poll_interval_seconds)
    logger.info(
        "RabbitMQ: %s:%d | DB: %s | Slack: %s",
        cfg.rabbitmq.host,
        cfg.rabbitmq.port,
        "enabled" if cfg.database.enabled else "disabled",
        "configured" if cfg.slack.webhook_url else "not configured",
    )

    rmq_client, monitor, notifier, db = build_components(cfg)

    if args.report:
        if not db:
            print("ERROR: Database not configured. Set DATABASE_URL to use --report.")
            sys.exit(1)
        print_report(db)
        db.disconnect()
        return

    if args.once:
        poll_once(rmq_client, monitor, notifier, db, cfg)
        if db:
            db.disconnect()
        return

    # Continuous polling loop
    try:
        while True:
            try:
                poll_once(rmq_client, monitor, notifier, db, cfg)
            except Exception as exc:
                logger.error("Unhandled error in poll cycle: %s", exc, exc_info=True)
            time.sleep(cfg.poll_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Shutting down (KeyboardInterrupt)")
    finally:
        if db:
            db.disconnect()


if __name__ == "__main__":
    main()
