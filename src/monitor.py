"""Core monitoring logic — threshold evaluation and alert generation."""

import logging

from src.models import Alert, AlertSeverity, AlertType, MonitorSummary, QueueMetrics, ThresholdConfig

logger = logging.getLogger(__name__)


class AlertEvaluator:
    """Evaluates queue metrics against thresholds and produces alerts."""

    def __init__(self, config: ThresholdConfig) -> None:
        self.config = config

    def _is_ignored(self, queue_name: str) -> bool:
        return any(queue_name.startswith(prefix) for prefix in self.config.ignored_prefixes)

    def evaluate(self, metrics: QueueMetrics) -> list[Alert]:
        """Return a list of alerts for a single queue snapshot. Empty list = healthy."""
        if self._is_ignored(metrics.name):
            return []

        alerts: list[Alert] = []

        # Dead-letter queue checks (separate thresholds — any DLQ message is notable)
        if metrics.is_dead_letter_queue and metrics.messages_total > 0:
            severity = (
                AlertSeverity.CRITICAL
                if metrics.messages_total >= self.config.dlq_depth_critical
                else AlertSeverity.WARNING
            )
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.DEAD_LETTER,
                    severity=severity,
                    message=(
                        f"Dead-letter queue has {metrics.messages_total} message(s). "
                        "These are messages that failed processing and were routed to the DLQ."
                    ),
                    current_value=float(metrics.messages_total),
                    threshold_value=float(self.config.dlq_depth_warning),
                )
            )
            return alerts  # Don't fire other alerts for DLQ — DLQ alert takes priority

        # Queue depth check
        if metrics.queue_depth >= self.config.queue_depth_critical:
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.QUEUE_DEPTH,
                    severity=AlertSeverity.CRITICAL,
                    message=(
                        f"Queue depth is {metrics.queue_depth:,} messages "
                        f"(critical threshold: {self.config.queue_depth_critical:,}). "
                        "Consumers may be overwhelmed or offline."
                    ),
                    current_value=float(metrics.queue_depth),
                    threshold_value=float(self.config.queue_depth_critical),
                )
            )
        elif metrics.queue_depth >= self.config.queue_depth_warning:
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.QUEUE_DEPTH,
                    severity=AlertSeverity.WARNING,
                    message=(
                        f"Queue depth is {metrics.queue_depth:,} messages "
                        f"(warning threshold: {self.config.queue_depth_warning:,})."
                    ),
                    current_value=float(metrics.queue_depth),
                    threshold_value=float(self.config.queue_depth_warning),
                )
            )

        # Consumer lag check
        if metrics.consumer_lag >= self.config.consumer_lag_critical:
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.CONSUMER_LAG,
                    severity=AlertSeverity.CRITICAL,
                    message=(
                        f"Consumer lag is {metrics.consumer_lag:,} unacknowledged messages "
                        f"(critical threshold: {self.config.consumer_lag_critical:,}). "
                        "Consumers are processing too slowly."
                    ),
                    current_value=float(metrics.consumer_lag),
                    threshold_value=float(self.config.consumer_lag_critical),
                )
            )
        elif metrics.consumer_lag >= self.config.consumer_lag_warning:
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.CONSUMER_LAG,
                    severity=AlertSeverity.WARNING,
                    message=(
                        f"Consumer lag is {metrics.consumer_lag:,} unacknowledged messages "
                        f"(warning threshold: {self.config.consumer_lag_warning:,})."
                    ),
                    current_value=float(metrics.consumer_lag),
                    threshold_value=float(self.config.consumer_lag_warning),
                )
            )

        # No consumers check — only alert if there are messages waiting
        if (
            self.config.alert_on_no_consumers
            and metrics.consumers == 0
            and metrics.messages_total > 0
            and not metrics.is_dead_letter_queue
        ):
            alerts.append(
                Alert(
                    queue_name=metrics.name,
                    vhost=metrics.vhost,
                    alert_type=AlertType.NO_CONSUMERS,
                    severity=AlertSeverity.CRITICAL,
                    message=(
                        f"Queue has {metrics.messages_total:,} message(s) but 0 active consumers. "
                        "Messages are accumulating with no processor."
                    ),
                    current_value=0.0,
                    threshold_value=1.0,
                )
            )

        return alerts


class QueueMonitor:
    """Orchestrates one polling cycle: collect → evaluate → return alerts + summary."""

    def __init__(self, evaluator: AlertEvaluator) -> None:
        self.evaluator = evaluator

    def run_cycle(self, metrics_list: list[QueueMetrics]) -> tuple[list[Alert], MonitorSummary]:
        """
        Evaluate all queue metrics and return:
            - All alerts that should be fired this cycle
            - A MonitorSummary for logging/storage
        """
        all_alerts: list[Alert] = []
        healthy = 0
        dlq_count = 0
        total_messages = 0

        for metrics in metrics_list:
            alerts = self.evaluator.evaluate(metrics)
            if alerts:
                all_alerts.extend(alerts)
                logger.warning(
                    "Queue '%s/%s': %d alert(s) fired",
                    metrics.vhost,
                    metrics.name,
                    len(alerts),
                )
            else:
                healthy += 1

            if metrics.is_dead_letter_queue:
                dlq_count += 1
            total_messages += metrics.messages_total

        summary = MonitorSummary(
            total_queues=len(metrics_list),
            healthy_queues=healthy,
            alerts_fired=len(all_alerts),
            dead_letter_queues=dlq_count,
            total_messages=total_messages,
        )

        logger.info(
            "Cycle complete — %d queues | %d healthy | %d alerts | %d DLQs | %d total messages",
            summary.total_queues,
            summary.healthy_queues,
            summary.alerts_fired,
            summary.dead_letter_queues,
            summary.total_messages,
        )

        return all_alerts, summary
