"""Tests for core monitoring logic — threshold evaluation and alert generation."""

from datetime import datetime

import pytest

from src.models import Alert, AlertSeverity, AlertType, QueueMetrics, ThresholdConfig
from src.monitor import AlertEvaluator, QueueMonitor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_queue(
    name: str = "orders",
    vhost: str = "/",
    messages_total: int = 0,
    messages_ready: int = 0,
    messages_unacknowledged: int = 0,
    consumers: int = 2,
    consumer_utilisation: float = 0.5,
    is_dead_letter_queue: bool = False,
) -> QueueMetrics:
    return QueueMetrics(
        name=name,
        vhost=vhost,
        messages_ready=messages_ready,
        messages_unacknowledged=messages_unacknowledged,
        messages_total=messages_total,
        consumers=consumers,
        consumer_utilisation=consumer_utilisation,
        message_publish_rate=0.0,
        message_deliver_rate=0.0,
        is_dead_letter_queue=is_dead_letter_queue,
        sampled_at=datetime.utcnow(),
    )


@pytest.fixture
def default_config() -> ThresholdConfig:
    return ThresholdConfig(
        queue_depth_warning=500,
        queue_depth_critical=2000,
        consumer_lag_warning=100,
        consumer_lag_critical=500,
        dlq_depth_warning=1,
        dlq_depth_critical=50,
        alert_on_no_consumers=True,
        ignored_prefixes=["amq."],
    )


@pytest.fixture
def evaluator(default_config: ThresholdConfig) -> AlertEvaluator:
    return AlertEvaluator(config=default_config)


# ---------------------------------------------------------------------------
# AlertEvaluator — healthy queue
# ---------------------------------------------------------------------------


class TestAlertEvaluatorHealthy:
    def test_healthy_queue_produces_no_alerts(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_total=10, consumers=2)
        assert evaluator.evaluate(q) == []

    def test_empty_queue_with_no_consumers_is_healthy(self, evaluator: AlertEvaluator) -> None:
        """Zero messages + zero consumers should NOT alert — queue is just idle."""
        q = make_queue(messages_total=0, consumers=0)
        assert evaluator.evaluate(q) == []

    def test_ignored_amq_prefix_queue(self, evaluator: AlertEvaluator) -> None:
        """amq.* queues are internal RabbitMQ queues and should be silently skipped."""
        q = make_queue(name="amq.direct", messages_total=50000, consumers=0)
        assert evaluator.evaluate(q) == []


# ---------------------------------------------------------------------------
# AlertEvaluator — queue depth
# ---------------------------------------------------------------------------


class TestQueueDepthAlerts:
    def test_below_warning_no_alert(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_total=499)
        assert evaluator.evaluate(q) == []

    def test_at_warning_threshold_fires_warning(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_total=500)
        alerts = evaluator.evaluate(q)
        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.QUEUE_DEPTH
        assert alerts[0].severity == AlertSeverity.WARNING

    def test_at_critical_threshold_fires_critical(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_total=2000)
        alerts = evaluator.evaluate(q)
        depth_alerts = [a for a in alerts if a.alert_type == AlertType.QUEUE_DEPTH]
        assert len(depth_alerts) == 1
        assert depth_alerts[0].severity == AlertSeverity.CRITICAL

    def test_critical_depth_does_not_also_fire_warning(self, evaluator: AlertEvaluator) -> None:
        """Only the highest-severity alert should be fired for the same metric."""
        q = make_queue(messages_total=5000)
        depth_alerts = [a for a in evaluator.evaluate(q) if a.alert_type == AlertType.QUEUE_DEPTH]
        assert len(depth_alerts) == 1
        assert depth_alerts[0].severity == AlertSeverity.CRITICAL

    def test_alert_contains_queue_name_and_vhost(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(name="payments", vhost="prod", messages_total=600)
        alerts = evaluator.evaluate(q)
        assert alerts[0].queue_name == "payments"
        assert alerts[0].vhost == "prod"


# ---------------------------------------------------------------------------
# AlertEvaluator — consumer lag
# ---------------------------------------------------------------------------


class TestConsumerLagAlerts:
    def test_below_warning_no_alert(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_unacknowledged=99)
        assert evaluator.evaluate(q) == []

    def test_at_lag_warning_fires(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_unacknowledged=100)
        lag_alerts = [a for a in evaluator.evaluate(q) if a.alert_type == AlertType.CONSUMER_LAG]
        assert len(lag_alerts) == 1
        assert lag_alerts[0].severity == AlertSeverity.WARNING

    def test_at_lag_critical_fires_critical(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_unacknowledged=500)
        lag_alerts = [a for a in evaluator.evaluate(q) if a.alert_type == AlertType.CONSUMER_LAG]
        assert len(lag_alerts) == 1
        assert lag_alerts[0].severity == AlertSeverity.CRITICAL

    def test_high_depth_and_high_lag_fire_independently(self, evaluator: AlertEvaluator) -> None:
        """A queue can breach both depth and lag thresholds simultaneously."""
        q = make_queue(messages_total=2500, messages_unacknowledged=600)
        alert_types = {a.alert_type for a in evaluator.evaluate(q)}
        assert AlertType.QUEUE_DEPTH in alert_types
        assert AlertType.CONSUMER_LAG in alert_types


# ---------------------------------------------------------------------------
# AlertEvaluator — dead-letter queues
# ---------------------------------------------------------------------------


class TestDeadLetterQueueAlerts:
    def test_empty_dlq_no_alert(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(name="orders.dlq", messages_total=0, is_dead_letter_queue=True)
        assert evaluator.evaluate(q) == []

    def test_dlq_with_messages_fires_warning(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(name="orders.dlq", messages_total=5, is_dead_letter_queue=True)
        alerts = evaluator.evaluate(q)
        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.DEAD_LETTER
        assert alerts[0].severity == AlertSeverity.WARNING

    def test_dlq_above_critical_threshold_fires_critical(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(name="payments.dead", messages_total=100, is_dead_letter_queue=True)
        alerts = evaluator.evaluate(q)
        assert alerts[0].alert_type == AlertType.DEAD_LETTER
        assert alerts[0].severity == AlertSeverity.CRITICAL

    def test_dlq_alert_suppresses_depth_alert(self, evaluator: AlertEvaluator) -> None:
        """DLQ alert takes full priority — no depth/lag alerts alongside it."""
        q = make_queue(
            name="jobs.dlq",
            messages_total=5000,
            messages_unacknowledged=1000,
            consumers=0,
            is_dead_letter_queue=True,
        )
        alerts = evaluator.evaluate(q)
        assert all(a.alert_type == AlertType.DEAD_LETTER for a in alerts)


# ---------------------------------------------------------------------------
# AlertEvaluator — no consumers
# ---------------------------------------------------------------------------


class TestNoConsumerAlerts:
    def test_no_consumers_with_messages_fires_critical(self, evaluator: AlertEvaluator) -> None:
        q = make_queue(messages_total=10, consumers=0)
        no_consumer_alerts = [a for a in evaluator.evaluate(q) if a.alert_type == AlertType.NO_CONSUMERS]
        assert len(no_consumer_alerts) == 1
        assert no_consumer_alerts[0].severity == AlertSeverity.CRITICAL

    def test_no_consumers_alert_disabled_by_config(self, default_config: ThresholdConfig) -> None:
        default_config.alert_on_no_consumers = False
        ev = AlertEvaluator(config=default_config)
        q = make_queue(messages_total=1000, consumers=0)
        no_consumer_alerts = [a for a in ev.evaluate(q) if a.alert_type == AlertType.NO_CONSUMERS]
        assert no_consumer_alerts == []


# ---------------------------------------------------------------------------
# QueueMonitor — cycle summary
# ---------------------------------------------------------------------------


class TestQueueMonitor:
    def test_all_healthy_summary(self, evaluator: AlertEvaluator) -> None:
        monitor = QueueMonitor(evaluator=evaluator)
        queues = [make_queue(name=f"q{i}", messages_total=10) for i in range(5)]
        alerts, summary = monitor.run_cycle(queues)
        assert alerts == []
        assert summary.total_queues == 5
        assert summary.healthy_queues == 5
        assert summary.alerts_fired == 0

    def test_mixed_queues_summary(self, evaluator: AlertEvaluator) -> None:
        monitor = QueueMonitor(evaluator=evaluator)
        queues = [
            make_queue(name="healthy", messages_total=10),
            make_queue(name="overloaded", messages_total=3000),
            make_queue(name="orders.dlq", messages_total=5, is_dead_letter_queue=True),
        ]
        alerts, summary = monitor.run_cycle(queues)
        assert summary.total_queues == 3
        assert summary.healthy_queues == 1
        assert summary.alerts_fired == 2
        assert summary.dead_letter_queues == 1

    def test_empty_queue_list_produces_empty_summary(self, evaluator: AlertEvaluator) -> None:
        monitor = QueueMonitor(evaluator=evaluator)
        alerts, summary = monitor.run_cycle([])
        assert summary.total_queues == 0
        assert alerts == []

    def test_total_messages_is_sum_across_all_queues(self, evaluator: AlertEvaluator) -> None:
        monitor = QueueMonitor(evaluator=evaluator)
        queues = [
            make_queue(name="q1", messages_total=100),
            make_queue(name="q2", messages_total=200),
            make_queue(name="q3", messages_total=50),
        ]
        _, summary = monitor.run_cycle(queues)
        assert summary.total_messages == 350
