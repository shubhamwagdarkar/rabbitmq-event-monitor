"""PostgreSQL persistence layer for queue metrics and alert history."""

import logging
from contextlib import contextmanager
from typing import Generator

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

from src.models import Alert, MonitorSummary, QueueMetrics

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS queue_metrics (
    id               SERIAL PRIMARY KEY,
    queue_name       TEXT        NOT NULL,
    vhost            TEXT        NOT NULL,
    messages_ready   INTEGER     NOT NULL DEFAULT 0,
    messages_unacked INTEGER     NOT NULL DEFAULT 0,
    messages_total   INTEGER     NOT NULL DEFAULT 0,
    consumers        INTEGER     NOT NULL DEFAULT 0,
    consumer_util    FLOAT       NOT NULL DEFAULT 0.0,
    publish_rate     FLOAT       NOT NULL DEFAULT 0.0,
    deliver_rate     FLOAT       NOT NULL DEFAULT 0.0,
    is_dlq           BOOLEAN     NOT NULL DEFAULT FALSE,
    sampled_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_history (
    id              SERIAL PRIMARY KEY,
    queue_name      TEXT        NOT NULL,
    vhost           TEXT        NOT NULL,
    alert_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    message         TEXT        NOT NULL,
    current_value   FLOAT       NOT NULL,
    threshold_value FLOAT       NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitor_summary (
    id               SERIAL PRIMARY KEY,
    total_queues     INTEGER     NOT NULL,
    healthy_queues   INTEGER     NOT NULL,
    alerts_fired     INTEGER     NOT NULL,
    dlq_count        INTEGER     NOT NULL,
    total_messages   INTEGER     NOT NULL,
    polled_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_queue_metrics_sampled_at ON queue_metrics (sampled_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_fired_at   ON alert_history (fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_queue      ON alert_history (queue_name, vhost);
"""


class Database:
    """Thin wrapper around psycopg2 for metric and alert persistence."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn = None

    def connect(self) -> None:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed. Run: pip install psycopg2-binary")
        self._conn = psycopg2.connect(self.dsn)
        logger.info("Connected to PostgreSQL")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Disconnected from PostgreSQL")

    @contextmanager
    def cursor(self) -> Generator:
        if not self._conn or self._conn.closed:
            self.connect()
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def ensure_schema(self) -> None:
        """Create tables if they don't exist yet."""
        with self.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        logger.info("Database schema verified")

    def insert_queue_metrics(self, metrics_list: list[QueueMetrics]) -> None:
        """Bulk-insert a list of QueueMetrics snapshots."""
        rows = [
            (
                m.name,
                m.vhost,
                m.messages_ready,
                m.messages_unacknowledged,
                m.messages_total,
                m.consumers,
                m.consumer_utilisation,
                m.message_publish_rate,
                m.message_deliver_rate,
                m.is_dead_letter_queue,
                m.sampled_at,
            )
            for m in metrics_list
        ]
        with self.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO queue_metrics
                    (queue_name, vhost, messages_ready, messages_unacked, messages_total,
                     consumers, consumer_util, publish_rate, deliver_rate, is_dlq, sampled_at)
                VALUES %s
                """,
                rows,
            )
        logger.debug("Inserted %d queue metric rows", len(rows))

    def insert_alert(self, alert: Alert) -> None:
        """Persist a fired alert to alert_history."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_history
                    (queue_name, vhost, alert_type, severity, message,
                     current_value, threshold_value, fired_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    alert.queue_name,
                    alert.vhost,
                    alert.alert_type.value,
                    alert.severity.value,
                    alert.message,
                    alert.current_value,
                    alert.threshold_value,
                    alert.fired_at,
                ),
            )

    def insert_summary(self, summary: MonitorSummary) -> None:
        """Persist a cycle summary."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monitor_summary
                    (total_queues, healthy_queues, alerts_fired, dlq_count,
                     total_messages, polled_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    summary.total_queues,
                    summary.healthy_queues,
                    summary.alerts_fired,
                    summary.dead_letter_queues,
                    summary.total_messages,
                    summary.polled_at,
                ),
            )

    def get_recent_alerts(self, limit: int = 50) -> list[dict]:
        """Return the most recent alerts for reporting."""
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM alert_history ORDER BY fired_at DESC LIMIT %s",
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
