# rabbitmq-event-monitor

Real-time RabbitMQ queue health monitor. Polls the RabbitMQ Management HTTP API, evaluates queue metrics against configurable thresholds, fires Slack alerts on breach, and persists all data to PostgreSQL for Grafana dashboards.

**Week 4 of 42** — Enterprise Automation Portfolio

---

## What It Does

- **Queue depth monitoring** — warns at 500 messages, critical at 2,000
- **Consumer lag detection** — flags queues where consumers can't keep up (unacknowledged message backlog)
- **Dead-letter queue alerts** — any message in a DLQ triggers immediate notification
- **No-consumer detection** — alerts when messages accumulate with zero active consumers
- **Slack notifications** — Block Kit formatted alerts with severity color-coding and 15-minute cooldown deduplication
- **PostgreSQL persistence** — full metric history queryable by Grafana
- **Single-poll mode** — `--once` flag for cron job / CI integration

---

## Architecture

```
RabbitMQ Management API (:15672)
        │
        ▼
RabbitMQManagementClient     ← wraps HTTP API, parses raw queue JSON
        │
        ▼
AlertEvaluator               ← evaluates ThresholdConfig rules per queue
        │
        ▼
QueueMonitor                 ← orchestrates one polling cycle
        │
   ┌────┴────┐
   ▼         ▼
SlackNotifier   Database     ← Slack webhook + PostgreSQL persistence
                    │
                    ▼
                Grafana       ← connects to PostgreSQL for dashboards
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- RabbitMQ with the Management Plugin enabled (`rabbitmq-plugins enable rabbitmq_management`)

### Install

```bash
git clone https://github.com/shubhamwagdarkar/rabbitmq-event-monitor.git
cd rabbitmq-event-monitor
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your RabbitMQ host, Slack webhook, and optional database URL
```

### Run

```bash
# Continuous polling (every 60 seconds by default)
python main.py

# Single poll then exit (good for cron jobs)
python main.py --once

# Custom config file
python main.py --config /path/to/settings.yaml

# Print recent alert history (requires DATABASE_URL)
python main.py --report
```

---

## Configuration

All settings live in `config/settings.yaml`. Environment variables override YAML values.

```yaml
poll_interval_seconds: 60

rabbitmq:
  host: localhost
  port: 15672
  username: guest
  password: guest

slack:
  webhook_url: ""      # Set via SLACK_WEBHOOK_URL
  channel: "#ops-alerts"

thresholds:
  queue_depth_warning: 500
  queue_depth_critical: 2000
  consumer_lag_warning: 100
  consumer_lag_critical: 500
  dlq_depth_warning: 1      # Any DLQ message = alert
  dlq_depth_critical: 50
  alert_on_no_consumers: true
  ignored_prefixes:
    - "amq."                # Skip internal RabbitMQ queues
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RABBITMQ_HOST` | RabbitMQ host |
| `RABBITMQ_PORT` | Management API port (default: 15672) |
| `RABBITMQ_USER` | Management API username |
| `RABBITMQ_PASS` | Management API password |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `SLACK_CHANNEL` | Slack channel (default: #ops-alerts) |
| `DATABASE_URL` | PostgreSQL DSN for metric persistence |
| `POLL_INTERVAL_SECONDS` | Override polling interval |
| `LOG_LEVEL` | DEBUG / INFO / WARNING / ERROR |

---

## Alert Types

| Alert | Trigger | Default Severity |
|-------|---------|-----------------|
| `queue_depth` | Messages > threshold | WARNING → CRITICAL |
| `consumer_lag` | Unacked messages > threshold | WARNING → CRITICAL |
| `dead_letter` | Any message in DLQ | WARNING → CRITICAL |
| `no_consumers` | Messages > 0, consumers = 0 | CRITICAL |
| `connection_failure` | Management API unreachable | CRITICAL |

Alerts fire at `WARNING` level first, escalating to `CRITICAL` at higher thresholds. Dead-letter queue alerts take exclusive priority — no depth/lag alerts fire alongside a DLQ alert. Repeated alerts for the same queue + type are suppressed for 15 minutes.

---

## Grafana Integration

When `DATABASE_URL` is configured, the monitor writes to three PostgreSQL tables:

- **`queue_metrics`** — per-queue snapshot every poll cycle (depth, consumers, rates)
- **`alert_history`** — every alert that fired with timestamp and threshold values
- **`monitor_summary`** — aggregate per-cycle summary (total queues, healthy, DLQ count)

Connect Grafana to your PostgreSQL instance and query these tables directly for time-series dashboards.

Example Grafana query for queue depth over time:
```sql
SELECT sampled_at AS time, queue_name, messages_total
FROM queue_metrics
WHERE $__timeFilter(sampled_at)
ORDER BY sampled_at
```

---

## Running Tests

```bash
pytest tests/ -v
```

```
53 passed in 0.32s
```

Tests cover:
- Threshold evaluation logic for all alert types
- Dead-letter queue detection (name patterns + AMQP arguments)
- Cooldown deduplication in Slack notifier
- Management API client: payload parsing, malformed entry handling, HTTP error propagation
- QueueMonitor cycle summary aggregation

---

## Project Structure

```
rabbitmq-event-monitor/
├── main.py                    # Entry point — polling loop + CLI args
├── src/
│   ├── models.py              # QueueMetrics, Alert, ThresholdConfig dataclasses
│   ├── rabbitmq_client.py     # Management HTTP API client
│   ├── monitor.py             # AlertEvaluator + QueueMonitor
│   ├── slack_notifier.py      # Slack webhook with deduplication
│   ├── db.py                  # PostgreSQL persistence layer
│   └── config.py              # YAML + env var config loader
├── config/
│   └── settings.yaml          # Default configuration
├── tests/
│   ├── test_monitor.py        # 22 tests — threshold evaluation
│   ├── test_rabbitmq_client.py # 15 tests — API client + DLQ detection
│   └── test_slack_notifier.py  # 12 tests — notifications + deduplication
├── .env.example
├── requirements.txt
└── README.md
```

---

## What I Learned

RabbitMQ's Management API returns `consumer_utilisation` as `null` when a queue has no active consumers — not `0` — which causes type errors if you don't guard against it. The DLQ detection heuristic (name patterns + AMQP x-dead-letter-exchange argument) catches both explicitly declared DLQs and queues that happen to be named with common conventions. Alert deduplication via in-process cache is sufficient for single-instance deployments; a production version would use Redis with TTL keys to share state across replicas.

---

## Author

Shubham Wagdarkar — [github.com/shubhamwagdarkar](https://github.com/shubhamwagdarkar)
