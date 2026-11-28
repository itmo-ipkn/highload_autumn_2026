"""Prometheus counters exposed by every producer/consumer pod on :9404.

Named in the plural, consistent with HW1. `pod` is the runtime POD_ID.
"""
import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

POD = os.environ.get('POD_ID', 'unknown')
METRICS_PORT = int(os.environ.get('METRICS_PORT', '9404'))

events_produced_total = Counter(
    'events_produced_total', 'Events acknowledged by brokers (delivery report success)', ['pod'])
produce_attempts_total = Counter(
    'produce_attempts_total', 'Events submitted to the producer', ['pod'])
produce_errors_total = Counter(
    'produce_errors_total', 'Delivery-report errors (never swallowed silently)', ['pod', 'error'])

events_processed_total = Counter(
    'events_processed_total', 'Events processed and written to the sink', ['pod'])
duplicates_dropped_total = Counter(
    'duplicates_dropped_total', 'Deliveries dropped by event_id deduplication', ['pod'])
dlt_produced_total = Counter(
    'dlt_produced_total', 'Poison events routed to events.dlt after 3 attempts', ['pod'])
poison_attempt_failures_total = Counter(
    'poison_attempt_failures_total', 'Failed poison handling attempts', ['pod'])
processing_delay_seconds = Histogram(
    'processing_delay_seconds', 'Simulated per-event processing delay', ['pod'],
    buckets=(0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.1, 0.2, 0.5, 1.0))
consumer_assigned_partitions = Gauge(
    'consumer_assigned_partitions', 'Partitions currently assigned to this pod', ['pod'])


def expose() -> None:
    start_http_server(METRICS_PORT)
