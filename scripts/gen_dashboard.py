"""Generates monitoring/grafana/dashboards/kafka.json (run once, output is committed)."""
import json

panels_spec = [
    ("Producers: acked events/s", [("sum by (pod) (rate(producer_events_acked_total[10s]))", "{{pod}}")], "ops"),
    ("Producers: buffered (unacked) messages", [("producer_queue_messages", "{{pod}}")], "short"),
    ("Consumers: written to sink, events/s by pod", [("sum by (pod) (rate(consumer_events_written_total[10s]))", "{{pod}}")], "ops"),
    ("Consumers: partition -> pod (events/s)", [("sum by (pod, partition) (rate(consumer_events_written_total[10s]))", "p{{partition}} @ {{pod}}")], "ops"),
    ("Consumer lag by partition", [("max by (partition) (consumer_lag)", "p{{partition}}")], "short"),
    ("Duplicates dropped by dedup (cumulative, by pod)", [
        ("sum by (pod) (consumer_duplicates_total)", "dups {{pod}}")], "short"),
    ("Brokers UP (as seen by producers)", [
        ("sum(min by (broker) (producer_broker_up))", "brokers up"),
        ("count(count by (broker) (producer_broker_up))", "brokers known")], "short"),
    ("Errors: delivery / commit / DLT / rebalances", [
        ("sum(rate(consumer_dlt_total[30s]))", "to DLT, ev/s"),
        ("sum by (error) (increase(producer_delivery_errors_total[30s]))", "delivery {{error}}"),
        ("sum by (pod) (increase(consumer_commit_errors_total[30s]))", "commit err {{pod}}"),
        ("sum by (pod) (changes(consumer_rebalances_total[10s]))", "rebalance {{pod}}")], "short"),
]

panels = []
for i, (title, targets, unit) in enumerate(panels_spec):
    panels.append({
        "id": i + 1,
        "type": "timeseries",
        "title": title,
        "datasource": {"type": "prometheus", "uid": "prom"},
        "gridPos": {"x": (i % 2) * 12, "y": (i // 2) * 8, "w": 12, "h": 8},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 5,
                                                              "spanNulls": False}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi"}},
        "targets": [{"refId": chr(65 + j), "expr": e, "legendFormat": l,
                     "datasource": {"type": "prometheus", "uid": "prom"}}
                    for j, (e, l) in enumerate(targets)],
    })

dash = {
    "uid": "kafka-hw2",
    "title": "HW2 Kafka events",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": panels,
}
json.dump(dash, open("monitoring/grafana/dashboards/kafka.json", "w"), indent=2)
print("ok", len(panels))
