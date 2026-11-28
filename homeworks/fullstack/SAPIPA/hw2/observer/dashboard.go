package main

import (
	"encoding/json"
	"fmt"
	"io"
)

func writeDashboard(w io.Writer) error {
	datasource := map[string]string{"type": "prometheus", "uid": "hw4-prometheus"}
	type query struct {
		expr, legend string
	}
	charts := []struct {
		title   string
		queries []query
	}{
		{"События в секунду", []query{
			{`sum(rate(hw4_end_offset{topic="events"}[30s]))`, "Kafka"},
			{`rate(hw4_sink_events_total[30s])`, "SQLite {{consumer}}"},
		}},
		{"Отставание по партициям", []query{{`hw4_group_lag`, "партиция {{partition}}"}}},
		{"Брокеры и консьюмеры", []query{{`sum(hw4_broker_up)`, "доступно брокеров"}, {`hw4_group_members`, "участники группы"}}},
		{"Реплики в ISR", []query{{`hw4_partition_isr`, "партиция {{partition}}"}}},
		{"Лидеры партиций", []query{{`hw4_partition_leader`, "партиция {{partition}}"}}},
		{"DLT и дубли за 30 секунд", []query{
			{`sum(increase(hw4_end_offset{topic="events.dlt"}[30s]))`, "DLT"},
			{`increase(hw4_sink_duplicates_total[30s])`, "дубли"},
		}},
	}
	panels := make([]map[string]any, 0, len(charts))
	for index, chart := range charts {
		targets := make([]map[string]any, 0, len(chart.queries))
		for i, q := range chart.queries {
			targets = append(targets, map[string]any{
				"refId": fmt.Sprintf("%c", 'A'+i), "expr": q.expr, "legendFormat": q.legend, "datasource": datasource,
			})
		}
		panels = append(panels, map[string]any{
			"id": index + 1, "title": chart.title, "type": "timeseries", "datasource": datasource, "targets": targets,
			"gridPos": map[string]int{"x": index % 2 * 12, "y": index / 2 * 7, "w": 12, "h": 7},
		})
	}
	return json.NewEncoder(w).Encode(map[string]any{
		"uid": "sapipa-kafka", "title": "Kafka · эксперименты", "schemaVersion": 39,
		"refresh": "5s", "time": map[string]string{"from": "now-15m", "to": "now"}, "panels": panels,
	})
}
