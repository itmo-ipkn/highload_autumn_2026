package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/nikita/highload_autumn_2026/hw1/scripts/cli/dto"
)

func main() {

	uid := os.Getenv("GRAFANA_UID")
	if uid == "" {
		panic("uid is empty")
	}

	dir := os.Getenv("DIR_DASHBOARD")
	if dir == "" {
		panic("dir for dashboard is empty")
	}

	promUID := os.Getenv("PROMETHEUS_UID")
	if dir == "" {
		panic("prometheus uid is empty")
	}

	uri := flag.String("uri", "", "URI to endpoint /handlers")
	refresh := flag.String("refresh", "", "")
	flag.Parse()

	if *uri == "" {
		panic("flag --uri is required")
	}

	if *refresh == "" {
		panic("flag --refresh is required")
	}

	handlers, err := getHandlers(*uri)
	if err != nil {
		panic(err)
	}

	dashboard := generateDashboard(handlers, uid, promUID, *refresh)

	err = saveDashboard(dashboard, dir)
	if err != nil {
		panic(err)
	}
}

func getHandlers(uri string) ([]dto.HandlerInfo, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(uri)
	if err != nil {
		return nil, fmt.Errorf("error with get handlers: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code not OK: %s", resp.Status)
	}

	var handlerInfo []dto.HandlerInfo
	if err = json.NewDecoder(resp.Body).Decode(&handlerInfo); err != nil {
		return nil, fmt.Errorf("error with read repsonse: %w", err)
	}

	return handlerInfo, nil
}

func saveDashboard(dashboard dto.Dashboard, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("error with create directory dashboard: %w", err)
	}

	data, err := json.MarshalIndent(dashboard, "", " ")
	if err != nil {
		return fmt.Errorf("error with marshal dashboard: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".dashboard-*.tmp")
	if err != nil {
		return fmt.Errorf("error with create temp file: %w", err)
	}

	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("error with write temp file: %w", err)
	}

	if err := tmp.Chmod(0644); err != nil {
		tmp.Close()
		return fmt.Errorf("error with chmod temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("error with close temp file: %w", err)
	}

	path := filepath.Join(dir, "dashboard.json")

	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("error with rename dashboard file: %w", err)
	}

	return nil
}
