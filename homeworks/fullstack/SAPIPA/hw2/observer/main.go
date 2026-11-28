package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	events = "events"
	dlt    = "events.dlt"
	group  = "events-processor"
)

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 1 && args[0] == "dashboard" {
		return writeDashboard(os.Stdout)
	}
	if len(args) == 0 || (args[0] != "verify" && args[0] != "metrics") {
		return fmt.Errorf("usage: observer verify [--since RFC3339] | metrics | dashboard")
	}
	brokers := strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"), ",")
	db, err := sql.Open("sqlite3", "file:"+env("SINK_PATH", "/app/state/events.db")+"?mode=ro&_busy_timeout=2000")
	if err != nil {
		return err
	}
	defer db.Close()
	if args[0] == "verify" {
		return verify(args[1:], brokers, db)
	}
	return serveMetrics(brokers, db)
}
