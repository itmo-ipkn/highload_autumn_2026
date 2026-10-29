package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"highload/lab1/internal/credentials"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "stack-init: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	groupID, err := groupIDFromEnv()
	if err != nil {
		return err
	}
	status, err := credentials.Ensure(credentials.Options{
		Dir:            envOr("CREDENTIALS_DIR", "/credentials"),
		GrafanaDataDir: envOr("GRAFANA_DATA_DIR", "/var/lib/grafana"),
		Username:       os.Getenv("GRAFANA_ADMIN_USER"),
		Password:       os.Getenv("GRAFANA_ADMIN_PASSWORD"),
		GroupID:        groupID,
	})
	if err != nil {
		return err
	}
	if status.Created {
		fmt.Fprintln(os.Stdout, "stack-init: credentials created")
		return nil
	}
	fmt.Fprintln(os.Stdout, "stack-init: saved credentials reused")
	return nil
}

func groupIDFromEnv() (int, error) {
	text := envOr("CREDENTIALS_GROUP_ID", "10000")
	groupID, err := strconv.Atoi(text)
	if err != nil || groupID <= 0 {
		return 0, errors.New("CREDENTIALS_GROUP_ID must be a positive integer")
	}
	return groupID, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
