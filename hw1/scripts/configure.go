package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"uuid"
)

const (
	envPath     = "../.env"
	lenPassword = 24
)

func main() {
	if err := createEnv(envPath); err != nil {
		panic(err)
	}

	fmt.Println("Successfully generated .env")
}

func createEnv(path string) error {
	environments := make(map[string]string)

	grafanaPassword, err := generatePassword(lenPassword)
	if err != nil {
		return err
	}

	environments["GRAFANA_ADMIN_PASSWORD"] = grafanaPassword
	environments["PROMETHEUS_UID"] = uuid.New().String()
	environments["GRAFANA_UID"] = uuid.New().String()
	environments["DIR_DASHBOARD"] = "/dashboards"

	var builder strings.Builder
	for key := range environments {
		fmt.Fprintf(&builder, "%s=%s\n", key, environments[key])
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("error with create .env file, %w", err)
	}

	if _, err := file.WriteString(builder.String()); err != nil {
		file.Close()
		os.Remove(path)
		return fmt.Errorf("error with write values to file, %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(path)
		return fmt.Errorf("error with close .env, %s", err)
	}

	return nil
}

func generatePassword(len int) (string, error) {
	data := make([]byte, len)

	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("error with generate password: %w", err)
	}

	return hex.EncodeToString(data), nil
}
