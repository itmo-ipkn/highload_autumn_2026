package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadFileAndDefaults(t *testing.T) {
	isolateEnv(t)
	setIdentity(t)
	path := writeConfig(t, "http_addr: \"127.0.0.1:9090\"\n")
	t.Setenv("CONFIG_PATH", path)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" {
		t.Fatalf("addr = %s", cfg.HTTPAddr)
	}
	if cfg.ReadTimeout != 5*time.Second || cfg.ReadHeaderTimeout != 2*time.Second {
		t.Fatalf("read timeouts = %s / %s", cfg.ReadTimeout, cfg.ReadHeaderTimeout)
	}
	if cfg.WriteTimeout != 10*time.Second || cfg.IdleTimeout != 60*time.Second {
		t.Fatalf("write/idle timeouts = %s / %s", cfg.WriteTimeout, cfg.IdleTimeout)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Fatalf("shutdown timeout = %s", cfg.ShutdownTimeout)
	}
	if cfg.MaxBodyBytes != 64*1024 {
		t.Fatalf("max body = %d", cfg.MaxBodyBytes)
	}
	if cfg.PodID != "pod-a" || cfg.Datacenter != "dc-a" {
		t.Fatalf("identity = %s/%s", cfg.PodID, cfg.Datacenter)
	}
}

func TestLoadFileValues(t *testing.T) {
	isolateEnv(t)
	setIdentity(t)
	path := writeConfig(t, `
http_addr: "127.0.0.1:9090"
read_timeout: 9s
read_header_timeout: 500ms
write_timeout: 3s
idle_timeout: 4s
shutdown_timeout: 6s
max_body_bytes: 128
`)
	t.Setenv("CONFIG_PATH", path)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ReadTimeout != 9*time.Second || cfg.MaxBodyBytes != 128 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.ReadHeaderTimeout != 500*time.Millisecond || cfg.WriteTimeout != 3*time.Second {
		t.Fatalf("timeouts = %+v", cfg)
	}
	if cfg.IdleTimeout != 4*time.Second || cfg.ShutdownTimeout != 6*time.Second {
		t.Fatalf("timeouts = %+v", cfg)
	}
}

func TestEnvironmentOverridesFile(t *testing.T) {
	isolateEnv(t)
	setIdentity(t)
	path := writeConfig(t, `
http_addr: ":8080"
read_timeout: 9s
read_header_timeout: 2s
write_timeout: 10s
idle_timeout: 60s
shutdown_timeout: 15s
max_body_bytes: 65536
`)
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("HTTP_READ_TIMEOUT", "1s")
	t.Setenv("HTTP_MAX_BODY_BYTES", "128")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ReadTimeout != time.Second || cfg.MaxBodyBytes != 128 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.WriteTimeout != 10*time.Second {
		t.Fatalf("file value was replaced: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		file string
		env  map[string]string
	}{
		{name: "addr in file", file: "http_addr: \"8080\"\n"},
		{name: "duration in file", file: "http_addr: \":8080\"\nread_timeout: soon\n"},
		{name: "addr in env", file: defaultFile, env: map[string]string{"HTTP_ADDR": "8080"}},
		{name: "duration in env", file: defaultFile, env: map[string]string{"HTTP_READ_TIMEOUT": "soon"}},
		{name: "zero duration", file: defaultFile, env: map[string]string{"HTTP_READ_TIMEOUT": "0s"}},
		{name: "negative duration", file: defaultFile, env: map[string]string{"HTTP_SHUTDOWN_TIMEOUT": "-1s"}},
		{name: "body text", file: defaultFile, env: map[string]string{"HTTP_MAX_BODY_BYTES": "nope"}},
		{name: "zero body", file: defaultFile, env: map[string]string{"HTTP_MAX_BODY_BYTES": "0"}},
		{name: "huge body", file: defaultFile, env: map[string]string{"HTTP_MAX_BODY_BYTES": "2097152"}},
		{name: "missing pod", file: defaultFile, env: map[string]string{"POD_ID": ""}},
		{name: "missing datacenter", file: defaultFile, env: map[string]string{"DATACENTER": ""}},
		{name: "pod with space", file: defaultFile, env: map[string]string{"POD_ID": "pod 1"}},
		{name: "datacenter too long", file: defaultFile, env: map[string]string{"DATACENTER": "dc-" + strings.Repeat("a", 63)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isolateEnv(t)
			setIdentity(t)
			t.Setenv("CONFIG_PATH", writeConfig(t, test.file))
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			if _, err := Load(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestIdentityComesFromEnvironmentOnly(t *testing.T) {
	isolateEnv(t)
	path := writeConfig(t, `
http_addr: ":8080"
pod_id: from-file
datacenter: from-file
read_timeout: 5s
read_header_timeout: 2s
write_timeout: 10s
idle_timeout: 60s
shutdown_timeout: 15s
max_body_bytes: 65536
`)
	t.Setenv("CONFIG_PATH", path)
	if _, err := Load(); err == nil {
		t.Fatal("identity from the config file was accepted")
	}

	setIdentity(t)
	t.Setenv("POD_ID", "pod-1")
	t.Setenv("DATACENTER", "dc-1")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.PodID != "pod-1" || cfg.Datacenter != "dc-1" {
		t.Fatalf("identity = %s/%s", cfg.PodID, cfg.Datacenter)
	}
}

func TestLoadMissingFile(t *testing.T) {
	isolateEnv(t)
	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	if _, err := Load(); err == nil {
		t.Fatal("missing config file was accepted")
	}
}

const defaultFile = `
http_addr: ":8080"
read_timeout: 5s
read_header_timeout: 2s
write_timeout: 10s
idle_timeout: 60s
shutdown_timeout: 15s
max_body_bytes: 65536
`

func isolateEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"CONFIG_PATH",
		"HTTP_ADDR",
		"HTTP_READ_TIMEOUT",
		"HTTP_READ_HEADER_TIMEOUT",
		"HTTP_WRITE_TIMEOUT",
		"HTTP_IDLE_TIMEOUT",
		"HTTP_SHUTDOWN_TIMEOUT",
		"HTTP_MAX_BODY_BYTES",
		"POD_ID",
		"DATACENTER",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func setIdentity(t *testing.T) {
	t.Helper()
	t.Setenv("POD_ID", "pod-a")
	t.Setenv("DATACENTER", "dc-a")
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
