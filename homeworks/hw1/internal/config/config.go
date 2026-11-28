package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"gopkg.in/yaml.v3"

	"highload/lab1/pkg/monitoring"
)

const (
	defaultConfigPath   = "config.yaml"
	maxAllowedBodyBytes = 1 << 20
)

type Config struct {
	HTTPAddr          string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	MaxBodyBytes      int64
	PodID             string
	Datacenter        string
}

type fileConfig struct {
	HTTPAddr          string   `yaml:"http_addr" env:"HTTP_ADDR" env-default:":8080"`
	ReadTimeout       duration `yaml:"read_timeout" env:"HTTP_READ_TIMEOUT" env-default:"5s"`
	ReadHeaderTimeout duration `yaml:"read_header_timeout" env:"HTTP_READ_HEADER_TIMEOUT" env-default:"2s"`
	WriteTimeout      duration `yaml:"write_timeout" env:"HTTP_WRITE_TIMEOUT" env-default:"10s"`
	IdleTimeout       duration `yaml:"idle_timeout" env:"HTTP_IDLE_TIMEOUT" env-default:"60s"`
	ShutdownTimeout   duration `yaml:"shutdown_timeout" env:"HTTP_SHUTDOWN_TIMEOUT" env-default:"15s"`
	MaxBodyBytes      int64    `yaml:"max_body_bytes" env:"HTTP_MAX_BODY_BYTES" env-default:"65536"`
	PodID             string   `env:"POD_ID"`
	Datacenter        string   `env:"DATACENTER"`
}

type duration time.Duration

func (d *duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	return d.UnmarshalText([]byte(raw))
}

func (d *duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = duration(parsed)
	return nil
}

func Load() (Config, error) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = defaultConfigPath
	}

	var raw fileConfig
	if err := cleanenv.ReadConfig(path, &raw); err != nil {
		return Config{}, err
	}
	cfg := Config{
		HTTPAddr:          raw.HTTPAddr,
		ReadTimeout:       time.Duration(raw.ReadTimeout),
		ReadHeaderTimeout: time.Duration(raw.ReadHeaderTimeout),
		WriteTimeout:      time.Duration(raw.WriteTimeout),
		IdleTimeout:       time.Duration(raw.IdleTimeout),
		ShutdownTimeout:   time.Duration(raw.ShutdownTimeout),
		MaxBodyBytes:      raw.MaxBodyBytes,
		PodID:             raw.PodID,
		Datacenter:        raw.Datacenter,
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return fmt.Errorf("http_addr %q: %w", c.HTTPAddr, err)
	}
	timeouts := []struct {
		name  string
		value time.Duration
	}{
		{name: "read_timeout", value: c.ReadTimeout},
		{name: "read_header_timeout", value: c.ReadHeaderTimeout},
		{name: "write_timeout", value: c.WriteTimeout},
		{name: "idle_timeout", value: c.IdleTimeout},
		{name: "shutdown_timeout", value: c.ShutdownTimeout},
	}
	for _, timeout := range timeouts {
		if timeout.value <= 0 {
			return fmt.Errorf("%s must be positive", timeout.name)
		}
	}
	if c.MaxBodyBytes <= 0 {
		return fmt.Errorf("max_body_bytes must be positive")
	}
	if c.MaxBodyBytes > maxAllowedBodyBytes {
		return fmt.Errorf("max_body_bytes must be at most %d", maxAllowedBodyBytes)
	}
	if err := monitoring.ValidateIdentity(c.PodID, c.Datacenter); err != nil {
		return err
	}
	return nil
}
