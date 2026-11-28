package dashboard

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	defaultAttempts = 3
	defaultBackoff  = 200 * time.Millisecond
	requestTimeout  = 10 * time.Second
	// RunTimeout bounds one generator run, including retries.
	RunTimeout = 45 * time.Second
)

// Config is the process environment the generator needs.
// Credentials come from the environment, not from a host .env file.
type Config struct {
	AppBaseURL string
	GrafanaURL string
	Username   string
	Password   string
	// Attempts is the limit for one HTTP call. Zero selects the default.
	Attempts int
	// Backoff is the first retry delay. Zero selects the default.
	// A negative value retries immediately.
	Backoff time.Duration
}

// ConfigFromEnv reads APP_BASE_URL, GRAFANA_URL and the admin basic-auth pair.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		AppBaseURL: strings.TrimSpace(getenv("APP_BASE_URL")),
		GrafanaURL: strings.TrimSpace(getenv("GRAFANA_URL")),
		Username:   getenv("GRAFANA_ADMIN_USER"),
		Password:   getenv("GRAFANA_ADMIN_PASSWORD"),
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.Username == "" {
		return fmt.Errorf("GRAFANA_ADMIN_USER is required")
	}
	if c.Password == "" {
		return fmt.Errorf("GRAFANA_ADMIN_PASSWORD is required")
	}
	if err := validateBaseURL("APP_BASE_URL", c.AppBaseURL); err != nil {
		return err
	}
	if err := validateBaseURL("GRAFANA_URL", c.GrafanaURL); err != nil {
		return err
	}
	return nil
}

func validateBaseURL(name, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", name)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("%s must be an absolute http or https URL", name)
	}
	if parsed.User != nil {
		return fmt.Errorf("%s must not include userinfo", name)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s must not include a query or fragment", name)
	}
	return nil
}

func (c Config) attempts() int {
	if c.Attempts > 0 {
		return c.Attempts
	}
	return defaultAttempts
}

func (c Config) backoff() time.Duration {
	if c.Backoff < 0 {
		return 0
	}
	if c.Backoff == 0 {
		return defaultBackoff
	}
	return c.Backoff
}

// redact removes the admin password from a message returned to the operator.
func redact(secret, message string) string {
	if secret == "" || message == "" || !strings.Contains(message, secret) {
		return message
	}
	return strings.ReplaceAll(message, secret, "[redacted]")
}
