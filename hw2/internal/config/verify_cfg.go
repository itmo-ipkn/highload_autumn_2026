package config

import (
	"fmt"
)

type VerifyConfig struct {
	Brokers []string
	User    string
	PgURL   string
}

func LoadVerifyConfig() (VerifyConfig, error) {
	cfg := VerifyConfig{
		Brokers: Brokers(),
		User:    getEnvStr("GITHUB_LOGIN", "unknown"),
		PgURL:   getEnvStr("DATABASE_URL", ""),
	}

	if len(cfg.Brokers) == 0 || cfg.PgURL == "" || cfg.User == "" {
		return VerifyConfig{}, fmt.Errorf(
			"Invalid environment variables: brokers=%v, pg_url=%q, user=%q",
			cfg.Brokers,
			cfg.PgURL,
			cfg.User,
		)
	}

	return cfg, nil
}
