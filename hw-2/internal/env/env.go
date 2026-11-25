// Package env читает настройки из переменных окружения.
package env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// String возвращает значение переменной или def, если она пустая.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// List - значения через запятую.
func List(key, def string) []string {
	var out []string
	for _, s := range strings.Split(String(key, def), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Int читает целое число.
func Int(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// Float читает число с плавающей точкой.
func Float(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

// Duration читает длительность в формате time.ParseDuration.
func Duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

// Millis читает число миллисекунд.
func Millis(key string, def int) (time.Duration, error) {
	n, err := Int(key, def)
	return time.Duration(n) * time.Millisecond, err
}
