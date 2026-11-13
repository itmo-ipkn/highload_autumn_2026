package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"highload-hw1/internal/load"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Нагрузка остановлена", "error", err)
		os.Exit(1)
	}
}

func run() error {
	rps := flag.Float64("rps", 30, "Суммарное число запросов в секунду")
	ramp := flag.Duration("ramp", 150*time.Second, "Время разгона")
	flag.Parse()
	if *rps < 1 || *rps > 50 || *ramp <= 0 {
		return fmt.Errorf("нужны rps от 1 до 50 и положительный ramp")
	}
	var targets []load.Target
	for _, address := range strings.Split(os.Getenv("APP_URLS"), ",") {
		address = strings.TrimRight(strings.TrimSpace(address), "/")
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
			return fmt.Errorf("APP_URLS должен содержать HTTP-адреса через запятую")
		}
		targets = append(targets, load.Target{URL: address})
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	slog.Info("Нагрузка запущена", "rps", *rps, "разгон", *ramp, "экземпляров", len(targets))
	load.Run(ctx, client, targets, *ramp, *rps, func(err error) { slog.Warn("Неожиданный ответ", "error", err) })
	slog.Info("Нагрузка остановлена")
	return nil
}
