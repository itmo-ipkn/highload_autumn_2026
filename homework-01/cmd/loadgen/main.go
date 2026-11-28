package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	minRPS      = 1.0
	maxInFlight = 200
	missingBase = 1_000_000_000
)

type config struct {
	targets       []string
	targetRPS     float64
	ramp          time.Duration
	notFoundRatio float64
	statsEvery    time.Duration
}

type kind int

const (
	kindCreate kind = iota
	kindGet
	kindUpdate
)

type request struct {
	kind   kind
	id     int64
	method string
	path   string
	body   []byte
}

type target struct {
	baseURL string
	maxID   atomic.Int64
}

type stats struct {
	mu     sync.Mutex
	sent   int
	codes  map[string]int
	errors int
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("invalid config", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxInFlight,
			MaxIdleConnsPerHost: maxInFlight,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	slog.Info("starting load", "targets", cfg.targets, "target_rps", cfg.targetRPS,
		"ramp", cfg.ramp, "not_found_ratio", cfg.notFoundRatio)

	run(ctx, cfg, client)
	slog.Info("stopped")
}

func run(ctx context.Context, cfg config, client *http.Client) *stats {
	targets := make([]*target, len(cfg.targets))
	for i, u := range cfg.targets {
		targets[i] = &target{baseURL: strings.TrimRight(u, "/")}
	}

	st := &stats{codes: map[string]int{}}
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()

	start := time.Now()
	next := start
	lastStats := start

	for i := 0; ; i++ {
		elapsed := time.Since(start)
		rps := rateAt(elapsed, cfg.ramp, cfg.targetRPS)

		if time.Since(lastStats) >= cfg.statsEvery {
			st.log(rps)
			lastStats = time.Now()
		}

		next = next.Add(time.Duration(float64(time.Second) / rps))
		if !sleepUntil(ctx, next) {
			st.log(rps)
			return st
		}

		t := targets[i%len(targets)]
		req := pickRequest(rand.Float64(), rand.Float64(), t.maxID.Load(), cfg.notFoundRatio)

		select {
		case sem <- struct{}{}:
		default:
			st.record("", errors.New("too many requests in flight, skipping"))
			continue
		}

		wg.Go(func() {
			defer func() { <-sem }()
			code, err := send(ctx, client, t, req)
			st.record(code, err)
		})
	}
}

func rateAt(elapsed, ramp time.Duration, target float64) float64 {
	if ramp <= 0 || elapsed >= ramp {
		return target
	}
	return max(minRPS, target*float64(elapsed)/float64(ramp))
}

func pickRequest(kindRoll, missRoll float64, maxID int64, notFoundRatio float64) request {
	k := kindCreate
	switch {
	case maxID == 0 || kindRoll < 0.2:
		k = kindCreate
	case kindRoll < 0.7:
		k = kindGet
	default:
		k = kindUpdate
	}

	if k == kindCreate {
		return request{kind: k, method: http.MethodPost, path: "/items", body: itemBody()}
	}

	id := rand.Int64N(maxID) + 1
	if missRoll < notFoundRatio {
		id = missingBase + rand.Int64N(missingBase)
	}

	path := "/items/" + strconv.FormatInt(id, 10)
	if k == kindGet {
		return request{kind: k, id: id, method: http.MethodGet, path: path}
	}
	return request{kind: k, id: id, method: http.MethodPut, path: path, body: itemBody()}
}

func itemBody() []byte {
	b, _ := json.Marshal(map[string]string{
		"name":  "item-" + strconv.Itoa(rand.IntN(1000)),
		"value": strconv.Itoa(rand.IntN(1_000_000)),
	})
	return b
}

func send(ctx context.Context, client *http.Client, t *target, r request) (string, error) {
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}

	req, err := http.NewRequestWithContext(ctx, r.method, t.baseURL+r.path, body)
	if err != nil {
		return "", err
	}
	if r.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", nil
		}
		return "", err
	}
	defer resp.Body.Close()

	if r.kind == kindCreate && resp.StatusCode == http.StatusCreated {
		var item struct {
			ID int64 `json:"id"`
		}
		if json.NewDecoder(resp.Body).Decode(&item) == nil {
			raiseMax(&t.maxID, item.ID)
		}
	}
	_, _ = io.Copy(io.Discard, resp.Body)

	return strconv.Itoa(resp.StatusCode), nil
}

func raiseMax(v *atomic.Int64, n int64) {
	for {
		cur := v.Load()
		if n <= cur || v.CompareAndSwap(cur, n) {
			return
		}
	}
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *stats) record(code string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errors++
		return
	}
	if code == "" {
		return
	}
	s.sent++
	s.codes[code]++
}

func (s *stats) snapshot() (int, map[string]int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	codes := make(map[string]int, len(s.codes))
	for k, v := range s.codes {
		codes[k] = v
	}
	return s.sent, codes, s.errors
}

func (s *stats) log(rps float64) {
	sent, codes, errs := s.snapshot()
	slog.Info("progress", "current_rps", fmt.Sprintf("%.1f", rps), "responses", sent, "codes", codes, "transport_errors", errs)
}

func loadConfig() (config, error) {
	cfg := config{statsEvery: 10 * time.Second}

	for _, u := range strings.Split(getenv("TARGETS", "http://app-1:8080,http://app-2:8080"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			cfg.targets = append(cfg.targets, u)
		}
	}
	if len(cfg.targets) == 0 {
		return cfg, errors.New("TARGETS is empty")
	}

	rps, err := strconv.ParseFloat(getenv("TARGET_RPS", "30"), 64)
	if err != nil || rps < minRPS {
		return cfg, fmt.Errorf("TARGET_RPS must be a number >= %v", minRPS)
	}
	cfg.targetRPS = rps

	ramp, err := time.ParseDuration(getenv("RAMP_DURATION", "2m"))
	if err != nil || ramp < 0 {
		return cfg, errors.New("RAMP_DURATION must be a non-negative duration like 2m")
	}
	cfg.ramp = ramp

	ratio, err := strconv.ParseFloat(getenv("NOT_FOUND_RATIO", "0.05"), 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return cfg, errors.New("NOT_FOUND_RATIO must be a number in [0, 1]")
	}
	cfg.notFoundRatio = ratio

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
