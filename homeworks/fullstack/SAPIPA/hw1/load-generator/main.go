package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Item struct {
	ID uint64 `json:"id"`
}

type Pod struct {
	Name string
	URL  string
	IDs  []uint64
}

type Stats struct {
	Requests int
	Success  int
	NotFound int
	Internal int
	Other    int
	Network  int
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		log.Fatalf("%s must be a positive integer", name)
	}
	return n
}

func request(
	ctx context.Context,
	client *http.Client,
	method, url string,
	payload any,
) (int, []byte, error) {
	var body io.Reader

	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return 0, nil, err
	}

	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	return resp.StatusCode, data, err
}

func createItem(
	ctx context.Context,
	client *http.Client,
	pod *Pod,
	sequence int,
) (int, error) {
	payload := map[string]string{
		"name": fmt.Sprintf("load-item-%d", sequence),
	}

	status, body, err := request(
		ctx, client, http.MethodPost,
		pod.URL+"/items", payload,
	)
	if err != nil {
		return 0, err
	}

	if status == http.StatusCreated {
		var item Item

		if err := json.Unmarshal(body, &item); err != nil {
			return status, fmt.Errorf("decode POST response: %w", err)
		}
		if item.ID == 0 {
			return status, fmt.Errorf("POST returned invalid item ID")
		}

		if len(pod.IDs) >= 500 {
			pod.IDs = pod.IDs[1:]
		}
		pod.IDs = append(pod.IDs, item.ID)
	}

	return status, nil
}

func existingID(pod *Pod) uint64 {
	return pod.IDs[len(pod.IDs)-1]
}

func runRequest(
	ctx context.Context,
	client *http.Client,
	pod *Pod,
	sequence int,
) (int, error) {

	roll := sequence % 100

	if roll == 11 || roll == 47 {
		url := fmt.Sprintf(
			"%s/items/%d", pod.URL, ^uint64(0),
		)
		status, _, err := request(
			ctx, client, http.MethodGet, url, nil,
		)
		return status, err
	}

	if roll == 79 {
		url := pod.URL + "/items/0?simulate_error=1"
		status, _, err := request(
			ctx, client, http.MethodGet, url, nil,
		)
		return status, err
	}

	if len(pod.IDs) == 0 {
		return createItem(ctx, client, pod, sequence)
	}

	switch sequence % 5 {
	case 0:
		return createItem(ctx, client, pod, sequence)

	case 1, 2:
		id := existingID(pod)
		url := fmt.Sprintf("%s/items/%d", pod.URL, id)
		status, _, err := request(
			ctx, client, http.MethodGet, url, nil,
		)
		if err == nil && status == http.StatusNotFound {
			pod.IDs = nil
		}
		return status, err

	default:
		id := existingID(pod)
		url := fmt.Sprintf("%s/items/%d", pod.URL, id)

		payload := map[string]string{
			"name": fmt.Sprintf("updated-%d", sequence),
		}

		status, _, err := request(
			ctx, client, http.MethodPut, url, payload,
		)
		if err == nil && status == http.StatusNotFound {
			pod.IDs = nil
		}
		return status, err
	}
}

func (s *Stats) Record(status int, err error) {
	s.Requests++

	if err != nil {
		s.Network++
		return
	}

	switch {
	case status >= 200 && status < 300:
		s.Success++
	case status == 404:
		s.NotFound++
	case status == 500:
		s.Internal++
	default:
		s.Other++
	}
}

func plannedRPS(elapsed, ramp time.Duration, target int) float64 {
	start := 5.0
	if float64(target) < start {
		start = float64(target)
	}

	if ramp <= 0 {
		return float64(target)
	}
	progress := elapsed.Seconds() / ramp.Seconds()
	if progress < 0 {
		progress = 0
	}
	if progress > 1 {
		progress = 1
	}

	return start + (float64(target)-start)*progress
}

func main() {
	target := envInt("TARGET_RPS", 30)
	ramp := time.Duration(
		envInt("RAMP_SECONDS", 180),
	) * time.Second

	pods := []*Pod{
		{
			Name: "app-1",
			URL:  strings.TrimRight(os.Getenv("APP_1_URL"), "/"),
		},
		{
			Name: "app-2",
			URL:  strings.TrimRight(os.Getenv("APP_2_URL"), "/"),
		},
	}

	for _, pod := range pods {
		if err := validateURL(pod.URL); err != nil {
			log.Fatalf("URL for %s: %v", pod.Name, err)
		}
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 25,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	defer client.CloseIdleConnections()

	if err := run(ctx, client, pods, target, ramp); err != nil {
		log.Fatal(err)
	}
}

func validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return fmt.Errorf("must be an absolute HTTP(S) base URL without credentials, query or fragment")
	}
	return nil
}

func run(ctx context.Context, client *http.Client, pods []*Pod, target int, ramp time.Duration) error {
	if len(pods) == 0 || target <= 0 || ramp <= 0 {
		return fmt.Errorf("at least one pod, positive TARGET_RPS and positive RAMP_SECONDS are required")
	}

	for index, pod := range pods {
		status, err := createItem(ctx, client, pod, index+1)
		if err != nil || status != http.StatusCreated {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf(
				"seed %s failed: status=%d error=%v",
				pod.Name, status, err,
			)
		}
		log.Printf(
			"%s seeded with item ID=%d",
			pod.Name, pod.IDs[0],
		)
	}

	log.Printf(
		"Starting load: %.1f -> %d RPS in %s, %d pods",
		plannedRPS(0, ramp, target), target, ramp, len(pods),
	)

	start := time.Now()
	next := start
	lastReport := start

	var window Stats
	var total Stats

	for sequence := 0; ; sequence++ {
		if ctx.Err() != nil {
			break
		}

		if wait := time.Until(next); wait > 0 {
			timer := time.NewTimer(wait)

			select {
			case <-ctx.Done():
				timer.Stop()
				log.Printf("Stopped: %+v", total)
				return nil
			case <-timer.C:
			}
		}

		now := time.Now()
		rps := plannedRPS(now.Sub(start), ramp, target)

		next = next.Add(
			time.Duration(float64(time.Second) / rps),
		)

		if next.Before(now) {
			next = now
		}

		pod := pods[sequence%len(pods)]

		status, err := runRequest(
			ctx, client, pod, sequence/len(pods),
		)

		window.Record(status, err)
		total.Record(status, err)

		if err != nil && ctx.Err() == nil {
			log.Printf("%s request error: %v", pod.Name, err)
		}

		if time.Since(lastReport) >= 10*time.Second {
			seconds := time.Since(lastReport).Seconds()
			actualRPS := float64(window.Requests) / seconds

			log.Printf(
				"target=%.1f RPS actual=%.1f RPS "+
					"requests=%d success=%d 404=%d 500=%d "+
					"other=%d network=%d",
				rps,
				actualRPS,
				window.Requests,
				window.Success,
				window.NotFound,
				window.Internal,
				window.Other,
				window.Network,
			)

			window = Stats{}
			lastReport = time.Now()
		}
	}

	log.Printf("Stopped: %+v", total)
	return nil
}
