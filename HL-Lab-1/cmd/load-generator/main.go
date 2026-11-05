package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type target struct {
	url    string
	itemID string
}

type item struct {
	ID string `json:"id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	targetURLs := strings.Split(env("ITEMS_URLS", "http://items-1:8080,http://items-2:8080"), ",")
	if len(targetURLs) != 2 || strings.TrimSpace(targetURLs[0]) == "" || strings.TrimSpace(targetURLs[1]) == "" {
		return errors.New("ITEMS_URLS must contain the two application URLs separated by a comma")
	}
	targetRPS := 45.0
	rampDuration := 3 * time.Minute
	client := &http.Client{Timeout: 5 * time.Second}
	targets := []target{{url: strings.TrimRight(strings.TrimSpace(targetURLs[0]), "/")}, {url: strings.TrimRight(strings.TrimSpace(targetURLs[1]), "/")}}

	for index := range targets {
		if err := createItem(client, &targets[index]); err != nil {
			return fmt.Errorf("warm up %s: %w", targets[index].url, err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("load started: target %.0f aggregate RPS, ramp %s across %d instances", targetRPS, rampDuration, len(targets))
	started := time.Now()
	nextRequestAt := started
	var sequence uint64
	var sent, errorsSeen uint64
	lastReport := started

	for {
		if ctx.Err() != nil {
			return nil
		}
		currentRPS := rampedRPS(time.Since(started), rampDuration, targetRPS)
		currentTarget := &targets[sequence%uint64(len(targets))]
		isNotFound := (sequence+1)%40 == 0
		if err := sendRequest(client, currentTarget, sequence, isNotFound); err != nil {
			errorsSeen++
			log.Printf("request failed: %v", err)
		}
		sequence++
		sent++

		if time.Since(lastReport) >= 30*time.Second {
			log.Printf("load running: %.1f target RPS, %d requests sent, %d transport failures", currentRPS, sent, errorsSeen)
			lastReport = time.Now()
		}
		nextRequestAt = nextRequestAt.Add(time.Duration(float64(time.Second) / currentRPS))
		waitDuration := time.Until(nextRequestAt)
		if waitDuration <= 0 {
			nextRequestAt = time.Now()
			continue
		}
		wait := time.NewTimer(waitDuration)
		select {
		case <-ctx.Done():
			wait.Stop()
			return nil
		case <-wait.C:
		}
	}
}

func createItem(client *http.Client, target *target) error {
	body := bytes.NewBufferString(`{"name":"load-test","value":"generated"}`)
	request, err := http.NewRequest(http.MethodPost, target.url+"/items", body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("POST /items returned %s", response.Status)
	}
	var created item
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return err
	}
	if created.ID == "" {
		return errors.New("POST /items returned an empty item ID")
	}
	target.itemID = created.ID
	return nil
}

func sendRequest(client *http.Client, target *target, sequence uint64, forceNotFound bool) error {
	method := []string{http.MethodPost, http.MethodGet, http.MethodPut}[sequence%3]
	if forceNotFound {
		method = http.MethodGet
	}
	path := "/items"
	var body io.Reader
	if method == http.MethodPost || method == http.MethodPut {
		body = bytes.NewBufferString(`{"name":"load-test","value":"generated"}`)
	}
	if method == http.MethodGet || method == http.MethodPut {
		id := target.itemID
		if forceNotFound {
			method = http.MethodGet
			id = "00000000-0000-4000-8000-000000000000"
		}
		path += "/" + id
	}
	request, err := http.NewRequest(method, target.url+path, body)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusCreated && method == http.MethodPost {
		var created item
		if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
			return err
		}
		if created.ID != "" {
			target.itemID = created.ID
		}
		return nil
	}
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode >= 500 && method == http.MethodPut {
		return nil // Expected injected server errors are part of this workload.
	}
	if response.StatusCode >= 400 && !(forceNotFound && response.StatusCode == http.StatusNotFound) {
		return fmt.Errorf("%s %s returned %s", method, path, response.Status)
	}
	return nil
}

func rampedRPS(elapsed, rampDuration time.Duration, targetRPS float64) float64 {
	if elapsed >= rampDuration {
		return targetRPS
	}
	progress := float64(elapsed) / float64(rampDuration)
	return 1 + (targetRPS-1)*progress
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
