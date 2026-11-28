package dashboard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type statusError struct {
	Method  string
	Path    string
	Status  int
	Snippet string
}

func (e *statusError) Error() string {
	if e.Snippet == "" {
		return fmt.Sprintf("%s %s: returned %d", e.Method, e.Path, e.Status)
	}
	return fmt.Sprintf("%s %s: returned %d: %s", e.Method, e.Path, e.Status, e.Snippet)
}

// NewHTTPClient is the client the generator uses for the app catalog and Grafana.
// The composition root creates it and passes it to Generate.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func do(ctx context.Context, httpClient *http.Client, cfg Config, username, password, method, target string, payload []byte, limit int64) (int, []byte, error) {
	attempts := cfg.attempts()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			delay := cfg.backoff() << (attempt - 2)
			if err := wait(ctx, delay); err != nil {
				return 0, nil, redactError(password, err)
			}
		}
		status, body, err := once(ctx, httpClient, username, password, method, target, payload, limit)
		if err != nil {
			lastErr = err
			if !retryable(err) || attempt == attempts {
				return 0, nil, redactError(password, err)
			}
			continue
		}
		if retryableStatus(status) {
			lastErr = &statusError{
				Method:  method,
				Path:    requestPath(target),
				Status:  status,
				Snippet: snippet(body, password),
			}
			if attempt == attempts {
				return 0, nil, lastErr
			}
			continue
		}
		return status, body, nil
	}
	if lastErr == nil {
		lastErr = errors.New("request failed")
	}
	return 0, nil, redactError(password, lastErr)
}

func once(ctx context.Context, httpClient *http.Client, username, password, method, target string, payload []byte, limit int64) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "highload-lab1-dashboard-generator")
	if username != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	received, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return 0, nil, err
	}
	if int64(len(received)) > limit {
		return 0, nil, fmt.Errorf("%s %s: response is larger than %d bytes", method, requestPath(target), limit)
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return 0, nil, fmt.Errorf("%s %s: unexpected redirect %d", method, requestPath(target), resp.StatusCode)
	}
	return resp.StatusCode, received, nil
}

func retryable(err error) bool {
	return !errors.Is(err, context.Canceled)
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func requestPath(target string) string {
	parsed, err := url.Parse(target)
	if err != nil || parsed.Path == "" {
		return target
	}
	return parsed.Path
}

func snippet(body []byte, secret string) string {
	text := redact(secret, strings.Join(strings.Fields(string(body)), " "))
	if len(text) > 180 {
		text = text[:180]
	}
	return text
}

func statusMessage(method, path string, status int, body []byte, secret string) string {
	return (&statusError{Method: method, Path: path, Status: status, Snippet: snippet(body, secret)}).Error()
}

func redactError(secret string, err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	cleaned := redact(secret, text)
	if cleaned == text {
		return err
	}
	return errors.New(cleaned)
}

func endpointURL(base, suffix string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + suffix
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.User = nil
	return parsed.String(), nil
}
