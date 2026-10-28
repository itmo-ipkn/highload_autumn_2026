package dashboard

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

func FetchHandlers(appURL string) []HandlerInfo {
	client := &http.Client{Timeout: 10 * time.Second}
	var resp *http.Response
	var err error

	for i := 0; i < 10; i++ {
		resp, err = client.Get(appURL + "/handlers")
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Printf("Waiting for app handlers endpoint... attempt %d/10", i+1)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("Failed to fetch handlers: %w", err)
	}
	if resp == nil {
		log.Fatalf("Failed to fetch handlers: nil response")
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Failed to fetch handlers: status=%d, body=%s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %w", err)
	}

	var handlers []HandlerInfo
	if err := json.Unmarshal(body, &handlers); err != nil {
		log.Fatalf("Failed to parse handlers: %w, body=%s", err, string(body))
	}
	return handlers
}
