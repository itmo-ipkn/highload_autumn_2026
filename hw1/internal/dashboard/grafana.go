package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

func Upload(grafanaURL, user, password string, payload map[string]interface{}) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Failed to marshal dashboard: %w", err)
	}

	url := grafanaURL + "/api/dashboards/db"
	client := &http.Client{Timeout: 10 * time.Second}

	var lastStatus int
	var lastBody []byte
	var lastErr error

	var resp *http.Response
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth(user, password)

		resp, err = client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("Grafana request error (attempt %d/10): %w", i+1, err)
			time.Sleep(3 * time.Second)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.Printf("Dashboard uploaded successfully (attempt %d)", i+1)
			return
		}

		lastStatus = resp.StatusCode
		lastBody = body
		log.Printf("Grafana response (attempt %d/10): status=%d, body=%s", i+1, resp.StatusCode, body)
		time.Sleep(3 * time.Second)
	}

	log.Fatalf("Failed to upload dashboard to Grafana: last_status=%d, last_err=%w, body=%s", lastStatus, lastErr, lastBody)
}
