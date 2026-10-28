package loadtester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
	mu      sync.Mutex
	itemIDs []string
	client  = &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}
)

func SendRequest(targets []string) {
	baseURL := targets[rand.Intn(len(targets))]
	r := rand.Float64()

	if r < 0.38 {
		// 1. POST /items
		body, _ := json.Marshal(map[string]string{"name": fmt.Sprintf("item-%d", rand.Intn(10000))})
		resp, err := client.Post(baseURL+"/items", "application/json", bytes.NewBuffer(body))
		if err != nil {
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			var item struct {
				ID int `json:"id"`
			}
			if json.NewDecoder(resp.Body).Decode(&item) == nil {
				mu.Lock()
				itemIDs = append(itemIDs, strconv.Itoa(item.ID))
				if len(itemIDs) > 100 {
					itemIDs = itemIDs[50:]
				}
				mu.Unlock()
			}
		}
	} else if r < 0.70 {
		// 2. GET /items/{id}
		mu.Lock()
		var targetID string
		if len(itemIDs) > 0 {
			targetID = itemIDs[rand.Intn(len(itemIDs))]
		}
		mu.Unlock()

		if targetID != "" {
			resp, err := client.Get(baseURL + "/items/" + targetID)
			if err == nil {
				resp.Body.Close()
			}
		} else {
			resp, err := client.Get(baseURL + "/health")
			if err == nil {
				resp.Body.Close()
			}
		}
	} else if r < 0.88 {
		// 3. PUT /items/{id}
		mu.Lock()
		var targetID string
		if len(itemIDs) > 0 {
			targetID = itemIDs[rand.Intn(len(itemIDs))]
		}
		mu.Unlock()

		if targetID != "" {
			body, _ := json.Marshal(map[string]string{"name": "updated-name"})
			req, _ := http.NewRequest(http.MethodPut, baseURL+"/items/"+targetID, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			if rand.Float64() < 0.05 {
				req.Header.Set("X-Simulate-Error", "500")
			}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}
	} else if r < 0.94 {
		// 4. 404
		resp, err := client.Get(fmt.Sprintf("%s/items/%d", baseURL, 1_000_000+rand.Intn(1_000_000)))
		if err == nil {
			resp.Body.Close()
		}
	} else if r < 0.97 {
		// 5. 400 / 500 (POST)
		simulate500 := rand.Float64() < 0.3
		body := `{invalid-json`
		if simulate500 {
			body = `{"name":"error-trigger"}`
		}
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/items", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		if simulate500 {
			req.Header.Set("X-Simulate-Error", "500")
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	} else {
		// 6. 500 (GET)
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/items/%d", baseURL, 1+rand.Intn(100)), nil)
		req.Header.Set("X-Simulate-Error", "500")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}
}
