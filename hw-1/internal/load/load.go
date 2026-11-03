package load

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const body = `{"name":"нагрузка","value":"учебная запись"}`

type Target struct {
	URL string
	ID  int64
}

// Темп общий для всех экземпляров.
func Rate(elapsed, ramp time.Duration, target float64) float64 {
	if elapsed >= ramp {
		return target
	}
	return 1 + (target-1)*float64(elapsed)/float64(ramp)
}

func (t *Target) Request(ctx context.Context, client *http.Client, step int) error {
	method, path, payload, expected := t.scenario(step)
	request, err := http.NewRequestWithContext(ctx, method, t.URL+path, strings.NewReader(payload))
	if err != nil {
		return err
	}
	if payload != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		return fmt.Errorf("%s %s: код %d, ожидался %d", method, path, response.StatusCode, expected)
	}
	if expected == http.StatusCreated {
		var item struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(response.Body).Decode(&item); err != nil {
			return err
		}
		if item.ID <= 0 {
			return fmt.Errorf("POST вернул неверный ID")
		}
		t.ID = item.ID
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}

func (t *Target) scenario(step int) (string, string, string, int) {
	switch step % 100 {
	case 24, 74:
		return http.MethodPost, "/items", `{}`, http.StatusBadRequest
	case 49, 99:
		return http.MethodGet, "/items/9223372036854775807", "", http.StatusNotFound
	}
	if t.ID == 0 || step%3 == 0 {
		return http.MethodPost, "/items", body, http.StatusCreated
	}
	path := "/items/" + strconv.FormatInt(t.ID, 10)
	if step%3 == 1 {
		return http.MethodGet, path, "", http.StatusOK
	}
	return http.MethodPut, path, body, http.StatusOK
}

// Медленный ответ снижает темп; пропущенные запросы не догоняем всплеском.
func Run(ctx context.Context, client *http.Client, targets []Target, ramp time.Duration, rps float64, report func(error)) {
	started := time.Now()
	for n := 0; ; n++ {
		interval := time.Duration(float64(time.Second) / Rate(time.Since(started), ramp, rps))
		tick := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			tick.Stop()
			return
		case <-tick.C:
		}
		target := &targets[n%len(targets)]
		if err := target.Request(ctx, client, n/len(targets)); err != nil && ctx.Err() == nil {
			report(err)
		}
	}
}
