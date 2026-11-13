package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

type Client struct {
	HTTP     *http.Client
	Username string
	Password string
}

func (c Client) JSON(ctx context.Context, method, address string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, address, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if c.Username != "" {
		request.SetBasicAuth(c.Username, c.Password)
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d", request.URL.Path, response.StatusCode)
	}
	if output == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(output)
}

func Discover(ctx context.Context, httpClient *http.Client, addresses []string) ([]Endpoint, error) {
	client := Client{HTTP: httpClient}
	unique := make(map[Endpoint]bool)
	for _, address := range addresses {
		var catalog struct {
			Handlers []Endpoint `json:"handlers"`
		}
		address = strings.TrimRight(strings.TrimSpace(address), "/")
		if err := client.JSON(ctx, "GET", address+"/handlers", nil, &catalog); err != nil {
			return nil, fmt.Errorf("получение ручек: %w", err)
		}
		for _, endpoint := range catalog.Handlers {
			if endpoint.Method == "" || !strings.HasPrefix(endpoint.Path, "/") {
				return nil, fmt.Errorf("некорректный метод или путь в /handlers")
			}
			unique[endpoint] = true
		}
	}
	if len(unique) == 0 {
		return nil, fmt.Errorf("приложения не вернули бизнес-ручки")
	}
	endpoints := make([]Endpoint, 0, len(unique))
	for endpoint := range unique {
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Path != endpoints[j].Path {
			return endpoints[i].Path < endpoints[j].Path
		}
		return endpoints[i].Method < endpoints[j].Method
	})
	return endpoints, nil
}

func (c Client) Publish(ctx context.Context, address string, document Object) error {
	address = strings.TrimRight(address, "/")
	if err := c.JSON(ctx, "GET", address+"/api/datasources/uid/"+DatasourceUID, nil, nil); err != nil {
		return fmt.Errorf("проверка источника данных: %w", err)
	}
	payload := Object{"dashboard": document, "overwrite": true}
	var result struct {
		UID    string `json:"uid"`
		Status string `json:"status"`
	}
	if err := c.JSON(ctx, "POST", address+"/api/dashboards/db", payload, &result); err != nil {
		return err
	}
	if result.UID != UID || result.Status != "success" {
		return fmt.Errorf("Grafana не подтвердила сохранение дашборда")
	}
	return nil
}
