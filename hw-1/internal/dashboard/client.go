package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	HTTP *http.Client
}

// FetchHandlers читает GET /handlers приложения.
func (c *Client) FetchHandlers(ctx context.Context, appURL string) ([]Handler, error) {
	var body struct {
		Handlers []Handler `json:"handlers"`
	}
	if err := c.do(ctx, http.MethodGet, appURL+"/handlers", nil, "", "", &body); err != nil {
		return nil, err
	}
	return body.Handlers, nil
}

// Grafana - доступ к API Grafana с учёткой администратора.
type Grafana struct {
	Client
	URL, User, Password string
}

func (g *Grafana) Health(ctx context.Context) error {
	return g.do(ctx, http.MethodGet, g.URL+"/api/health", nil, "", "", nil)
}

// Upsert создаёт или перезаписывает дашборд с тем же uid.
func (g *Grafana) Upsert(ctx context.Context, d Dashboard) error {
	payload := map[string]any{"dashboard": d, "overwrite": true, "message": "generated"}
	return g.do(ctx, http.MethodPost, g.URL+"/api/dashboards/db", payload, g.User, g.Password, nil)
}

// EnsureViewer заводит пользователя с ролью Viewer или обновляет ему пароль.
func (g *Grafana) EnsureViewer(ctx context.Context, login, password string) error {
	var user struct {
		ID int64 `json:"id"`
	}
	err := g.do(ctx, http.MethodGet, g.URL+"/api/users/lookup?loginOrEmail="+url.QueryEscape(login), nil, g.User, g.Password, &user)
	var se *StatusError
	switch {
	case errors.As(err, &se) && se.Code == http.StatusNotFound:
		body := map[string]any{"name": login, "login": login, "password": password, "OrgId": 1}
		if err := g.do(ctx, http.MethodPost, g.URL+"/api/admin/users", body, g.User, g.Password, &user); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		body := map[string]string{"password": password}
		if err := g.do(ctx, http.MethodPut, fmt.Sprintf("%s/api/admin/users/%d/password", g.URL, user.ID), body, g.User, g.Password, nil); err != nil {
			return err
		}
	}
	role := map[string]string{"role": "Viewer"}
	return g.do(ctx, http.MethodPatch, fmt.Sprintf("%s/api/org/users/%d", g.URL, user.ID), role, g.User, g.Password, nil)
}

// SetHome делает дашборд домашним для организации.
func (g *Grafana) SetHome(ctx context.Context, uid string) error {
	body := map[string]string{"homeDashboardUID": uid}
	return g.do(ctx, http.MethodPut, g.URL+"/api/org/preferences", body, g.User, g.Password, nil)
}

type StatusError struct {
	Code int
	Msg  string
}

func (e *StatusError) Error() string { return e.Msg }

func (c *Client) do(ctx context.Context, method, target string, in any, user, pass string, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &StatusError{Code: resp.StatusCode, Msg: fmt.Sprintf("%s %s: %s: %s", method, target, resp.Status, bytes.TrimSpace(msg))}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Retry повторяет f, пока не выйдет timeout.
func Retry(ctx context.Context, timeout, interval time.Duration, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := f(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-time.After(interval):
		}
	}
}
