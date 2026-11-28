package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"

	gf "github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

const maxDashboardBytes = 1 << 20

type savedDashboard struct {
	resourceVersion string
	spec            Spec
}

// Apply creates or replaces the dashboard with UID items-service.
// A previously saved non-editable dashboard is updated through the admin API.
func Apply(ctx context.Context, httpClient *http.Client, cfg Config, spec Spec) error {
	if deref(spec.Uid) != dashboardUID {
		return fmt.Errorf("dashboard uid must be %s", dashboardUID)
	}
	if spec.Editable == nil || *spec.Editable {
		return fmt.Errorf("dashboard must be saved with editable false")
	}
	var conflict error
	for attempt := 1; attempt <= cfg.attempts(); attempt++ {
		current, found, err := getDashboard(ctx, httpClient, cfg)
		if err != nil {
			return err
		}
		var saved savedDashboard
		if !found {
			saved, err = createDashboard(ctx, httpClient, cfg, spec)
		} else {
			next := uint32(1)
			if current.spec.Version != nil {
				next = *current.spec.Version + 1
			}
			spec.Version = &next
			saved, err = updateDashboard(ctx, httpClient, cfg, current.resourceVersion, spec)
		}
		if err == nil {
			if err := sameDashboard(spec, saved.spec); err != nil {
				return fmt.Errorf("grafana did not store the dashboard: %w", err)
			}
			confirmed, ok, err := getDashboard(ctx, httpClient, cfg)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("grafana did not store the dashboard")
			}
			if err := sameDashboard(spec, confirmed.spec); err != nil {
				return fmt.Errorf("grafana did not store the dashboard: %w", err)
			}
			return nil
		}
		var status *statusError
		if errors.As(err, &status) && status.Status == http.StatusConflict && attempt < cfg.attempts() {
			conflict = err
			continue
		}
		return err
	}
	if conflict == nil {
		conflict = errors.New("dashboard update conflicted")
	}
	return conflict
}

func getDashboard(ctx context.Context, httpClient *http.Client, cfg Config) (savedDashboard, bool, error) {
	target, err := dashboardItemURL(cfg.GrafanaURL)
	if err != nil {
		return savedDashboard{}, false, err
	}
	status, body, err := do(ctx, httpClient, cfg, cfg.Username, cfg.Password, http.MethodGet, target, nil, maxDashboardBytes)
	if err != nil {
		return savedDashboard{}, false, fmt.Errorf("read dashboard: %w", err)
	}
	if status == http.StatusNotFound {
		return savedDashboard{}, false, nil
	}
	if status != http.StatusOK {
		return savedDashboard{}, false, fmt.Errorf("read dashboard: %s", statusMessage(http.MethodGet, requestPath(target), status, body, cfg.Password))
	}
	saved, err := decodeDashboard(body)
	if err != nil {
		return savedDashboard{}, false, fmt.Errorf("read dashboard: %w", err)
	}
	return saved, true, nil
}

func createDashboard(ctx context.Context, httpClient *http.Client, cfg Config, spec Spec) (savedDashboard, error) {
	return writeDashboard(ctx, httpClient, cfg, http.MethodPost, "create dashboard", "", spec)
}

func updateDashboard(ctx context.Context, httpClient *http.Client, cfg Config, resourceVersion string, spec Spec) (savedDashboard, error) {
	return writeDashboard(ctx, httpClient, cfg, http.MethodPut, "update dashboard", resourceVersion, spec)
}

func writeDashboard(ctx context.Context, httpClient *http.Client, cfg Config, method, action, resourceVersion string, spec Spec) (savedDashboard, error) {
	var (
		target string
		err    error
	)
	if method == http.MethodPost {
		target, err = dashboardCollectionURL(cfg.GrafanaURL)
	} else {
		target, err = dashboardItemURL(cfg.GrafanaURL)
	}
	if err != nil {
		return savedDashboard{}, err
	}
	payload, err := encodeDashboard(spec, resourceVersion)
	if err != nil {
		return savedDashboard{}, err
	}
	status, body, err := do(ctx, httpClient, cfg, cfg.Username, cfg.Password, method, target, payload, maxDashboardBytes)
	if err != nil {
		return savedDashboard{}, fmt.Errorf("%s: %w", action, err)
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return savedDashboard{}, dashboardStatus(action, method, requestPath(target), status, body, cfg.Password)
	}
	saved, err := decodeDashboard(body)
	if err != nil {
		return savedDashboard{}, fmt.Errorf("%s: %w", action, err)
	}
	return saved, nil
}

func encodeDashboard(spec Spec, resourceVersion string) ([]byte, error) {
	document := resource{
		APIVersion: apiVersion,
		Kind:       apiKind,
		Metadata: metadata{
			Name:            dashboardUID,
			ResourceVersion: resourceVersion,
		},
		Spec: spec,
	}
	payload, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode dashboard: %w", err)
	}
	return payload, nil
}

func decodeDashboard(body []byte) (savedDashboard, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var document resource
	if err := decoder.Decode(&document); err != nil {
		return savedDashboard{}, fmt.Errorf("response is not a dashboard resource: %w", err)
	}
	if document.Metadata.Name != "" && document.Metadata.Name != dashboardUID {
		return savedDashboard{}, fmt.Errorf("response dashboard name is %s", document.Metadata.Name)
	}
	return savedDashboard{
		resourceVersion: document.Metadata.ResourceVersion,
		spec:            document.Spec,
	}, nil
}

func sameDashboard(want, got Spec) error {
	if got.Editable == nil || *got.Editable {
		return fmt.Errorf("stored dashboard is editable")
	}
	if uid := deref(got.Uid); uid != "" && uid != dashboardUID {
		return fmt.Errorf("stored dashboard uid is %s", uid)
	}
	if err := samePanels(want.Panels, got.Panels); err != nil {
		return err
	}
	return sameVariables(want.Templating.List, got.Templating.List)
}

func samePanels(want, got []gf.PanelOrRowPanel) error {
	if len(got) != len(want) {
		return fmt.Errorf("stored dashboard has %d panels, want %d", len(got), len(want))
	}
	for i := range want {
		wantPanel := want[i].Panel
		gotPanel := got[i].Panel
		if wantPanel == nil || gotPanel == nil {
			return fmt.Errorf("stored panel %d is missing", i)
		}
		if deref(gotPanel.Title) != deref(wantPanel.Title) {
			return fmt.Errorf("stored panel %d title is %s", i, deref(gotPanel.Title))
		}
		if err := samePanelQueries(wantPanel, gotPanel); err != nil {
			return err
		}
	}
	return nil
}

func samePanelQueries(wantPanel, gotPanel *gf.Panel) error {
	wantExpr := panelExpressions(wantPanel)
	gotExpr := panelExpressions(gotPanel)
	if !slices.Equal(wantExpr, gotExpr) {
		return fmt.Errorf("stored panel %s queries do not match", deref(wantPanel.Title))
	}
	return nil
}

func sameVariables(want, got []gf.VariableModel) error {
	if len(got) != len(want) {
		return fmt.Errorf("stored dashboard has %d variables, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i].Name || queryText(got[i].Query) != queryText(want[i].Query) {
			return fmt.Errorf("stored variable %s does not match", want[i].Name)
		}
		if !boolValue(got[i].IncludeAll) || !boolValue(got[i].Multi) || deref(got[i].AllValue) != allRegexValue {
			return fmt.Errorf("stored variable %s lost All or multi-select", want[i].Name)
		}
		if got[i].Current == nil || optionString(got[i].Current.Value) != allVariableValue {
			return fmt.Errorf("stored variable %s is not initially All", want[i].Name)
		}
		if got[i].Datasource == nil || deref(got[i].Datasource.Uid) != datasourceUID {
			return fmt.Errorf("stored variable %s datasource is %s", want[i].Name, deref(datasourceUIDOf(got[i])))
		}
		if got[i].Refresh == nil || want[i].Refresh == nil || *got[i].Refresh != *want[i].Refresh {
			return fmt.Errorf("stored variable %s refresh is %s", want[i].Name, refreshText(got[i].Refresh))
		}
	}
	return nil
}

func boolValue(value *bool) bool {
	return value != nil && *value
}

func datasourceUIDOf(variable gf.VariableModel) *string {
	if variable.Datasource == nil {
		return nil
	}
	return variable.Datasource.Uid
}

func refreshText(value *gf.VariableRefresh) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%d", *value)
}

func dashboardStatus(action, method, path string, status int, body []byte, secret string) error {
	return fmt.Errorf("%s: %w", action, &statusError{
		Method:  method,
		Path:    path,
		Status:  status,
		Snippet: snippet(body, secret),
	})
}

func dashboardCollectionURL(base string) (string, error) {
	return endpointURL(base, "/apis/dashboard.grafana.app/v1/namespaces/"+namespace+"/dashboards")
}

func dashboardItemURL(base string) (string, error) {
	return endpointURL(base, "/apis/dashboard.grafana.app/v1/namespaces/"+namespace+"/dashboards/"+dashboardUID)
}
