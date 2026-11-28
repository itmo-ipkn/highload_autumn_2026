package dashboard

import gf "github.com/grafana/grafana-foundation-sdk/go/dashboard"

const (
	dashboardUID   = "items-service"
	dashboardTitle = "Items service"
	datasourceUID  = "prometheus"
	namespace      = "default"
	// schemaVersion is the dashboard model Grafana 13.3 persists through
	// dashboard.grafana.app/v1. The pinned Foundation SDK writes the same value.
	schemaVersion = 42

	apiVersion = "dashboard.grafana.app/v1"
	apiKind    = "Dashboard"

	allVariableValue = "$__all"
	allRegexValue    = ".*"
)

// Spec is the dashboard model stored in the Grafana resource spec.
// Panel and variable shapes come from the Foundation SDK.
type Spec = gf.Dashboard

type resource struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}

type metadata struct {
	Name            string `json:"name"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}
