// Package dashboard - модель дашборда Grafana в объёме, нужном генератору.
package dashboard

type Dashboard struct {
	UID           string     `json:"uid"`
	Title         string     `json:"title"`
	Tags          []string   `json:"tags"`
	Editable      bool       `json:"editable"`
	Refresh       string     `json:"refresh"`
	SchemaVersion int        `json:"schemaVersion"`
	Time          TimeRange  `json:"time"`
	Templating    Templating `json:"templating"`
	Panels        []Panel    `json:"panels"`
}

type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Templating struct {
	List []Variable `json:"list"`
}

type Variable struct {
	Name       string     `json:"name"`
	Label      string     `json:"label"`
	Type       string     `json:"type"`
	Datasource Datasource `json:"datasource"`
	Query      string     `json:"query"`
	Definition string     `json:"definition"`
	Refresh    int        `json:"refresh"`
	Multi      bool       `json:"multi"`
	IncludeAll bool       `json:"includeAll"`
	AllValue   string     `json:"allValue"`
	Current    Current    `json:"current"`
	Sort       int        `json:"sort"`
}

type Current struct {
	Text  []string `json:"text"`
	Value []string `json:"value"`
}

type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type Panel struct {
	ID          int           `json:"id"`
	Type        string        `json:"type"`
	Title       string        `json:"title"`
	GridPos     GridPos       `json:"gridPos"`
	Datasource  *Datasource   `json:"datasource,omitempty"`
	Targets     []Target      `json:"targets,omitempty"`
	FieldConfig *FieldConfig  `json:"fieldConfig,omitempty"`
	Options     *PanelOptions `json:"options,omitempty"`
	Collapsed   *bool         `json:"collapsed,omitempty"`
	Panels      []Panel       `json:"panels,omitempty"`
}

type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type Target struct {
	RefID        string     `json:"refId"`
	Datasource   Datasource `json:"datasource"`
	Expr         string     `json:"expr"`
	LegendFormat string     `json:"legendFormat"`
}

type FieldConfig struct {
	Defaults  FieldDefaults `json:"defaults"`
	Overrides []any         `json:"overrides"`
}

type FieldDefaults struct {
	Unit string `json:"unit"`
	Min  *int   `json:"min,omitempty"`
}

type PanelOptions struct {
	Tooltip Tooltip `json:"tooltip"`
	Legend  Legend  `json:"legend"`
}

type Tooltip struct {
	Mode string `json:"mode"`
	Sort string `json:"sort"`
}

type Legend struct {
	DisplayMode string `json:"displayMode"`
	Placement   string `json:"placement"`
	ShowLegend  bool   `json:"showLegend"`
}
