package dto

type Dashboard struct {
	UID           string     `json:"uid"`
	Title         string     `json:"title"`
	Tags          []string   `json:"tags"`
	Timezone      string     `json:"timezone"`
	SchemaVersion int        `json:"schemaVersion"`
	Version       int        `json:"version"`
	Refresh       string     `json:"refresh"`
	Time          Time       `json:"time"`
	Editable      bool       `json:"editable"`
	Panels        []Panel    `json:"panels"`
	Templating    Templating `json:"templating"`
}

type Time struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Panel struct {
	ID          int          `json:"id"`
	Type        string       `json:"type"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Datasource  Datasource   `json:"datasource"`
	GridPos     GridPosition `json:"gridPos"`
	Targets     []Target     `json:"targets"`
	FieldConfig FieldConfig  `json:"fieldConfig"`
	Options     Option       `json:"options"`
}

type Templating struct {
	List []Variable `json:"list"`
}

type Variable struct {
	Name       string          `json:"name"`
	Label      string          `json:"label"`
	Type       string          `json:"type"`
	Datasource Datasource      `json:"datasource"`
	Query      string          `json:"query"`
	Multi      bool            `json:"multi"`
	IncludeAll bool            `json:"includeAll"`
	AllValue   string          `json:"allValue"`
	Refresh    int             `json:"refresh"`
	Sort       int             `json:"sort"`
	Current    VariableCurrent `json:"current"`
}

type VariableCurrent struct {
	Selected bool   `json:"selected"`
	Text     string `json:"text"`
	Value    string `json:"value"`
}

type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type GridPosition struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

type Target struct {
	RefID        string     `json:"refId"`
	Expr         string     `json:"expr"`
	LegendFormat string     `json:"legendFormat"`
	Datasource   Datasource `json:"datasource"`
}

type FieldConfig struct {
	Defaults  DefaultFieldConfig `json:"defaults"`
	Overrides []Override         `json:"overrides"`
}

type DefaultFieldConfig struct {
	Unit   string      `json:"unit"`
	Min    int         `json:"min"`
	Max    *int        `json:"max,omitempty"`
	Custom CustomField `json:"custom"`
}

type CustomField struct {
	DrawStyle   string `json:"drawStyle"`
	LineWidth   int    `json:"lineWidth"`
	FillOpacity int    `json:"fillOpacity"`
	SpanNulls   bool   `json:"spanNulls"`
}

type Override struct {
	Matcher    Matcher    `json:"matcher"`
	Properties []Property `json:"properties"`
}

type Matcher struct {
	ID      string `json:"id"`
	Options string `json:"options"`
}

type Property struct {
	ID    string `json:"id"`
	Value any    `json:"value"`
}

type Option struct {
	Legend  Legend  `json:"legend"`
	Tooltip Tooltip `json:"tooltip"`
}

type Legend struct {
	DisplayMode string `json:"displayMode"`
	Placement   string `json:"placement"`
}

type Tooltip struct {
	Mode string `json:"mode"`
}
