package graf

import (
	"encoding/json"
)

const panelHeight = "8"

var id uint = 0

func NewRow(title string, panels []Panel, collapse bool) Row {
	return Row{
		Title:     title,
		ShowTitle: true,
		Panels:    panels,
		Editable:  true,
		Height:    panelHeight,
		Collapse:  collapse,
	}
}

func nextID() uint {
	id++
	return id
}

type Dashboard struct {
	UID           string      `json:"uid,omitempty"`
	Title         string      `json:"title,omitempty"`
	Templating    Templating  `json:"templating"`
	Rows          []Row       `json:"rows"`
	Panels        []Panel     `json:"panels"`
	Editable      bool        `json:"editable"`
	Timezone      string      `json:"timezone"`
	Refresh       string      `json:"refresh"`
	Time          Time        `json:"time"`
	TimePicker    TimePicker  `json:"timepicker"`
	Annotations   Annotations `json:"annotations"`
	Links         []Link      `json:"links"`
	SchemaVersion uint        `json:"schemaVersion"`
}

type Row struct {
	Title     string  `json:"title"`
	ShowTitle bool    `json:"showTitle"`
	Collapse  bool    `json:"collapse"`
	Editable  bool    `json:"editable"`
	Height    string  `json:"height"`
	Panels    []Panel `json:"panels"`
}

type Panel interface {
	json.Marshaler
	Type() string
	GetGridPos() *GridPos
}
type Templating struct {
	List []TemplateVar `json:"list"`
}

type TemplateVar struct {
	Name        string   `json:"name"`
	Datasource  string   `json:"datasource"`
	Label       string   `json:"label"`
	Type        string   `json:"type"`
	Auto        bool     `json:"auto,omitempty"`
	Refresh     int      `json:"refresh"`
	Options     []Option `json:"options"`
	IncludeAll  bool     `json:"includeAll"`
	AllFormat   string   `json:"allFormat"`
	AllValue    string   `json:"allValue"`
	Multi       bool     `json:"multi"`
	MultiFormat string   `json:"multiFormat"`
	Query       string   `json:"query"`
	Current     Current  `json:"current"`
	Hide        uint8    `json:"hide"`
	Sort        int      `json:"sort"`
}

type Option struct {
	Text     string `json:"text"`
	Value    string `json:"value"`
	Selected bool   `json:"selected"`
}

type Current struct {
	Tags  []string    `json:"tags,omitempty"`
	Text  string      `json:"text"`
	Value interface{} `json:"value"`
}

type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type BasePanel struct {
	ID          uint    `json:"id"`
	Title       string  `json:"title"`
	Datasource  string  `json:"datasource,omitempty"`
	Editable    bool    `json:"editable"`
	GridPos     GridPos `json:"gridPos"`
	Transparent bool    `json:"transparent"`
	Height      string  `json:"height,omitempty"`
	Links       []Link  `json:"links"`
	Renderer    string  `json:"renderer,omitempty"`
	Span        float32 `json:"span"`
	Error       bool    `json:"error"`
}

func (p *BasePanel) GetGridPos() *GridPos {
	gridPos := p.GridPos
	return &gridPos
}

type Target struct {
	RefID          string `json:"refId"`
	Expr           string `json:"expr,omitempty"`
	IntervalFactor int    `json:"intervalFactor,omitempty"`
	Step           int    `json:"step,omitempty"`
	LegendFormat   string `json:"legendFormat"`
	Instant        bool   `json:"instant,omitempty"`
	Format         string `json:"format,omitempty"`
}

type Legend struct {
	Show    bool `json:"show"`
	Max     bool `json:"max"`
	Min     bool `json:"min"`
	Values  bool `json:"values"`
	Avg     bool `json:"avg"`
	Current bool `json:"current"`
	Total   bool `json:"total"`
}

type Tooltip struct {
	Shared       bool   `json:"shared"`
	ValueType    string `json:"value_type"`
	MsResolution bool   `json:"msResolution,omitempty"`
	Sort         int    `json:"sort,omitempty"`
}

type XAxis struct {
	Format  string `json:"format"`
	LogBase int    `json:"logBase"`
	Show    bool   `json:"show"`
	Mode    string `json:"mode"`
}

type YAxis struct {
	Label   *string  `json:"label"`
	Show    bool     `json:"show"`
	LogBase int      `json:"logBase"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Format  string   `json:"format"`
}

type Time struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type TimePicker struct {
	RefreshIntervals []string `json:"refresh_intervals"`
	TimeOptions      []string `json:"time_options"`
}

type ValueMap struct {
	Value string `json:"value"`
	Op    string `json:"op"`
	Text  string `json:"text"`
}

type MappingType struct {
	Name  string `json:"name"`
	Value uint   `json:"value"`
}

type RangeMap struct {
	From string `json:"from"`
	To   string `json:"to"`
	Text string `json:"text"`
}

type SparkLine struct {
	Show      bool     `json:"show"`
	Full      bool     `json:"full"`
	YMin      *float64 `json:"ymin"`
	YMax      *float64 `json:"ymax"`
	LineColor string   `json:"lineColor"`
	FillColor string   `json:"fillColor"`
}

type Gauge struct {
	Show             bool `json:"show"`
	MinValue         int  `json:"minValue"`
	MaxValue         int  `json:"maxValue"`
	ThresholdMarkers bool `json:"thresholdMarkers"`
	ThresholdLabels  bool `json:"thresholdLabels"`
}

type Link struct {
}

type Threshold struct {
}

type AliasColors struct {
}

type Annotations struct {
	List []Annotation `json:"list"`
}

type Annotation struct {
}
