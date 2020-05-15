package graf

import "encoding/json"

type GraphPanel struct {
	BasePanel
	Targets       []Target    `json:"targets"`
	XAxis         XAxis       `json:"xaxis"`
	YAxes         []YAxis     `json:"yaxes"`
	Legend        Legend      `json:"legend"`
	Fill          int         `json:"fill"`
	LineWidth     uint        `json:"linewidth"`
	NullPointMode string      `json:"nullPointMode"`
	Thresholds    []Threshold `json:"thresholds"`
	Lines         bool        `json:"lines"`
	Bars          bool        `json:"bars"`
	Tooltip       Tooltip     `json:"tooltip"`
	AliasColors   AliasColors `json:"aliasColors"`
	SteppedLine   bool        `json:"steppedLine"`
}

func (GraphPanel) Type() string {
	return "graph"
}

func (p GraphPanel) MarshalJSON() ([]byte, error) {
	type PanelAlias GraphPanel
	typedPanel := struct {
		Type string `json:"type"`
		PanelAlias
	}{
		p.Type(),
		(PanelAlias)(p),
	}
	return json.Marshal(typedPanel)
}

func NewGraphPanel(title string, unit string) GraphPanel {
	yAxisMin := 0.0
	return GraphPanel{
		BasePanel: BasePanel{
			ID:       nextID(),
			Title:    title,
			Editable: true,
			Span:     4,
			Renderer: "flot",
		},
		Legend:    Legend{Show: true},
		Fill:      1,
		LineWidth: 2,
		XAxis: XAxis{
			Show: true,
			Mode: "time",
		},
		YAxes: []YAxis{
			YAxis{
				Format:  unit,
				LogBase: 1,
				Show:    true,
				Min:     &yAxisMin,
			},
			YAxis{
				Format:  "short",
				LogBase: 1,
				Show:    true,
				Min:     &yAxisMin,
			},
		},
		Tooltip:       Tooltip{ValueType: "individual"},
		NullPointMode: "null",
	}
}
