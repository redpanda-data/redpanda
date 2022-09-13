// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package graf

import "encoding/json"

type GraphPanel struct {
	*BasePanel
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
	Interval      string      `json:"interval,omitempty"`
}

func (*GraphPanel) Type() string {
	return "graph"
}

func (p *GraphPanel) GetGridPos() *GridPos {
	return &p.BasePanel.GridPos
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

func NewGraphPanel(title string, unit string) *GraphPanel {
	yAxisMin := 0.0
	return &GraphPanel{
		BasePanel: &BasePanel{
			ID:       nextID(),
			Title:    title,
			Editable: true,
			Span:     4,
			Renderer: "flot",
			GridPos:  GridPos{H: 6, W: 8},
		},
		Legend:    Legend{Show: true},
		Fill:      1,
		LineWidth: 2,
		XAxis: XAxis{
			Show: true,
			Mode: "time",
		},
		YAxes: []YAxis{
			{
				Format:  unit,
				LogBase: 1,
				Show:    true,
				Min:     &yAxisMin,
			},
			{
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
