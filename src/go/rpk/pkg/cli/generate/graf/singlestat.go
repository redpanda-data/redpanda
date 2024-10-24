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

type SingleStatPanel struct {
	*BasePanel
	Targets         []Target      `json:"targets"`
	Format          string        `json:"format"`
	Prefix          string        `json:"prefix"`
	Postfix         string        `json:"postfix"`
	MaxDataPoints   uint          `json:"maxDataPoints"`
	ValueMaps       []ValueMap    `json:"valueMaps"`
	MappingTypes    []MappingType `json:"mappingTypes"`
	RangeMaps       []RangeMap    `json:"rangeMaps"`
	MappingType     uint          `json:"mappingType"`
	NullPointMode   string        `json:"nullPointMode"`
	ValueName       string        `json:"valueName"`
	ValueFontSize   string        `json:"valueFontSize"`
	PrefixFontSize  string        `json:"prefixFontSize"`
	PostfixFontSize string        `json:"postfixFontSize"`
	ColorBackground bool          `json:"colorBackground"`
	ColorValue      bool          `json:"colorValue"`
	Colors          []string      `json:"colors"`
	Thresholds      string        `json:"thresholds"`
	SparkLine       SparkLine     `json:"sparkline"`
	Gauge           Gauge         `json:"gauge"`
	Links           []Link        `json:"links"`
	Interval        *string       `json:"interval"`
	From            *string       `json:"timeFrom"`
	TimeShift       *string       `json:"timeShift"`
	NullText        *string       `json:"nullText"`
	CacheTimeout    *string       `json:"cacheTimeout"`
	TableColumn     string        `json:"tableColumn"`
}

func (*SingleStatPanel) Type() string {
	return "singlestat"
}

func (p *SingleStatPanel) GetGridPos() *GridPos {
	return &p.BasePanel.GridPos
}

func (p *SingleStatPanel) MarshalJSON() ([]byte, error) {
	type PanelAlias SingleStatPanel
	typedPanel := struct {
		Type string `json:"type"`
		PanelAlias
	}{
		p.Type(),
		(PanelAlias)(*p),
	}
	return json.Marshal(typedPanel)
}

func NewSingleStatPanel(title string) *SingleStatPanel {
	return &SingleStatPanel{
		BasePanel: &BasePanel{
			ID:       nextID(),
			Title:    title,
			Editable: true,
			GridPos:  GridPos{W: 8, H: 8},
			Span:     1,
		},
		Format:        "none",
		MaxDataPoints: 100,
		ValueMaps: []ValueMap{
			{Value: "null", Op: "=", Text: "N/A"},
		},
		MappingTypes: []MappingType{
			{Name: "value to text", Value: 1},
			{Name: "range to text", Value: 2},
		},
		RangeMaps: []RangeMap{
			{From: "null", To: "null", Text: "N/A"},
		},
		MappingType:     1,
		NullPointMode:   "connected",
		ValueName:       "current",
		ValueFontSize:   "200%",
		PrefixFontSize:  "50%",
		PostfixFontSize: "50%",
		ColorValue:      true,
		Colors: []string{
			"#299c46",
			"rgba(237, 129, 40, 0.89)",
			"#d44a3a",
		},
		SparkLine: SparkLine{
			Show:      false,
			Full:      false,
			LineColor: "rgb(31, 120, 193)",
			FillColor: "rgba(31, 118, 189, 0.18)",
		},
		Gauge: Gauge{MinValue: 0, MaxValue: 100, ThresholdMarkers: true},
		Links: []Link{},
	}
}
