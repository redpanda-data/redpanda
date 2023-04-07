// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package graf_test

import (
	"encoding/json"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/generate/graf"
	"github.com/stretchr/testify/require"
)

func TestNewGraphPanel(t *testing.T) {
	yAxisMin := 0.0
	title := "graph panel"
	unit := "ops"
	id := uint(0)
	expected := &graf.GraphPanel{
		BasePanel: &graf.BasePanel{
			ID:       id,
			Title:    title,
			Editable: true,
			Span:     4,
			Renderer: "flot",
			GridPos:  graf.GridPos{H: 6, W: 8},
		},
		Legend:    graf.Legend{Show: true},
		Fill:      1,
		LineWidth: 2,
		XAxis:     graf.XAxis{Show: true, Mode: "time"},
		YAxes: []graf.YAxis{
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
		Tooltip:       graf.Tooltip{ValueType: "individual"},
		NullPointMode: "null",
	}
	actual := graf.NewGraphPanel(title, unit)
	actual.ID = id
	require.Exactly(t, expected, actual)
}

func TestGraphPanelType(t *testing.T) {
	panel := &graf.GraphPanel{}
	require.Equal(t, "graph", panel.Type())
}

func TestGraphPanelMarshalType(t *testing.T) {
	graphJSON, err := json.Marshal(graf.GraphPanel{})
	require.NoError(t, err)
	require.Contains(t, string(graphJSON), `"type":"graph"`)
}
