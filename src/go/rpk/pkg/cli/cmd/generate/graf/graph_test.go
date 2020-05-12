package graf_test

import (
	"encoding/json"
	"testing"
	"vectorized/pkg/cli/cmd/generate/graf"

	"github.com/stretchr/testify/require"
)

func TestNewGraphPanel(t *testing.T) {
	yAxisMin := 0.0
	title := "graph panel"
	unit := "ops"
	id := uint(0)
	expected := graf.GraphPanel{
		BasePanel: graf.BasePanel{
			ID:       id,
			Title:    title,
			Editable: true,
			Span:     4,
			Renderer: "flot",
		},
		Legend:    graf.Legend{Show: true},
		Fill:      1,
		LineWidth: 2,
		XAxis:     graf.XAxis{Show: true, Mode: "time"},
		YAxes: []graf.YAxis{
			graf.YAxis{
				Format:  unit,
				LogBase: 1,
				Show:    true,
				Min:     &yAxisMin,
			},
			graf.YAxis{
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
	require.Equal(t, "graph", graf.GraphPanel{}.Type())
}

func TestGraphPanelMarshalType(t *testing.T) {
	graphJSON, err := json.Marshal(graf.GraphPanel{})
	require.NoError(t, err)
	require.Contains(t, string(graphJSON), `"type":"graph"`)
}
