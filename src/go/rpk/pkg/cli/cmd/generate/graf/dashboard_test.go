package graf_test

import (
	"encoding/json"
	"testing"
	"vectorized/pkg/cli/cmd/generate/graf"

	"github.com/stretchr/testify/require"
)

func defaultDashboard() graf.Dashboard {
	datasource := "prometheus"
	graph := graf.NewGraphPanel("graph panel 1", "ops")
	graph.Datasource = datasource
	graph.Targets = []graf.Target{graf.Target{
		Expr:           `sum(%s{instance=~"[[node]]",shard=~"[[node_shard]]"}) by ([[aggr_criteria]])`,
		LegendFormat:   `instance: {{instance}}`,
		Format:         "time_series",
		Step:           10,
		IntervalFactor: 2,
	}}
	graph.Tooltip = graf.Tooltip{
		MsResolution: true,
		Shared:       true,
		ValueType:    "cumulative",
	}
	singlestat := graf.NewGraphPanel("single stat 1", "bytes")
	singlestat.Datasource = datasource
	singlestat.NullPointMode = "connected"
	singlestat.Targets = []graf.Target{graf.Target{
		Expr: `count(up{job=~"node"})`,
	}}
	rows := []graf.Row{graf.Row{
		Title:     "row 1",
		ShowTitle: true,
		Panels:    []graf.Panel{graph, singlestat},
		Editable:  true,
		Height:    "250px",
		Collapse:  true,
	}}
	timePicker := graf.TimePicker{
		RefreshIntervals: []string{"10s", "1m", "5m", "15m", "1h"},
		TimeOptions:      []string{"5m", "15m", "1h", "1d", "7d"},
	}
	return graf.Dashboard{
		Title: "Redpanda",
		Templating: graf.Templating{
			List: []graf.TemplateVar{graf.TemplateVar{
				Name:       "var 1",
				Datasource: datasource,
				Label:      "label 1",
				Multi:      true,
				Refresh:    1,
				Sort:       1,
				Options: []graf.Option{
					graf.Option{
						Text:  "Cluster",
						Value: "",
					},
					graf.Option{
						Text:  "Instance",
						Value: "instance,",
					},
				},
			}},
		},
		Rows:          rows,
		Editable:      true,
		Refresh:       "10s",
		Time:          graf.Time{From: "now-1h", To: "now"},
		TimePicker:    timePicker,
		Timezone:      "utc",
		SchemaVersion: 12,
	}
}

func TestDashboardMarshalJSON(t *testing.T) {

	tests := []struct {
		name      string
		dashboard func() graf.Dashboard
		expected  string
	}{
		{
			name:      "should marshal the dashboard correctly",
			dashboard: defaultDashboard,
			expected:  `{"title":"Redpanda","templating":{"list":[{"name":"var 1","datasource":"prometheus","label":"label 1","type":"","refresh":1,"options":[{"text":"Cluster","value":"","selected":false},{"text":"Instance","value":"instance,","selected":false}],"includeAll":false,"allFormat":"","allValue":"","multi":true,"multiFormat":"","query":"","current":{"text":"","value":null},"hide":0,"sort":1}]},"rows":[{"title":"row 1","showTitle":true,"collapse":true,"editable":true,"height":"250px","panels":[{"type":"graph","id":5577006791947779410,"title":"graph panel 1","datasource":"prometheus","editable":true,"gridPos":{"h":0,"w":0,"x":0,"y":0},"links":null,"renderer":"flot","span":4,"targets":[{"refId":"","expr":"sum(%s{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}) by ([[aggr_criteria]])","intervalFactor":2,"interval":"","step":10,"legendFormat":"instance: {{instance}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"ops"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null","thresholds":null,"lines":false,"bars":false,"tooltip":{"shared":true,"value_type":"cumulative","msResolution":true},"aliasColors":{}},{"type":"graph","id":8674665223082153551,"title":"single stat 1","datasource":"prometheus","editable":true,"gridPos":{"h":0,"w":0,"x":0,"y":0},"links":null,"renderer":"flot","span":4,"targets":[{"refId":"","expr":"count(up{job=~\"node\"})","interval":"","legendFormat":""}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"bytes"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"connected","thresholds":null,"lines":false,"bars":false,"tooltip":{"shared":false,"value_type":"individual"},"aliasColors":{}}]}],"panels":null,"editable":true,"timezone":"utc","refresh":"10s","time":{"from":"now-1h","to":"now"},"timepicker":{"refresh_intervals":["10s","1m","5m","15m","1h"],"time_options":["5m","15m","1h","1d","7d"]},"annotations":{"list":null},"links":null,"schemaVersion":12}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			d := tt.dashboard()
			jsonDashboard, err := json.Marshal(d)
			require.NoError(st, err)
			require.Equal(st, tt.expected, string(jsonDashboard))
		})
	}
}
