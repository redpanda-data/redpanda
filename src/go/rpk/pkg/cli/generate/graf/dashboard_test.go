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

func defaultDashboard() graf.Dashboard {
	datasource := "prometheus"
	graph := graf.NewGraphPanel("graph panel 1", "ops")
	graph.ID = 1
	graph.Datasource = datasource
	graph.Targets = []graf.Target{{
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
	singlestat.ID = 2
	singlestat.Datasource = datasource
	singlestat.NullPointMode = "connected"
	singlestat.Targets = []graf.Target{{
		Expr: `count(up{job=~"node"})`,
	}}
	rows := []graf.Panel{graf.NewRowPanel("row 1", graph, singlestat)}
	timePicker := graf.TimePicker{
		RefreshIntervals: []string{"10s", "1m", "5m", "15m", "1h"},
		TimeOptions:      []string{"5m", "15m", "1h", "1d", "7d"},
	}
	return graf.Dashboard{
		Title: "Redpanda",
		Templating: graf.Templating{
			List: []graf.TemplateVar{{
				Name:       "var 1",
				Datasource: datasource,
				Label:      "label 1",
				Multi:      true,
				Refresh:    1,
				Sort:       1,
				Options: []graf.Option{
					{
						Text:  "Cluster",
						Value: "",
					},
					{
						Text:  "Instance",
						Value: "instance,",
					},
				},
			}},
		},
		Panels:        rows,
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
			expected:  `{"title":"Redpanda","templating":{"list":[{"name":"var 1","datasource":"prometheus","label":"label 1","type":"","refresh":1,"options":[{"text":"Cluster","value":"","selected":false},{"text":"Instance","value":"instance,","selected":false}],"includeAll":false,"allFormat":"","allValue":"","multi":true,"multiFormat":"","query":"","current":{"text":"","value":null},"hide":0,"sort":1}]},"panels":[{"type":"row","collapsed":true,"id":3,"title":"row 1","editable":true,"gridPos":{"h":6,"w":24,"x":0,"y":0},"transparent":false,"links":null,"span":0,"error":false,"panels":[{"type":"graph","id":1,"title":"graph panel 1","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":0,"y":0},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"","expr":"sum(%s{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}) by ([[aggr_criteria]])","intervalFactor":2,"step":10,"legendFormat":"instance: {{instance}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"ops"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null","thresholds":null,"lines":false,"bars":false,"tooltip":{"shared":true,"value_type":"cumulative","msResolution":true},"aliasColors":{},"steppedLine":false},{"type":"graph","id":2,"title":"single stat 1","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":0,"y":0},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"","expr":"count(up{job=~\"node\"})","legendFormat":""}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"bytes"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"connected","thresholds":null,"lines":false,"bars":false,"tooltip":{"shared":false,"value_type":"individual"},"aliasColors":{},"steppedLine":false}]}],"editable":true,"timezone":"utc","refresh":"10s","time":{"from":"now-1h","to":"now"},"timepicker":{"refresh_intervals":["10s","1m","5m","15m","1h"],"time_options":["5m","15m","1h","1d","7d"]},"annotations":{"list":null},"links":null,"schemaVersion":12}`,
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
