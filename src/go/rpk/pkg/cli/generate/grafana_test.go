// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrometheusURLFlagDeprecation(t *testing.T) {
	p := new(config.Params)
	cmd := newGrafanaDashboardCmd(p)
	cmd.SetArgs([]string{
		"--prometheus-url", "localhost:8888/metrics",
		"--datasource", "prometheus",
	})
	require.Contains(t, cmd.Flag("prometheus-url").Deprecated, "Use --metrics-endpoint instead")
}

func TestGrafanaParseResponse(t *testing.T) {
	res := `# HELP vectorized_vectorized_internal_rpc_consumed_mem Amount of memory consumed for requests processing
# TYPE vectorized_vectorized_internal_rpc_consumed_mem gauge
vectorized_vectorized_internal_rpc_consumed_mem{shard="0",type="gauge"} 0.000000
vectorized_vectorized_internal_rpc_consumed_mem{shard="1",type="gauge"} 0.000000
# HELP vectorized_vectorized_internal_rpc_corrupted_headers Number of requests with corrupted headers
# TYPE vectorized_vectorized_internal_rpc_corrupted_headers counter
vectorized_vectorized_internal_rpc_corrupted_headers{shard="0",type="derive"} 0
vectorized_vectorized_internal_rpc_corrupted_headers{shard="1",type="derive"} 0
# HELP vectorized_vectorized_internal_rpc_dispatch_handler_latency Latency of service handler dispatch
# TYPE vectorized_vectorized_internal_rpc_dispatch_handler_latency histogram
vectorized_vectorized_internal_rpc_dispatch_handler_latency_sum{shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_dispatch_handler_latency_count{shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_dispatch_handler_latency_bucket{le="10.000000",shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_dispatch_handler_latency_bucket{le="20.000000",shard="0",type="histogram"} 0
# HELP vectorized_memory_allocated_memory_bytes Allocated memory size in bytes
# TYPE vectorized_memory_allocated_memory_bytes counter
vectorized_memory_allocated_memory_bytes{shard="0",type="bytes"} 40837120
vectorized_memory_allocated_memory_bytes{shard="1",type="bytes"} 36986880
`
	expected := `{"title":"Redpanda","templating":{"list":[{"name":"node","datasource":"prometheus","label":"Node","type":"query","refresh":1,"options":[],"includeAll":true,"allFormat":"","allValue":".*","multi":true,"multiFormat":"","query":"label_values(instance)","current":{"text":"","value":null},"hide":0,"sort":1},{"name":"node_shard","datasource":"prometheus","label":"Shard","type":"query","refresh":1,"options":[],"includeAll":true,"allFormat":"","allValue":".*","multi":true,"multiFormat":"","query":"label_values(shard)","current":{"text":"","value":null},"hide":0,"sort":1},{"name":"aggr_criteria","datasource":"prometheus","label":"Aggregate by","type":"custom","refresh":1,"options":[{"text":"Cluster","value":"","selected":false},{"text":"Instance","value":"instance,","selected":false},{"text":"Instance, Shard","value":"instance,shard,","selected":false}],"includeAll":false,"allFormat":"","allValue":"","multi":false,"multiFormat":"","query":"Cluster : cluster,Instance : instance,Instance\\,Shard : instance\\,shard","current":{"text":"Cluster","value":""},"hide":0,"sort":1}]},"panels":[{"type":"text","id":1,"title":"","editable":true,"gridPos":{"h":2,"w":24,"x":0,"y":0},"transparent":true,"links":null,"span":1,"error":false,"content":"<h1 style=\"color:#87CEEB; border-bottom: 3px solid #87CEEB;\">Redpanda Summary</h1>","mode":"html"},{"type":"singlestat","id":2,"title":"Nodes Up","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":2,"x":0,"y":2},"transparent":true,"span":1,"error":false,"targets":[{"refId":"","expr":"count by (app) (vectorized_application_uptime)","intervalFactor":1,"step":40,"legendFormat":"Nodes Up"}],"format":"none","prefix":"","postfix":"","maxDataPoints":100,"valueMaps":[{"value":"null","op":"=","text":"N/A"}],"mappingTypes":[{"name":"value to text","value":1},{"name":"range to text","value":2}],"rangeMaps":[{"from":"null","to":"null","text":"N/A"}],"mappingType":1,"nullPointMode":"connected","valueName":"current","valueFontSize":"200%","prefixFontSize":"50%","postfixFontSize":"50%","colorBackground":false,"colorValue":true,"colors":["#299c46","rgba(237, 129, 40, 0.89)","#d44a3a"],"thresholds":"","sparkline":{"show":false,"full":false,"ymin":null,"ymax":null,"lineColor":"rgb(31, 120, 193)","fillColor":"rgba(31, 118, 189, 0.18)"},"gauge":{"show":false,"minValue":0,"maxValue":100,"thresholdMarkers":true,"thresholdLabels":false},"links":[],"interval":null,"timeFrom":null,"timeShift":null,"nullText":null,"cacheTimeout":null,"tableColumn":""},{"type":"singlestat","id":3,"title":"Partitions","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":2,"x":2,"y":8},"transparent":true,"span":1,"error":false,"targets":[{"refId":"","expr":"count(count by (topic,partition) (vectorized_storage_log_partition_size{namespace=\"kafka\"}))","legendFormat":"Partition count"}],"format":"none","prefix":"","postfix":"","maxDataPoints":100,"valueMaps":[{"value":"null","op":"=","text":"N/A"}],"mappingTypes":[{"name":"value to text","value":1},{"name":"range to text","value":2}],"rangeMaps":[{"from":"null","to":"null","text":"N/A"}],"mappingType":1,"nullPointMode":"connected","valueName":"current","valueFontSize":"200%","prefixFontSize":"50%","postfixFontSize":"50%","colorBackground":false,"colorValue":true,"colors":["#299c46","rgba(237, 129, 40, 0.89)","#d44a3a"],"thresholds":"","sparkline":{"show":false,"full":false,"ymin":null,"ymax":null,"lineColor":"rgb(31, 120, 193)","fillColor":"rgba(31, 118, 189, 0.18)"},"gauge":{"show":false,"minValue":0,"maxValue":100,"thresholdMarkers":true,"thresholdLabels":false},"links":[],"interval":null,"timeFrom":null,"timeShift":null,"nullText":null,"cacheTimeout":null,"tableColumn":""},{"type":"text","id":5,"title":"","editable":true,"gridPos":{"h":2,"w":12,"x":12,"y":14},"transparent":true,"links":null,"span":1,"error":false,"content":"<h1 style=\"color:#87CEEB; border-bottom: 3px solid #87CEEB;\">Throughput</h1>","mode":"html"},{"type":"row","collapsed":true,"id":7,"title":"memory","editable":true,"gridPos":{"h":6,"w":24,"x":0,"y":20},"transparent":false,"links":null,"span":0,"error":false,"panels":[{"type":"graph","id":6,"interval":"1m","title":"Rate - Allocated memory size in bytes","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":0,"y":20},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"","expr":"sum(irate(vectorized_memory_allocated_memory_bytes{instance=~\"$node\",shard=~\"$node_shard\"}[$__rate_interval])) by ($aggr_criteria)","intervalFactor":2,"step":10,"legendFormat":"node: {{instance}}, shard: {{shard}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"Bps"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null","thresholds":null,"lines":true,"bars":false,"tooltip":{"shared":true,"sort":2,"value_type":"cumulative","msResolution":true},"aliasColors":{},"steppedLine":false}]},{"type":"row","collapsed":true,"id":9,"title":"vectorized_internal_rpc","editable":true,"gridPos":{"h":6,"w":24,"x":0,"y":21},"transparent":false,"links":null,"span":0,"error":false,"panels":[{"type":"graph","id":8,"title":"Amount of memory consumed for requests processing","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":0,"y":21},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"","expr":"sum(vectorized_vectorized_internal_rpc_consumed_mem{instance=~\"$node\",shard=~\"$node_shard\"}) by ($aggr_criteria)","intervalFactor":2,"step":10,"legendFormat":"node: {{instance}}, shard: {{shard}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"short"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null","thresholds":null,"lines":true,"bars":false,"tooltip":{"shared":true,"sort":2,"value_type":"cumulative","msResolution":true},"aliasColors":{},"steppedLine":true},{"type":"graph","id":10,"interval":"1m","title":"Rate - Number of requests with corrupted headers","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":8,"y":21},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"","expr":"sum(irate(vectorized_vectorized_internal_rpc_corrupted_headers{instance=~\"$node\",shard=~\"$node_shard\"}[$__rate_interval])) by ($aggr_criteria)","intervalFactor":2,"step":10,"legendFormat":"node: {{instance}}, shard: {{shard}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"ops"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null","thresholds":null,"lines":true,"bars":false,"tooltip":{"shared":true,"sort":2,"value_type":"cumulative","msResolution":true},"aliasColors":{},"steppedLine":false},{"type":"graph","id":11,"interval":"1m","title":"Latency of service handler dispatch (p95)","datasource":"prometheus","editable":true,"gridPos":{"h":6,"w":8,"x":16,"y":21},"transparent":false,"links":null,"renderer":"flot","span":4,"error":false,"targets":[{"refId":"A","expr":"histogram_quantile(0.95, sum(rate(vectorized_vectorized_internal_rpc_dispatch_handler_latency_bucket{instance=~\"$node\",shard=~\"$node_shard\"}[$__rate_interval])) by (le, $aggr_criteria))","intervalFactor":2,"step":10,"legendFormat":"node: {{instance}}, shard: {{shard}}","format":"time_series"}],"xaxis":{"format":"","logBase":0,"show":true,"mode":"time"},"yaxes":[{"label":null,"show":true,"logBase":1,"min":0,"format":"µs"},{"label":null,"show":true,"logBase":1,"min":0,"format":"short"}],"legend":{"show":true,"max":false,"min":false,"values":false,"avg":false,"current":false,"total":false},"fill":1,"linewidth":2,"nullPointMode":"null as zero","thresholds":null,"lines":true,"bars":false,"tooltip":{"shared":true,"sort":2,"value_type":"individual","msResolution":true},"aliasColors":{},"steppedLine":true}]}],"editable":true,"timezone":"utc","refresh":"1m","time":{"from":"now-1h","to":"now"},"timepicker":{"refresh_intervals":["30s","1m","5m","15m","30m","1h","2h","1d"],"time_options":["5m","15m","1h","6h","12h","24h","2d","7d","30d"]},"annotations":{"list":null},"links":null,"schemaVersion":12}`
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res))
		}),
	)
	out, err := executeGrafanaDashboard(ts.URL, "prometheus")
	require.NoError(t, err)
	require.JSONEq(t, expected, out)
}

func TestGrafanaInvalidResponse(t *testing.T) {
	res := `# HELP vectorized_vectorized_internal_rpc_consumed_mem Amount of memory consumed for requests processing
# TYPE vectorized_vectorized_internal_rpc_consumed_mem gauge
vectorized_vectorized_in
`
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res))
		}),
	)
	_, err := executeGrafanaDashboard(ts.URL, "any")
	require.EqualError(t, err, "text format parsing error in line 3: expected float as value, got \"\"")
}

// Test_embeddedDecompressAndPrint tests that the embedded files are actually a
// valid json files and can be decompressed.
func Test_embeddedDecompressAndPrint(t *testing.T) {
	type tt struct {
		name    string
		path    string
		expHash string
	}

	var tests []tt
	for k, v := range dashboardMap {
		if k == "legacy" {
			// Legacy dashboard is not embedded and is tested above.
			continue
		}
		tests = append(tests, tt{
			name:    fmt.Sprintf("parse %v correctly", k),
			path:    filepath.Join("grafana-dashboards", v.Location+".gz"),
			expHash: v.Hash,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &bytes.Buffer{}
			err := decompressAndPrint(dashFS, tt.path, writer)
			require.NoError(t, err)

			b := writer.Bytes()

			sum := sha256.Sum256(b)
			require.Equal(t, tt.expHash, fmt.Sprintf("%x", sum))

			var dash map[string]any
			err = json.Unmarshal(b, &dash)
			require.NoError(t, err)
		})
	}
}
