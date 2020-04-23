package generate_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"vectorized/pkg/cli/cmd/generate"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestGrafanaHostNoServer(t *testing.T) {
	var out bytes.Buffer
	logrus.SetOutput(&out)
	cmd := generate.NewGrafanaDashboardCmd()
	cmd.SetArgs([]string{"--prometheus-url", "localhost:8888/metrics"})
	err := cmd.Execute()
	require.EqualError(
		t,
		err,
		"Get http://localhost:8888/metrics: dial tcp [::1]:8888: connect: connection refused",
	)
}

func TestGrafanaParseResponse(t *testing.T) {
	res := `# HELP vectorized_vectorized_internal_rpc_protocol_consumed_mem Amount of memory consumed for requests processing
# TYPE vectorized_vectorized_internal_rpc_protocol_consumed_mem gauge
vectorized_vectorized_internal_rpc_protocol_consumed_mem{shard="0",type="gauge"} 0.000000
vectorized_vectorized_internal_rpc_protocol_consumed_mem{shard="1",type="gauge"} 0.000000
# HELP vectorized_vectorized_internal_rpc_protocol_corrupted_headers Number of requests with corrupted headers
# TYPE vectorized_vectorized_internal_rpc_protocol_corrupted_headers counter
vectorized_vectorized_internal_rpc_protocol_corrupted_headers{shard="0",type="derive"} 0
vectorized_vectorized_internal_rpc_protocol_corrupted_headers{shard="1",type="derive"} 0
# HELP vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency Latency of service handler dispatch
# TYPE vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency histogram
vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_sum{shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_count{shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_bucket{le="10.000000",shard="0",type="histogram"} 0
vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_bucket{le="20.000000",shard="0",type="histogram"} 0
# HELP vectorized_memory_allocated_memory Allocated memory size in bytes
# TYPE vectorized_memory_allocated_memory counter
vectorized_memory_allocated_memory{shard="0",type="bytes"} 40837120
vectorized_memory_allocated_memory{shard="1",type="bytes"} 36986880
`
	expected := `{
 "id": 1,
 "slug": "",
 "title": "Redpanda",
 "originalTitle": "",
 "tags": null,
 "style": "dark",
 "timezone": "utc",
 "editable": true,
 "hideControls": false,
 "sharedCrosshair": false,
 "panels": null,
 "rows": [
  {
   "title": "memory",
   "showTitle": true,
   "collapse": false,
   "editable": true,
   "height": "250px",
   "panels": [
    {
     "datasource": "prometheus",
     "editable": true,
     "error": false,
     "gridPos": {},
     "id": 0,
     "isNew": true,
     "renderer": "flot",
     "span": 4,
     "title": "Rate - Allocated memory size in bytes",
     "transparent": false,
     "type": "graph",
     "aliasColors": {},
     "bars": false,
     "fill": 1,
     "legend": {
      "alignAsTable": false,
      "avg": false,
      "current": false,
      "hideEmpty": false,
      "hideZero": false,
      "max": false,
      "min": false,
      "rightSide": false,
      "show": true,
      "total": false,
      "values": false
     },
     "lines": true,
     "linewidth": 2,
     "nullPointMode": "connected",
     "percentage": false,
     "pointradius": 5,
     "points": false,
     "stack": false,
     "steppedLine": false,
     "targets": [
      {
       "refId": "",
       "expr": "irate(vectorized_memory_allocated_memory{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])",
       "intervalFactor": 2,
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "x-axis": true,
     "y-axis": true,
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true
     },
     "yaxes": [
      {
       "format": "bytes",
       "logBase": 1,
       "min": 0,
       "show": true
      },
      {
       "format": "short",
       "logBase": 1,
       "min": 0,
       "show": true
      }
     ]
    }
   ],
   "repeat": null
  },
  {
   "title": "vectorized_internal_rpc_protocol",
   "showTitle": true,
   "collapse": false,
   "editable": true,
   "height": "250px",
   "panels": [
    {
     "datasource": "prometheus",
     "editable": true,
     "error": false,
     "gridPos": {},
     "id": 1,
     "isNew": true,
     "renderer": "flot",
     "span": 4,
     "title": "Amount of memory consumed for requests processing",
     "transparent": false,
     "type": "graph",
     "aliasColors": {},
     "bars": true,
     "fill": 1,
     "legend": {
      "alignAsTable": false,
      "avg": false,
      "current": false,
      "hideEmpty": false,
      "hideZero": false,
      "max": false,
      "min": false,
      "rightSide": false,
      "show": true,
      "total": false,
      "values": false
     },
     "lines": false,
     "linewidth": 2,
     "nullPointMode": "connected",
     "percentage": false,
     "pointradius": 5,
     "points": false,
     "stack": false,
     "steppedLine": false,
     "targets": [
      {
       "refId": "",
       "expr": "vectorized_vectorized_internal_rpc_protocol_consumed_mem{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}",
       "intervalFactor": 2,
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "x-axis": true,
     "y-axis": true,
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true
     },
     "yaxes": [
      {
       "format": "short",
       "logBase": 1,
       "min": 0,
       "show": true
      },
      {
       "format": "short",
       "logBase": 1,
       "min": 0,
       "show": true
      }
     ]
    },
    {
     "datasource": "prometheus",
     "editable": true,
     "error": false,
     "gridPos": {},
     "id": 2,
     "isNew": true,
     "renderer": "flot",
     "span": 4,
     "title": "Rate - Number of requests with corrupted headers",
     "transparent": false,
     "type": "graph",
     "aliasColors": {},
     "bars": false,
     "fill": 1,
     "legend": {
      "alignAsTable": false,
      "avg": false,
      "current": false,
      "hideEmpty": false,
      "hideZero": false,
      "max": false,
      "min": false,
      "rightSide": false,
      "show": true,
      "total": false,
      "values": false
     },
     "lines": true,
     "linewidth": 2,
     "nullPointMode": "connected",
     "percentage": false,
     "pointradius": 5,
     "points": false,
     "stack": false,
     "steppedLine": false,
     "targets": [
      {
       "refId": "",
       "expr": "irate(vectorized_vectorized_internal_rpc_protocol_corrupted_headers{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])",
       "intervalFactor": 2,
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "x-axis": true,
     "y-axis": true,
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true
     },
     "yaxes": [
      {
       "format": "ops",
       "logBase": 1,
       "min": 0,
       "show": true
      },
      {
       "format": "short",
       "logBase": 1,
       "min": 0,
       "show": true
      }
     ]
    }
   ],
   "repeat": null
  }
 ],
 "templating": {
  "list": [
   {
    "name": "node",
    "type": "query",
    "datasource": "prometheus",
    "refresh": 1,
    "options": [],
    "includeAll": false,
    "allFormat": "",
    "allValue": "",
    "multi": true,
    "multiFormat": "",
    "query": "label_values(instance)",
    "regex": "",
    "current": {
     "text": "",
     "value": null
    },
    "label": "Node",
    "hide": 0,
    "sort": 1
   },
   {
    "name": "node_shard",
    "type": "query",
    "datasource": "prometheus",
    "refresh": 1,
    "options": [],
    "includeAll": false,
    "allFormat": "",
    "allValue": "",
    "multi": true,
    "multiFormat": "",
    "query": "label_values(shard)",
    "regex": "",
    "current": {
     "text": "",
     "value": null
    },
    "label": "shard",
    "hide": 0,
    "sort": 1
   }
  ]
 },
 "annotations": {
  "list": null
 },
 "refresh": "10s",
 "schemaVersion": 12,
 "version": 0,
 "links": null,
 "time": {
  "from": "now-1h",
  "to": "now"
 },
 "timepicker": {
  "refresh_intervals": [
   "5s",
   "10s",
   "30s",
   "1m",
   "5m",
   "15m",
   "30m",
   "1h",
   "2h",
   "1d"
  ],
  "time_options": [
   "5m",
   "15m",
   "1h",
   "6h",
   "12h",
   "24h",
   "2d",
   "7d",
   "30d"
  ]
 }
}`
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res))
		}),
	)
	var out bytes.Buffer
	logrus.SetOutput(&out)
	cmd := generate.NewGrafanaDashboardCmd()
	cmd.SetOutput(&out)
	cmd.SetArgs([]string{"--prometheus-url", ts.URL})
	err := cmd.Execute()
	require.NoError(t, err)
	require.Equal(t, expected, out.String())
}

func TestGrafanaInvalidResponse(t *testing.T) {
	res := `# HELP vectorized_vectorized_internal_rpc_protocol_consumed_mem Amount of memory consumed for requests processing
# TYPE vectorized_vectorized_internal_rpc_protocol_consumed_mem gauge
vectorized_vectorized_in
`
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res))
		}),
	)
	var out bytes.Buffer
	logrus.SetOutput(&out)
	cmd := generate.NewGrafanaDashboardCmd()
	cmd.SetOutput(&out)
	cmd.SetArgs([]string{"--prometheus-url", ts.URL})
	err := cmd.Execute()
	require.EqualError(t, err, "text format parsing error in line 3: expected float as value, got \"\"")
}
