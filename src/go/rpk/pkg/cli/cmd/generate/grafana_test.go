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
	require.Error(t, err)
	require.Contains(t, err.Error(), "Get http://localhost:8888/metrics: dial tcp")
	require.Contains(t, err.Error(), "connect: connection refused")
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
 "title": "Redpanda",
 "templating": {
  "list": [
   {
    "name": "node",
    "datasource": "prometheus",
    "label": "Node",
    "type": "query",
    "refresh": 1,
    "options": null,
    "includeAll": true,
    "allFormat": "",
    "allValue": ".*",
    "multi": true,
    "multiFormat": "",
    "query": "label_values(instance)",
    "current": {
     "text": "",
     "value": null
    },
    "hide": 0,
    "sort": 1
   },
   {
    "name": "node_shard",
    "datasource": "prometheus",
    "label": "shard",
    "type": "query",
    "refresh": 1,
    "options": null,
    "includeAll": true,
    "allFormat": "",
    "allValue": ".*",
    "multi": true,
    "multiFormat": "",
    "query": "label_values(shard)",
    "current": {
     "text": "",
     "value": null
    },
    "hide": 0,
    "sort": 1
   },
   {
    "name": "aggregate",
    "datasource": "prometheus",
    "label": "Aggregate by",
    "type": "custom",
    "refresh": 1,
    "options": [
     {
      "text": "None",
      "value": "",
      "selected": true
     },
     {
      "text": "Instance",
      "value": "sum by(instance)",
      "selected": false
     },
     {
      "text": "Cluster",
      "value": "sum",
      "selected": false
     }
    ],
    "includeAll": false,
    "allFormat": "",
    "allValue": "",
    "multi": false,
    "multiFormat": "",
    "query": "",
    "current": {
     "text": "Cluster",
     "value": "sum"
    },
    "hide": 0,
    "sort": 1
   }
  ]
 },
 "rows": [
  {
   "title": "memory",
   "showTitle": true,
   "collapse": true,
   "editable": true,
   "height": "250px",
   "panels": [
    {
     "type": "graph",
     "id": 0,
     "title": "Rate - Allocated memory size in bytes",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 0,
      "w": 0,
      "x": 0,
      "y": 0
     },
     "links": null,
     "renderer": "flot",
     "span": 4,
     "targets": [
      {
       "refId": "",
       "expr": "[[aggregate]] (irate(vectorized_memory_allocated_memory{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m]))",
       "intervalFactor": 2,
       "interval": "",
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true,
      "mode": "time"
     },
     "yaxes": [
      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "Bps"
      },
      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "short"
      }
     ],
     "legend": {
      "show": true,
      "max": false,
      "min": false,
      "values": false,
      "avg": false,
      "current": false,
      "total": false
     },
     "fill": 1,
     "linewidth": 2,
     "nullPointMode": "null",
     "thresholds": null,
     "lines": true,
     "bars": false,
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "aliasColors": {}
    }
   ]
  },
  {
   "title": "vectorized_internal_rpc_protocol",
   "showTitle": true,
   "collapse": true,
   "editable": true,
   "height": "250px",
   "panels": [
    {
     "type": "graph",
     "id": 1,
     "title": "Amount of memory consumed for requests processing",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 0,
      "w": 0,
      "x": 0,
      "y": 0
     },
     "links": null,
     "renderer": "flot",
     "span": 4,
     "targets": [
      {
       "refId": "",
       "expr": "[[aggregate]] (vectorized_vectorized_internal_rpc_protocol_consumed_mem{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"})",
       "intervalFactor": 2,
       "interval": "",
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true,
      "mode": "time"
     },
     "yaxes": [
      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "short"
      },
      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "short"
      }
     ],
     "legend": {
      "show": true,
      "max": false,
      "min": false,
      "values": false,
      "avg": false,
      "current": false,
      "total": false
     },
     "fill": 1,
     "linewidth": 2,
     "nullPointMode": "null",
     "thresholds": null,
     "lines": false,
     "bars": true,
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "aliasColors": {}
    },
    {
     "type": "graph",
     "id": 2,
     "title": "Rate - Number of requests with corrupted headers",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 0,
      "w": 0,
      "x": 0,
      "y": 0
     },
     "links": null,
     "renderer": "flot",
     "span": 4,
     "targets": [
      {
       "refId": "",
       "expr": "[[aggregate]] (irate(vectorized_vectorized_internal_rpc_protocol_corrupted_headers{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m]))",
       "intervalFactor": 2,
       "interval": "",
       "step": 10,
       "legendFormat": "node: {{instance}}, shard: {{shard}}",
       "format": "time_series"
      }
     ],
     "xaxis": {
      "format": "",
      "logBase": 0,
      "show": true,
      "mode": "time"
     },
     "yaxes": [

      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "ops"
      },
      {
       "label": null,
       "show": true,
       "logBase": 1,
       "min": 0,
       "format": "short"
      }
     ],
     "legend": {
      "show": true,
      "max": false,
      "min": false,
      "values": false,
      "avg": false,
      "current": false,
      "total": false
     },
     "fill": 1,
     "linewidth": 2,
     "nullPointMode": "null",
     "thresholds": null,
     "lines": true,
     "bars": false,
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "aliasColors": {}
    }
   ]
  }
 ],
 "panels": null,
 "editable": true,
 "timezone": "utc",
 "refresh": "10s",
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
 },
 "annotations": {
  "list": null
 },
 "links": null,
 "schemaVersion": 12
}
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
	require.NoError(t, err)

	require.JSONEq(t, expected, out.String())
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
