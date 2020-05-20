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
	cmd.SetArgs([]string{
		"--prometheus-url", "localhost:8888/metrics",
		"--datasource", "prometheus",
	})
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
    "label": "Shard",
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
    "name": "aggr_criteria",
    "datasource": "prometheus",
    "label": "Aggregate by",
    "type": "custom",
    "refresh": 1,
    "options": [
     {
      "text": "Cluster",
      "value": "",
      "selected": false
     },
     {
      "text": "Instance",
      "value": "instance,",
      "selected": false
     },
     {
      "text": "Instance, Shard",
      "value": "instance,shard,",
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
     "value": ""
    },
    "hide": 0,
    "sort": 1
   }
  ]
 },
 "panels": [
  {
   "type": "text",
   "id": 1,
   "title": "",
   "editable": true,
   "gridPos": {
    "h": 2,
    "w": 24,
    "x": 0,
    "y": 0
   },
   "transparent": true,
   "links": null,
   "span": 1,
   "error": false,
   "content": "\u003ch1 style=\"color:#CB3805; border-bottom: 3px solid #CB3805;\"\u003eRedpanda Summary\u003c/h1\u003e",
   "mode": "html"
  },
  {
   "type": "singlestat",
   "id": 2,
   "title": "Nodes Up",
   "datasource": "prometheus",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 2,
    "x": 0,
    "y": 2
   },
   "transparent": true,
   "span": 1,
   "error": false,
   "targets": [
    {
     "refId": "",
     "expr": "count(up{job=\"node\"})",
     "intervalFactor": 1,
     "step": 40,
     "legendFormat": "Nodes Up"
    }
   ],
   "format": "none",
   "prefix": "",
   "postfix": "",
   "maxDataPoints": 100,
   "valueMaps": [
    {
     "value": "null",
     "op": "=",
     "text": "N/A"
    }
   ],
   "mappingTypes": [
    {
     "name": "value to text",
     "value": 1
    },
    {
     "name": "range to text",
     "value": 2
    }
   ],
   "rangeMaps": [
    {
     "from": "null",
     "to": "null",
     "text": "N/A"
    }
   ],
   "mappingType": 1,
   "nullPointMode": "connected",
   "valueName": "current",
   "valueFontSize": "200%",
   "prefixFontSize": "50%",
   "postfixFontSize": "50%",
   "colorBackground": false,
   "colorValue": true,
   "colors": [
    "#299c46",
    "rgba(237, 129, 40, 0.89)",
    "#d44a3a"
   ],
   "thresholds": "",
   "sparkline": {
    "show": false,
    "full": false,
    "ymin": null,
    "ymax": null,
    "lineColor": "rgb(31, 120, 193)",
    "fillColor": "rgba(31, 118, 189, 0.18)"
   },
   "gauge": {
    "show": false,
    "minValue": 0,
    "maxValue": 100,
    "thresholdMarkers": true,
    "thresholdLabels": false
   },
   "links": [],
   "interval": null,
   "timeFrom": null,
   "timeShift": null,
   "nullText": null,
   "cacheTimeout": null,
   "tableColumn": ""
  },
  {
   "type": "singlestat",
   "id": 3,
   "title": "Partitions",
   "datasource": "prometheus",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 2,
    "x": 2,
    "y": 8
   },
   "transparent": true,
   "span": 1,
   "error": false,
   "targets": [
    {
     "refId": "",
     "expr": "sum(vectorized_raft_leader_for)",
     "legendFormat": "Partition count"
    }
   ],
   "format": "none",
   "prefix": "",
   "postfix": "",
   "maxDataPoints": 100,
   "valueMaps": [
    {
     "value": "null",
     "op": "=",
     "text": "N/A"
    }
   ],
   "mappingTypes": [
    {
     "name": "value to text",
     "value": 1
    },
    {
     "name": "range to text",
     "value": 2
    }
   ],
   "rangeMaps": [
    {
     "from": "null",
     "to": "null",
     "text": "N/A"
    }
   ],
   "mappingType": 1,
   "nullPointMode": "connected",
   "valueName": "current",
   "valueFontSize": "200%",
   "prefixFontSize": "50%",
   "postfixFontSize": "50%",
   "colorBackground": false,
   "colorValue": true,
   "colors": [
    "#299c46",
    "rgba(237, 129, 40, 0.89)",
    "#d44a3a"
   ],
   "thresholds": "",
   "sparkline": {
    "show": false,
    "full": false,
    "ymin": null,
    "ymax": null,
    "lineColor": "rgb(31, 120, 193)",
    "fillColor": "rgba(31, 118, 189, 0.18)"
   },
   "gauge": {
    "show": false,
    "minValue": 0,
    "maxValue": 100,
    "thresholdMarkers": true,
    "thresholdLabels": false
   },
   "links": [],
   "interval": null,
   "timeFrom": null,
   "timeShift": null,
   "nullText": null,
   "cacheTimeout": null,
   "tableColumn": ""
  },
  {
   "type": "text",
   "id": 4,
   "title": "",
   "editable": true,
   "gridPos": {
    "h": 2,
    "w": 12,
    "x": 0,
    "y": 14
   },
   "transparent": true,
   "links": null,
   "span": 1,
   "error": false,
   "content": "\u003ch1 style=\"color:#CB3805; border-bottom: 3px solid #CB3805;\"\u003eInternal RPC Latency\u003c/h1\u003e",
   "mode": "html"
  },
  {
   "type": "graph",
   "id": 5,
   "title": "Latency of service handler dispatch (p95)",
   "datasource": "prometheus",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 6,
    "x": 0,
    "y": 16
   },
   "transparent": false,
   "links": null,
   "renderer": "flot",
   "span": 4,
   "error": false,
   "targets": [
    {
     "refId": "A",
     "expr": "histogram_quantile(0.95, sum(rate(vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_bucket{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])) by (le, [[aggr_criteria]]))",
     "intervalFactor": 2,
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
     "format": "µs"
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
    "value_type": "individual",
    "msResolution": true
   },
   "aliasColors": {},
   "steppedLine": false
  },
  {
   "type": "graph",
   "id": 6,
   "title": "Latency of service handler dispatch (p99)",
   "datasource": "prometheus",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 6,
    "x": 6,
    "y": 16
   },
   "transparent": false,
   "links": null,
   "renderer": "flot",
   "span": 4,
   "error": false,
   "targets": [
    {
     "refId": "A",
     "expr": "histogram_quantile(0.99, sum(rate(vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_bucket{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])) by (le, [[aggr_criteria]]))",
     "intervalFactor": 2,
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
     "format": "µs"
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
    "value_type": "individual",
    "msResolution": true
   },
   "aliasColors": {},
   "steppedLine": false
  },
  {
   "type": "text",
   "id": 7,
   "title": "",
   "editable": true,
   "gridPos": {
    "h": 2,
    "w": 12,
    "x": 12,
    "y": 14
   },
   "transparent": true,
   "links": null,
   "span": 1,
   "error": false,
   "content": "\u003ch1 style=\"color:#CB3805; border-bottom: 3px solid #CB3805;\"\u003eThroughput\u003c/h1\u003e",
   "mode": "html"
  },
  {
   "type": "row",
   "collapsed": true,
   "id": 9,
   "title": "memory",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 24,
    "x": 0,
    "y": 20
   },
   "transparent": false,
   "links": null,
   "span": 0,
   "error": false,
   "panels": [
    {
     "type": "graph",
     "id": 8,
     "title": "Rate - Allocated memory size in bytes",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 6,
      "w": 8,
      "x": 0,
      "y": 20
     },
     "transparent": false,
     "links": null,
     "renderer": "flot",
     "span": 4,
     "error": false,
     "targets": [
      {
       "refId": "",
       "expr": "sum(irate(vectorized_memory_allocated_memory{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])) by ([[aggr_criteria]])",
       "intervalFactor": 2,
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
     "aliasColors": {},
     "steppedLine": false
    }
   ]
  },
  {
   "type": "row",
   "collapsed": true,
   "id": 11,
   "title": "vectorized_internal_rpc_protocol",
   "editable": true,
   "gridPos": {
    "h": 6,
    "w": 24,
    "x": 0,
    "y": 21
   },
   "transparent": false,
   "links": null,
   "span": 0,
   "error": false,
   "panels": [
    {
     "type": "graph",
     "id": 10,
     "title": "Amount of memory consumed for requests processing",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 6,
      "w": 8,
      "x": 0,
      "y": 21
     },
     "transparent": false,
     "links": null,
     "renderer": "flot",
     "span": 4,
     "error": false,
     "targets": [
      {
       "refId": "",
       "expr": "sum(vectorized_vectorized_internal_rpc_protocol_consumed_mem{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}) by ([[aggr_criteria]])",
       "intervalFactor": 2,
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
     "lines": true,
     "bars": false,
     "tooltip": {
      "shared": true,
      "value_type": "cumulative",
      "msResolution": true
     },
     "aliasColors": {},
     "steppedLine": true
    },
    {
     "type": "graph",
     "id": 12,
     "title": "Rate - Number of requests with corrupted headers",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 6,
      "w": 8,
      "x": 8,
      "y": 21
     },
     "transparent": false,
     "links": null,
     "renderer": "flot",
     "span": 4,
     "error": false,
     "targets": [
      {
       "refId": "",
       "expr": "sum(irate(vectorized_vectorized_internal_rpc_protocol_corrupted_headers{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])) by ([[aggr_criteria]])",
       "intervalFactor": 2,
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
     "aliasColors": {},
     "steppedLine": false
    },
    {
     "type": "graph",
     "id": 13,
     "title": "Latency of service handler dispatch (p95)",
     "datasource": "prometheus",
     "editable": true,
     "gridPos": {
      "h": 6,
      "w": 8,
      "x": 16,
      "y": 21
     },
     "transparent": false,
     "links": null,
     "renderer": "flot",
     "span": 4,
     "error": false,
     "targets": [
      {
       "refId": "A",
       "expr": "histogram_quantile(0.95, sum(rate(vectorized_vectorized_internal_rpc_protocol_dispatch_handler_latency_bucket{instance=~\"[[node]]\",shard=~\"[[node_shard]]\"}[1m])) by (le, [[aggr_criteria]]))",
       "intervalFactor": 2,
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
       "format": "µs"
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
      "value_type": "individual",
      "msResolution": true
     },
     "aliasColors": {},
     "steppedLine": false
    }
   ]
  }
 ],
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
	cmd.SetArgs([]string{
		"--prometheus-url", ts.URL,
		"--datasource", "prometheus",
	})
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
	cmd.SetArgs([]string{
		"--prometheus-url", ts.URL,
		"--datasource", "prometheus",
	})
	err := cmd.Execute()
	require.EqualError(t, err, "text format parsing error in line 3: expected float as value, got \"\"")
}
