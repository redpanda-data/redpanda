package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendMetrics(t *testing.T) {
	body := metricsBody{
		NodeUuid:     "asdfas-asdf2w23sd-907asdf",
		Organization: "io.vectorized",
		NodeId:       1,
		SentAt:       time.Now(),
		MetricsPayload: MetricsPayload{
			FreeMemoryMB:  100,
			FreeSpaceMB:   200,
			CpuPercentage: 89,
		},
	}
	bs, err := json.Marshal(body)
	require.NoError(t, err)

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)
			require.Exactly(t, bs, b)
			w.WriteHeader(http.StatusOK)
		}))
	defer ts.Close()

	err = SendMetricsToUrl(body, ts.URL)
	require.NoError(t, err)
}

func TestSendEnvironment(t *testing.T) {
	body := environmentBody{
		Payload: EnvironmentPayload{
			Checks: []CheckPayload{
				{
					Name:     "check 1",
					Current:  "1",
					Required: "2",
				},
				{
					Name:     "check 2",
					Current:  "something",
					Required: "something better",
				},
			},
			Tuners: []TunerPayload{
				{
					Name:      "tuner 1",
					Enabled:   true,
					Supported: false,
				},
				{
					Name:      "tuner 2",
					ErrorMsg:  "tuner 2 failed",
					Enabled:   true,
					Supported: true,
				},
			},
			ErrorMsg: "tuner 2 failed",
		},
		SentAt: time.Now(),
		Config: `{
  "nodeUuid": "abc123-hu234-234kh",
  "clusterId": "cluster 1",
  "redpanda": {
    "directory": "/var/lib/redpanda/",
    "rpcServer": {"0.0.0.0", 33145},
    "kafkaAPI": {"0.0.0.0", 9092},
  },
  rpk: {
    enableUsageStats: true,
  },
}
`,
	}
	bs, err := json.Marshal(body)
	require.NoError(t, err)

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)
			require.Exactly(t, bs, b)
			w.WriteHeader(http.StatusOK)
		}))
	defer ts.Close()
	err = SendEnvironmentToUrl(body, ts.URL)
	require.NoError(t, err)
}
