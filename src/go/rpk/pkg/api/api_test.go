package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"vectorized/pkg/config"

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

	conf := config.DefaultConfig()
	conf.Rpk.EnableUsageStats = true
	err = sendMetricsToUrl(body, ts.URL, conf)
	require.NoError(t, err)
}

func TestSkipSendMetrics(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.FailNow(
				t,
				"The request shouldn't have been sent if metrics collection is disabled",
			)
		}),
	)
	defer ts.Close()

	conf := config.DefaultConfig()
	conf.Rpk.EnableUsageStats = false
	err := sendMetricsToUrl(metricsBody{}, ts.URL, conf)
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
		NodeUuid:     "awe-1231-sdfasd-13-saddasdf-as123sdf",
		NodeId:       1,
		Organization: "test.vectorized.io",
		SentAt:       time.Now(),
		Config: map[string]interface{}{
			"nodeUuid":  "abc123-hu234-234kh",
			"clusterId": "cluster 1",
			"redpanda": map[string]interface{}{
				"directory": "/var/lib/redpanda/",
				"rpcServer": map[string]interface{}{
					"address": "0.0.0.0",
					"port":    33145,
				},
				"kafkaAPI": map[string]interface{}{
					"address": "0.0.0.0",
					"port":    9092,
				},
			},
			"rpk": map[string]interface{}{
				"enableUsageStats": true,
			},
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
		}),
	)
	defer ts.Close()

	conf := config.DefaultConfig()
	conf.Rpk.EnableUsageStats = true
	err = sendEnvironmentToUrl(body, ts.URL, conf)
	require.NoError(t, err)
}

func TestSkipSendEnvironment(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.FailNow(
				t,
				"The request shouldn't have been sent if metrics collection is disabled",
			)
		}),
	)
	defer ts.Close()

	conf := config.DefaultConfig()
	conf.Rpk.EnableUsageStats = false
	err := sendEnvironmentToUrl(environmentBody{}, ts.URL, conf)
	require.NoError(t, err)
}
