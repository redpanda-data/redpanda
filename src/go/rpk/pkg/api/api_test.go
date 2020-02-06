package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
	"vectorized/pkg/config"
)

func TestSendMetrics(t *testing.T) {
	body := metricsBody{
		NodeUuid:     "asdfas-asdf2w23sd-907asdf",
		Organization: "io.vectorized",
		NodeId:       1,
		SentAt:       time.Now(),
		MetricsPayload: MetricsPayload{
			FreeMemory:    100,
			FreeSpace:     200,
			CpuPercentage: 89,
		},
	}
	bs, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(b, bs) {
				t.Errorf("expected: %v, got %v", bs, b)
			}
			w.WriteHeader(http.StatusOK)
		}))
	defer ts.Close()

	err = SendMetricsToUrl(body, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSendEnvironment(t *testing.T) {
	body := environmentBody{
		EnvironmentPayload: EnvironmentPayload{
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
		Config: config.Config{
			NodeUuid:  "abc123-hu234-234kh",
			ClusterId: "cluster 1",
			Redpanda:  &config.RedpandaConfig{},
			Rpk: &config.RpkConfig{
				EnableUsageStats: true,
			},
		},
	}
	bs, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(b, bs) {
				t.Errorf("expected: %v, got %v", bs, b)
			}
			w.WriteHeader(http.StatusOK)
		}))
	defer ts.Close()
	err = SendEnvironmentToUrl(body, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
}
