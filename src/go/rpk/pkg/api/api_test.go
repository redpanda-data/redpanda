// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
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

	conf := config.Default()
	conf.Rpk.EnableUsageStats = true
	err = sendMetricsToUrl(body, ts.URL, *conf)
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

	conf := config.Default()
	conf.Rpk.EnableUsageStats = false
	err := sendMetricsToUrl(metricsBody{}, ts.URL, *conf)
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
		CPUCores:     12,
		CPUModel:     "AMD Ryzen 9 3900X 12-Core Processor",
		CloudVendor:  "AWS",
		RPVersion:    "release-0.99.8 (rev a2b48491)",
		VMType:       "i3.4xlarge",
		OSInfo:       "x86_64 5.8.9-200.fc32.x86_64 #1 SMP Mon Sep 14 18:28:45 UTC 2020 \"Fedora release 32 (Thirty Two)\"",
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

	// Deep-copy the body that will be sent.
	expected := environmentBody{}
	err = json.Unmarshal(bs, &expected)
	require.NoError(t, err)

	env := "only-testing-nbd"
	os.Setenv("REDPANDA_ENVIRONMENT", env)
	defer func() {
		os.Setenv("REDPANDA_ENVIRONMENT", "")
	}()

	expected.Environment = env

	expectedBytes, err := json.Marshal(expected)
	require.NoError(t, err)

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)
			require.Exactly(t, expectedBytes, b)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer ts.Close()

	conf := config.Default()
	conf.Rpk.EnableUsageStats = true
	err = sendEnvironmentToUrl(body, ts.URL, *conf)
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

	conf := config.Default()
	conf.Rpk.EnableUsageStats = false
	err := sendEnvironmentToUrl(environmentBody{}, ts.URL, *conf)
	require.NoError(t, err)
}
