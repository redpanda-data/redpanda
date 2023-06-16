// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartAutomatedRecovery(t *testing.T) {
	type testCase struct {
		name   string
		testFn func(t *testing.T) http.HandlerFunc
		exp    RecoveryStartResponse
	}

	successfulStartResponse := RecoveryStartResponse{
		Code:    200,
		Message: "Automated recovery started",
	}

	runTest := func(t *testing.T, test testCase) {
		server := httptest.NewServer(test.testFn(t))
		defer server.Close()

		client, err := NewAdminAPI([]string{server.URL}, BasicCredentials{}, nil)
		assert.NoError(t, err)

		response, err := client.StartAutomatedRecovery(context.Background(), ".*")

		assert.NoError(t, err)
		assert.Equal(t, test.exp, response)
	}

	tests := []testCase{
		{
			name: "should call the correct endpoint",
			testFn: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "/v1/cloud_storage/automated_recovery", r.URL.Path)
					w.WriteHeader(http.StatusOK)
					resp, err := json.Marshal(successfulStartResponse)
					assert.NoError(t, err)
					w.Write(resp)
				}
			},
			exp: successfulStartResponse,
		},
		{
			name: "should have content-type application-json",
			testFn: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
					w.WriteHeader(http.StatusOK)
					resp, err := json.Marshal(successfulStartResponse)
					assert.NoError(t, err)
					w.Write(resp)
				}
			},
			exp: successfulStartResponse,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest(t, test)
		})
	}
}

func TestPollAutomatedRecoveryStatus(t *testing.T) {
	type testCase struct {
		name   string
		testFn func(t *testing.T) http.HandlerFunc
		exp    *TopicRecoveryStatus
	}

	pendingResponse := &TopicRecoveryStatus{
		State: "pending",
		TopicDownloads: []TopicDownloadCounts{
			{
				TopicNamespace:      "test-namespace",
				PendingDownloads:    1,
				SuccessfulDownloads: 0,
				FailedDownloads:     0,
			},
		},
		RecoveryRequest: RecoveryRequestParams{
			TopicNamesPattern: ".*",
		},
	}

	runTest := func(t *testing.T, test testCase) {
		server := httptest.NewServer(test.testFn(t))
		defer server.Close()

		client, err := NewAdminAPI([]string{server.URL}, BasicCredentials{}, nil)
		assert.NoError(t, err)

		resp, err := client.PollAutomatedRecoveryStatus(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, resp, test.exp)
	}

	tests := []testCase{
		{
			name: "should return TopicRecoveryStatus when server returns 200",
			testFn: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)

					respBodyBytes, err := json.Marshal(pendingResponse)
					assert.NoError(t, err)

					w.Write(respBodyBytes)
				}
			},
			exp: pendingResponse,
		},
		{
			name: "should call the correct endpoint",
			testFn: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "/v1/cloud_storage/automated_recovery", r.URL.Path)

					w.WriteHeader(http.StatusOK)

					resp, err := json.Marshal(pendingResponse)
					assert.NoError(t, err)

					w.Write(resp)
				}
			},
			exp: pendingResponse,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest(t, test)
		})
	}
}
