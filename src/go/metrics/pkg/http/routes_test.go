package http_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	vhttp "metrics/pkg/http"
	"metrics/pkg/storage"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockRepo struct {
	fail bool
}

func (r *mockRepo) SaveMetrics(_ storage.Metrics) error {
	if r.fail {
		return errors.New("repository error")
	}
	return nil
}

func TestMetricsHandler(t *testing.T) {
	tests := []struct {
		name             string
		path             string
		body             string
		expectedResponse string
		expectedStatus   int
		repo             *mockRepo
	}{
		{
			name: "it saves the metrics payload",
			path: "/",
			body: "{\"timestamp\": \"2020-02-05T15:13:44.586863092-05:00\"," +
				"\"organization\": \"vectorized\"," +
				"\"clusterId\": \"test\"," +
				"\"nodeId\": 1," +
				"\"nodeUuid\": \"123e4567-e89b-12d3-a456-426655440000\"," +
				"\"freeMemory\": 123456789," +
				"\"freeSpace\": 123456789," +
				"\"cpuPercentage\": 80}",
			expectedStatus: 200,
			repo:           &mockRepo{false},
		},
		{
			name:             "it fails if the body is not valid JSON",
			path:             "/",
			body:             "{\"timestamp\": \"2020-02-",
			expectedStatus:   400,
			expectedResponse: "unexpected end of JSON input",
			repo:             &mockRepo{false},
		},
		{
			name:           "it still works if any fields are null",
			path:           "/",
			body:           "{}",
			expectedStatus: 200,
			repo:           &mockRepo{false},
		},
		{
			name:           "it returns a 500 if the repo fails",
			path:           "/",
			body:           "{}",
			expectedStatus: 500,
			repo:           &mockRepo{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mh := &vhttp.MetricsHandler{tt.repo}
			eh := &vhttp.EnvHandler{tt.repo}
			server := &vhttp.Server{0, mh, eh}
			ts := httptest.NewServer(server)
			defer ts.Close()

			req, err := http.NewRequest(
				http.MethodPost,
				fmt.Sprintf("%s%s", ts.URL, tt.path),
				bytes.NewBuffer([]byte(tt.body)),
			)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{}
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf(
					"expected code '%d' but got '%d'",
					tt.expectedStatus,
					res.StatusCode,
				)
			}
			bs, err := ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if tt.expectedResponse != "" {
				if !strings.Contains(string(bs), tt.expectedResponse) {
					t.Errorf(
						"expected response '%s' but got '%s'",
						tt.expectedResponse,
						string(bs),
					)
				}
			}
		})
	}
}
