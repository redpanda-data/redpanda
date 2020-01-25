package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestSendMetrics(t *testing.T) {
	body := metricsBody{
		NodeUuid:     "asdfas-asdf2w23sd-907asdf",
		Organization: "io.vectorized",
		NodeId:       1,
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
