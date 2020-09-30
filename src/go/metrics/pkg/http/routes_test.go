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

func (r *mockRepo) SaveEnvironment(_ storage.Environment) error {
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

func TestEnvironmentHandler(t *testing.T) {
	tests := []struct {
		name             string
		body             string
		expectedResponse string
		expectedStatus   int
		repo             *mockRepo
	}{
		{
			name: "it saves the environment payload - version 0.99.8 and previous",
			body: `{
"timestamp": "2020-02-05T15:13:44.586863092-05:00",
"organization": "vectorized",
"clusterId": "test",
"nodeId": 1,
"nodeUuid": "123e4567-e89b-12d3-a456-426655440000",
"payload":{"checks":[{"name":"check 1","current":"0","required":"0"}],"tuners":[{"name":"tuner 1","enabled":true,"supported":true}]},"config":{"organization":"vectorized","cluster_id":"test","redpanda":{"data_directory":"/var/lib/redpanda/data","node_id":1,"seed_servers":[],"rpc_server":{"address":"0.0.0.0","port":33145},"kafka_api":{"address":"0.0.0.0","port":9092},"admin":{"address":"0.0.0.0","port":9644},"auto_create_topics_enabled":true},"rpk":{"enable_usage_stats":true,"tune_network":true,"tune_disk_scheduler":true,"tune_disk_nomerges":true,"tune_disk_irq":true,"tune_cpu":true,"tune_aio_events":true,"tune_clocksource":true,"tune_swappiness":true,"enable_memory_locking":false,"tune_coredump":false,"coredump_dir":"/var/lib/redpanda/coredump"}}
}`,
			expectedStatus: 200,
			repo:           &mockRepo{false},
		},
		{
			name: "it saves the environment payload - after version 0.99.8",
			body: `{"payload":{"checks":[{"name":"Clock Source","errorMsg":"","current":"kvm-clock","required":"tsc"},{"name":"Config file valid","errorMsg":"","current":"true","required":"true"},{"name":"Connections listen backlog size","errorMsg":"","current":"128","required":"\u003e= 4096"},{"name":"Data directory filesystem type","errorMsg":"","current":"xfs","required":"xfs"},{"name":"Data directory is writable","errorMsg":"","current":"true","required":"true"},{"name":"Data partition free space [GB]","errorMsg":"","current":"17.32","required":"\u003e= 10"},{"name":"Dir '/var/lib/redpanda/data' IRQs affinity set","errorMsg":"","current":"false","required":"true"},{"name":"Dir '/var/lib/redpanda/data' IRQs affinity static","errorMsg":"","current":"false","required":"true"},{"name":"Dir '/var/lib/redpanda/data' nomerges tuned","errorMsg":"","current":"false","required":"true"},{"name":"Dir '/var/lib/redpanda/data' scheduler tuned","errorMsg":"","current":"true","required":"true"},{"name":"Free memory per CPU [MB]","errorMsg":"","current":"2577","required":"2048 per CPU"},{"name":"Fstrim systemd service and timer active","errorMsg":"Rejected send message, 2 matched rules; type=\"method_call\", sender=\":1.72\" (uid=996 pid=5134 comm=\"/usr/bin/rpk start --check=true \") interface=\"org.freedesktop.systemd1.Manager\" member=\"ListUnitsByNames\" error name=\"(unset)\" requested_reply=\"0\" destination=\"org.freedesktop.systemd1\" (uid=0 pid=1 comm=\"/usr/lib/systemd/systemd --switched-root --system \")","current":"","required":"true"},{"name":"I/O config file present","errorMsg":"","current":"false","required":"true"},{"name":"Kernel Version","errorMsg":"kernel version is too old","current":"4.14.192-147.314.amzn2.x86_64","required":"4.19"},{"name":"Max AIO Events","errorMsg":"","current":"1048576","required":"\u003e= 1048576"},{"name":"Max syn backlog size","errorMsg":"","current":"2048","required":"\u003e= 4096"},{"name":"NIC IRQs affinity static","errorMsg":"","current":"false","required":"true"},{"name":"NIC eth0 IRQ affinity set","errorMsg":"","current":"false","required":"true"},{"name":"NIC eth0 RFS set","errorMsg":"","current":"false","required":"true"},{"name":"NIC eth0 RPS set","errorMsg":"","current":"false","required":"true"},{"name":"NIC eth0 XPS set","errorMsg":"","current":"false","required":"true"},{"name":"NTP Synced","errorMsg":"","current":"false","required":"true"},{"name":"RFS Table entries","errorMsg":"","current":"0","required":"\u003e= 32768"},{"name":"Swap enabled","errorMsg":"","current":"false","required":"true"},{"name":"Swappiness","errorMsg":"","current":"60","required":"1"},{"name":"Transparent huge pages active","errorMsg":"","current":"true","required":"true"}],"tuners":[],"errorMsg":""},"config":{"cluster_id":"","license_key":"","organization":"","redpanda":{"admin":{"address":"0.0.0.0","port":9644},"auto_create_topics_enabled":true,"data_directory":"/var/lib/redpanda/data","kafka_api":{"address":"0.0.0.0","port":9092},"node_id":1,"rpc_server":{"address":"0.0.0.0","port":33145},"seed_servers":[]},"rpk":{"coredump_dir":"/var/lib/redpanda/coredump","enable_memory_locking":false,"enable_usage_stats":true,"tls":{"cert_file":"","key_file":"","truststore_file":""},"tune_aio_events":false,"tune_clocksource":false,"tune_coredump":false,"tune_cpu":false,"tune_disk_irq":false,"tune_disk_nomerges":false,"tune_disk_scheduler":false,"tune_fstrim":false,"tune_network":false,"tune_swappiness":false}},
"sentAt":"2020-09-28T20:59:36.889931684Z",
"nodeUuid":"QWdaWVvoQeqhNDN7CXPC7pfcQX5EaYtM29BxVkSmnyeNQ1GVb",
"organization":"vectorized",
"clusterId":"test",
"nodeId":1,
"cloudVendor":"aws",
"vmType":"c5n.4xlarge",
"osInfo":"x86_64 4.14.192-147.314.amzn2.x86_64 #1 SMP Mon Aug 17 06:07:07 UTC 2020",
"cpuModel":"Intel(R) Xeon(R) Platinum 8124M CPU @ 3.00GHz",
"cpuCores":16,
"rpVersion":"release-0.99.8 (rev a2b48491)"
}`,
			expectedStatus: 200,
			repo:           &mockRepo{false},
		},
		{
			name:             "it fails if the body is not valid JSON",
			body:             `{"timestamp": "2020-02-`,
			expectedStatus:   400,
			expectedResponse: "unexpected end of JSON input",
			repo:             &mockRepo{false},
		},
		{
			name:           "it still works if any fields are null",
			body:           "{}",
			expectedStatus: 200,
			repo:           &mockRepo{false},
		},
		{
			name:           "it returns a 500 if the repo fails",
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
				fmt.Sprintf("%s%s", ts.URL, "/env"),
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
