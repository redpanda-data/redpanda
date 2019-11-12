package cmd_test

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
	"vectorized/pkg/cli/cmd"

	"github.com/spf13/afero"
)

const validConfig string = `redpanda:
  data_directory: /var/lib/redpanda/data
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 127.0.0.1
    port: 9092
  node_id: 1
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
    node_id: 1
  - host:
      address: 127.0.0.1
      port: 33146
    node_id: 2
`

func TestTimeout(t *testing.T) {
	configPath := "/etc/redpanda/redpanda.yaml"
	expectedIotuneErrMsg := "exec: \"iotune\": executable file not found in $PATH"
	tests := []struct {
		name            string
		timeout         string
		duration        string
		expectedTimeout time.Duration
	}{
		{
			name:            "the total timeout should equal the duration + the timeout",
			timeout:         "1200ms",
			duration:        "3600",
			expectedTimeout: 3600*time.Second + 1200*time.Millisecond,
		},
		{
			name: "the total timeout should be the maximum by default" +
				"if no timeout is specified",
			timeout:         "",
			duration:        "3600",
			expectedTimeout: time.Duration(math.MaxInt64),
		},
		{
			name: "the total timeout should be the maximum" +
				"if the duration + the timeout surpasses the maximum",
			timeout:         time.Duration(math.MaxInt64).String(),
			duration:        "3600",
			expectedTimeout: time.Duration(math.MaxInt64),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			err := afero.WriteFile(fs, configPath, []byte(validConfig), 0644)
			if err != nil {
				t.Errorf("got an error creating the config file: %v", err)
			}
			cmd := cmd.NewIoTuneCmd(fs)
			args := []string{
				"--duration", tt.duration,
				"--redpanda-cfg", configPath,
			}
			if len(tt.timeout) > 0 {
				args = append(args, "--timeout", tt.timeout)
			}
			cmd.SetArgs(args)
			var out bytes.Buffer
			cmd.SetOut(&out)
			err = cmd.Execute()
			if err != nil && !strings.Contains(err.Error(), expectedIotuneErrMsg) {
				t.Errorf("got an error executing the command: %v", err)
			}
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				t.Errorf("got an error retrieving the timeout flag value: %v", err)
			}
			if tt.expectedTimeout != timeout {
				t.Errorf("expected:\n%v\ngot:\n%v", tt.expectedTimeout, timeout)
			}
		})
	}
}
