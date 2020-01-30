package cmd_test

import (
	"bytes"
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

func TestTimeoutDuration(t *testing.T) {
	configPath := "/etc/redpanda/redpanda.yaml"
	tests := []struct {
		name             string
		timeout          string
		duration         string
		expectedTimeout  time.Duration
		expectedDuration int
	}{
		{
			name:             "the total timeout should equal the duration + the timeout",
			timeout:          "1200ms",
			duration:         "3600",
			expectedTimeout:  3600*time.Second + 1200*time.Millisecond,
			expectedDuration: 3600,
		},
		{
			name:             "the total timeout should be 1hr by default",
			timeout:          "",
			duration:         "0",
			expectedTimeout:  1 * time.Hour,
			expectedDuration: 0,
		},
		{
			name:             "the default duration should be 30s",
			timeout:          "",
			duration:         "",
			expectedTimeout:  30*time.Second + 1*time.Hour,
			expectedDuration: 30,
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
			args := []string{"--redpanda-cfg", configPath}
			if len(tt.duration) > 0 {
				args = append(args, "--duration", tt.duration)
			}
			if len(tt.timeout) > 0 {
				args = append(args, "--timeout", tt.timeout)
			}
			cmd.SetArgs(args)
			var out bytes.Buffer
			cmd.SetOut(&out)
			err = cmd.Execute()
			if err != nil {
				t.Logf("got an error executing the command: %v", err)
			}
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				t.Errorf("got an error retrieving the timeout flag value: %v", err)
			}
			if tt.expectedTimeout != timeout {
				t.Errorf("expected timeout:\n%v\ngot:\n%v", tt.expectedTimeout, timeout)
			}
			duration, err := cmd.Flags().GetInt("duration")
			if err != nil {
				t.Errorf("got an error retrieving the duration flag value: %v", err)
			}
			if tt.expectedDuration != duration {
				t.Errorf("expected duration:\n%v\ngot:\n%v", tt.expectedDuration, duration)
			}
		})
	}
}
