package cmd_test

import (
	"bytes"
	"testing"
	"time"
	"vectorized/pkg/cli/cmd"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
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
		expectedDuration time.Duration
	}{
		{
			name:             "the total timeout should equal the duration + the timeout",
			timeout:          "1200ms",
			duration:         "3600ms",
			expectedTimeout:  3600*time.Millisecond + 1200*time.Millisecond,
			expectedDuration: 3600 * time.Millisecond,
		},
		{
			name:             "the total timeout should be 1hr by default",
			timeout:          "",
			duration:         "0",
			expectedTimeout:  1 * time.Hour,
			expectedDuration: 0,
		},
		{
			name:             "the default duration should be 10m",
			timeout:          "",
			duration:         "",
			expectedTimeout:  10*time.Minute + 1*time.Hour,
			expectedDuration: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			err := afero.WriteFile(fs, configPath, []byte(validConfig), 0644)
			require.NoError(t, err)
			cmd := cmd.NewIoTuneCmd(fs)
			args := []string{"--config", configPath}
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
			// It always fails, because iotune or libs such as cryptopp aren't found.
			if err != nil {
				t.Log(err)
			}
			timeout, err := cmd.Flags().GetDuration("timeout")
			require.NoError(t, err, "got an error retrieving the timeout flag value")
			require.Exactly(t, tt.expectedTimeout, timeout)
			duration, err := cmd.Flags().GetDuration("duration")
			require.NoError(t, err)
			require.Exactly(t, tt.expectedDuration, duration)
		})
	}
}
