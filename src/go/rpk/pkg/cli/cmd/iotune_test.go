// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
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
		name                string
		args                []string
		expectedDirectories []string
		expectedTimeout     time.Duration
		expectedDuration    time.Duration
	}{
		{
			name:                "the total timeout should equal the duration + the timeout",
			args:                []string{"--timeout", "1200ms", "--duration", "3600ms"},
			expectedDirectories: []string{},
			expectedTimeout:     3600*time.Millisecond + 1200*time.Millisecond,
			expectedDuration:    3600 * time.Millisecond,
		},
		{
			name:                "the total timeout should be 1hr by default",
			args:                []string{"--duration", "0"},
			expectedDirectories: []string{},
			expectedTimeout:     1 * time.Hour,
			expectedDuration:    0,
		},
		{
			name:                "the default duration should be 10m",
			expectedDirectories: []string{},
			expectedTimeout:     10*time.Minute + 1*time.Hour,
			expectedDuration:    10 * time.Minute,
		},
		{
			name:                "it should capture the passed directories",
			args:                []string{"--directories", "/mnt/redpanda/data"},
			expectedDirectories: []string{"/mnt/redpanda/data"},
			expectedTimeout:     10*time.Minute + 1*time.Hour,
			expectedDuration:    10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := config.NewManager(fs)
			err := afero.WriteFile(fs, configPath, []byte(validConfig), 0644)
			require.NoError(t, err)
			cmd := cmd.NewIoTuneCmd(fs, mgr)
			args := []string{"--config", configPath}
			args = append(args, tt.args...)
			cmd.SetArgs(args)
			var out bytes.Buffer
			cmd.SetOut(&out)
			err = cmd.Execute()
			// It always fails, because iotune or libs such as cryptopp aren't found.
			if err != nil {
				t.Log(err)
			}
			directories, err := cmd.Flags().GetStringSlice("directories")
			require.NoError(t, err)
			require.Exactly(t, tt.expectedDirectories, directories)
			timeout, err := cmd.Flags().GetDuration("timeout")
			require.NoError(t, err, "got an error retrieving the timeout flag value")
			require.Exactly(t, tt.expectedTimeout, timeout)
			duration, err := cmd.Flags().GetDuration("duration")
			require.NoError(t, err)
			require.Exactly(t, tt.expectedDuration, duration)
		})
	}
}
