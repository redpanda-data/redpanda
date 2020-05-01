package cmd

import (
	"bytes"
	"testing"
	"vectorized/pkg/config"
	"vectorized/pkg/utils"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestMergeFlags(t *testing.T) {
	tests := []struct {
		name      string
		current   map[string]string
		overrides []string
		expected  map[string]string
	}{
		{
			name:      "it should override the existent values",
			current:   map[string]string{"a": "true", "b": "2", "c": "127.0.0.1"},
			overrides: []string{"--a false", "b 42"},
			expected:  map[string]string{"a": "false", "b": "42", "c": "127.0.0.1"},
		}, {
			name:    "it should override the existent values (2)",
			current: map[string]string{"lock-memory": "true", "cpumask": "0-1", "logger-log-level": "'exception=debug'"},
			overrides: []string{"--overprovisioned", "--unsafe-bypass-fsync 1",
				"--default-log-level=trace", "--logger-log-level='exception=debug'",
				"--fail-on-abandoned-failed-futures"},
			expected: map[string]string{
				"lock-memory":                        "true",
				"cpumask":                            "0-1",
				"logger-log-level":                   "'exception=debug'",
				"overprovisioned":                    "",
				"unsafe-bypass-fsync":                "1",
				"default-log-level":                  "trace",
				"--fail-on-abandoned-failed-futures": "",
			},
		}, {
			name:      "it should create values not present in the current flags",
			current:   map[string]string{},
			overrides: []string{"b 42", "c 127.0.0.1"},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		}, {
			name:      "it shouldn't change the current flags if no overrides are given",
			current:   map[string]string{"b": "42", "c": "127.0.0.1"},
			overrides: []string{},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := mergeFlags(tt.current, tt.overrides)
			require.Equal(t, len(flags), len(tt.expected))
			if len(flags) != len(tt.expected) {
				t.Fatal("the flags dicts differ in size")
			}

			for k, v := range flags {
				require.Equal(t, tt.expected[k], v)
			}
		})
	}

}

func TestFailExistingPID(t *testing.T) {
	fs := afero.NewMemMapFs()
	var out bytes.Buffer
	conf := config.DefaultConfig()
	_, err := utils.WriteBytes(fs, []byte("1234"), conf.PidFile)
	require.NoError(t, err)

	logrus.SetOutput(&out)
	cmd := NewStartCommand(fs)
	cmd.SetOutput(&out)
	cmd.SetArgs([]string{"--install-dir", "/opt/install"})
	err = cmd.Execute()

	errMsg := "couldn't write the PID file: found an existing PID in" +
		" '/var/lib/redpanda/pid'. Please stop the current redpanda" +
		" instance with 'systemctl stop redpanda' or 'rpk stop'."
	require.EqualError(t, err, errMsg)
}
