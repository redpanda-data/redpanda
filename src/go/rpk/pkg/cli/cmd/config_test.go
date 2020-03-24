package cmd_test

import (
	"reflect"
	"testing"
	"vectorized/pkg/cli/cmd"
	"vectorized/pkg/config"

	"github.com/spf13/afero"
	"github.com/spf13/viper"
)

func TestSet(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		args      []string
		expected  interface{}
		expectErr bool
	}{
		{
			name:     "it should set single integer fields",
			key:      "redpanda.node_id",
			value:    "54312",
			expected: 54312,
		},
		{
			name:     "it should set single float fields",
			key:      "redpanda.float_field",
			value:    "42.3",
			expected: 42.3,
		},
		{
			name:     "it should set single string fields",
			key:      "redpanda.data_directory",
			value:    "'/var/lib/differentdir'",
			expected: "'/var/lib/differentdir'",
		},
		{
			name:     "it should set single bool fields",
			key:      "rpk.enable_usage_stats",
			value:    "true",
			expected: true,
		},
		{
			name:  "it should partially set map fields (yaml)",
			key:   "rpk",
			value: `tune_disk_irq: false`,
			args:  []string{"--format", "yaml"},
			expected: map[string]interface{}{
				"enable_usage_stats":    true,
				"tune_network":          true,
				"tune_disk_scheduler":   true,
				"tune_disk_nomerges":    true,
				"tune_disk_irq":         false,
				"tune_cpu":              true,
				"tune_aio_events":       true,
				"tune_clocksource":      true,
				"tune_swappiness":       true,
				"enable_memory_locking": true,
				"tune_coredump":         false,
				"coredump_dir":          "/var/lib/redpanda/coredump",
			},
		},
		{
			name: "it should partially set map fields (json)",
			key:  "redpanda.kafka_api",
			value: `{
  "address": "192.168.54.2"
}`,
			args: []string{"--format", "json"},
			expected: map[string]interface{}{
				"address": "192.168.54.2",
				"port":    9092,
			},
		},
		{
			name:      "it should fail if the new value is invalid",
			key:       "redpanda",
			value:     `{"data_directory": ""}`,
			args:      []string{"--format", "json"},
			expectErr: true,
		},
		{
			name:      "it should fail if the value isn't well formatted (json)",
			key:       "redpanda",
			value:     `{"seed_servers": []`,
			args:      []string{"--format", "json"},
			expectErr: true,
		},
		{
			name: "it should fail if the value isn't well formatted (yaml)",
			key:  "redpanda",
			value: `seed_servers:
- host:
  address: "123.`,
			args:      []string{"--format", "yaml"},
			expectErr: true,
		},
		{
			name:      "it should fail if the format isn't supported",
			key:       "redpanda",
			value:     `node_id=1`,
			args:      []string{"--format", "toml"},
			expectErr: true,
		},
		{
			name:      "it should fail if no key is passed",
			value:     `node_id=1`,
			expectErr: true,
		},
		{
			name:      "it should fail if no value is passed",
			key:       "rpk.tune_coredump",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			conf := config.DefaultConfig()
			err := config.WriteConfig(fs, &conf, conf.ConfigFile)
			if err != nil {
				t.Error(err.Error())
			}

			c := cmd.NewConfigCommand(fs)
			args := []string{"set"}
			if tt.key != "" {
				args = append(args, tt.key)
			}
			if tt.value != "" {
				args = append(args, tt.value)
			}
			c.SetArgs(append(args, tt.args...))
			err = c.Execute()
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("got an unexpected error: %v", err.Error())
			}
			v := viper.New()
			v.SetFs(fs)
			v.SetConfigType("yaml")
			v.SetConfigFile(conf.ConfigFile)
			err = v.ReadInConfig()
			if err != nil {
				t.Fatal(err)
			}
			val := v.Get(tt.key)
			if !reflect.DeepEqual(val, tt.expected) {
				t.Fatalf(
					"expected: \n'%+v'\n but got: \n'%+v'\n for key '%s'",
					tt.expected,
					val,
					tt.key,
				)
			}
		})
	}
}
