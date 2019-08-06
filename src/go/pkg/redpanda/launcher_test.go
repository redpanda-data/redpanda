package redpanda

import (
	"reflect"
	"testing"
)

func Test_collectRedpandaArgs(t *testing.T) {

	tests := []struct {
		name string
		args RedpandaArgs
		want []string
	}{
		{
			name: "shall include config file path into args",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
			},
		},
		{
			name: "shall include memory setting into command line",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				Memory:         "1G",
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--memory",
				"1G",
			},
		},
		{
			name: "shall include cpuset setting into command line",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				CpuSet:         "0-1",
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--cpuset",
				"0-1",
			},
		},
		{
			name: "shall include io-properties-file setting into command line",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				IoConfigFile:   "/etc/redpanda/io-config.yaml",
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--io-properties-file",
				"/etc/redpanda/io-config.yaml",
			},
		},
		{
			name: "shall include memory lock",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				LockMemory:     true,
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--lock-memory",
				"1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collectRedpandaArgs(&tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("collectRedpandaArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}
