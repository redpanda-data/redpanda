package redpanda

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
				SeastarFlags: map[string]string{
					"memory": "1G",
				},
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--memory=1G",
			},
		},
		{
			name: "shall include cpuset setting into command line",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				SeastarFlags: map[string]string{
					"cpuset": "0-1",
				},
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--cpuset=0-1",
			},
		},
		{
			name: "shall include io-properties-file setting into command line",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				SeastarFlags: map[string]string{
					"io-properties-file": "/etc/redpanda/io-config.yaml",
				},
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--io-properties-file=/etc/redpanda/io-config.yaml",
			},
		},
		{
			name: "shall include memory lock",
			args: RedpandaArgs{
				ConfigFilePath: "/etc/redpanda/redpanda.yaml",
				SeastarFlags: map[string]string{
					"lock-memory": fmt.Sprint(true),
				},
			},
			want: []string{
				"redpanda",
				"--redpanda-cfg",
				"/etc/redpanda/redpanda.yaml",
				"--lock-memory",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectRedpandaArgs(&tt.args)
			require.Exactly(t, tt.want, got)
		})
	}
}
