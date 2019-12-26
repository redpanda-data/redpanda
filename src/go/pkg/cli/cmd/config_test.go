package cmd_test

import (
	"fmt"
	"testing"
	"vectorized/pkg/cli/cmd"
	"vectorized/pkg/redpanda"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func getValidConfig() redpanda.Config {
	return redpanda.Config{
		Redpanda: &redpanda.RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: redpanda.SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: redpanda.SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*redpanda.SeedServer{
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
	}
}

func TestSetId(t *testing.T) {
	configPath := "/etc/redpanda/redpanda.yaml"
	tests := []struct {
		name      string
		id        string
		args      []string
		expectErr bool
	}{
		{
			name: "it should set the given id",
			id:   "2",
		},
		{
			name:      "it should fail if no id is given",
			expectErr: true,
		},
		{
			name:      "it should fail if the id isn't numeric",
			id:        "nope",
			expectErr: true,
		},
		{
			name:      "it should fail if the id is not an int",
			id:        "12.4",
			expectErr: true,
		},
		{
			name:      "it should fail if the given path to the config doesn't exist",
			id:        "12",
			args:      []string{"--redpanda-cfg", "/does/not/exist.yml"},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			bs, err := yaml.Marshal(getValidConfig())
			if err != nil {
				t.Error(err.Error())
			}
			err = afero.WriteFile(fs, configPath, bs, 0644)
			if err != nil {
				t.Error(err.Error())
			}

			c := cmd.NewConfigCommand(fs)
			c.SetArgs(append([]string{"set", "id", tt.id}, tt.args...))
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
			config, err := redpanda.ReadConfigFromPath(fs, configPath)
			if err != nil {
				t.Fatalf("got an unexpected error while reading %s: %v", configPath, err)
			}
			if fmt.Sprint(config.Redpanda.Id) != tt.id {
				t.Errorf("got %v, expected %v", config.Redpanda.Id, tt.id)
			}
		})
	}
}
