package cmd_test

import (
	"fmt"
	"strings"
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

func TestSetSeedNodes(t *testing.T) {
	configPath := "/etc/redpanda/redpanda.yaml"
	tests := []struct {
		name      string
		hosts     []string
		ids       []string
		ports     []string
		args      []string
		expectErr bool
	}{
		{
			name:  "it should set the given seed nodes",
			hosts: []string{"127.0.0.0", "some.domain.com"},
		},
		{
			name:      "it should fail if no hosts are given",
			expectErr: true,
		},
		{
			name:  "it should succeed if there are enough ports",
			hosts: []string{"127.0.0.0", "some.domain.com"},
			ports: []string{"1234", "2345"},
		},
		{
			name:      "it should fail if there aren't enough ports",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ports:     []string{"1234"},
			expectErr: true,
		},
		{
			name:      "it should fail if there are more ports than hosts",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ports:     []string{"1234", "2345", "3456"},
			expectErr: true,
		},
		{
			name:  "it should succeed if there are enough ids",
			hosts: []string{"127.0.0.0", "some.domain.com"},
			ids:   []string{"1", "2"},
		},
		{
			name:      "it should fail if there aren't enough ids",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"1"},
			expectErr: true,
		},
		{
			name:      "it should fail if there are more ids than hosts",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"1", "2", "3"},
			expectErr: true,
		},
		{
			name:      "it should fail if the ids aren't numeric",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"not", "numbers"},
			expectErr: true,
		},
		{
			name:      "it should fail if the ids aren't ints",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"12e-10", "12.3"},
			expectErr: true,
		},
		{
			name:      "it should fail if the ports aren't numeric",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"not", "numbers"},
			expectErr: true,
		},
		{
			name:      "it should fail if the ports aren't ints",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
			ids:       []string{"12e-10", "12.3"},
			expectErr: true,
		},
		{
			name:      "it should fail if the given path to the config doesn't exist",
			hosts:     []string{"127.0.0.0", "some.domain.com"},
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
			args := []string{"set", "seed-nodes"}
			if len(tt.hosts) > 0 {
				hosts := strings.Join(tt.hosts, ",")
				args = append(args, "--hosts", hosts)
			}
			if len(tt.ids) > 0 {
				ids := strings.Join(tt.ids, ",")
				args = append(args, "--ids", ids)
			}
			if len(tt.ports) > 0 {
				ports := strings.Join(tt.ports, ",")
				args = append(args, "--ports", ports)
			}
			args = append(args, tt.args...)
			t.Log(args)
			c.SetArgs(append([]string{"set", "seed-nodes"}, args...))
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
			for i, h := range config.Redpanda.SeedServers {
				if fmt.Sprint(h.Host.Address) != tt.hosts[i] {
					t.Fatalf(
						"expected host '%s' but got '%s' for node #%d",
						tt.hosts[i],
						h.Host.Address,
						i,
					)
				}
				var expectedId string
				if len(tt.ids) != 0 {
					expectedId = tt.ids[i]
				} else {
					expectedId = fmt.Sprint(i)
				}
				if fmt.Sprint(h.Id) != expectedId {
					t.Fatalf(
						"expected node ID '%s' but got '%d' for node #%d",
						expectedId,
						h.Id,
						i,
					)
				}
				var expectedPort string
				if len(tt.ports) != 0 {
					expectedPort = fmt.Sprint(tt.ports[i])
				} else {
					expectedPort = "33145"
				}
				if fmt.Sprint(h.Host.Port) != expectedPort {
					t.Fatalf(
						"expected port '%s' but got '%d' for node #%d",
						expectedPort,
						h.Host.Port,
						i,
					)
				}
			}
		})
	}
}
