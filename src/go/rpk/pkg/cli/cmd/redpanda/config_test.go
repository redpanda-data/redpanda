// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"strconv"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBootstrap(t *testing.T) {
	defaultRPCPort := config.DevDefault().Redpanda.RPCServer.Port
	tests := []struct {
		name           string
		ips            []string
		expSeedServers []config.SeedServer
		self           string
		id             string
		expectedErr    string
	}{
		{
			name: "it should omit node id without errors",
			self: "192.168.34.5",
		},
		{
			name: "it should set the root node config for a single node",
			id:   "1",
			self: "192.168.34.5",
		},
		{
			name: "it should fill the seed servers",
			ips:  []string{"187.89.76.3", "192.168.34.5", "192.168.45.8"},
			expSeedServers: []config.SeedServer{
				{
					Host: config.SocketAddress{
						Address: "187.89.76.3",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "192.168.34.5",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "192.168.45.8",
						Port:    defaultRPCPort,
					},
				},
			},
			self: "192.168.34.5",
			id:   "1",
		},
		{
			name: "it should fill the seed servers with hostnames",
			ips:  []string{"187.89.76.3", "localhost", "redpanda.com", "test-url.net", "187.89.76.3:80"},
			expSeedServers: []config.SeedServer{
				{
					Host: config.SocketAddress{
						Address: "187.89.76.3",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "localhost",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "redpanda.com",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "test-url.net",
						Port:    defaultRPCPort,
					},
				},
				{
					Host: config.SocketAddress{
						Address: "187.89.76.3",
						Port:    80,
					},
				},
			},
			self: "192.168.34.5",
			id:   "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			c := bootstrap(fs)
			var args []string
			if len(tt.ips) != 0 {
				args = append(
					args,
					"--ips",
					strings.Join(tt.ips, ","),
				)
			}
			if tt.self != "" {
				args = append(args, "--self", tt.self)
			}
			if tt.id != "" {
				args = append(args, "--id", tt.id)
			}
			c.SetArgs(args)
			err := c.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			_, err = fs.Stat(config.DefaultPath)
			require.NoError(t, err)
			conf, err := new(config.Params).Load(fs)
			require.NoError(t, err)

			if tt.id != "" {
				require.NotNil(t, conf.Redpanda.ID)
				id, err := strconv.Atoi(tt.id)
				require.Nil(t, err)
				require.Equal(t, id, *conf.Redpanda.ID)
			} else {
				require.Nil(t, conf.Redpanda.ID)
			}

			require.Equal(t, tt.self, conf.Redpanda.RPCServer.Address)
			require.Equal(t, tt.self, conf.Redpanda.KafkaAPI[0].Address)
			require.Equal(t, tt.self, conf.Redpanda.AdminAPI[0].Address)
			if len(tt.ips) == 1 {
				require.Equal(
					t,
					[]*config.SeedServer{},
					conf.Redpanda.SeedServers,
				)
				return
			}
			require.ElementsMatch(t, tt.expSeedServers, conf.Redpanda.SeedServers)
			require.Contains(t, conf.Redpanda.Other, "empty_seed_starts_cluster")
			require.Equal(t, false, conf.Redpanda.Other["empty_seed_starts_cluster"])
		})
	}
}

func TestInitNode(t *testing.T) {
	for _, test := range []struct {
		name   string
		prevID string
	}{
		{name: "without UUID"},
		{name: "with UUID", prevID: "my_id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			c := config.DevDefault()
			if test.prevID != "" {
				c.NodeUUID = test.prevID
			}

			bs, err := yaml.Marshal(c)
			require.NoError(t, err)
			err = afero.WriteFile(fs, config.DefaultPath, bs, 0o644)
			require.NoError(t, err)

			cmd := initNode(fs)
			err = cmd.Execute()
			require.NoError(t, err)

			conf, err := new(config.Params).Load(fs)
			require.NoError(t, err)

			if test.prevID != "" {
				require.Exactly(t, conf.NodeUUID, test.prevID)
			} else {
				require.NotEmpty(t, conf.NodeUUID)
			}
		})
	}
}

// This is a top level command test, individual cases for set are
// tested in 'rpk/pkg/config/config_test.go'.
func TestSetCommand(t *testing.T) {
	for _, test := range []struct {
		name    string
		cfgFile string
		exp     string
		args    []string
	}{
		{
			name: "set without config file on disk",
			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    rack: redpanda-rack
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    coredump_dir: /var/lib/redpanda/coredump
    overprovisioned: true
pandaproxy: {}
schema_registry: {}
`,
			args: []string{"redpanda.rack", "redpanda-rack"},
		},
		{
			name: "set with loaded config",
			cfgFile: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    tune_network: true
    tune_disk_scheduler: true
`,
			exp: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    enable_usage_stats: true
    tune_network: true
    tune_disk_scheduler: true
`,
			args: []string{"rpk.enable_usage_stats", "true"},
		},
	} {
		fs := afero.NewMemMapFs()

		// We create a config file in default redpanda location
		if test.cfgFile != "" {
			err := afero.WriteFile(fs, "/etc/redpanda/redpanda.yaml", []byte(test.cfgFile), 0o644)
			if err != nil {
				t.Errorf("unexpected failure writing passed config file: %v", err)
			}
		}

		c := set(fs)
		c.SetArgs(test.args)
		err := c.Execute()
		if err != nil {
			t.Errorf("error during command execution: %v", err)
		}

		// Read back from that default location and compare.
		file, err := afero.ReadFile(fs, "/etc/redpanda/redpanda.yaml")
		if err != nil {
			t.Errorf("unexpected failure reading config file: %v", err)
		}
		require.Equal(t, test.exp, string(file))
	}
}
