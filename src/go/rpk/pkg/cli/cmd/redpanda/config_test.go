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
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBootstrap(t *testing.T) {
	defaultRPCPort := config.Default().Redpanda.RPCServer.Port
	tests := []struct {
		name           string
		ips            []string
		expSeedServers []config.SeedServer
		self           string
		id             string
		expectedErr    string
	}{
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
			_, err = fs.Stat(config.Default().ConfigFile)
			require.NoError(t, err)
			conf, err := new(config.Params).Load(fs)
			require.NoError(t, err)

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
			c := config.Default()
			if test.prevID != "" {
				c.NodeUUID = test.prevID
			}

			bs, err := yaml.Marshal(c)
			require.NoError(t, err)
			err = afero.WriteFile(fs, c.ConfigFile, bs, 0o644)
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
