// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configuration_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	rpkcfg "github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigMode(t *testing.T) {
	config := configuration.For("v22.1.1-test")
	assert.Equal(t, config.Mode, configuration.DefaultCentralizedMode())
	config = configuration.For("v21.1.1-test")
	assert.Equal(t, config.Mode, configuration.GlobalConfigurationModeClassic)
}

func TestRedpandaProperties(t *testing.T) {
	config := configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeCentralized}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.NotContains(t, config.NodeConfiguration.Redpanda.Other, "a")

	config = configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeClassic}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.NotContains(t, config.ClusterConfiguration, "a")
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])

	config = configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeMixed}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
}

func TestFlatProperties(t *testing.T) {
	config := configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeCentralized}
	err := config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUUID)
	assert.NotContains(t, config.NodeConfiguration.Redpanda.Other, "a")

	config = configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeClassic}
	err = config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUUID)
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
	assert.NotContains(t, config.ClusterConfiguration, "a")

	config = configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeMixed}
	err = config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUUID)
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
}

func TestKnownNodeProperties(t *testing.T) {
	config := configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeCentralized}
	require.NoError(t, config.SetAdditionalFlatProperties(map[string]string{
		"redpanda.cloud_storage_cache_directory": "/tmp",
		"redpanda.rpc_server.port":               "8080",
		"redpanda.cloud_storage_region":          "us-west-1",
	}))
	assert.Equal(t, "/tmp", config.NodeConfiguration.Redpanda.CloudStorageCacheDirectory)
	assert.Equal(t, 8080, config.NodeConfiguration.Redpanda.RPCServer.Port)
	assert.Len(t, config.ClusterConfiguration, 1)
	assert.Equal(t, "us-west-1", config.ClusterConfiguration["cloud_storage_region"])
}

func TestDeleteProperties(t *testing.T) {
	config := configuration.GlobalConfiguration{Mode: configuration.GlobalConfigurationModeCentralized}
	config.SetAdditionalRedpandaProperty("a1", "x")
	config.SetAdditionalRedpandaProperty("a2", "x")
	config.SetAdditionalRedpandaProperty("a3", "x")
	config.SetAdditionalRedpandaProperty("a4", "x")
	config.SetAdditionalRedpandaProperty("a5", "x")
	config.SetAdditionalRedpandaProperty("a6", "x")
	config.SetAdditionalRedpandaProperty("b", "y")
	assert.Len(t, config.ClusterConfiguration, 7)
	config.SetAdditionalRedpandaProperty("a1", nil)
	config.SetAdditionalRedpandaProperty("a2", "")
	var nilPtr *string
	config.SetAdditionalRedpandaProperty("a3", nilPtr)
	var nilInterfacePtr interface{} = nilPtr
	config.SetAdditionalRedpandaProperty("a4", nilInterfacePtr)
	var nilInterface interface{}
	config.SetAdditionalRedpandaProperty("a5", nilInterface)
	var nilSlice []string
	config.SetAdditionalRedpandaProperty("a6", nilSlice)
	assert.Len(t, config.ClusterConfiguration, 1)
	assert.Equal(t, "y", config.ClusterConfiguration["b"])
}

func TestStringSliceProperties(t *testing.T) {
	tests := []configuration.GlobalConfigurationMode{
		configuration.GlobalConfigurationModeClassic,
		configuration.GlobalConfigurationModeCentralized,
		configuration.GlobalConfigurationModeMixed,
	}
	for i, mode := range tests {
		func(m configuration.GlobalConfigurationMode) bool {
			return t.Run(fmt.Sprintf("test property slices %d", i), func(t *testing.T) {
				t.Parallel()

				config := configuration.GlobalConfiguration{Mode: m}
				assert.NoError(t, config.AppendToAdditionalRedpandaProperty("superusers", "a"))
				assert.NoError(t, config.AppendToAdditionalRedpandaProperty("superusers", "b"))
				assert.NoError(t, config.AppendToAdditionalRedpandaProperty("superusers", "c"))
				assert.Equal(t, []string{"a", "b", "c"}, config.GetAdditionalRedpandaProperty("superusers"))

				config.SetAdditionalRedpandaProperty("superusers", "nonslice")
				assert.Error(t, config.AppendToAdditionalRedpandaProperty("superusers", "value"))
			})
		}(mode)
	}
}

func TestHash_FieldsWithNoHashChange(t *testing.T) {
	cfg := configuration.For("v22.1.1-test")
	cfg.NodeConfiguration.Redpanda.SeedServers = []rpkcfg.SeedServer{}
	cfg.NodeConfiguration.PandaproxyClient = &rpkcfg.KafkaClient{Brokers: []rpkcfg.SocketAddress{}}
	cfg.NodeConfiguration.SchemaRegistryClient = &rpkcfg.KafkaClient{Brokers: []rpkcfg.SocketAddress{}}
	nodeConfHash, err := cfg.GetNodeConfigurationHash()
	require.NoError(t, err)
	allConfHash, err := cfg.GetFullConfigurationHash()
	require.NoError(t, err)

	cfg.NodeConfiguration.Redpanda.SeedServers = []rpkcfg.SeedServer{{Host: rpkcfg.SocketAddress{Address: "redpanda.com", Port: 9090}}}
	cfg.NodeConfiguration.PandaproxyClient = &rpkcfg.KafkaClient{Brokers: []rpkcfg.SocketAddress{{Address: "redpanda.com", Port: 9091}}}
	cfg.NodeConfiguration.SchemaRegistryClient = &rpkcfg.KafkaClient{Brokers: []rpkcfg.SocketAddress{{Address: "redpanda.com", Port: 9092}}}
	nodeConfHashNew, err := cfg.GetNodeConfigurationHash()
	require.NoError(t, err)
	allConfHashNew, err := cfg.GetFullConfigurationHash()
	require.NoError(t, err)

	// seed servers, pandaproxy clients, and schema registry clients should not affect the
	// hash, so rolling restarts do not take place, e.g., when scaling out/in a cluster.
	require.Equal(t, allConfHash, allConfHashNew, "all conf")
	require.Equal(t, nodeConfHash, nodeConfHashNew, "node conf")
}
