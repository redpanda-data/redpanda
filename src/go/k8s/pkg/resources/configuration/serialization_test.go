package configuration_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerde(t *testing.T) {
	conf := configuration.GlobalConfiguration{
		NodeConfiguration: config.Config{
			NodeUuid: "uuid",
		},
		ClusterConfiguration: map[string]interface{}{
			"a": "b",
		},
		Mode: configuration.GlobalConfigurationModeCentralized,
	}
	ser, err := conf.Serialize()
	require.NoError(t, err)
	conf2, err := ser.Deserialize(configuration.GlobalConfigurationModeCentralized)
	require.NoError(t, err)
	require.NotNil(t, conf2)
	assert.Equal(t, "uuid", conf2.NodeConfiguration.NodeUuid)
	assert.Equal(t, "b", conf2.ClusterConfiguration["a"])
	ser2, err := conf.Serialize()
	require.NoError(t, err)
	assert.YAMLEq(t, string(ser.RedpandaFile), string(ser2.RedpandaFile))
	assert.YAMLEq(t, string(ser.BootstrapFile), string(ser2.BootstrapFile))
}
