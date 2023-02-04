// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package configuration provide tools for the operator to manage cluster configuration
package configuration

import (
	"crypto/md5" //nolint:gosec // this is not encrypting secure info
	"fmt"
	"reflect"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

const (
	// useMixedConfiguration can be temporarily used until .boostrap.yaml is fully supported.
	useMixedConfiguration = true

	redpandaPropertyPrefix = "redpanda."
)

// knownNodeProperties is initialized with all well known node properties from the "redpanda" configuration tree.
var knownNodeProperties map[string]bool

// GlobalConfiguration is a configuration object that holds both node/local configuration and global configuration.
type GlobalConfiguration struct {
	NodeConfiguration    config.Config
	ClusterConfiguration map[string]interface{}
	Mode                 GlobalConfigurationMode
}

// For constructs a GlobalConfiguration for the given version of the cluster (considering feature gates).
func For(version string) *GlobalConfiguration {
	if featuregates.CentralizedConfiguration(version) {
		return &GlobalConfiguration{
			Mode: DefaultCentralizedMode(),
		}
	}
	// Use classic also when version is not present for some reason
	return &GlobalConfiguration{
		Mode: GlobalConfigurationModeClassic,
	}
}

// DefaultCentralizedMode determines the default strategy to use when centralized configuration is enabled
func DefaultCentralizedMode() GlobalConfigurationMode {
	// Use mixed config temporarily
	if useMixedConfiguration {
		return GlobalConfigurationModeMixed
	}
	return GlobalConfigurationModeCentralized
}

// SetAdditionalRedpandaProperty allows setting an unstructured redpanda property on the configuration.
// Depending on the mode, it will be put in node configuration, centralized configuration, or both.
func (c *GlobalConfiguration) SetAdditionalRedpandaProperty(
	key string, value interface{},
) {
	c.Mode.SetAdditionalRedpandaProperty(c, key, value)
}

// AppendToAdditionalRedpandaProperty allows appending values to string slices in additional redpanda properties.
//
//nolint:goerr113 // no need to define static error
func (c *GlobalConfiguration) AppendToAdditionalRedpandaProperty(
	key string, value string,
) error {
	val := c.GetAdditionalRedpandaProperty(key)
	valAsSlice, ok := val.([]string)
	if !ok && val != nil {
		return fmt.Errorf("property %q is not a string slice: %v", key, val)
	}
	valAsSlice = append(valAsSlice, value)
	c.SetAdditionalRedpandaProperty(key, valAsSlice)
	return nil
}

// SetAdditionalFlatProperties allows setting additional properties on the configuration from a key/value configuration format.
// Properties will be put in the right bucket (node and/or cluster) depending on the configuration mode.
func (c *GlobalConfiguration) SetAdditionalFlatProperties(
	props map[string]string,
) error {
	return c.Mode.SetAdditionalFlatProperties(c, props)
}

// GetCentralizedConfigurationHash computes a hash of the centralized configuration considering only the
// cluster properties that require a restart (this is why the schema is needed).
func (c *GlobalConfiguration) GetCentralizedConfigurationHash(
	schema admin.ConfigSchema,
) (string, error) {
	clone := *c

	// Ignore cluster properties that don't require restart
	clone.ClusterConfiguration = make(map[string]interface{})
	for k, v := range c.ClusterConfiguration {
		// Unknown properties should be ignored as they might be user errors
		if meta, ok := schema[k]; ok && meta.NeedsRestart {
			clone.ClusterConfiguration[k] = v
		}
	}
	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}
	// We keep using md5 for having the same format as node hash
	md5Hash := md5.Sum(serialized.BootstrapFile) //nolint:gosec // this is not encrypting secure info
	return fmt.Sprintf("%x", md5Hash), nil
}

// GetNodeConfigurationHash computes a hash of the node configuration considering only node properties
// but excluding fields that trigger unnecessary restarts.
func (c *GlobalConfiguration) GetNodeConfigurationHash() (string, error) {
	clone := *c
	// clean any cluster property from config before serializing
	clone.ClusterConfiguration = nil
	removeFieldsThatShouldNotTriggerRestart(&clone)
	props := clone.NodeConfiguration.Redpanda.Other
	clone.NodeConfiguration.Redpanda.Other = make(map[string]interface{})
	for k, v := range props {
		if isKnownNodeProperty(fmt.Sprintf("%s%s", redpandaPropertyPrefix, k)) {
			clone.NodeConfiguration.Redpanda.Other[k] = v
		}
	}
	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}
	md5Hash := md5.Sum(serialized.RedpandaFile) //nolint:gosec // this is not encrypting secure info
	return fmt.Sprintf("%x", md5Hash), nil
}

// GetFullConfigurationHash computes the hash of the full configuration, i.e., the plain
// "redpanda.yaml" file, with the exception of fields that trigger unnecessary restarts.
func (c *GlobalConfiguration) GetFullConfigurationHash() (string, error) {
	clone := *c
	removeFieldsThatShouldNotTriggerRestart(&clone)
	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}
	md5Hash := md5.Sum(serialized.RedpandaFile) //nolint:gosec // this is not encoding secure info
	return fmt.Sprintf("%x", md5Hash), nil
}

// Ignore seeds in the hash computation such that any seed changes do not
// trigger a rolling restart across the nodes. Similarly for pandaproxy and
// schema registry clients.
func removeFieldsThatShouldNotTriggerRestart(c *GlobalConfiguration) {
	c.NodeConfiguration.Redpanda.SeedServers = []config.SeedServer{}
	if c.NodeConfiguration.PandaproxyClient != nil {
		c.NodeConfiguration.PandaproxyClient.Brokers = []config.SocketAddress{}
	}
	if c.NodeConfiguration.SchemaRegistryClient != nil {
		c.NodeConfiguration.SchemaRegistryClient.Brokers = []config.SocketAddress{}
	}
}

// GetAdditionalRedpandaProperty retrieves a configuration option
func (c *GlobalConfiguration) GetAdditionalRedpandaProperty(
	prop string,
) interface{} {
	return c.Mode.GetAdditionalRedpandaProperty(c, prop)
}

func isKnownNodeProperty(prop string) bool {
	if v, ok := knownNodeProperties[prop]; ok {
		return v
	}
	for k := range knownNodeProperties {
		if strings.HasPrefix(prop, fmt.Sprintf("%s.", k)) {
			return true
		}
	}
	return false
}

func init() {
	knownNodeProperties = make(map[string]bool)

	// The assumption here is that all explicit fields of RedpandaNodeConfig are node properties
	cfg := reflect.TypeOf(config.RedpandaNodeConfig{})
	for i := 0; i < cfg.NumField(); i++ {
		tag := cfg.Field(i).Tag
		yamlTag := tag.Get("yaml")
		parts := strings.Split(yamlTag, ",")
		if len(parts) > 0 && len(parts[0]) > 0 {
			knownNodeProperties[fmt.Sprintf("redpanda.%s", parts[0])] = true
		}
	}
}
