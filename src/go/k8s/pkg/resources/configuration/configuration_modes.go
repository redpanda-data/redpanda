// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configuration

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// GlobalConfigurationMode changes the behavior of the global configuration when reading/writing properties.
type GlobalConfigurationMode interface {
	// SetAdditionalRedpandaProperty can add or remove provided key.
	// To remove `key` the `value` need to be on of:
	// * empty string
	// * nil pointer
	// * empty slice
	SetAdditionalRedpandaProperty(targetConfig *GlobalConfiguration, key string, value interface{})
	GetAdditionalRedpandaProperty(targetConfig *GlobalConfiguration, key string) interface{}
	SetAdditionalFlatProperties(targetConfig *GlobalConfiguration, props map[string]string) error
}

var (
	// GlobalConfigurationModeClassic puts everything in "redpanda.yaml"
	GlobalConfigurationModeClassic GlobalConfigurationMode = globalConfigurationModeClassic{}
	// GlobalConfigurationModeCentralized splits properties between "redpanda.yaml" and ".bootstrap.yaml"
	GlobalConfigurationModeCentralized GlobalConfigurationMode = globalConfigurationModeCentralized{}
	// GlobalConfigurationModeMixed puts central configuration properties in both "redpanda.yaml" and ".bootstrap.yaml"
	GlobalConfigurationModeMixed GlobalConfigurationMode = globalConfigurationModeMixed{}
)

// globalConfigurationModeClassic provides classic configuration rules
type globalConfigurationModeClassic struct{}

func (r globalConfigurationModeClassic) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	if targetConfig.NodeConfiguration.Redpanda.Other == nil {
		targetConfig.NodeConfiguration.Redpanda.Other = make(map[string]interface{})
	}
	if !isEmpty(value) {
		targetConfig.NodeConfiguration.Redpanda.Other[key] = value
	} else {
		delete(targetConfig.NodeConfiguration.Redpanda.Other, key)
	}
}

func (r globalConfigurationModeClassic) GetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string,
) interface{} {
	return targetConfig.NodeConfiguration.Redpanda.Other[key]
}

func (r globalConfigurationModeClassic) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	// Add arbitrary parameters to configuration
	for k, v := range props {
		if builtInType(v) {
			err := targetConfig.NodeConfiguration.Set(k, v, "")
			if err != nil {
				return fmt.Errorf("setting built-in type: %w", err)
			}
		} else {
			err := targetConfig.NodeConfiguration.Set(k, v, "")
			if err != nil {
				return fmt.Errorf("setting complex type: %w", err)
			}
		}
	}
	return nil
}

// globalConfigurationModeCentralized provides centralized configuration rules
type globalConfigurationModeCentralized struct{}

func (r globalConfigurationModeCentralized) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	if targetConfig.ClusterConfiguration == nil {
		targetConfig.ClusterConfiguration = make(map[string]interface{})
	}
	if !isEmpty(value) {
		targetConfig.ClusterConfiguration[key] = value
	} else {
		delete(targetConfig.ClusterConfiguration, key)
	}
}

func (r globalConfigurationModeCentralized) GetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string,
) interface{} {
	return targetConfig.ClusterConfiguration[key]
}

func (r globalConfigurationModeCentralized) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	remaining := make(map[string]string, len(props))
	for key, value := range props {
		if nodeProp := isKnownNodeProperty(key); !nodeProp && strings.HasPrefix(key, redpandaPropertyPrefix) {
			newKey := strings.TrimPrefix(key, redpandaPropertyPrefix)
			if targetConfig.ClusterConfiguration == nil {
				targetConfig.ClusterConfiguration = make(map[string]interface{})
			}
			targetConfig.ClusterConfiguration[newKey] = value
		} else {
			remaining[key] = value
		}
	}
	if len(remaining) > 0 {
		return GlobalConfigurationModeClassic.SetAdditionalFlatProperties(targetConfig, remaining)
	}
	return nil
}

// globalConfigurationModeMixed provides mixed configuration rules
type globalConfigurationModeMixed struct{}

func (r globalConfigurationModeMixed) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	GlobalConfigurationModeClassic.SetAdditionalRedpandaProperty(targetConfig, key, value)
	GlobalConfigurationModeCentralized.SetAdditionalRedpandaProperty(targetConfig, key, value)
}

func (r globalConfigurationModeMixed) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	// We put unknown properties in both buckets e.g. during upgrades
	if err := GlobalConfigurationModeClassic.SetAdditionalFlatProperties(targetConfig, props); err != nil {
		return err
	}
	return GlobalConfigurationModeCentralized.SetAdditionalFlatProperties(targetConfig, props)
}

func (r globalConfigurationModeMixed) GetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string,
) interface{} {
	if v := GlobalConfigurationModeCentralized.GetAdditionalRedpandaProperty(targetConfig, key); v != nil {
		return v
	}
	return GlobalConfigurationModeClassic.GetAdditionalRedpandaProperty(targetConfig, key)
}

func builtInType(value string) bool {
	if _, err := strconv.Atoi(value); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseBool(value); err == nil {
		return true
	}
	return false
}

// isEmpty helps to keep the "omitempty" behavior on additional fields
//
//nolint:exhaustive // just care about these types
func isEmpty(val interface{}) bool {
	if val == nil {
		return true
	}
	refVal := reflect.ValueOf(val)
	switch refVal.Kind() {
	case reflect.String:
		return refVal.Len() == 0
	case reflect.Slice:
		return refVal.Len() == 0
	case reflect.Ptr:
		if refVal.IsNil() {
			return true
		}
		pointed := refVal.Elem()
		if pointed.Kind() == reflect.String {
			return pointed.Len() == 0
		}
		return false
	default:
		return false
	}
}
