// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package networking defines common networking logic for redpanda clusters
package networking

import (
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

// PortsDefinition defines internal/external pair of ports for exposed services
type PortsDefinition struct {
	Internal *resources.NamedServicePort
	External *resources.NamedServicePort
	// if this is set to true, it means that if using nodeport, we should let it
	// generate nodeport rather than fixing it to the given number. If this
	// property is set to false, External port will be used for both container
	// port as well as hostPort
	ExternalPortIsGenerated bool
	// For the Kafka API we support the option of having a bootstrap load balancer
	ExternalBootstrap *resources.NamedServicePort
}

// RedpandaPorts defines ports for all redpanda listeners
type RedpandaPorts struct {
	KafkaAPI       PortsDefinition
	AdminAPI       PortsDefinition
	PandaProxy     PortsDefinition
	SchemaRegistry PortsDefinition
}

// NewRedpandaPorts intializes ports for all exposed services based on provided
// configuration and internal conventions.
func NewRedpandaPorts(rpCluster *redpandav1alpha1.Cluster) *RedpandaPorts {
	internalListener := rpCluster.InternalListener()
	externalListener := rpCluster.ExternalListener()
	adminAPIInternal := rpCluster.AdminAPIInternal()
	adminAPIExternal := rpCluster.AdminAPIExternal()
	proxyAPIInternal := rpCluster.PandaproxyAPIInternal()
	proxyAPIExternal := rpCluster.PandaproxyAPIExternal()

	result := &RedpandaPorts{}
	if internalListener != nil {
		result.KafkaAPI.Internal = &resources.NamedServicePort{
			Port: internalListener.Port,
			Name: resources.InternalListenerName,
		}
	}
	if externalListener != nil {
		if externalListener.Port != 0 {
			// if port is defined, we use the port as external port, this is right
			// now supported only for kafkaAPI
			result.KafkaAPI.External = &resources.NamedServicePort{
				Port: externalListener.Port,
				Name: resources.ExternalListenerName,
			}
		} else {
			result.KafkaAPI.External = &resources.NamedServicePort{
				Port: internalListener.Port + 1,
				Name: resources.ExternalListenerName,
			}
			result.KafkaAPI.ExternalPortIsGenerated = true
		}
		if externalListener.External.Bootstrap != nil {
			result.KafkaAPI.ExternalBootstrap = &resources.NamedServicePort{
				Port:       externalListener.External.Bootstrap.Port,
				TargetPort: result.KafkaAPI.External.Port,
				Name:       resources.ExternalListenerBootstrapName,
			}
		}
	}
	if adminAPIInternal != nil {
		result.AdminAPI.Internal = &resources.NamedServicePort{
			Port: adminAPIInternal.Port,
			Name: resources.AdminPortName,
		}
	}
	if adminAPIExternal != nil {
		if adminAPIExternal.Port != 0 {
			// if port is defined, we use the port as external port
			result.AdminAPI.External = &resources.NamedServicePort{
				Port: adminAPIExternal.Port,
				Name: resources.AdminPortExternalName,
			}
		} else {
			// for admin API, we default to internal + 1
			result.AdminAPI.External = &resources.NamedServicePort{
				Port: adminAPIInternal.Port + 1,
				Name: resources.AdminPortExternalName,
			}
			result.AdminAPI.ExternalPortIsGenerated = true
		}
	}
	if proxyAPIInternal != nil {
		result.PandaProxy.Internal = &resources.NamedServicePort{
			Port: proxyAPIInternal.Port,
			Name: resources.PandaproxyPortInternalName,
		}
	}
	if proxyAPIExternal != nil && proxyAPIInternal != nil {
		if proxyAPIExternal.Port != 0 {
			// if port is defined, we use the port as external port
			result.PandaProxy.External = &resources.NamedServicePort{
				Port: proxyAPIExternal.Port,
				Name: resources.PandaproxyPortExternalName,
			}
		} else {
			// for pandaproxy, we default to internal + 1
			result.PandaProxy.External = &resources.NamedServicePort{
				Port: proxyAPIInternal.Port + 1,
				Name: resources.PandaproxyPortExternalName,
			}
			result.PandaProxy.ExternalPortIsGenerated = true
		}
	}

	// for schema registry we have only one listener right now and depending on
	// whether external connectivity is enabled, it should be treated as
	// external listener or not
	if rpCluster.Spec.Configuration.SchemaRegistry != nil {
		schemaRegistryPort := &resources.NamedServicePort{
			Port: rpCluster.Spec.Configuration.SchemaRegistry.Port,
			Name: resources.SchemaRegistryPortName,
		}
		if rpCluster.IsSchemaRegistryExternallyAvailable() {
			result.SchemaRegistry.External = schemaRegistryPort
			result.SchemaRegistry.ExternalPortIsGenerated = !rpCluster.Spec.Configuration.SchemaRegistry.External.StaticNodePort
		} else {
			result.SchemaRegistry.Internal = schemaRegistryPort
		}
	}

	return result
}

// ToNamedServiceNodePort returns named node port if available for given API. If
// no external port is defined, this will be nil
func (pd PortsDefinition) ToNamedServiceNodePort() *resources.NamedServiceNodePort {
	if pd.External == nil {
		return nil
	}
	return &resources.NamedServiceNodePort{NamedServicePort: resources.NamedServicePort{Name: pd.External.Name, Port: pd.External.Port}, GenerateNodePort: pd.ExternalPortIsGenerated}
}

// InternalPort returns port of the internal listener
func (pd PortsDefinition) InternalPort() *int {
	if pd.Internal == nil {
		return nil
	}
	return &pd.Internal.Port
}

// ExternalPort returns port of the external listener
func (pd PortsDefinition) ExternalPort() *int {
	if pd.External == nil {
		return nil
	}
	return &pd.External.Port
}
