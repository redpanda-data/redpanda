// Package networking defines common networking logic for redpanda clusters
package networking

import (
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
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
	}
	if adminAPIInternal != nil {
		result.AdminAPI.Internal = &resources.NamedServicePort{
			Port: adminAPIInternal.Port,
			Name: resources.AdminPortName,
		}
	}
	if adminAPIExternal != nil {
		// for admin API, we only default to internal + 1
		result.AdminAPI.External = &resources.NamedServicePort{
			Port: adminAPIInternal.Port + 1,
			Name: resources.AdminPortExternalName,
		}
		result.AdminAPI.ExternalPortIsGenerated = true
	}
	if proxyAPIInternal != nil {
		result.PandaProxy.Internal = &resources.NamedServicePort{
			Port: proxyAPIInternal.Port,
			Name: resources.PandaproxyPortInternalName,
		}
	}
	if proxyAPIExternal != nil && proxyAPIInternal != nil {
		// for pandaproxy, we only default to internal + 1
		result.PandaProxy.External = &resources.NamedServicePort{
			Port: proxyAPIInternal.Port + 1,
			Name: resources.PandaproxyPortExternalName,
		}
		result.PandaProxy.ExternalPortIsGenerated = true
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
			result.SchemaRegistry.ExternalPortIsGenerated = true
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

// GetNodePorts returns all node ports and their configuration for this cluster
func (rp *RedpandaPorts) GetNodePorts() []resources.NamedServiceNodePort {
	nodeports := []resources.NamedServiceNodePort{}
	kafkaAPINamedNodePort := rp.KafkaAPI.ToNamedServiceNodePort()
	if kafkaAPINamedNodePort != nil {
		nodeports = append(nodeports, *kafkaAPINamedNodePort)
	}
	adminAPINodePort := rp.AdminAPI.ToNamedServiceNodePort()
	if adminAPINodePort != nil {
		nodeports = append(nodeports, *adminAPINodePort)
	}
	pandaProxyNodePort := rp.PandaProxy.ToNamedServiceNodePort()
	if pandaProxyNodePort != nil {
		nodeports = append(nodeports, *pandaProxyNodePort)
	}
	schemaRegistryNodePort := rp.SchemaRegistry.ToNamedServiceNodePort()
	if schemaRegistryNodePort != nil {
		nodeports = append(nodeports, *schemaRegistryNodePort)
	}
	return nodeports
}

// GetHeadlessPorts returns all ports that are configured using headless service
// for this cluster
func (rp *RedpandaPorts) GetHeadlessPorts() []resources.NamedServicePort {
	headlessPorts := []resources.NamedServicePort{}
	if rp.AdminAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: rp.AdminAPI.Internal.Name, Port: *rp.AdminAPI.InternalPort()})
	}
	if rp.KafkaAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: rp.KafkaAPI.Internal.Name, Port: *rp.KafkaAPI.InternalPort()})
	}
	if rp.PandaProxy.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: rp.PandaProxy.Internal.Name, Port: *rp.PandaProxy.InternalPort()})
	}
	return headlessPorts
}

// GetClusterIPPorts returns all ports that should be configured using service of type ClusterIP
func (rp *RedpandaPorts) GetClusterIPPorts() []resources.NamedServicePort {
	clusterPorts := []resources.NamedServicePort{}
	if rp.PandaProxy.External != nil {
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: rp.PandaProxy.External.Name, Port: *rp.PandaProxy.ExternalPort()})
	}
	// for schema registry we can have only external OR internal listener right
	// now but not both and both are configured using clusterIP right now
	if rp.SchemaRegistry.External != nil {
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: rp.SchemaRegistry.External.Name, Port: *rp.SchemaRegistry.ExternalPort()})
	}
	if rp.SchemaRegistry.Internal != nil {
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: rp.SchemaRegistry.Internal.Name, Port: *rp.SchemaRegistry.InternalPort()})
	}
	return clusterPorts
}
