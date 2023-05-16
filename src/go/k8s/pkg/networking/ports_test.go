// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package networking_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

//nolint:funlen // this is ok for a test
func TestRedpandaPorts(t *testing.T) {
	tests := []struct {
		name           string
		inputCluster   *vectorizedv1alpha1.Cluster
		expectedOutput *networking.RedpandaPorts
	}{
		{"all with both internal and external", &vectorizedv1alpha1.Cluster{
			Spec: vectorizedv1alpha1.ClusterSpec{
				Configuration: vectorizedv1alpha1.RedpandaConfig{
					AdminAPI:      []vectorizedv1alpha1.AdminAPI{{Port: 345}, {External: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					KafkaAPI:      []vectorizedv1alpha1.KafkaAPI{{Port: 123}, {External: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					PandaproxyAPI: []vectorizedv1alpha1.PandaproxyAPI{{Port: 333}, {External: vectorizedv1alpha1.PandaproxyExternalConnectivityConfig{ExternalConnectivityConfig: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}}}},
					SchemaRegistry: &vectorizedv1alpha1.SchemaRegistryAPI{Port: 444, External: &vectorizedv1alpha1.SchemaRegistryExternalConnectivityConfig{
						ExternalConnectivityConfig: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true},
					}},
				},
			},
		}, &networking.RedpandaPorts{
			KafkaAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.InternalListenerName,
					Port: 123,
				},
				External: &resources.NamedServicePort{
					Name: resources.ExternalListenerName,
					Port: 124,
				},
				ExternalPortIsGenerated: true,
			},
			AdminAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.AdminPortName,
					Port: 345,
				},
				External: &resources.NamedServicePort{
					Name: resources.AdminPortExternalName,
					Port: 346,
				},
				ExternalPortIsGenerated: true,
			},
			PandaProxy: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.PandaproxyPortInternalName,
					Port: 333,
				},
				External: &resources.NamedServicePort{
					Name: resources.PandaproxyPortExternalName,
					Port: 334,
				},
				ExternalPortIsGenerated: true,
			},
			SchemaRegistry: networking.PortsDefinition{
				External: &resources.NamedServicePort{
					Name: resources.SchemaRegistryPortName,
					Port: 444,
				},
				ExternalPortIsGenerated: true,
			},
		}},
		{"internal only", &vectorizedv1alpha1.Cluster{
			Spec: vectorizedv1alpha1.ClusterSpec{
				Configuration: vectorizedv1alpha1.RedpandaConfig{
					AdminAPI:       []vectorizedv1alpha1.AdminAPI{{Port: 345}},
					KafkaAPI:       []vectorizedv1alpha1.KafkaAPI{{Port: 123}},
					PandaproxyAPI:  []vectorizedv1alpha1.PandaproxyAPI{{Port: 333}},
					SchemaRegistry: &vectorizedv1alpha1.SchemaRegistryAPI{Port: 444},
				},
			},
		}, &networking.RedpandaPorts{
			KafkaAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.InternalListenerName,
					Port: 123,
				},
			},
			AdminAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.AdminPortName,
					Port: 345,
				},
			},
			PandaProxy: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.PandaproxyPortInternalName,
					Port: 333,
				},
			},
			SchemaRegistry: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.SchemaRegistryPortName,
					Port: 444,
				},
			},
		}},
		{"apis have nodeport explicitly specified", &vectorizedv1alpha1.Cluster{
			Spec: vectorizedv1alpha1.ClusterSpec{
				Configuration: vectorizedv1alpha1.RedpandaConfig{
					KafkaAPI: []vectorizedv1alpha1.KafkaAPI{{Port: 123}, {Port: 30001, External: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					AdminAPI: []vectorizedv1alpha1.AdminAPI{{Port: 234}, {Port: 30002, External: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					PandaproxyAPI: []vectorizedv1alpha1.PandaproxyAPI{{Port: 345}, {Port: 30003, External: vectorizedv1alpha1.PandaproxyExternalConnectivityConfig{
						ExternalConnectivityConfig: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true},
					}}},
					SchemaRegistry: &vectorizedv1alpha1.SchemaRegistryAPI{Port: 30004, External: &vectorizedv1alpha1.SchemaRegistryExternalConnectivityConfig{
						ExternalConnectivityConfig: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true},
						StaticNodePort:             true,
					}},
				},
			},
		}, &networking.RedpandaPorts{
			KafkaAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.InternalListenerName,
					Port: 123,
				},
				External: &resources.NamedServicePort{
					Name: resources.ExternalListenerName,
					Port: 30001,
				},
			},
			AdminAPI: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.AdminPortName,
					Port: 234,
				},
				External: &resources.NamedServicePort{
					Name: resources.AdminPortExternalName,
					Port: 30002,
				},
			},
			PandaProxy: networking.PortsDefinition{
				Internal: &resources.NamedServicePort{
					Name: resources.PandaproxyPortInternalName,
					Port: 345,
				},
				External: &resources.NamedServicePort{
					Name: resources.PandaproxyPortExternalName,
					Port: 30003,
				},
			},
			SchemaRegistry: networking.PortsDefinition{
				External: &resources.NamedServicePort{
					Name: resources.SchemaRegistryPortName,
					Port: 30004,
				},
			},
		}},
		{
			"kafka api external has bootstrap loadbalancer",
			&vectorizedv1alpha1.Cluster{
				Spec: vectorizedv1alpha1.ClusterSpec{
					Configuration: vectorizedv1alpha1.RedpandaConfig{
						KafkaAPI: []vectorizedv1alpha1.KafkaAPI{
							{
								Port: 123,
							},
							{
								External: vectorizedv1alpha1.ExternalConnectivityConfig{
									Enabled: true,
									Bootstrap: &vectorizedv1alpha1.LoadBalancerConfig{
										Port: 1234,
									},
								},
							},
						},
					},
				},
			},
			&networking.RedpandaPorts{
				KafkaAPI: networking.PortsDefinition{
					Internal: &resources.NamedServicePort{
						Name: resources.InternalListenerName,
						Port: 123,
					},
					External: &resources.NamedServicePort{
						Name: resources.ExternalListenerName,
						Port: 124,
					},
					ExternalPortIsGenerated: true,
					ExternalBootstrap: &resources.NamedServicePort{
						Name:       resources.ExternalListenerBootstrapName,
						Port:       1234,
						TargetPort: 123 + 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		actual := networking.NewRedpandaPorts(tt.inputCluster)
		assert.Equal(t, *tt.expectedOutput, *actual)
	}
}
