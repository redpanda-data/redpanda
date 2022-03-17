package networking_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

// nolint:funlen // this is ok for a test
func TestRedpandaPorts(t *testing.T) {
	tests := []struct {
		name           string
		inputCluster   *redpandav1alpha1.Cluster
		expectedOutput *networking.RedpandaPorts
	}{
		{"all with both internal and external", &redpandav1alpha1.Cluster{
			Spec: redpandav1alpha1.ClusterSpec{
				Configuration: redpandav1alpha1.RedpandaConfig{
					AdminAPI:       []redpandav1alpha1.AdminAPI{{Port: 345}, {External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					KafkaAPI:       []redpandav1alpha1.KafkaAPI{{Port: 123}, {External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					PandaproxyAPI:  []redpandav1alpha1.PandaproxyAPI{{Port: 333}, {External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}}},
					SchemaRegistry: &redpandav1alpha1.SchemaRegistryAPI{Port: 444, External: &redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}},
				},
			}}, &networking.RedpandaPorts{
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
		{"internal only", &redpandav1alpha1.Cluster{
			Spec: redpandav1alpha1.ClusterSpec{
				Configuration: redpandav1alpha1.RedpandaConfig{
					AdminAPI:       []redpandav1alpha1.AdminAPI{{Port: 345}},
					KafkaAPI:       []redpandav1alpha1.KafkaAPI{{Port: 123}},
					PandaproxyAPI:  []redpandav1alpha1.PandaproxyAPI{{Port: 333}},
					SchemaRegistry: &redpandav1alpha1.SchemaRegistryAPI{Port: 444},
				},
			}}, &networking.RedpandaPorts{
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
		{"kafka api has nodeport explicitly specifies", &redpandav1alpha1.Cluster{
			Spec: redpandav1alpha1.ClusterSpec{
				Configuration: redpandav1alpha1.RedpandaConfig{
					KafkaAPI: []redpandav1alpha1.KafkaAPI{{Port: 123}, {Port: 30001, External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}}},
				},
			}}, &networking.RedpandaPorts{
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
		}},
		{"kafka api external has bootstrap loadbalancer",
			&redpandav1alpha1.Cluster{
				Spec: redpandav1alpha1.ClusterSpec{
					Configuration: redpandav1alpha1.RedpandaConfig{
						KafkaAPI: []redpandav1alpha1.KafkaAPI{
							{
								Port: 123,
							},
							{
								External: redpandav1alpha1.ExternalConnectivityConfig{
									Enabled: true,
									Bootstrap: &redpandav1alpha1.LoadBalancerConfig{
										Port: 1234,
									},
								},
							},
						},
					},
				}},
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
				}},
		},
	}
	for _, tt := range tests {
		actual := networking.NewRedpandaPorts(tt.inputCluster)
		assert.Equal(t, *tt.expectedOutput, *actual)
	}
}
